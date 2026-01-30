
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

def migrate_to_partitioned_logs(db):
    """
    Checks if logs table is partitioned and migrates it if not.
    This is an automatic migration requested by the user.
    """
    try:
        check_query = """
            SELECT c.relkind 
            FROM pg_class c 
            JOIN pg_namespace n ON n.oid = c.relnamespace 
            WHERE n.nspname = 'seguridad' AND c.relname = 'log_actividad'
        """
        
        with db.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(check_query)
                res = cur.fetchone()
                if not res:
                    return # Table doesn't exist?
                
                relkind = res[0]
                if relkind == 'p':
                    # Already partitioned
                    return

                print("INFO: Iniciando migración automática a tabla de logs particionada...")
                
                # 1. Create partition structure
                _create_partition_structure(conn, cur)
                
                # 2. Migrate Data
                print("INFO: Migrando datos existentes de logs. Esto puede tomar unos instantes...")
                _migrate_data(conn, cur)
                
                # 3. Swap Tables
                print("INFO: Finalizando particionamiento...")
                _swap_tables(conn, cur)
                
                conn.commit()
                print("SUCCESS: Tabla de logs particionada exitosamente.")
                
    except Exception as e:
        print(f"ERROR: Falló la migración automática de particiones: {e}")
        # Don't raise, let the app continue with non-partitioned table

def _create_partition_structure(conn, cur):
    # Read sql from file is safer but for simplicity and self-containment we use the definitions here
    # Based on database/particion_logs.sql
    
    # 1. Parent table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS seguridad.log_actividad_partitioned (
          id                  BIGINT NOT NULL,
          id_usuario          BIGINT,
          id_tipo_evento_log  BIGINT NOT NULL,
          fecha_hora          TIMESTAMPTZ NOT NULL DEFAULT now(),
          entidad             VARCHAR(100),
          id_entidad          BIGINT,
          accion              VARCHAR(100),
          resultado           VARCHAR(10) NOT NULL DEFAULT 'OK',
          ip                  INET,
          user_agent          VARCHAR(500),
          session_id          VARCHAR(255),
          detalle             JSONB,
          CONSTRAINT ck_log_part_resultado CHECK (resultado IN ('OK', 'FAIL', 'WARNING')),
          PRIMARY KEY (id, fecha_hora)
        ) PARTITION BY RANGE (fecha_hora);
    """)
    
    # 2. Sequence
    cur.execute("CREATE SEQUENCE IF NOT EXISTS seguridad.log_actividad_id_seq")
    
    # 3. Functions
    update_partition_functions(conn, cur)
    
    # Create future partitions
    cur.execute("SELECT seguridad.crear_particion_log_semanal(CURRENT_DATE)")
    cur.execute("SELECT seguridad.crear_particion_log_semanal((CURRENT_DATE + INTERVAL '1 week')::DATE)")
    cur.execute("SELECT seguridad.crear_particion_log_semanal((CURRENT_DATE + INTERVAL '2 week')::DATE)")

def update_partition_functions(conn, cur):
    """
    Creates or updates the partition maintenance functions.
    Can be called to repair broken function definitions.
    """
    cur.execute("""
        CREATE OR REPLACE FUNCTION seguridad.crear_particion_log_semanal(p_fecha DATE DEFAULT CURRENT_DATE)
        RETURNS TEXT AS $$
        DECLARE
          v_start DATE;
          v_end DATE;
          v_partition_name TEXT;
          v_sql TEXT;
        BEGIN
          v_start := date_trunc('week', p_fecha)::DATE;
          v_end := v_start + INTERVAL '7 days';
          v_partition_name := 'log_actividad_' || to_char(v_start, 'YYYY') || '_w' || to_char(v_start, 'IW');
          
          IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'seguridad' AND c.relname = v_partition_name) THEN
            RETURN 'Partición ya existe: ' || v_partition_name;
          END IF;
          
          v_sql := format('CREATE TABLE seguridad.%I PARTITION OF seguridad.log_actividad FOR VALUES FROM (%L) TO (%L)', v_partition_name, v_start, v_end);
          EXECUTE v_sql;
          
          EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%s_fecha ON seguridad.%I (fecha_hora DESC)', v_partition_name, v_partition_name);
          EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%s_usuario ON seguridad.%I (id_usuario)', v_partition_name, v_partition_name);
          
          RETURN 'Partición creada: ' || v_partition_name;
        END;
        $$ LANGUAGE plpgsql;
    """)

    cur.execute("""
        CREATE OR REPLACE FUNCTION seguridad.mantener_particiones_log(p_semanas_futuras INT DEFAULT 4, p_dias_retencion INT DEFAULT 90)
        RETURNS TABLE(accion TEXT, detalle TEXT) AS $$
        DECLARE
          i INT;
        BEGIN
          FOR i IN 0..p_semanas_futuras-1 LOOP
            accion := 'CREAR';
            detalle := seguridad.crear_particion_log_semanal((CURRENT_DATE + (i * INTERVAL '1 week'))::DATE);
            RETURN NEXT;
          END LOOP;
        END;
        $$ LANGUAGE plpgsql;
    """)

def _migrate_data(conn, cur):
    # 1. Create historical partitions needed
    cur.execute("SELECT MIN(fecha_hora), MAX(fecha_hora) FROM seguridad.log_actividad")
    row = cur.fetchone()
    if row and row[0]:
        min_date = row[0]
        max_date = row[1]
        
        # Use simple format for loop logic to avoid PL/pgSQL parameter binding issues
        # We inject the dates as string literals safely since they come from DB
        sql_migracion = f"""
            DO $$
            DECLARE
              v_start DATE := date_trunc('week', '{min_date}'::DATE)::DATE;
              v_end DATE := date_trunc('week', '{max_date}'::DATE)::DATE + 7;
            BEGIN
              WHILE v_start <= v_end LOOP
                PERFORM seguridad.crear_particion_log_semanal(v_start);
                v_start := v_start + INTERVAL '1 week';
              END LOOP;
            END $$;
        """
        cur.execute(sql_migracion)

    # 2. Copy Data
    cur.execute("""
        INSERT INTO seguridad.log_actividad_partitioned 
        (id, id_usuario, id_tipo_evento_log, fecha_hora, entidad, id_entidad, accion, resultado, ip, user_agent, session_id, detalle)
        SELECT id, id_usuario, id_tipo_evento_log, fecha_hora, entidad, id_entidad, accion, resultado, ip, user_agent, session_id, detalle
        FROM seguridad.log_actividad
    """)

    # 3. Sync Sequence
    cur.execute("""
        SELECT setval('seguridad.log_actividad_id_seq', (SELECT COALESCE(MAX(id), 1) FROM seguridad.log_actividad_partitioned))
    """)

def _swap_tables(conn, cur):
    cur.execute("ALTER TABLE seguridad.log_actividad RENAME TO log_actividad_old")
    cur.execute("ALTER TABLE seguridad.log_actividad_partitioned RENAME TO log_actividad")
    
    # Restore default value for ID to use sequence
    cur.execute("ALTER TABLE seguridad.log_actividad ALTER COLUMN id SET DEFAULT nextval('seguridad.log_actividad_id_seq')")
    # Restore constraints/indices not created by partition? 
    # The partitioned table should handle most, but we miss foreign keys on the parent?
    # Partitioned tables support FKs
    pass
