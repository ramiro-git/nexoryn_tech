-- ============================================================================
-- NEXORYN TECH - Database Schema (PostgreSQL)
-- Version: 2.0 - Optimized with snake_case, ARCA/AFIP integration, Remitos
-- ============================================================================

-- Extensions
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Schemas
CREATE SCHEMA IF NOT EXISTS ref;
CREATE SCHEMA IF NOT EXISTS app;
CREATE SCHEMA IF NOT EXISTS seguridad;

-- Revoke public access for security
REVOKE ALL ON SCHEMA ref FROM PUBLIC;
REVOKE ALL ON SCHEMA app FROM PUBLIC;
REVOKE ALL ON SCHEMA seguridad FROM PUBLIC;

-- ============================================================================
-- REFERENCE TABLES (ref schema)
-- ============================================================================

CREATE TABLE IF NOT EXISTS ref.provincia (
  id      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  nombre  VARCHAR(50) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS ref.localidad (
  id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  nombre        VARCHAR(100) NOT NULL,
  id_provincia  BIGINT NOT NULL REFERENCES ref.provincia(id) ON UPDATE CASCADE ON DELETE RESTRICT,
  CONSTRAINT uq_localidad_provincia UNIQUE (id_provincia, nombre)
);

CREATE TABLE IF NOT EXISTS ref.condicion_iva (
  id      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  nombre  VARCHAR(50) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS ref.tipo_iva (
  id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  codigo      INTEGER NOT NULL UNIQUE,
  porcentaje  DECIMAL(6,2) NOT NULL UNIQUE,
  descripcion VARCHAR(50),
  CONSTRAINT ck_tipo_iva_porcentaje CHECK (porcentaje >= 0 AND porcentaje <= 100)
);

CREATE TABLE IF NOT EXISTS ref.marca (
  id      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  nombre  VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS ref.rubro (
  id      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  nombre  VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS ref.unidad_medida (
  id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  nombre       VARCHAR(30) NOT NULL UNIQUE,
  abreviatura  VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS ref.deposito (
  id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  nombre     VARCHAR(100) NOT NULL UNIQUE,
  ubicacion  TEXT,
  activo     BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS ref.lista_precio (
  id      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  nombre  VARCHAR(50) NOT NULL UNIQUE,
  activa  BOOLEAN NOT NULL DEFAULT TRUE,
  orden   SMALLINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS ref.forma_pago (
  id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  descripcion  VARCHAR(50) NOT NULL UNIQUE,
  activa       BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS ref.tipo_porcentaje (
  id    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  tipo  VARCHAR(10) NOT NULL UNIQUE,
  CONSTRAINT ck_tipo_porcentaje_tipo CHECK (tipo IN ('MARGEN', 'DESCUENTO'))
);

CREATE TABLE IF NOT EXISTS ref.tipo_documento (
  id                       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  nombre                   VARCHAR(20) NOT NULL UNIQUE,
  clase                    VARCHAR(6) NOT NULL,
  afecta_stock             BOOLEAN NOT NULL DEFAULT FALSE,
  afecta_cuenta_corriente  BOOLEAN NOT NULL DEFAULT FALSE,
  codigo_afip              INTEGER,
  letra                    CHAR(1),
  CONSTRAINT ck_tipo_documento_clase CHECK (clase IN ('VENTA', 'COMPRA'))
);

CREATE TABLE IF NOT EXISTS ref.tipo_movimiento_articulo (
  id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  nombre      VARCHAR(50) NOT NULL UNIQUE,
  signo_stock SMALLINT NOT NULL,
  CONSTRAINT ck_tipo_movimiento_signo CHECK (signo_stock IN (-1, 1))
);

-- ============================================================================
-- SECURITY TABLES (seguridad schema)
-- ============================================================================

CREATE TABLE IF NOT EXISTS seguridad.rol (
  id      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  nombre  VARCHAR(20) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS seguridad.usuario (
  id                   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  nombre               VARCHAR(100) NOT NULL,
  email                VARCHAR(150) NOT NULL UNIQUE,
  contrasena_hash      VARCHAR(255) NOT NULL,
  id_rol               BIGINT NOT NULL REFERENCES seguridad.rol(id) ON UPDATE CASCADE ON DELETE RESTRICT,
  activo               BOOLEAN NOT NULL DEFAULT TRUE,
  fecha_creacion       TIMESTAMPTZ NOT NULL DEFAULT now(),
  fecha_actualizacion  TIMESTAMPTZ NOT NULL DEFAULT now(),
  ultimo_login         TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS seguridad.tipo_evento_log (
  id      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  codigo  VARCHAR(20) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS seguridad.log_actividad (
  id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  id_usuario          BIGINT REFERENCES seguridad.usuario(id) ON UPDATE CASCADE ON DELETE SET NULL,
  id_tipo_evento_log  BIGINT NOT NULL REFERENCES seguridad.tipo_evento_log(id) ON UPDATE CASCADE ON DELETE RESTRICT,
  fecha_hora          TIMESTAMPTZ NOT NULL DEFAULT now(),
  entidad             VARCHAR(100),
  id_entidad          BIGINT,
  accion              VARCHAR(100),
  resultado           VARCHAR(10) NOT NULL DEFAULT 'OK',
  ip                  INET,
  user_agent          VARCHAR(500),
  session_id          VARCHAR(255),
  detalle             JSONB,
  CONSTRAINT ck_log_resultado CHECK (resultado IN ('OK', 'FAIL', 'WARNING'))
);

CREATE TABLE IF NOT EXISTS seguridad.backup_config (
  id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  frecuencia      VARCHAR(10) NOT NULL DEFAULT 'OFF',
  hora            TIME NOT NULL DEFAULT '00:00:00',
  ultimo_run      TIMESTAMPTZ,
  destino_local   TEXT,
  retencion_dias  INTEGER NOT NULL DEFAULT 30,
  -- Campos para rastrear último backup de cada tipo (detección de backups perdidos)
  ultimo_daily    TIMESTAMPTZ,
  ultimo_weekly   TIMESTAMPTZ,
  ultimo_monthly  TIMESTAMPTZ,
  CONSTRAINT ck_backup_freq CHECK (frecuencia IN ('DIARIA', 'SEMANAL', 'MENSUAL', 'OFF'))
);

CREATE TABLE IF NOT EXISTS seguridad.config_sistema (
  clave        VARCHAR(100) PRIMARY KEY,
  valor        TEXT,
  tipo         VARCHAR(20) NOT NULL DEFAULT 'TEXT',
  descripcion  VARCHAR(255),
  CONSTRAINT ck_config_tipo CHECK (tipo IN ('TEXT', 'NUMBER', 'BOOLEAN', 'COLOR', 'PATH'))
);

-- ============================================================================
-- APPLICATION TABLES (app schema)
-- ============================================================================

CREATE TABLE IF NOT EXISTS app.entidad_comercial (
  id                   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  apellido             VARCHAR(100),
  nombre               VARCHAR(100),
  razon_social         VARCHAR(200),
  domicilio            VARCHAR(255),
  id_localidad         BIGINT REFERENCES ref.localidad(id) ON UPDATE CASCADE ON DELETE RESTRICT,
  cuit                 VARCHAR(13),
  id_condicion_iva     BIGINT REFERENCES ref.condicion_iva(id) ON UPDATE CASCADE ON DELETE RESTRICT,
  notas                TEXT,
  fecha_creacion       TIMESTAMPTZ NOT NULL DEFAULT now(),
  fecha_actualizacion  TIMESTAMPTZ NOT NULL DEFAULT now(),
  activo               BOOLEAN NOT NULL DEFAULT TRUE,
  telefono             VARCHAR(100),
  email                VARCHAR(150),
  tipo                 VARCHAR(10),
  CONSTRAINT ck_entidad_tipo CHECK (tipo IS NULL OR tipo IN ('CLIENTE', 'PROVEEDOR', 'AMBOS'))
);

CREATE TABLE IF NOT EXISTS app.lista_cliente (
  id_entidad_comercial  BIGINT PRIMARY KEY REFERENCES app.entidad_comercial(id) ON UPDATE CASCADE ON DELETE CASCADE,
  id_lista_precio       BIGINT NOT NULL REFERENCES ref.lista_precio(id) ON UPDATE CASCADE ON DELETE RESTRICT,
  descuento             NUMERIC(6,2) NOT NULL DEFAULT 0,
  limite_credito        NUMERIC(14,2) NOT NULL DEFAULT 0,
  saldo_cuenta          NUMERIC(14,2) NOT NULL DEFAULT 0,
  CONSTRAINT ck_lista_cliente_desc CHECK (descuento >= 0 AND descuento <= 100),
  CONSTRAINT ck_lista_cliente_lim CHECK (limite_credito >= 0)
);

CREATE TABLE IF NOT EXISTS app.articulo (
  id                     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  nombre                 VARCHAR(200) NOT NULL,
  id_marca               BIGINT REFERENCES ref.marca(id) ON UPDATE CASCADE ON DELETE RESTRICT,
  id_rubro               BIGINT REFERENCES ref.rubro(id) ON UPDATE CASCADE ON DELETE RESTRICT,
  id_tipo_iva            BIGINT REFERENCES ref.tipo_iva(id) ON UPDATE CASCADE ON DELETE RESTRICT,
  costo                  NUMERIC(14,4) NOT NULL DEFAULT 0,
  stock_minimo           NUMERIC(14,4) NOT NULL DEFAULT 0,
  id_unidad_medida       BIGINT REFERENCES ref.unidad_medida(id) ON UPDATE CASCADE ON DELETE RESTRICT,
  id_proveedor           BIGINT REFERENCES app.entidad_comercial(id) ON UPDATE CASCADE ON DELETE RESTRICT,
  descuento_base         NUMERIC(6,2) NOT NULL DEFAULT 0,
  redondeo               BOOLEAN NOT NULL DEFAULT FALSE,
  porcentaje_ganancia_2  NUMERIC(6,2) DEFAULT NULL,
  activo                 BOOLEAN NOT NULL DEFAULT TRUE,
  observacion            TEXT,
  ubicacion              VARCHAR(100),
  fecha_creacion         TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT ck_art_costo CHECK (costo >= 0),
  CONSTRAINT ck_art_stock_min CHECK (stock_minimo >= 0),
  CONSTRAINT ck_art_desc_base CHECK (descuento_base >= 0 AND descuento_base <= 100),
  CONSTRAINT ck_art_pgan2 CHECK (porcentaje_ganancia_2 IS NULL OR (porcentaje_ganancia_2 >= 0 AND porcentaje_ganancia_2 <= 1000))
);

CREATE TABLE IF NOT EXISTS app.articulo_stock_resumen (
  id_articulo          BIGINT PRIMARY KEY REFERENCES app.articulo(id) ON DELETE CASCADE,
  stock_total          NUMERIC(14,4) NOT NULL DEFAULT 0,
  ultima_actualizacion TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS app.articulo_precio (
  id_articulo          BIGINT NOT NULL REFERENCES app.articulo(id) ON UPDATE CASCADE ON DELETE CASCADE,
  id_lista_precio      BIGINT NOT NULL REFERENCES ref.lista_precio(id) ON UPDATE CASCADE ON DELETE CASCADE,
  precio               NUMERIC(14,4),
  porcentaje           NUMERIC(6,2),
  id_tipo_porcentaje   BIGINT REFERENCES ref.tipo_porcentaje(id) ON UPDATE CASCADE ON DELETE RESTRICT,
  fecha_actualizacion  TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (id_articulo, id_lista_precio),
  CONSTRAINT ck_art_precio_precio CHECK (precio IS NULL OR precio >= 0),
  CONSTRAINT ck_art_precio_pct CHECK (porcentaje IS NULL OR porcentaje >= 0)
);

CREATE TABLE IF NOT EXISTS app.documento (
  id                      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  id_tipo_documento       BIGINT NOT NULL REFERENCES ref.tipo_documento(id) ON UPDATE CASCADE ON DELETE RESTRICT,
  fecha                   TIMESTAMPTZ NOT NULL DEFAULT now(),
  numero_serie            VARCHAR(20),
  id_entidad_comercial    BIGINT NOT NULL REFERENCES app.entidad_comercial(id) ON UPDATE CASCADE ON DELETE RESTRICT,
  estado                  VARCHAR(12) NOT NULL DEFAULT 'BORRADOR',
  id_lista_precio         BIGINT REFERENCES ref.lista_precio(id) ON UPDATE CASCADE ON DELETE RESTRICT,
  descuento_porcentaje    NUMERIC(6,2) NOT NULL DEFAULT 0,
  descuento_importe       NUMERIC(14,4) NOT NULL DEFAULT 0,
  observacion             TEXT,
  direccion_entrega       TEXT,
  fecha_vencimiento       DATE,
  id_deposito             BIGINT REFERENCES ref.deposito(id) ON UPDATE CASCADE ON DELETE RESTRICT,
  neto                    NUMERIC(14,4) NOT NULL DEFAULT 0,
  subtotal                NUMERIC(14,4) NOT NULL DEFAULT 0,
  iva_total               NUMERIC(14,4) NOT NULL DEFAULT 0,
  total                   NUMERIC(14,4) NOT NULL DEFAULT 0,
  sena                    NUMERIC(14,4) NOT NULL DEFAULT 0,
  id_usuario              BIGINT REFERENCES seguridad.usuario(id) ON UPDATE CASCADE ON DELETE SET NULL,
  -- ARCA/AFIP Fields for Electronic Invoicing
  punto_venta             INTEGER,
  tipo_comprobante_afip   INTEGER,
  cae                     VARCHAR(14),
  cae_vencimiento         DATE,
  cuit_emisor             VARCHAR(11),
  qr_data                 TEXT,
  CONSTRAINT ck_doc_estado CHECK (estado IN ('BORRADOR', 'CONFIRMADO', 'ANULADO', 'PAGADO')),
  CONSTRAINT ck_doc_desc CHECK (descuento_porcentaje >= 0 AND descuento_porcentaje <= 100),
  CONSTRAINT ck_doc_totales CHECK (TRUE), -- Relaxed to allow legacy negative values
  CONSTRAINT ck_doc_punto_venta CHECK (punto_venta IS NULL OR (punto_venta >= 1 AND punto_venta <= 99999))
);

CREATE TABLE IF NOT EXISTS app.documento_detalle (
  id_documento           BIGINT NOT NULL REFERENCES app.documento(id) ON UPDATE CASCADE ON DELETE CASCADE,
  nro_linea              INTEGER NOT NULL,
  descripcion_historica  VARCHAR(255),
  id_articulo            BIGINT NOT NULL REFERENCES app.articulo(id) ON UPDATE CASCADE ON DELETE RESTRICT,
  cantidad               NUMERIC(14,4) NOT NULL,
  precio_unitario        NUMERIC(14,4) NOT NULL DEFAULT 0,
  descuento_porcentaje   NUMERIC(6,2) NOT NULL DEFAULT 0,
  porcentaje_iva         NUMERIC(6,2) NOT NULL DEFAULT 0,
  total_linea            NUMERIC(14,4) NOT NULL DEFAULT 0,
  id_lista_precio        BIGINT REFERENCES ref.lista_precio(id) ON UPDATE CASCADE ON DELETE RESTRICT,
  observacion            TEXT,
  PRIMARY KEY (id_documento, nro_linea),
  CONSTRAINT ck_det_cant CHECK (TRUE), -- Relaxed to allow legacy negative values
  CONSTRAINT ck_det_precio CHECK (TRUE),
  CONSTRAINT ck_det_total CHECK (TRUE)
);

CREATE TABLE IF NOT EXISTS app.movimiento_articulo (
  id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  id_articulo         BIGINT NOT NULL REFERENCES app.articulo(id) ON UPDATE CASCADE ON DELETE RESTRICT,
  id_tipo_movimiento  BIGINT NOT NULL REFERENCES ref.tipo_movimiento_articulo(id) ON UPDATE CASCADE ON DELETE RESTRICT,
  fecha               TIMESTAMPTZ NOT NULL DEFAULT now(),
  cantidad            NUMERIC(14,4) NOT NULL,
  observacion         TEXT,
  id_deposito         BIGINT NOT NULL REFERENCES ref.deposito(id) ON UPDATE CASCADE ON DELETE RESTRICT,
  id_documento        BIGINT REFERENCES app.documento(id) ON UPDATE CASCADE ON DELETE SET NULL,
  id_usuario          BIGINT REFERENCES seguridad.usuario(id) ON UPDATE CASCADE ON DELETE SET NULL,
  CONSTRAINT ck_mov_cant CHECK (TRUE)
);

CREATE TABLE IF NOT EXISTS app.pago (
  id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  id_documento   BIGINT NOT NULL REFERENCES app.documento(id) ON UPDATE CASCADE ON DELETE CASCADE,
  id_forma_pago  BIGINT NOT NULL REFERENCES ref.forma_pago(id) ON UPDATE CASCADE ON DELETE RESTRICT,
  fecha          TIMESTAMPTZ NOT NULL DEFAULT now(),
  monto          NUMERIC(14,4) NOT NULL,
  referencia     VARCHAR(255),
  observacion    TEXT,
  CONSTRAINT ck_pago_monto CHECK (TRUE)
);

-- ============================================================================
-- REMITOS (Delivery Notes)
-- ============================================================================

CREATE TABLE IF NOT EXISTS app.remito (
  id                    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  numero                VARCHAR(20) NOT NULL,
  fecha                 TIMESTAMPTZ NOT NULL DEFAULT now(),
  id_documento          BIGINT REFERENCES app.documento(id) ON UPDATE CASCADE ON DELETE SET NULL,
  id_entidad_comercial  BIGINT NOT NULL REFERENCES app.entidad_comercial(id) ON UPDATE CASCADE ON DELETE RESTRICT,
  id_deposito           BIGINT NOT NULL REFERENCES ref.deposito(id) ON UPDATE CASCADE ON DELETE RESTRICT,
  direccion_entrega     TEXT,
  observacion           TEXT,
  estado                VARCHAR(12) NOT NULL DEFAULT 'PENDIENTE',
  fecha_despacho        TIMESTAMPTZ,
  fecha_entrega         TIMESTAMPTZ,
  id_usuario            BIGINT REFERENCES seguridad.usuario(id) ON UPDATE CASCADE ON DELETE SET NULL,
  CONSTRAINT ck_remito_estado CHECK (estado IN ('PENDIENTE', 'DESPACHADO', 'ENTREGADO', 'ANULADO'))
);

CREATE TABLE IF NOT EXISTS app.remito_detalle (
  id_remito    BIGINT NOT NULL REFERENCES app.remito(id) ON UPDATE CASCADE ON DELETE CASCADE,
  nro_linea    INTEGER NOT NULL,
  id_articulo  BIGINT NOT NULL REFERENCES app.articulo(id) ON UPDATE CASCADE ON DELETE RESTRICT,
  cantidad     NUMERIC(14,4) NOT NULL,
  observacion  VARCHAR(255),
  PRIMARY KEY (id_remito, nro_linea),
  CONSTRAINT ck_remito_det_cant CHECK (cantidad > 0)
);

-- ============================================================================
-- VIEWS
-- ============================================================================

DROP VIEW IF EXISTS seguridad.v_usuario_publico CASCADE;
CREATE OR REPLACE VIEW seguridad.v_usuario_publico AS
SELECT
  u.id,
  u.nombre,
  u.email,
  u.activo,
  r.nombre AS rol,
  u.fecha_creacion,
  u.fecha_actualizacion,
  u.ultimo_login
FROM seguridad.usuario u
JOIN seguridad.rol r ON r.id = u.id_rol;

DROP VIEW IF EXISTS app.v_stock_actual CASCADE;
CREATE OR REPLACE VIEW app.v_stock_actual AS
SELECT
  ma.id_articulo,
  a.nombre AS articulo,
  ma.id_deposito,
  d.nombre AS deposito,
  SUM(ma.cantidad * tma.signo_stock) AS stock_actual
FROM app.movimiento_articulo ma
JOIN app.articulo a ON a.id = ma.id_articulo
JOIN ref.deposito d ON d.id = ma.id_deposito
JOIN ref.tipo_movimiento_articulo tma ON tma.id = ma.id_tipo_movimiento
GROUP BY ma.id_articulo, a.nombre, ma.id_deposito, d.nombre;

DROP VIEW IF EXISTS app.v_stock_total CASCADE;
CREATE OR REPLACE VIEW app.v_stock_total AS
SELECT
  a.id AS id_articulo,
  a.nombre AS articulo,
  a.stock_minimo,
  COALESCE(sr.stock_total, 0) AS stock_total
FROM app.articulo a
LEFT JOIN app.articulo_stock_resumen sr ON a.id = sr.id_articulo;

CREATE OR REPLACE VIEW app.v_movimientos_full AS
SELECT 
  m.id,
  m.fecha,
  a.nombre AS articulo,
  tm.nombre AS tipo_movimiento,
  m.cantidad,
  tm.signo_stock,
  d.nombre AS deposito,
  u.nombre AS usuario,
  m.observacion,
  doc.id AS id_documento,
  td.nombre AS tipo_documento,
  doc.numero_serie AS nro_comprobante,
  COALESCE(ec.razon_social, TRIM(COALESCE(ec.apellido, '') || ' ' || COALESCE(ec.nombre, ''))) AS entidad
FROM app.movimiento_articulo m
JOIN app.articulo a ON m.id_articulo = a.id
JOIN ref.tipo_movimiento_articulo tm ON m.id_tipo_movimiento = tm.id
JOIN ref.deposito d ON m.id_deposito = d.id
LEFT JOIN app.documento doc ON m.id_documento = doc.id
LEFT JOIN ref.tipo_documento td ON doc.id_tipo_documento = td.id
LEFT JOIN app.entidad_comercial ec ON doc.id_entidad_comercial = ec.id
LEFT JOIN seguridad.usuario u ON m.id_usuario = u.id;

DROP VIEW IF EXISTS app.v_documento_resumen CASCADE;
CREATE OR REPLACE VIEW app.v_documento_resumen AS
SELECT
  doc.id,
  td.nombre AS tipo_documento,
  td.clase,
  td.letra,
  td.codigo_afip,
  doc.fecha,
  doc.numero_serie,
  doc.estado,
  doc.total,
  doc.neto,
  doc.subtotal,
  doc.iva_total,
  doc.sena,
  doc.descuento_porcentaje,
  doc.descuento_importe,
  doc.cae,
  doc.cae_vencimiento,
  doc.observacion,
  ec.id AS id_entidad,
  COALESCE(ec.razon_social, TRIM(COALESCE(ec.apellido, '') || ' ' || COALESCE(ec.nombre, ''))) AS entidad,
  ec.cuit AS cuit_receptor,
  u.nombre AS usuario,
  doc.id_usuario,
  (SELECT fp.descripcion FROM app.pago p JOIN ref.forma_pago fp ON fp.id = p.id_forma_pago WHERE p.id_documento = doc.id ORDER BY p.id LIMIT 1) as forma_pago
FROM app.documento doc
JOIN ref.tipo_documento td ON td.id = doc.id_tipo_documento
JOIN app.entidad_comercial ec ON ec.id = doc.id_entidad_comercial
LEFT JOIN seguridad.usuario u ON u.id = doc.id_usuario;

DROP VIEW IF EXISTS app.v_entidad_detallada CASCADE;
CREATE OR REPLACE VIEW app.v_entidad_detallada AS
SELECT
  e.id,
  e.tipo,
  COALESCE(e.razon_social, TRIM(COALESCE(e.apellido, '') || ' ' || COALESCE(e.nombre, ''))) AS nombre_completo,
  e.apellido,
  e.nombre,
  e.razon_social,
  e.cuit,
  e.domicilio,
  l.nombre AS localidad,
  p.nombre AS provincia,
  ci.nombre AS condicion_iva,
  e.telefono,
  e.email,
  e.notas,
  e.activo,
  e.fecha_creacion,
  e.id_localidad,
  l.id_provincia,
  e.id_condicion_iva,
  lc.id_lista_precio,
  lp.nombre AS lista_precio,
  lc.descuento,
  lc.limite_credito,
  lc.saldo_cuenta
FROM app.entidad_comercial e
LEFT JOIN ref.localidad l ON l.id = e.id_localidad
LEFT JOIN ref.provincia p ON p.id = l.id_provincia
LEFT JOIN ref.condicion_iva ci ON ci.id = e.id_condicion_iva
LEFT JOIN app.lista_cliente lc ON lc.id_entidad_comercial = e.id
LEFT JOIN ref.lista_precio lp ON lp.id = lc.id_lista_precio;

DROP VIEW IF EXISTS app.v_articulo_detallado CASCADE;
CREATE OR REPLACE VIEW app.v_articulo_detallado AS
SELECT
  a.id,
  a.nombre,
  a.id_marca,
  m.nombre AS marca,
  a.id_rubro,
  r.nombre AS rubro,
  a.costo,
  a.id_tipo_iva,
  ti.porcentaje AS porcentaje_iva,
  a.id_unidad_medida,
  um.nombre AS unidad_medida,
  um.abreviatura AS unidad_abreviatura,
  a.id_proveedor,
  COALESCE(prov.razon_social, TRIM(COALESCE(prov.apellido, '') || ' ' || COALESCE(prov.nombre, ''))) AS proveedor,
  a.stock_minimo,
  a.descuento_base,
  a.redondeo,
  a.porcentaje_ganancia_2,
  a.activo,
  a.observacion,
  a.ubicacion,
  COALESCE(st.stock_total, 0) AS stock_actual,
  ap.precio AS precio_lista
FROM app.articulo a
LEFT JOIN ref.marca m ON m.id = a.id_marca
LEFT JOIN ref.rubro r ON r.id = a.id_rubro
LEFT JOIN ref.tipo_iva ti ON ti.id = a.id_tipo_iva
LEFT JOIN ref.unidad_medida um ON um.id = a.id_unidad_medida
LEFT JOIN app.entidad_comercial prov ON prov.id = a.id_proveedor
LEFT JOIN app.v_stock_total st ON st.id_articulo = a.id
LEFT JOIN app.articulo_precio ap ON ap.id_articulo = a.id AND ap.id_lista_precio = 1;

-- ============================================================================
-- AUDIT FUNCTION
-- ============================================================================

CREATE OR REPLACE FUNCTION seguridad.trg_audit_dml()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  v_user_id BIGINT;
  v_event_id BIGINT;
  v_id_entidad BIGINT;
  v_payload JSONB;
BEGIN
  BEGIN
    v_user_id := NULLIF(current_setting('app.user_id', true), '')::BIGINT;
  EXCEPTION WHEN others THEN
    v_user_id := NULL;
  END;

  SELECT id INTO v_event_id
  FROM seguridad.tipo_evento_log
  WHERE codigo = TG_OP;

  IF v_event_id IS NULL THEN
    SELECT id INTO v_event_id
    FROM seguridad.tipo_evento_log
    WHERE codigo = 'ERROR';
  END IF;

  IF TG_OP = 'DELETE' THEN
    v_payload := to_jsonb(OLD);
  ELSE
    v_payload := to_jsonb(NEW);
  END IF;

  v_id_entidad := NULLIF(v_payload->>'id', '')::BIGINT;

  INSERT INTO seguridad.log_actividad (
    id_usuario,
    id_tipo_evento_log,
    entidad,
    id_entidad,
    accion,
    resultado,
    ip
  )
  VALUES (
    v_user_id,
    v_event_id,
    TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME,
    v_id_entidad,
    TG_OP,
    'OK',
    NULLIF(current_setting('app.ip', true), '')::INET
  );

  IF TG_OP = 'DELETE' THEN
    RETURN OLD;
  END IF;
  RETURN NEW;
END;
$$;

-- Function to keep stock summary synchronized
CREATE OR REPLACE FUNCTION app.fn_sync_stock_resumen()
RETURNS TRIGGER AS $$
DECLARE
  v_signo INTEGER;
BEGIN
  -- Get the sign from the movement type
  SELECT signo_stock INTO v_signo 
  FROM ref.tipo_movimiento_articulo 
  WHERE id = COALESCE(NEW.id_tipo_movimiento, OLD.id_tipo_movimiento);

  IF (TG_OP = 'INSERT') THEN
    INSERT INTO app.articulo_stock_resumen (id_articulo, stock_total)
    VALUES (NEW.id_articulo, NEW.cantidad * v_signo)
    ON CONFLICT (id_articulo) DO UPDATE 
    SET stock_total = app.articulo_stock_resumen.stock_total + (NEW.cantidad * v_signo),
        ultima_actualizacion = now();
            
  ELSIF (TG_OP = 'UPDATE') THEN
    UPDATE app.articulo_stock_resumen 
    SET stock_total = stock_total - (OLD.cantidad * v_signo) + (NEW.cantidad * v_signo),
        ultima_actualizacion = now()
    WHERE id_articulo = NEW.id_articulo;
        
  ELSIF (TG_OP = 'DELETE') THEN
    UPDATE app.articulo_stock_resumen 
    SET stock_total = stock_total - (OLD.cantidad * v_signo),
        ultima_actualizacion = now()
    WHERE id_articulo = OLD.id_articulo;
  END IF;
    
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- TRIGGERS
-- ============================================================================

DROP TRIGGER IF EXISTS tr_audit_documento ON app.documento;
CREATE TRIGGER tr_audit_documento
AFTER INSERT OR UPDATE OR DELETE ON app.documento
FOR EACH ROW EXECUTE FUNCTION seguridad.trg_audit_dml();

DROP TRIGGER IF EXISTS tr_audit_documento_detalle ON app.documento_detalle;
CREATE TRIGGER tr_audit_documento_detalle
AFTER INSERT OR UPDATE OR DELETE ON app.documento_detalle
FOR EACH ROW EXECUTE FUNCTION seguridad.trg_audit_dml();

DROP TRIGGER IF EXISTS tr_audit_pago ON app.pago;
CREATE TRIGGER tr_audit_pago
AFTER INSERT OR UPDATE OR DELETE ON app.pago
FOR EACH ROW EXECUTE FUNCTION seguridad.trg_audit_dml();

DROP TRIGGER IF EXISTS tr_audit_movimiento_articulo ON app.movimiento_articulo;
CREATE TRIGGER tr_audit_movimiento_articulo
AFTER INSERT OR UPDATE OR DELETE ON app.movimiento_articulo
FOR EACH ROW EXECUTE FUNCTION seguridad.trg_audit_dml();

DROP TRIGGER IF EXISTS tr_audit_remito ON app.remito;
CREATE TRIGGER tr_audit_remito
AFTER INSERT OR UPDATE OR DELETE ON app.remito
FOR EACH ROW EXECUTE FUNCTION seguridad.trg_audit_dml();

DROP TRIGGER IF EXISTS tr_audit_articulo ON app.articulo;
CREATE TRIGGER tr_audit_articulo
AFTER INSERT OR UPDATE OR DELETE ON app.articulo
FOR EACH ROW EXECUTE FUNCTION seguridad.trg_audit_dml();

DROP TRIGGER IF EXISTS tr_audit_entidad ON app.entidad_comercial;
CREATE TRIGGER tr_audit_entidad
AFTER INSERT OR UPDATE OR DELETE ON app.entidad_comercial
FOR EACH ROW EXECUTE FUNCTION seguridad.trg_audit_dml();

-- Trigger for stock summary synchronization
DROP TRIGGER IF EXISTS trg_sync_stock_resumen ON app.movimiento_articulo;
CREATE TRIGGER trg_sync_stock_resumen
AFTER INSERT OR UPDATE OR DELETE ON app.movimiento_articulo
FOR EACH ROW EXECUTE FUNCTION app.fn_sync_stock_resumen();

-- Initialize the summary table with current totals (Ensures consistency if data exists)
INSERT INTO app.articulo_stock_resumen (id_articulo, stock_total)
SELECT id_articulo, stock_total 
FROM app.v_stock_total
ON CONFLICT (id_articulo) DO UPDATE 
SET stock_total = EXCLUDED.stock_total, 
    ultima_actualizacion = now();

-- ============================================================================
-- INDEXES
-- ============================================================================

-- Reference tables
CREATE INDEX IF NOT EXISTS idx_localidad_provincia ON ref.localidad(id_provincia);

-- Entity indexes
CREATE INDEX IF NOT EXISTS idx_entidad_localidad ON app.entidad_comercial(id_localidad);
CREATE INDEX IF NOT EXISTS idx_entidad_condicion_iva ON app.entidad_comercial(id_condicion_iva);
CREATE INDEX IF NOT EXISTS idx_entidad_tipo ON app.entidad_comercial(tipo);
CREATE INDEX IF NOT EXISTS idx_entidad_activo ON app.entidad_comercial(activo) WHERE activo = true;
CREATE INDEX IF NOT EXISTS idx_entidad_cuit ON app.entidad_comercial(cuit) WHERE cuit IS NOT NULL;

-- Full-text search (trigram) for entities
CREATE INDEX IF NOT EXISTS idx_entidad_razon_trgm ON app.entidad_comercial USING gin (razon_social gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_entidad_apellido_trgm ON app.entidad_comercial USING gin (apellido gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_entidad_nombre_trgm ON app.entidad_comercial USING gin (nombre gin_trgm_ops);

-- Article indexes
CREATE INDEX IF NOT EXISTS idx_articulo_rubro ON app.articulo(id_rubro);
CREATE INDEX IF NOT EXISTS idx_articulo_marca ON app.articulo(id_marca);
CREATE INDEX IF NOT EXISTS idx_articulo_proveedor ON app.articulo(id_proveedor);
CREATE INDEX IF NOT EXISTS idx_articulo_tipo_iva ON app.articulo(id_tipo_iva);
CREATE INDEX IF NOT EXISTS idx_articulo_unidad ON app.articulo(id_unidad_medida);
CREATE INDEX IF NOT EXISTS idx_articulo_activo ON app.articulo(activo) WHERE activo = true;

-- Full-text search for articles
CREATE INDEX IF NOT EXISTS idx_articulo_nombre_trgm ON app.articulo USING gin (nombre gin_trgm_ops);

-- Document indexes
CREATE INDEX IF NOT EXISTS idx_doc_fecha ON app.documento(fecha);
CREATE INDEX IF NOT EXISTS idx_doc_tipo ON app.documento(id_tipo_documento);
CREATE INDEX IF NOT EXISTS idx_doc_entidad ON app.documento(id_entidad_comercial);
CREATE INDEX IF NOT EXISTS idx_doc_estado ON app.documento(estado);
CREATE INDEX IF NOT EXISTS idx_doc_entidad_fecha ON app.documento(id_entidad_comercial, fecha);
CREATE INDEX IF NOT EXISTS idx_doc_tipo_numero ON app.documento(id_tipo_documento, numero_serie);
CREATE INDEX IF NOT EXISTS idx_doc_usuario ON app.documento(id_usuario);
CREATE INDEX IF NOT EXISTS idx_doc_lista_precio ON app.documento(id_lista_precio);
CREATE INDEX IF NOT EXISTS idx_doc_deposito ON app.documento(id_deposito);
CREATE INDEX IF NOT EXISTS idx_doc_cae ON app.documento(cae) WHERE cae IS NOT NULL;

-- Document detail indexes
CREATE INDEX IF NOT EXISTS idx_det_articulo ON app.documento_detalle(id_articulo);

-- Article price indexes
CREATE INDEX IF NOT EXISTS idx_art_precio_lista ON app.articulo_precio(id_lista_precio);
CREATE INDEX IF NOT EXISTS idx_art_precio_tipo ON app.articulo_precio(id_tipo_porcentaje);

-- Movement indexes
CREATE INDEX IF NOT EXISTS idx_mov_articulo ON app.movimiento_articulo(id_articulo);
CREATE INDEX IF NOT EXISTS idx_mov_articulo_fecha ON app.movimiento_articulo(id_articulo, fecha);
CREATE INDEX IF NOT EXISTS idx_mov_deposito ON app.movimiento_articulo(id_deposito);
CREATE INDEX IF NOT EXISTS idx_mov_deposito_fecha ON app.movimiento_articulo(id_deposito, fecha);
CREATE INDEX IF NOT EXISTS idx_mov_documento ON app.movimiento_articulo(id_documento);
CREATE INDEX IF NOT EXISTS idx_mov_tipo ON app.movimiento_articulo(id_tipo_movimiento);
CREATE INDEX IF NOT EXISTS idx_mov_fecha_desc ON app.movimiento_articulo(fecha DESC);

-- Payment indexes
CREATE INDEX IF NOT EXISTS idx_pago_documento ON app.pago(id_documento);
CREATE INDEX IF NOT EXISTS idx_pago_fecha ON app.pago(fecha);
CREATE INDEX IF NOT EXISTS idx_pago_forma ON app.pago(id_forma_pago);

-- Client list indexes
CREATE INDEX IF NOT EXISTS idx_lista_cliente_precio ON app.lista_cliente(id_lista_precio);

-- Remito indexes
CREATE INDEX IF NOT EXISTS idx_remito_documento ON app.remito(id_documento);
CREATE INDEX IF NOT EXISTS idx_remito_entidad ON app.remito(id_entidad_comercial);
CREATE INDEX IF NOT EXISTS idx_remito_fecha ON app.remito(fecha);
CREATE INDEX IF NOT EXISTS idx_remito_estado ON app.remito(estado);
CREATE INDEX IF NOT EXISTS idx_remito_numero ON app.remito(numero);

-- Log indexes
CREATE INDEX IF NOT EXISTS idx_log_usuario_fecha ON seguridad.log_actividad(id_usuario, fecha_hora);
CREATE INDEX IF NOT EXISTS idx_log_tipo_fecha ON seguridad.log_actividad(id_tipo_evento_log, fecha_hora);
CREATE INDEX IF NOT EXISTS idx_log_entidad ON seguridad.log_actividad(entidad);
CREATE INDEX IF NOT EXISTS idx_log_fecha_desc ON seguridad.log_actividad(fecha_hora DESC);

-- ============================================================================
-- SEED DATA (Universal)
-- ============================================================================

-- Roles
INSERT INTO seguridad.rol(nombre) VALUES ('ADMIN'), ('GERENTE'), ('EMPLEADO') ON CONFLICT (nombre) DO NOTHING;

-- Event log types
INSERT INTO seguridad.tipo_evento_log(codigo) VALUES 
  ('LOGIN_OK'), ('LOGIN_FAIL'), ('LOGOUT'), 
  ('INSERT'), ('UPDATE'), ('DELETE'), ('ERROR'),
  ('BACKUP'), ('RESTORE'), ('EXPORT'), ('IMPORT'),
  ('VIEW'), ('SELECT'), ('VIEW_DETAIL'), ('CONFIG_TAB'), ('SISTEMA')
ON CONFLICT (codigo) DO NOTHING;

-- Percentage types
INSERT INTO ref.tipo_porcentaje(tipo) VALUES ('MARGEN'), ('DESCUENTO') ON CONFLICT (tipo) DO NOTHING;

-- IVA types (Argentina)
INSERT INTO ref.tipo_iva(codigo, porcentaje, descripcion) VALUES 
  (3, 0.00, 'No Gravado'),
  (4, 10.50, 'IVA 10.5%'),
  (5, 21.00, 'IVA 21%'),
  (6, 27.00, 'IVA 27%'),
  (8, 5.00, 'IVA 5%'),
  (9, 2.50, 'IVA 2.5%')
ON CONFLICT (codigo) DO NOTHING;

-- Payment methods
INSERT INTO ref.forma_pago(descripcion) VALUES 
  ('Efectivo / Contado'), ('Cheque'), ('Cuenta Corriente'), 
  ('Tarjeta de Crédito'), ('Tarjeta de Débito'),
  ('Transferencia Bancaria'), ('MercadoPago')
ON CONFLICT (descripcion) DO NOTHING;

-- Document types (with AFIP codes)
INSERT INTO ref.tipo_documento(nombre, clase, afecta_stock, afecta_cuenta_corriente, codigo_afip, letra) VALUES 
  ('PRESUPUESTO', 'VENTA', TRUE, FALSE, NULL, NULL),
  ('FACTURA A', 'VENTA', TRUE, TRUE, 1, 'A'),
  ('FACTURA B', 'VENTA', TRUE, TRUE, 6, 'B'),
  ('FACTURA C', 'VENTA', TRUE, TRUE, 11, 'C'),
  ('NOTA CREDITO A', 'VENTA', TRUE, TRUE, 3, 'A'),
  ('NOTA CREDITO B', 'VENTA', TRUE, TRUE, 8, 'B'),
  ('NOTA CREDITO C', 'VENTA', TRUE, TRUE, 13, 'C'),
  ('NOTA DEBITO A', 'VENTA', TRUE, TRUE, 2, 'A'),
  ('NOTA DEBITO B', 'VENTA', TRUE, TRUE, 7, 'B'),
  ('ORDEN COMPRA', 'COMPRA', TRUE, FALSE, NULL, NULL),
  ('FACTURA COMPRA', 'COMPRA', TRUE, TRUE, NULL, NULL)
ON CONFLICT (nombre) DO UPDATE SET afecta_stock = EXCLUDED.afecta_stock;

-- Movement types
INSERT INTO ref.tipo_movimiento_articulo(nombre, signo_stock) VALUES 
  ('Compra', +1),
  ('Venta', -1),
  ('Devolución Cliente', +1),
  ('Devolución Proveedor', -1),
  ('Ajuste Positivo', +1),
  ('Ajuste Negativo', -1),
  ('Robo/Pérdida', -1),
  ('Uso Interno', -1),
  ('Transferencia Entrada', +1),
  ('Transferencia Salida', -1)
ON CONFLICT (nombre) DO NOTHING;

-- Default deposit
INSERT INTO ref.deposito(nombre, ubicacion) VALUES ('Depósito Central', 'Casa Central') ON CONFLICT (nombre) DO NOTHING;

-- IVA conditions
INSERT INTO ref.condicion_iva(nombre) VALUES 
  ('Responsable Inscripto'),
  ('Monotributista'),
  ('Exento'),
  ('Consumidor Final'),
  ('No Responsable')
ON CONFLICT (nombre) DO NOTHING;

-- Brands
INSERT INTO ref.marca(nombre) VALUES ('Genérica') ON CONFLICT (nombre) DO NOTHING;

-- Rubros
INSERT INTO ref.rubro(nombre) VALUES ('Genérico') ON CONFLICT (nombre) DO NOTHING;

-- Unit measures
INSERT INTO ref.unidad_medida(nombre, abreviatura) VALUES 
  ('Unidad', 'u'),
  ('Kilogramo', 'kg'),
  ('Litro', 'lt'),
  ('Metro', 'm'),
  ('Caja', 'cj'),
  ('Docena', 'doc'),
  ('Par', 'par')
ON CONFLICT (nombre) DO NOTHING;

-- Default price lists
INSERT INTO ref.lista_precio(nombre, activa, orden) VALUES 
  ('Lista 1', TRUE, 1),
  ('Lista 2', TRUE, 2),
  ('Lista 3', TRUE, 3),
  ('Lista 4', TRUE, 4),
  ('Lista 5', TRUE, 5),
  ('Lista 6', TRUE, 6),
  ('Lista 7', TRUE, 7),
  ('Lista Gremio', TRUE, 8)
ON CONFLICT (nombre) DO NOTHING;

-- Default admin user
INSERT INTO seguridad.usuario(nombre, id_rol, activo, contrasena_hash, email)
SELECT
  'Administrador',
  r.id,
  TRUE,
  crypt('Nx@r7n!2024#SecureAdmin$', gen_salt('bf', 12)),
  'admin@nexoryn.com'
FROM seguridad.rol r
WHERE r.nombre = 'ADMIN'
ON CONFLICT (email) DO NOTHING;

-- Default backup config
INSERT INTO seguridad.backup_config(frecuencia, hora, retencion_dias)
SELECT 'OFF', '03:00:00', 30
WHERE NOT EXISTS (SELECT 1 FROM seguridad.backup_config);

-- Default system configuration
INSERT INTO seguridad.config_sistema(clave, valor, tipo, descripcion) VALUES
  ('nombre_sistema', 'Nexoryn Tech', 'TEXT', 'Nombre del sistema que aparece en la interfaz'),
  ('logo_path', '', 'PATH', 'Ruta al archivo de logo del sistema'),
  ('razon_social', '', 'TEXT', 'Razón social de la empresa'),
  ('cuit_empresa', '', 'TEXT', 'CUIT de la empresa'),
  ('domicilio_empresa', '', 'TEXT', 'Domicilio fiscal de la empresa'),
  ('telefono_empresa', '', 'TEXT', 'Teléfono principal de la empresa'),
  ('email_empresa', '', 'TEXT', 'Email de contacto de la empresa'),
  ('slogan', '', 'TEXT', 'Slogan o lema de la empresa')
ON CONFLICT (clave) DO NOTHING;

-- ============================================================================
-- REPORTING VIEWS
-- ============================================================================

DROP VIEW IF EXISTS app.v_reporte_ventas_mensual CASCADE;
CREATE OR REPLACE VIEW app.v_reporte_ventas_mensual AS
SELECT
  date_trunc('month', d.fecha) AS mes,
  SUM(d.total) AS total_ventas,
  COUNT(d.id) AS cantidad_operaciones,
  AVG(d.total) AS ticket_promedio
FROM app.documento d
JOIN ref.tipo_documento td ON d.id_tipo_documento = td.id
WHERE td.clase = 'VENTA' AND d.estado IN ('CONFIRMADO', 'PAGADO')
GROUP BY 1
ORDER BY 1 DESC;

DROP VIEW IF EXISTS app.v_top_articulos_mes CASCADE;
CREATE OR REPLACE VIEW app.v_top_articulos_mes AS
SELECT
  a.nombre,
  r.nombre AS rubro,
  SUM(dd.cantidad) AS cantidad_vendida,
  SUM(dd.total_linea) AS total_facturado
FROM app.documento_detalle dd
JOIN app.documento d ON dd.id_documento = d.id
JOIN app.articulo a ON dd.id_articulo = a.id
JOIN ref.rubro r ON a.id_rubro = r.id
JOIN ref.tipo_documento td ON d.id_tipo_documento = td.id
WHERE td.clase = 'VENTA' 
  AND d.estado IN ('CONFIRMADO', 'PAGADO')
  AND d.fecha >= date_trunc('month', now())
GROUP BY a.nombre, r.nombre
ORDER BY total_facturado DESC
LIMIT 20;

DROP VIEW IF EXISTS app.v_deudores CASCADE;
CREATE OR REPLACE VIEW app.v_deudores AS
SELECT
  ec.id,
  COALESCE(ec.razon_social, ec.apellido || ' ' || ec.nombre) AS entidad,
  ec.telefono,
  lc.saldo_cuenta
FROM app.lista_cliente lc
JOIN app.entidad_comercial ec ON lc.id_entidad_comercial = ec.id
WHERE lc.saldo_cuenta > 0
ORDER BY lc.saldo_cuenta DESC;

-- ============================================================================
-- ROW LEVEL SECURITY (RLS) POLICIES
-- ============================================================================

-- Enable RLS on core tables
ALTER TABLE app.documento ENABLE ROW LEVEL SECURITY;
ALTER TABLE app.entidad_comercial ENABLE ROW LEVEL SECURITY;
ALTER TABLE app.movimiento_articulo ENABLE ROW LEVEL SECURITY;

-- Policy: Admin sees everything. Users see data if they have access (Simulated for this app context)
-- Since this is a desktop app likely running as a single DB user, these policies strictly enforce
-- that the application MUST set 'app.user_id' to trace actions, otherwise writes might fail or be logged anonymously.
-- For reading, we mostly allow all since it's a shared internal system, but we restrict modification.

-- Generic Policy for "Internal System" (Allows logic to rely on app.user_id for audit, but doesn't hide data from the app itself)
-- If we wanted strict multi-tenant, we would filter by tenant_id. Here we ensure auditability.

DROP POLICY IF EXISTS "Audit required for modification" ON app.documento;
CREATE POLICY "Audit required for modification" ON app.documento
  FOR ALL
  USING (true)
  WITH CHECK (current_setting('app.user_id', true) IS NOT NULL);

DROP POLICY IF EXISTS "Audit required for modification entity" ON app.entidad_comercial;
CREATE POLICY "Audit required for modification entity" ON app.entidad_comercial
  FOR ALL
  USING (true)
  WITH CHECK (current_setting('app.user_id', true) IS NOT NULL);

DROP POLICY IF EXISTS "Audit required for modification movimiento" ON app.movimiento_articulo;
CREATE POLICY "Audit required for modification movimiento" ON app.movimiento_articulo
  FOR ALL
  USING (true)
  WITH CHECK (current_setting('app.user_id', true) IS NOT NULL);

-- ============================================================================
-- OPTIMIZED INDEXES (ADDITIONAL)
-- ============================================================================

-- Case-insensitive lookup/search optimization
CREATE INDEX IF NOT EXISTS idx_entidad_email_lower ON app.entidad_comercial (lower(email));
CREATE INDEX IF NOT EXISTS idx_usuario_email_lower ON seguridad.usuario (lower(email));
CREATE INDEX IF NOT EXISTS idx_usuario_nombre_lower_trgm ON seguridad.usuario USING gin (lower(nombre) gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_usuario_email_lower_trgm ON seguridad.usuario USING gin (lower(email) gin_trgm_ops);

-- Covering indexes for commonly joined columns
CREATE INDEX IF NOT EXISTS idx_articulo_lookup_covering 
  ON app.articulo (id, nombre, costo) INCLUDE (id_rubro, id_marca);

-- Performance for reporting
CREATE INDEX IF NOT EXISTS idx_documento_fecha_estado ON app.documento (fecha, estado) WHERE estado IN ('CONFIRMADO', 'PAGADO');

-- ============================================================================
-- EXTREME PERFORMANCE OPTIMIZATIONS (ADDED FOR USER)
-- ============================================================================

-- Document sorting optimizations (DESC for recent items first)
CREATE INDEX IF NOT EXISTS idx_doc_tipo_fecha_desc ON app.documento(id_tipo_documento, fecha DESC);
CREATE INDEX IF NOT EXISTS idx_doc_entidad_fecha_desc ON app.documento(id_entidad_comercial, fecha DESC);
CREATE INDEX IF NOT EXISTS idx_doc_estado_fecha_desc ON app.documento(estado, fecha DESC);

-- Stock movement sorting
CREATE INDEX IF NOT EXISTS idx_mov_articulo_fecha_desc ON app.movimiento_articulo(id_articulo, fecha DESC);

-- Trigram indices on LOWER() for rapid case-insensitive filtering
-- Matching the specific pattern: lower(col) LIKE %...%
CREATE INDEX IF NOT EXISTS idx_entidad_razon_lower_trgm ON app.entidad_comercial USING gin (lower(razon_social) gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_entidad_apellido_lower_trgm ON app.entidad_comercial USING gin (lower(apellido) gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_entidad_nombre_lower_trgm ON app.entidad_comercial USING gin (lower(nombre) gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_entidad_cuit_lower_trgm ON app.entidad_comercial USING gin (lower(cuit) gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_entidad_localidad_lower_trgm ON ref.localidad USING gin (lower(nombre) gin_trgm_ops);

-- Index for Entity Search (matching the view's nombre_completo logic)
-- COALESCE(razon_social, TRIM(COALESCE(apellido, '') || ' ' || COALESCE(nombre, '')))
CREATE INDEX IF NOT EXISTS idx_entidad_nombre_completo_trgm ON app.entidad_comercial USING gin (
  lower(COALESCE(razon_social, TRIM(COALESCE(apellido, '') || ' ' || COALESCE(nombre, '')))) gin_trgm_ops
);

CREATE INDEX IF NOT EXISTS idx_articulo_nombre_lower_trgm ON app.articulo USING gin (lower(nombre) gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_articulo_ubicacion_lower_trgm ON app.articulo USING gin (lower(ubicacion) gin_trgm_ops);

-- Document Search
CREATE INDEX IF NOT EXISTS idx_doc_serie_trgm ON app.documento USING gin (numero_serie gin_trgm_ops);

-- Catalog + lookup search (case-insensitive LIKE)
CREATE INDEX IF NOT EXISTS idx_marca_nombre_lower_trgm ON ref.marca USING gin (lower(nombre) gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_rubro_nombre_lower_trgm ON ref.rubro USING gin (lower(nombre) gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_provincia_nombre_lower_trgm ON ref.provincia USING gin (lower(nombre) gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_deposito_nombre_lower_trgm ON ref.deposito USING gin (lower(nombre) gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_lista_precio_nombre_lower_trgm ON ref.lista_precio USING gin (lower(nombre) gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_unidad_medida_nombre_lower_trgm ON ref.unidad_medida USING gin (lower(nombre) gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_condicion_iva_nombre_lower_trgm ON ref.condicion_iva USING gin (lower(nombre) gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_forma_pago_desc_lower_trgm ON ref.forma_pago USING gin (lower(descripcion) gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_tipo_iva_desc_lower_trgm ON ref.tipo_iva USING gin (lower(descripcion) gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_tipo_porcentaje_tipo_lower_trgm ON ref.tipo_porcentaje USING gin (lower(tipo) gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_pago_referencia_lower_trgm ON app.pago USING gin (lower(referencia) gin_trgm_ops);

-- Document type search
CREATE INDEX IF NOT EXISTS idx_tipo_documento_nombre_lower_trgm ON ref.tipo_documento USING gin (lower(nombre) gin_trgm_ops);

-- Log search optimizations
CREATE INDEX IF NOT EXISTS idx_log_accion_lower_trgm ON seguridad.log_actividad USING gin (lower(accion) gin_trgm_ops);

-- Fast adjustments lookup (movimientos sin documento)
CREATE INDEX IF NOT EXISTS idx_mov_ajuste_fecha ON app.movimiento_articulo (fecha DESC) WHERE id_documento IS NULL;
