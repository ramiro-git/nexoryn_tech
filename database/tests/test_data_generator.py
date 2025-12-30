import random
from decimal import Decimal
from datetime import datetime, timedelta
from faker import Faker
import psycopg
from typing import List, Dict, Any, Optional

class TestDataGenerator:
    def __init__(self, conn: psycopg.Connection):
        self.conn = conn
        self.faker = Faker('es_AR')
        self.cache = {}

    def _get_ids(self, table: str) -> List[int]:
        if table in self.cache:
            return self.cache[table]
        with self.conn.cursor() as cur:
            cur.execute(f"SELECT id FROM {table}")
            ids = [row[0] for row in cur.fetchall()]
            self.cache[table] = ids
            return ids

    def generate_entities(self, count: int = 100):
        print(f"Generating {count} entities...")
        loc_ids = self._get_ids("ref.localidad")
        iva_ids = self._get_ids("ref.condicion_iva")
        
        entities = []
        for _ in range(count):
            tipo = random.choice(['CLIENTE', 'PROVEEDOR', 'AMBOS'])
            is_company = random.random() > 0.7
            
            if is_company:
                razon_social = self.faker.company()
                apellido = None
                nombre = None
            else:
                razon_social = None
                apellido = self.faker.last_name()
                nombre = self.faker.first_name()
                
            entities.append({
                'apellido': apellido,
                'nombre': nombre,
                'razon_social': razon_social,
                'domicilio': self.faker.address().replace('\n', ', '),
                'id_localidad': random.choice(loc_ids) if loc_ids else None,
                'cuit': f"{random.randint(20, 33)}-{random.randint(10000000, 99999999)}-{random.randint(0, 9)}",
                'id_condicion_iva': random.choice(iva_ids) if iva_ids else None,
                'tipo': tipo,
                'telefono': self.faker.phone_number()[:30],
                'email': self.faker.email(),
                'activo': True
            })
            
        with self.conn.cursor() as cur:
            cols = entities[0].keys()
            query = f"INSERT INTO app.entidad_comercial ({', '.join(cols)}) VALUES ({', '.join(['%s']*len(cols))})"
            cur.executemany(query, [list(e.values()) for e in entities])
        self.conn.commit()

    def generate_articles(self, count: int = 500):
        print(f"Generating {count} articles...")
        marcas = self._get_ids("ref.marca")
        rubros = self._get_ids("ref.rubro")
        ivas = self._get_ids("ref.tipo_iva")
        unidades = self._get_ids("ref.unidad_medida")
        provs = [id for id in self._get_ids("app.entidad_comercial")] # Too many, maybe filter?
        
        articles = []
        for _ in range(count):
            costo = Decimal(random.uniform(10.0, 5000.0)).quantize(Decimal('0.01'))
            articles.append({
                'nombre': self.faker.catch_phrase()[:200],
                'id_marca': random.choice(marcas) if marcas else None,
                'id_rubro': random.choice(rubros) if rubros else None,
                'id_tipo_iva': random.choice(ivas) if ivas else None,
                'costo': costo,
                'stock_minimo': random.randint(1, 20),
                'id_unidad_medida': random.choice(unidades) if unidades else None,
                'id_proveedor': random.choice(provs) if provs else None,
                'activo': True
            })

        with self.conn.cursor() as cur:
            cols = articles[0].keys()
            query = f"INSERT INTO app.articulo ({', '.join(cols)}) VALUES ({', '.join(['%s']*len(cols))}) RETURNING id"
            cur.executemany(query, [list(a.values()) for a in articles])
        self.conn.commit()
        
        # After inserting articles, generate prices
        self._generate_prices()

    def _generate_prices(self):
        print("Generating article prices...")
        art_ids = self._get_ids("app.articulo")
        listas = self._get_ids("ref.lista_precio")
        tipo_pct = self._get_ids("ref.tipo_porcentaje") # Assume 1: MARGEN, 2: DESCUENTO
        
        prices = []
        for aid in art_ids:
            # For each article, generate price for each list
            with self.conn.cursor() as cur:
                cur.execute("SELECT costo FROM app.articulo WHERE id = %s", (aid,))
                costo = cur.fetchone()[0]
                
            for lid in listas:
                if lid == 1: # Retail
                    margen = Decimal(random.uniform(0.2, 0.6)).quantize(Decimal('0.01'))
                    precio = (costo * (1 + margen)).quantize(Decimal('0.01'))
                    prices.append((aid, lid, precio, margen * 100, 1)) # 1: MARGEN
                else:
                    desc = Decimal(random.uniform(0.05, 0.25)).quantize(Decimal('0.01'))
                    # Price based on Retail or Cost? Let's say based on Retail - discount
                    # But simpler: Cost + lower margen
                    margen = Decimal(random.uniform(0.1, 0.3)).quantize(Decimal('0.01'))
                    precio = (costo * (1 + margen)).quantize(Decimal('0.01'))
                    prices.append((aid, lid, precio, desc * 100, 2)) # 2: DESCUENTO

        with self.conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO app.articulo_precio (id_articulo, id_lista_precio, precio, porcentaje, id_tipo_porcentaje) VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
                prices
            )
        self.conn.commit()

    def generate_documents(self, count: int = 200):
        print(f"Generating {count} documents...")
        tds = self._get_ids("ref.tipo_documento")
        ents = self._get_ids("app.entidad_comercial")
        depositos = self._get_ids("ref.deposito")
        users = self._get_ids("seguridad.usuario")
        arts = self._get_ids("app.articulo")
        
        for _ in range(count):
            ent_id = random.choice(ents)
            doc_type = random.choice(tds)
            dep_id = random.choice(depositos)
            user_id = random.choice(users) if users else None
            
            fecha = datetime.now() - timedelta(days=random.randint(0, 90))
            
            # Create Document Header
            doc_data = {
                'id_tipo_documento': doc_type,
                'fecha': fecha,
                'numero_serie': f"0001-{random.randint(1, 999999):08d}",
                'id_entidad_comercial': ent_id,
                'estado': 'CONFIRMADO',
                'id_deposito': dep_id,
                'id_usuario': user_id
            }
            
            with self.conn.cursor() as cur:
                cols = doc_data.keys()
                query = f"INSERT INTO app.documento ({', '.join(cols)}) VALUES ({', '.join(['%s']*len(cols))}) RETURNING id"
                cur.execute(query, list(doc_data.values()))
                doc_id = cur.fetchone()[0]
                
                # Generate line items
                num_items = random.randint(1, 10)
                total_neto = Decimal('0.00')
                total_iva = Decimal('0.00')
                
                for i in range(1, num_items + 1):
                    art_id = random.choice(arts)
                    cur.execute("SELECT costo, nombre FROM app.articulo WHERE id = %s", (art_id,))
                    costo, art_name = cur.fetchone()
                    
                    # Get price for List 1
                    cur.execute("SELECT precio FROM app.articulo_precio WHERE id_articulo = %s AND id_lista_precio = 1", (art_id,))
                    res = cur.fetchone()
                    precio = res[0] if res else costo * Decimal('1.5')
                    
                    cantidad = Decimal(random.randint(1, 5))
                    total_linea = (precio * cantidad).quantize(Decimal('0.01'))
                    
                    cur.execute("""
                        INSERT INTO app.documento_detalle (id_documento, nro_linea, descripcion_historica, id_articulo, cantidad, precio_unitario, total_linea)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (doc_id, i, art_name, art_id, cantidad, precio, total_linea))
                    
                    total_neto += total_linea / Decimal('1.21')
                    total_iva += total_linea - (total_linea / Decimal('1.21'))
                    
                    # Generate Stock Movement
                    with self.conn.cursor() as cur_mov:
                        # Get sign - using the correct column name 'afecta_stock'
                        cur_mov.execute("SELECT signo_stock FROM ref.tipo_movimiento_articulo WHERE id = (SELECT CASE WHEN afecta_stock THEN 2 ELSE 1 END FROM ref.tipo_documento WHERE id = %s)", (doc_type,))
                        # Sign logic based on doc_type (e.g., 2=FACTURA A, 3=FACTURA B, etc. are sales)
                        signo = -1 if doc_type in [2, 3, 4, 5, 6, 7] else 1 
                        
                        cur_mov.execute("""
                            INSERT INTO app.movimiento_articulo (id_articulo, id_tipo_movimiento, fecha, cantidad, id_deposito, id_documento)
                            VALUES (%s, %s, %s, %s, %s, %s)
                        """, (art_id, 2 if signo < 0 else 1, fecha, cantidad, dep_id, doc_id))

                total = (total_neto + total_iva).quantize(Decimal('0.01'))
                cur.execute("""
                    UPDATE app.documento 
                    SET neto = %s, iva_total = %s, total = %s, subtotal = %s
                    WHERE id = %s
                """, (total_neto.quantize(Decimal('0.01')), total_iva.quantize(Decimal('0.01')), total, total_neto.quantize(Decimal('0.01')), doc_id))
                
                # Generate Payment
                if random.random() > 0.2:
                    fp_ids = self._get_ids("ref.forma_pago")
                    cur.execute("""
                        INSERT INTO app.pago (id_documento, id_forma_pago, fecha, monto, referencia)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (doc_id, random.choice(fp_ids), fecha, total, "Pago Test Data"))

        self.conn.commit()

    def generate_logs(self, count: int = 1000):
        print(f"Generating {count} logs...")
        users = self._get_ids("seguridad.usuario")
        event_types = self._get_ids("seguridad.tipo_evento_log")
        
        logs = []
        for _ in range(count):
            logs.append((
                random.choice(users) if users else None,
                random.choice(event_types),
                datetime.now() - timedelta(minutes=random.randint(0, 43200)), # 30 days
                random.choice(['app.articulo', 'app.entidad_comercial', 'app.documento', 'SISTEMA']),
                random.randint(1, 1000),
                random.choice(['INSERT', 'UPDATE', 'DELETE', 'LOGIN', 'LOGOUT', 'VIEW']),
                'OK',
                self.faker.ipv4()
            ))
            
        with self.conn.cursor() as cur:
            cur.executemany("""
                INSERT INTO seguridad.log_actividad (id_usuario, id_tipo_evento_log, fecha_hora, entidad, id_entidad, accion, resultado, ip)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, logs)
        self.conn.commit()
