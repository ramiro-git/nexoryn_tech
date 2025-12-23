
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

CREATE INDEX IF NOT EXISTS idx_articulo_nombre_lower_trgm ON app.articulo USING gin (lower(nombre) gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_articulo_ubicacion_lower_trgm ON app.articulo USING gin (lower(ubicacion) gin_trgm_ops);

-- Document Search
CREATE INDEX IF NOT EXISTS idx_doc_serie_trgm ON app.documento USING gin (numero_serie gin_trgm_ops);

-- Functional index for Entity Search (matching the view's nombre_completo logic)
-- logic: COALESCE(razon_social, TRIM(COALESCE(apellido, '') || ' ' || COALESCE(nombre, '')))
-- We use the underlying table expression
CREATE INDEX IF NOT EXISTS idx_entidad_nombre_completo_trgm ON app.entidad_comercial USING gin (
  lower(COALESCE(razon_social, TRIM(COALESCE(apellido, '') || ' ' || COALESCE(nombre, '')))) gin_trgm_ops
);

-- Note: Date-cast indices (fecha::date) removed because timestamptz->date is not immutable.
-- Queries in database.py have been updated to use direct range comparisons on original 'fecha' column.
