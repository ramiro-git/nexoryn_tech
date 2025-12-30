# Tests y Diagnóstico de Base de Datos

Scripts para verificar la integridad, el rendimiento y el estado de la base de datos.

## Generación de Datos

### `test_data_generator.py`
Generador robusto de datos ficticios usando la librería `Faker`. No se suele ejecutar solo, ya que es invocado por `master_script.py --seed`.

## Pruebas de Rendimiento

### `stress_test.py`
Realiza pruebas de carga simulando múltiples consultas concurrentes para medir latencias.
- **Ejecución**:
  ```bash
  # Ejecutar 500 consultas con 10 trabajadores paralelos
  python stress_test.py --queries 500 --workers 10
  ```

## Herramientas de Diagnóstico y Verificación

### `check_db.py`
Verificación rápida de contenido. Imprime el recuento de filas de todas las tablas en los esquemas `app`, `ref` y `seguridad`.
- **Ejecución**:
  ```bash
  python check_db.py
  ```

### `check_seqs.py`
Muestra el valor actual de todas las secuencias (IDs autoincrementales). Útil para diagnosticar errores de clave duplicada.
- **Ejecución**:
  ```bash
  python check_seqs.py
  ```

### `check_locks.py`
Muestra las consultas que están actualmente en ejecución y podrían estar bloqueando la base de datos.
- **Ejecución**:
  ```bash
  python check_locks.py
  ```

### `debug_copy.py`
Script de un solo propósito para diagnosticar problemas específicos con el protocolo `COPY` durante las importaciones masivas.
