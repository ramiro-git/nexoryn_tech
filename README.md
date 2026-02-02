# Nexoryn Tech - Sistema de Gestión Empresarial

Nexoryn Tech es una solución integral de gestión empresarial (ERP) diseñada para pequeñas y medianas empresas, con soporte para facturación electrónica (AFIP/ARCA), control de inventario, ventas y gestión de clientes.

## 🚀 Inicio Rápido

Para ejecutar el proyecto en entorno de desarrollo:

1.  **Clonar el repositorio**.
2.  **Instalar dependencias**:
    ```bash
    pip install -r requirements.txt
    ```
3.  **Configurar el archivo `.env`**:
    Copia la plantilla de [Requisitos de Instalación](docs/REQUISITOS_INSTALACION.md) y completa tus datos.
4.  **Ejecutar la aplicación**:
    ```bash
    python desktop_app/main.py
    ```

## 📖 Documentación Detallada

Hemos organizado la documentación en guías específicas para facilitar la configuración y el mantenimiento:

### Configuración y Despliegue
- [**Requisitos de Instalación**](docs/REQUISITOS_INSTALACION.md): Software necesario y configuración del `.env`.
- [**Guía de Empaquetado**](docs/GUIA_EMPAQUETADO.md): Cómo generar el ejecutable (`.exe`) y manejar activos.
- [**Guía de Red Local (LAN)**](docs/GUIA_RED_LOCAL.md): Configuración para múltiples terminales en una misma oficina.

### Funcionalidades Específicas
- [**Integración AFIP (ARCA)**](docs/AFIP_ARCA.md): Pasos para habilitar la factura electrónica.
- [**Guía Portal AFIP**](docs/GUIA_AFIP_PORTAL.md): Cómo realizar los trámites en la web de AFIP.
- [**Sistema de Backups**](docs/BACKUP_SYSTEM.md): Configuración de copias de seguridad incrementales y profesionales.

### Base de Datos
- [**Gestión de Base de Datos**](docs/DATABASE.md): Estructura, inicialización y sincronización automática del esquema.

## 🛠️ Tecnologías Utilizadas

- **Frontend**: [Flet](https://flet.dev/) (Flutter para Python).
- **Backend**: Python 3.12+.
- **Base de Datos**: PostgreSQL 16+.
- **Integraciones**: OpenSSL (para certificados AFIP).

---

> [!NOTE]
> Este proyecto está en constante evolución. Consulta la documentación antes de realizar cambios estructurales.
