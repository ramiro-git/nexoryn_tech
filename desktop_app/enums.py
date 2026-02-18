"""
Enums centralizados para estados, tipos y clases de documentos.
Previene typos y proporciona un único punto de verdad para constantes del negocio.
"""

from enum import Enum


class DocumentoEstado(str, Enum):
    """Estados posibles de un documento (Factura, Presupuesto, etc.)."""
    BORRADOR = "BORRADOR"
    CONFIRMADO = "CONFIRMADO"
    ANULADO = "ANULADO"
    PAGADO = "PAGADO"


class RemotoEstado(str, Enum):
    """Estados posibles de un remito (nota de entrega)."""
    PENDIENTE = "PENDIENTE"
    DESPACHADO = "DESPACHADO"
    ENTREGADO = "ENTREGADO"
    ANULADO = "ANULADO"


class BackupEstado(str, Enum):
    """Estados posibles de un backup."""
    PENDIENTE = "PENDIENTE"
    EN_PROGRESO = "EN_PROGRESO"
    COMPLETADO = "COMPLETADO"
    FALLIDO = "FALLIDO"
    VALIDANDO = "VALIDANDO"


class ClaseDocumento(str, Enum):
    """Clases de documentos: ingreso o egreso."""
    VENTA = "VENTA"
    COMPRA = "COMPRA"


class TipoComprobante(str, Enum):
    """Tipos de comprobantes según AFIP."""
    PRESUPUESTO = "PRESUPUESTO"
    FACTURA_A = "FACTURA A"
    FACTURA_B = "FACTURA B"
    FACTURA_C = "FACTURA C"
    RECIBO = "RECIBO"
    NOTA_CREDITO = "NOTA DE CREDITO"
    NOTA_DEBITO = "NOTA DE DEBITO"


# Convenience groups para validaciones y filtros comunes
DOCUMENTO_ESTADOS_CONFIRMADOS = (DocumentoEstado.CONFIRMADO, DocumentoEstado.PAGADO)
DOCUMENTO_ESTADOS_PENDIENTES = (DocumentoEstado.BORRADOR, DocumentoEstado.CONFIRMADO)
DOCUMENTO_ESTADOS_ACTIVOS = (DocumentoEstado.BORRADOR, DocumentoEstado.CONFIRMADO, DocumentoEstado.PAGADO)
