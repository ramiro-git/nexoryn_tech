import os
import datetime
from typing import Dict, Any, Optional
from afip import Afip

class AfipService:
    def __init__(self, cuit: str, cert_path: str, key_path: str, production: bool = False):
        """
        Inicializa el servicio de AFIP.
        :param cuit: CUIT del emisor sin guiones.
        :param cert_path: Ruta al archivo .crt
        :param key_path: Ruta al archivo .key
        :param production: True si se usa ambiente de producción, False para homologación.
        """
        self.cuit = cuit
        self.cert_path = cert_path
        self.key_path = key_path
        self.production = production
        
        # Inicializar SDK
        # Nota: El SDK suele buscar los archivos en la ruta provista.
        # En la vida real, se deben manejar errores de lectura aquí.
        try:
            self.afip = Afip({
                "CUIT": cuit,
                "cert": cert_path,
                "key": key_path,
                "production": production
            })
        except Exception as e:
            print(f"Error inicializando AFIP SDK: {e}")
            self.afip = None

    def get_server_status(self) -> bool:
        """Verifica si el servidor de AFIP está online."""
        if not self.afip: return False
        try:
            status = self.afip.ElectronicBilling.getServerStatus()
            return status.get("AppServer") == "OK"
        except Exception:
            return False

    def get_last_voucher_number(self, punto_venta: int, tipo_comprobante: int) -> int:
        """Obtiene el último número de comprobante registrado en AFIP."""
        if not self.afip: return 0
        try:
            return self.afip.ElectronicBilling.getLastVoucher(punto_venta, tipo_comprobante)
        except Exception as e:
            print(f"Error obteniendo último comprobante: {e}")
            return 0

    def authorize_invoice(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Autoriza una factura ante AFIP.
        data: {
            "CantReg": 1,
            "PtoVta": 1,
            "CbteTipo": 1, (1 para Factura A, 6 para B, 11 para C)
            "Concepto": 1, (1 Productos, 2 Servicios, 3 Ambos)
            "DocTipo": 80, (80 CUIT, 96 DNI)
            "DocNro": 20123456789,
            "CbteDesde": 1,
            "CbteHasta": 1,
            "CbteFch": "20231027",
            "ImpTotal": 121.00,
            "ImpTotConc": 0,
            "ImpNeto": 100.00,
            "ImpOpEx": 0,
            "ImpIVA": 21.00,
            "ImpTrib": 0,
            "MonId": "PES",
            "MonCotiz": 1,
            "Iva": [
                {
                    "Id": 5, (21%)
                    "BaseImp": 100.00,
                    "Importe": 21.00
                }
            ]
        }
        """
        if not self.afip:
            return {"error": "Servicio no inicializado"}

        try:
            res = self.afip.ElectronicBilling.createVoucher(data)
            return {
                "CAE": res.get("CAE"),
                "CAEFchVto": res.get("CAEFchVto"),
                "success": True
            }
        except Exception as e:
            return {
                "success": False,
                "error": str(e)
            }

# Template de uso sugerido:
# afip = AfipService("20301234567", "certs/cert.pem", "certs/key.pem", production=False)
# if afip.get_server_status():
#     print("AFIP Conectado")
