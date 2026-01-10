import base64
import datetime as dt
import os
import shutil
import subprocess
import tempfile
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Dict, Optional

import requests
from zeep import Client, Settings
from zeep.exceptions import Fault
from zeep.helpers import serialize_object
from zeep.transports import Transport

WSAA_WSDL_HOMO = "https://wsaahomo.afip.gov.ar/ws/services/LoginCms?WSDL"
WSAA_WSDL_PROD = "https://wsaa.afip.gov.ar/ws/services/LoginCms?WSDL"
WSFE_WSDL_HOMO = "https://wswhomo.afip.gov.ar/wsfev1/service.asmx?WSDL"
WSFE_WSDL_PROD = "https://wsfe.afip.gov.ar/wsfev1/service.asmx?WSDL"


@dataclass
class AfipToken:
    token: str
    sign: str
    expires_at: dt.datetime


class AfipService:
    def __init__(self, cuit: str, cert_path: str, key_path: str, production: bool = False):
        """
        Direct AFIP WSAA + WSFEv1 integration (no SDK).
        """
        self.cuit = "".join(ch for ch in str(cuit) if ch.isdigit())
        self.cert_path = cert_path
        self.key_path = key_path
        self.production = production

        self._token: Optional[AfipToken] = None
        self._wsaa_client: Optional[Client] = None
        self._wsfe_client: Optional[Client] = None
        self._condicion_map: Optional[Dict[str, int]] = None
        self._det_field_names: Optional[set] = None
        self._ta_cache_path: Optional[Path] = None

        self._session = requests.Session()
        self._transport = Transport(session=self._session, timeout=30)
        self._settings = Settings(strict=False, xml_huge_tree=True)

        self._openssl_path = shutil.which("openssl")

    def _wsaa_wsdl(self) -> str:
        return WSAA_WSDL_PROD if self.production else WSAA_WSDL_HOMO

    def _wsfe_wsdl(self) -> str:
        return WSFE_WSDL_PROD if self.production else WSFE_WSDL_HOMO

    def _get_wsaa_client(self) -> Client:
        if self._wsaa_client is None:
            self._wsaa_client = Client(self._wsaa_wsdl(), transport=self._transport, settings=self._settings)
        return self._wsaa_client

    def _get_wsfe_client(self) -> Client:
        if self._wsfe_client is None:
            self._wsfe_client = Client(self._wsfe_wsdl(), transport=self._transport, settings=self._settings)
        return self._wsfe_client

    def _get_ta_cache_path(self) -> Path:
        if self._ta_cache_path is None:
            suffix = "prod" if self.production else "homo"
            base = Path("logs")
            base.mkdir(parents=True, exist_ok=True)
            self._ta_cache_path = base / f"afip_ta_wsfe_{suffix}.xml"
        return self._ta_cache_path

    def _load_cached_token(self) -> Optional[AfipToken]:
        path = self._get_ta_cache_path()
        if not path.exists():
            return None
        try:
            xml_text = path.read_text(encoding="utf-8")
            token = self._parse_login_response(xml_text)
        except Exception:
            return None
        if token.expires_at <= (dt.datetime.utcnow() + dt.timedelta(minutes=1)):
            return None
        self._token = token
        return token

    def _store_cached_token(self, xml_text: str) -> None:
        path = self._get_ta_cache_path()
        try:
            path.write_text(xml_text, encoding="utf-8")
        except Exception:
            return

    def _get_det_field_names(self) -> set:
        if self._det_field_names is not None:
            return self._det_field_names
        try:
            client = self._get_wsfe_client()
            det_type = client.get_type("ns0:FECAEDetRequest")
            names = {name for name, _ in det_type.elements}
        except Exception:
            names = set()
        self._det_field_names = names
        return names

    def _create_tra_xml(self, service: str) -> str:
        now = dt.datetime.utcnow() + dt.timedelta(hours=-3)
        unique_id = int(now.timestamp())
        gen_time = (now - dt.timedelta(minutes=10)).strftime("%Y-%m-%dT%H:%M:%S")
        exp_time = (now + dt.timedelta(minutes=10)).strftime("%Y-%m-%dT%H:%M:%S")
        return (
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
            "<loginTicketRequest version=\"1.0\">"
            "<header>"
            f"<uniqueId>{unique_id}</uniqueId>"
            f"<generationTime>{gen_time}</generationTime>"
            f"<expirationTime>{exp_time}</expirationTime>"
            "</header>"
            f"<service>{service}</service>"
            "</loginTicketRequest>"
        )

    def _sign_tra(self, tra_xml: str) -> str:
        if not self._openssl_path:
            raise RuntimeError("openssl no encontrado. Instale OpenSSL o agreguelo al PATH.")
        if not os.path.exists(self.cert_path):
            raise RuntimeError("Certificado no encontrado.")
        if not os.path.exists(self.key_path):
            raise RuntimeError("Clave privada no encontrada.")

        with tempfile.TemporaryDirectory() as tmp:
            tra_path = os.path.join(tmp, "tra.xml")
            cms_path = os.path.join(tmp, "tra.cms")
            with open(tra_path, "w", encoding="utf-8") as fh:
                fh.write(tra_xml)

            cmd = [
                self._openssl_path,
                "smime",
                "-sign",
                "-signer",
                self.cert_path,
                "-inkey",
                self.key_path,
                "-outform",
                "DER",
                "-nodetach",
                "-binary",
                "-in",
                tra_path,
                "-out",
                cms_path,
            ]
            res = subprocess.run(cmd, capture_output=True, text=True)
            if res.returncode != 0:
                raise RuntimeError(f"Error generando CMS: {res.stderr.strip() or res.stdout.strip()}")

            with open(cms_path, "rb") as fh:
                cms = fh.read()
        return base64.b64encode(cms).decode("ascii")

    def _parse_login_response(self, xml_text: str) -> AfipToken:
        root = ET.fromstring(xml_text)
        token = root.findtext(".//token")
        sign = root.findtext(".//sign")
        exp = root.findtext(".//expirationTime")
        if not token or not sign:
            raise RuntimeError("Respuesta WSAA invalida: falta token o sign.")
        expires_at = self._parse_datetime(exp) if exp else dt.datetime.utcnow() + dt.timedelta(minutes=10)
        return AfipToken(token=token, sign=sign, expires_at=expires_at)

    def _parse_datetime(self, value: str) -> dt.datetime:
        cleaned = value.replace("Z", "+00:00")
        try:
            parsed = dt.datetime.fromisoformat(cleaned)
        except ValueError:
            return dt.datetime.utcnow() + dt.timedelta(minutes=10)
        if parsed.tzinfo:
            return parsed.astimezone(dt.timezone.utc).replace(tzinfo=None)
        return parsed

    def _get_token(self) -> AfipToken:
        now = dt.datetime.utcnow()
        if self._token and self._token.expires_at > (now + dt.timedelta(minutes=1)):
            return self._token
        cached = self._load_cached_token()
        if cached:
            return cached

        tra_xml = self._create_tra_xml("wsfe")
        cms = self._sign_tra(tra_xml)
        client = self._get_wsaa_client()
        try:
            response_xml = client.service.loginCms(cms)
        except Fault as exc:
            msg = str(exc)
            if "ta valido" in msg.lower():
                cached = self._load_cached_token()
                if cached:
                    return cached
            raise RuntimeError(f"WSAA error: {exc}") from exc

        self._token = self._parse_login_response(response_xml)
        self._store_cached_token(response_xml)
        return self._token

    def _auth(self) -> Dict[str, Any]:
        token = self._get_token()
        return {"Token": token.token, "Sign": token.sign, "Cuit": int(self.cuit)}

    def get_server_status(self) -> bool:
        try:
            client = self._get_wsfe_client()
            status = client.service.FEDummy()
            data = serialize_object(status)
            return all(data.get(k) == "OK" for k in ("AppServer", "DbServer", "AuthServer"))
        except Exception:
            return False

    def get_last_voucher_number(self, punto_venta: int, tipo_comprobante: int) -> int:
        try:
            client = self._get_wsfe_client()
            auth = self._auth()
            last = client.service.FECompUltimoAutorizado(
                Auth=auth,
                PtoVta=int(punto_venta),
                CbteTipo=int(tipo_comprobante),
            )
            if isinstance(last, int):
                return last
            payload = serialize_object(last)
            if isinstance(payload, dict):
                for key in ("CbteNro", "cbteNro"):
                    if key in payload:
                        return int(payload.get(key) or 0)
                nested = payload.get("FECompUltimoAutorizadoResult")
                if isinstance(nested, dict):
                    for key in ("CbteNro", "cbteNro"):
                        if key in nested:
                            return int(nested.get(key) or 0)
            return 0
        except Exception as e:
            print(f"Error obteniendo ultimo comprobante: {e}")
            return 0

    def authorize_invoice(self, data: Dict[str, Any]) -> Dict[str, Any]:
        if not data:
            return {"success": False, "error": "Datos de factura incompletos"}

        try:
            client = self._get_wsfe_client()
            auth = self._auth()
            request = self._build_fe_caereq(data)
            result = client.service.FECAESolicitar(Auth=auth, FeCAEReq=request)
            payload = serialize_object(result)
        except Exception as exc:
            return {"success": False, "error": str(exc)}

        errors_msg = self._format_errors(payload.get("Errors"))
        if errors_msg:
            return {"success": False, "error": errors_msg}

        det_resp = payload.get("FeDetResp") or {}
        det_list = det_resp.get("FECAEDetResponse") if isinstance(det_resp, dict) else None
        if isinstance(det_list, dict):
            det_list = [det_list]
        if not det_list:
            return {"success": False, "error": "Respuesta AFIP invalida"}

        det = det_list[0]
        if det.get("Resultado") == "A" and det.get("CAE"):
            return {"success": True, "CAE": det.get("CAE"), "CAEFchVto": det.get("CAEFchVto")}

        obs_msg = self._format_errors(det.get("Observaciones"))
        return {"success": False, "error": obs_msg or "AFIP rechazo la solicitud"}

    def _build_fe_caereq(self, data: Dict[str, Any]) -> Dict[str, Any]:
        cab = {
            "CantReg": int(data.get("CantReg", 1)),
            "PtoVta": int(data.get("PtoVta")),
            "CbteTipo": int(data.get("CbteTipo")),
        }

        det: Dict[str, Any] = {
            "Concepto": int(data.get("Concepto", 1)),
            "DocTipo": int(data.get("DocTipo", 99)),
            "DocNro": int(data.get("DocNro", 0)),
            "CbteDesde": int(data.get("CbteDesde")),
            "CbteHasta": int(data.get("CbteHasta")),
            "CbteFch": data.get("CbteFch"),
            "ImpTotal": self._afip_amount(data.get("ImpTotal", 0)),
            "ImpTotConc": self._afip_amount(data.get("ImpTotConc", 0)),
            "ImpNeto": self._afip_amount(data.get("ImpNeto", 0)),
            "ImpOpEx": self._afip_amount(data.get("ImpOpEx", 0)),
            "ImpIVA": self._afip_amount(data.get("ImpIVA", 0)),
            "ImpTrib": self._afip_amount(data.get("ImpTrib", 0)),
            "MonId": data.get("MonId", "PES"),
            "MonCotiz": float(data.get("MonCotiz", 1)),
        }

        iva_list = data.get("Iva")
        if iva_list:
            det["Iva"] = {"AlicIva": self._normalize_iva_list(iva_list)}

        cond_id = data.get("CondicionIVAReceptorId")
        if cond_id is not None:
            field_name = self._resolve_condicion_field_name()
            if field_name:
                det[field_name] = int(cond_id)

        return {
            "FeCabReq": cab,
            "FeDetReq": {"FECAEDetRequest": [det]},
        }

    def get_condicion_iva_receptor_id(self, name: str) -> Optional[int]:
        if not name:
            return None
        mapping = self._get_condicion_map()
        if not mapping:
            return None
        target = self._normalize(name)
        for desc, cid in mapping.items():
            if target == self._normalize(desc):
                return cid
        for desc, cid in mapping.items():
            if target in self._normalize(desc) or self._normalize(desc) in target:
                return cid
        return None

    def _resolve_condicion_field_name(self) -> Optional[str]:
        fields = self._get_det_field_names()
        if not fields:
            return None
        for candidate in ("CondicionIVAReceptorId", "CondicionIVAReceptor", "CondicionIvaReceptor"):
            if candidate in fields:
                return candidate
        return None

    def _get_condicion_map(self) -> Dict[str, int]:
        if self._condicion_map is not None:
            return self._condicion_map
        try:
            client = self._get_wsfe_client()
            auth = self._auth()
            res = client.service.FEParamGetCondicionIvaReceptor(Auth=auth)
            payload = serialize_object(res)
            items = self._extract_param_items(payload)
            mapping: Dict[str, int] = {}
            for item in items:
                if not isinstance(item, dict):
                    continue
                cid = item.get("Id") or item.get("id")
                desc = item.get("Desc") or item.get("Descripcion") or item.get("desc") or item.get("descripcion")
                if cid is not None and desc:
                    mapping[str(desc)] = int(cid)
            self._condicion_map = mapping
            return mapping
        except Exception:
            self._condicion_map = {}
            return self._condicion_map

    def _extract_param_items(self, payload: Any) -> list:
        if payload is None:
            return []
        if isinstance(payload, list):
            if payload and all(isinstance(x, dict) for x in payload):
                return payload
            for item in payload:
                res = self._extract_param_items(item)
                if res:
                    return res
            return []
        if isinstance(payload, dict):
            if "ResultGet" in payload:
                return self._extract_param_items(payload.get("ResultGet"))
            for value in payload.values():
                res = self._extract_param_items(value)
                if res:
                    return res
            return []
        return []

    def _normalize(self, value: str) -> str:
        return "".join(ch for ch in value.lower() if ch.isalnum() or ch.isspace()).strip()

    def _format_errors(self, errors: Any) -> str:
        if not errors:
            return ""
        if isinstance(errors, dict) and "Err" in errors:
            errors = errors.get("Err")
        if isinstance(errors, dict) and "Obs" in errors:
            errors = errors.get("Obs")
        if isinstance(errors, dict):
            errors = [errors]
        if not isinstance(errors, list):
            return str(errors)
        messages = []
        for err in errors:
            if isinstance(err, dict):
                code = err.get("Code") or err.get("code")
                msg = err.get("Msg") or err.get("msg")
                if code and msg:
                    messages.append(f"{code}: {msg}")
                else:
                    messages.append(str(err))
            else:
                messages.append(str(err))
        return " | ".join(messages)

    def _afip_amount(self, value: Any) -> Decimal:
        try:
            dec = Decimal(str(value))
        except Exception:
            dec = Decimal("0")
        return dec.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    def _normalize_iva_list(self, iva_list: Any) -> list:
        if not isinstance(iva_list, list):
            return iva_list
        normalized = []
        for item in iva_list:
            if not isinstance(item, dict):
                normalized.append(item)
                continue
            normalized.append({
                **item,
                "BaseImp": self._afip_amount(item.get("BaseImp", 0)),
                "Importe": self._afip_amount(item.get("Importe", 0)),
            })
        return normalized
