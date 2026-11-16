from datetime import datetime, timezone
from threading import RLock
from uuid import uuid4


def fecha_utc() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


# INICIO: fragmento elaborado con apoyo de IA y revisado por Dome.
class AlmacenPagos:
    def __init__(self) -> None:
        self._pagos: dict[str, dict] = {}
        self._lock = RLock()

    def registrar_pedido(self, evento: dict) -> bool:
        """Crea el cobro PENDIENTE al recibir pedido.creado."""
        id_pedido = evento.get("idPedido")
        if not id_pedido:
            return False

        with self._lock:
            if id_pedido in self._pagos:
                return False

            fecha_pendiente = fecha_utc()
            self._pagos[id_pedido] = {
                "idPedido": id_pedido,
                "estado": "PENDIENTE",
                "monto": int(evento["total"]),
                "metodo": None,
                "idConfirmacion": None,
                "fechaPago": None,
                "confirmadoFacturacion": False,
                "idEventoPendiente": str(uuid4()),
                "fechaEventoPendiente": fecha_pendiente,
                "eventoPendientePublicado": False,
                "idEventoPago": None,
                "eventoPagoPublicado": False,
            }
            return True
# FIN: fragmento elaborado con apoyo de IA.

    def obtener_pago(self, id_pedido: str) -> dict | None:
        with self._lock:
            pago = self._pagos.get(id_pedido)
            return pago.copy() if pago else None

    def obtener_evento_pendiente(self, id_pedido: str) -> dict | None:
        with self._lock:
            pago = self._pagos.get(id_pedido)
            if (
                pago is None
                or pago["estado"] != "PENDIENTE"
                or pago["eventoPendientePublicado"]
            ):
                return None

            return {
                "idEvento": pago["idEventoPendiente"],
                "idPedido": id_pedido,
                "tipo": "PAGO",
                "estado": "PENDIENTE",
                "origen": "caja",
                "detalle": {},
                "fecha": pago["fechaEventoPendiente"],
            }

    def marcar_evento_pendiente_publicado(self, id_pedido: str) -> None:
        with self._lock:
            pago = self._pagos.get(id_pedido)
            if pago is not None:
                pago["eventoPendientePublicado"] = True

    def registrar_pago(
        self,
        id_pedido: str,
        metodo: str = "EFECTIVO",
    ) -> dict | None:
        """Marca el pago como PAGADO; repetirlo conserva la confirmación."""
        with self._lock:
            pago = self._pagos.get(id_pedido)
            if pago is None:
                return None

            if pago["estado"] == "PENDIENTE":
                pago["estado"] = "PAGADO"
                pago["metodo"] = metodo
                pago["idEventoPago"] = str(uuid4())
                pago["eventoPagoPublicado"] = False
                pago["idConfirmacion"] = f"PAG-{uuid4().hex[:12].upper()}"
                pago["fechaPago"] = fecha_utc()

            return pago.copy()

    def obtener_evento_pago(self, id_pedido: str) -> dict | None:
        with self._lock:
            pago = self._pagos.get(id_pedido)
            if (
                pago is None
                or pago["estado"] != "PAGADO"
                or pago["eventoPagoPublicado"]
            ):
                return None

            return {
                "idEvento": pago["idEventoPago"],
                "idPedido": id_pedido,
                "tipo": "PAGO",
                "estado": "PAGADO",
                "origen": "caja",
                "detalle": {
                    "idConfirmacion": pago["idConfirmacion"],
                    "monto": pago["monto"],
                },
                "fecha": pago["fechaPago"],
            }

    def marcar_evento_pago_publicado(self, id_pedido: str) -> None:
        with self._lock:
            pago = self._pagos.get(id_pedido)
            if pago is not None:
                pago["eventoPagoPublicado"] = True
                
# INICIO: fragmento elaborado con apoyo de IA y revisado por Dome.
    def confirmar_para_facturacion(
        self,
        id_pedido: str,
    ) -> tuple[dict | None, bool]:
        """Devuelve el pago y si la confirmación para facturar es repetida."""
        with self._lock:
            pago = self._pagos.get(id_pedido)
            if pago is None:
                return None, False

            if pago["estado"] != "PAGADO":
                return pago.copy(), False

            repetida = pago["confirmadoFacturacion"]
            pago["confirmadoFacturacion"] = True
            return pago.copy(), repetida
# FIN: fragmento elaborado con apoyo de IA.

almacen_pagos = AlmacenPagos()
