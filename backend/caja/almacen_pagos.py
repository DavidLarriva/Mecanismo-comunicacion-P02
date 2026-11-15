from datetime import datetime, timezone
from threading import RLock
from uuid import uuid4


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
            # Ignora pedidos repetidos: el consumidor Kafka puede reintentarlos.
            if id_pedido in self._pagos:
                return False

            self._pagos[id_pedido] = {
                "idPedido": id_pedido,
                "estado": "PENDIENTE",
                "monto": int(evento.get("total", 0)),
                "metodo": None,
                "idConfirmacion": None,
                "fechaPago": None,
                "confirmadoFacturacion": False,
                "idEventoPago": None,
                "eventoPagoPublicado": False,
            }
            return True

    def obtener_pago(self, id_pedido: str) -> dict | None:
        with self._lock:
            pago = self._pagos.get(id_pedido)
            return pago.copy() if pago else None

    def registrar_pago(self, id_pedido: str, metodo: str = "EFECTIVO") -> dict | None:
        """Marca el pago como PAGADO; repetirlo devuelve la misma confirmación."""
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
                pago["fechaPago"] = (
                    datetime.now(timezone.utc)
                    .isoformat(timespec="seconds")
                    .replace("+00:00", "Z")
                )
                

            return pago.copy()

    def confirmar_para_facturacion(
        self, id_pedido: str
    ) -> tuple[dict | None, bool]:
        """Entrega la confirmación a Facturación una sola vez."""
        with self._lock:
            pago = self._pagos.get(id_pedido)
            if pago is None:
                return None, False

            if pago["estado"] != "PAGADO":
                return pago.copy(), False

            repetida = pago["confirmadoFacturacion"]
            pago["confirmadoFacturacion"] = True
            return pago.copy(), repetida


    def marcar_evento_pago_publicado(self, id_pedido: str) -> None:
        with self._lock:
            pago = self._pagos.get(id_pedido)
            if pago is not None:
                pago["eventoPagoPublicado"] = True


almacen_pagos = AlmacenPagos()