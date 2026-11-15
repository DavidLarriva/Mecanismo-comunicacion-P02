"""STUB de Caja y Facturación (responsable: Dome). Solo para probar mientras Dome no entrega lo real.

NO ejecutar junto al servicio real del mismo nombre: comparten consumer group y se repartirían los pedidos.
  python -m backend.kafka.stubs.servicio_stub caja
  python -m backend.kafka.stubs.servicio_stub facturacion
  ... --fallar-todo    # procesar() siempre falla: sirve para ver reintentos y DLQ
"""
import argparse
import logging

from .. import config
from ..consumer_base import ConsumerBase


class ServicioStub(ConsumerBase):
    def __init__(self, servicio, fallar_todo=False, **kw):
        super().__init__(servicio, config.GRUPOS_PEDIDO_CREADO[servicio], **kw)
        self.fallar_todo = fallar_todo

    def procesar(self, ev):
        if self.fallar_todo:
            raise RuntimeError("fallo simulado")
        if self.servicio == "caja":      # simula que el cajero cobró al instante
            self.publicar_estado(ev["idPedido"], "PAGO", "PAGADO",
                                 {"idConfirmacion": "PAG-STUB", "monto": ev["total"]})
        else:                            # facturación
            self.publicar_estado(ev["idPedido"], "FACTURACION", "FACTURADO", {"idFactura": "FAC-STUB"})
        self.log.info("procesado %s", ev["idPedido"][:8])


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)-14s %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("servicio", choices=["caja", "facturacion"])
    ap.add_argument("--fallar-todo", action="store_true")
    a = ap.parse_args()
    ServicioStub(a.servicio, a.fallar_todo).iniciar()


if __name__ == "__main__":
    main()
