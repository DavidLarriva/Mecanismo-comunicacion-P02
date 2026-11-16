# [IA]
# Revisado por: Carolina Fortmann
"""STUB: imprime lo que llega a 'pedido.estado'. Simula a Notificaciones (Cinthy) y a la API.
Usa un grupo propio (no del contrato) para no quitarles eventos a los servicios reales.

  python -m backend.kafka.stubs.monitor_estados
"""
import logging

from .. import config
from ..consumer_base import ConsumerBase


class MonitorEstados(ConsumerBase):
    def __init__(self, **kw):
        super().__init__("monitor", config.GRUPO_MONITOR, config.TOPIC_PEDIDO_ESTADO, **kw)

    def procesar(self, ev):
        print(f"[estado] {ev['idPedido'][:8]} {ev['tipo']:<11} {ev['estado']:<22} "
              f"origen={ev['origen']:<11} detalle={ev['detalle']}")


def main():
    logging.basicConfig(level=logging.WARNING)
    MonitorEstados().iniciar()


if __name__ == "__main__":
    main()
