# [IA]
# Revisado por: Carolina Fortmann
"""Cocina (contrato: responsable Caro, sin puerto).

Consume 'pedido.creado' (grupo 'cocina') y publica en 'pedido.estado':
PREPARACION/EN_PREPARACION y, tras la preparación, PREPARACION/LISTO.
Decisión del contrato: cocina prepara SIN esperar el pago.

Ejecutar desde la raíz del repo:  python -m backend.cocina.main [--demora 10]
"""
import argparse
import logging
import time

from backend.kafka import config
from backend.kafka.consumer_base import ConsumerBase


class Cocina(ConsumerBase):
    def __init__(self, demora_s=config.COCINA_DEMORA_S, preparar=time.sleep, **kw):
        super().__init__("cocina", config.GRUPOS_PEDIDO_CREADO["cocina"], **kw)
        self.demora_s = demora_s
        self._preparar = preparar     # inyectable para los tests
        self._pasos = {}              # idPedido -> último paso ya publicado (evita repetir al reintentar)

    def procesar(self, evento):
        id_pedido = evento["idPedido"]
        paso = self._pasos.get(id_pedido, 0)
        if paso < 1:
            self.publicar_estado(id_pedido, "PREPARACION", "EN_PREPARACION")
            self._pasos[id_pedido] = 1
            self.log.info("pedido %s EN_PREPARACION", id_pedido[:8])
        self._preparar(self.demora_s)           # simula la preparación (bloquea esta partición)
        self.publicar_estado(id_pedido, "PREPARACION", "LISTO")
        self._pasos.pop(id_pedido, None)
        self.log.info("pedido %s LISTO", id_pedido[:8])


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)-14s %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--demora", type=float, default=config.COCINA_DEMORA_S,
                    help="segundos simulados de preparación")
    Cocina(ap.parse_args().demora).iniciar()


if __name__ == "__main__":
    main()
