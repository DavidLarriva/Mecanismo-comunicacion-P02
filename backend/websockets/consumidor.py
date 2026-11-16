"""Consumidor de 'pedido.estado' para Notificaciones (grupo 'notificaciones').

Reutiliza ConsumerBase de Caro (backend/kafka/consumer_base.py), que ya hace:
  - commit manual del offset DESPUÉS de procesar (enable_auto_commit=False),
  - deduplicación por idEvento (Kafka entrega "al menos una vez"),
  - ignorar y registrar combinaciones tipo/estado/origen que no están en el contrato.

Problema a resolver: kafka-python es BLOQUEANTE y el servidor WebSocket corre en el loop de
asyncio. Si el consumidor corriera en el loop, lo congelaría y nadie podría conectarse.
Solución: el consumidor corre en un hilo aparte y cada evento se entrega al loop con
asyncio.run_coroutine_threadsafe(...), que es la forma segura de llamar a asyncio desde otro hilo.
"""
# Generado con IA (bitácora #1, Claude), revisado y adaptado por Cinthy.
import asyncio

from backend.kafka import config
from backend.kafka.consumer_base import ConsumerBase

TIMEOUT_ENVIO_S = 5


class ConsumidorNotificaciones(ConsumerBase):
    def __init__(self, notificador, loop, **kw):
        super().__init__("notificaciones", config.GRUPOS_PEDIDO_ESTADO["notificaciones"],
                         config.TOPIC_PEDIDO_ESTADO, **kw)
        self.notificador = notificador
        self.loop = loop

    def procesar(self, evento):
        # Este método corre en el hilo del consumidor. Se entrega el evento al loop de asyncio
        # y se ESPERA a que termine el envío: así el commit del offset (que hace ConsumerBase
        # al volver de aquí) ocurre después de notificar, como pide el contrato.
        futuro = asyncio.run_coroutine_threadsafe(self.notificador.notificar(evento), self.loop)
        futuro.result(timeout=TIMEOUT_ENVIO_S)
