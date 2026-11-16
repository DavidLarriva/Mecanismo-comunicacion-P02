# [IA]
# Revisado por: Carolina Fortmann
"""Productor Kafka (kafka-python). Interfaz usada por la API y los demás servicios.

Contrato: acks='all' y se espera la confirmación con future.get(timeout=5).
Si no hay confirmación se lanza KafkaNoDisponible -> la API responde 503 y no registra el pedido.

Limitaciones (para el ADR):
  - kafka-python NO tiene productor idempotente (enable.idempotence). Un reintento interno de red
    podría duplicar un mensaje; por eso los consumidores deduplican.
  - Si salta el timeout, el mensaje PODRÍA haber llegado igual al broker (falló la confirmación,
    no necesariamente la escritura). Mismo motivo: consumidores idempotentes.
"""
import json
import logging
import threading

from kafka import KafkaProducer
from kafka.errors import KafkaError

from . import config
from .eventos import clave_particion, crear_evento_estado, crear_evento_pedido_creado

log = logging.getLogger("kafka.producer")


class KafkaNoDisponible(Exception):
    """No se pudo confirmar la publicación en Kafka."""


class EventProducer:
    def __init__(self, bootstrap_servers=config.BOOTSTRAP_SERVERS,
                 timeout_s=config.PRODUCER_TIMEOUT_S, producer=None):
        self._bootstrap = bootstrap_servers
        self._timeout_s = timeout_s
        self._producer = producer          # se crea la primera vez que se usa (no al importar)
        self._lock = threading.Lock()

    def _conseguir_productor(self):
        with self._lock:
            if self._producer is None:
                ms = int(self._timeout_s * 1000)
                try:
                    self._producer = KafkaProducer(
                        bootstrap_servers=self._bootstrap,
                        acks="all",
                        retries=3,
                        max_in_flight_requests_per_connection=1,   # conserva el orden al reintentar
                        bootstrap_timeout_ms=ms,                   # sin esto, el constructor espera 30 s
                        max_block_ms=ms,
                        request_timeout_ms=ms,
                        delivery_timeout_ms=ms,
                    )
                except KafkaError as e:
                    raise KafkaNoDisponible(f"no hay conexión con Kafka: {e!r}") from e
            return self._producer

    def publicar(self, topic, key, valor):
        """Publica un dict como JSON y espera la confirmación. Devuelve (particion, offset)."""
        productor = self._conseguir_productor()
        try:
            futuro = productor.send(
                topic,
                key=key.encode("utf-8") if key else None,
                value=json.dumps(valor, ensure_ascii=False).encode("utf-8"),
            )
            meta = futuro.get(timeout=self._timeout_s)
        except KafkaError as e:   # incluye KafkaTimeoutError
            raise KafkaNoDisponible(f"Kafka no confirmó la publicación: {e!r}") from e
        log.info("publicado en %s [p%s o%s] key=%s", topic, meta.partition, meta.offset, key)
        return meta.partition, meta.offset

    def publicar_pedido_creado(self, pedido):
        evento = crear_evento_pedido_creado(pedido)
        return self.publicar(config.TOPIC_PEDIDO_CREADO, clave_particion(evento), evento)

    def publicar_estado(self, id_pedido, tipo, estado, origen, detalle=None):
        evento = crear_evento_estado(id_pedido, tipo, estado, origen, detalle)
        return self.publicar(config.TOPIC_PEDIDO_ESTADO, clave_particion(evento), evento)


# --- Interfaz de módulo: un productor compartido por proceso ---
_productor = EventProducer()


def publicar_pedido_creado(pedido):
    """API (David): pasa el objeto pedido de la sección 2 del contrato.
    Si lanza KafkaNoDisponible -> responder 503 SERVICIO_NO_DISPONIBLE y no registrar el pedido."""
    return _productor.publicar_pedido_creado(pedido)


def publicar_estado(id_pedido, tipo, estado, origen, detalle=None):
    """tipo: PREPARACION | PAGO | FACTURACION | ENTREGA (ver tabla de combinaciones del contrato)."""
    return _productor.publicar_estado(id_pedido, tipo, estado, origen, detalle)
