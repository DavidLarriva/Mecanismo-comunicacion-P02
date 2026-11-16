# [IA]
# Revisado por: Carolina Fortmann
"""Clase base de los consumidores (kafka-python, bloqueante: corre en el hilo principal o en un hilo).

Política de entrega (at-least-once + deduplicación):
  1. commit del offset SOLO después de procesar con éxito (contrato).
  2. Se ignoran los duplicados: pedido.creado por idPedido, pedido.estado por idEvento (contrato).
  3. Si procesar() falla: hasta MAX_REINTENTOS con espera creciente (1 s, 2 s, 4 s...).
  4. Agotados los reintentos, o mensaje mal formado: topic DLQ + commit (no bloquea la partición).
  5. Si ni la DLQ responde: NO se hace commit y el consumidor se detiene (se reprocesa al reiniciar).
  6. Combinación de estado no válida: se ignora y se registra en el log (contrato, sección 1).
"""
import json
import logging
import signal
import threading
import time
from abc import ABC, abstractmethod

from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError
from kafka.structs import OffsetAndMetadata

from . import config
from .eventos import CombinacionInvalida, _ahora, clave_idempotencia, validar_evento
from .idempotencia import RegistroProcesados
from .producer import EventProducer, KafkaNoDisponible


class ConsumerBase(ABC):
    def __init__(self, servicio, group_id, topic=config.TOPIC_PEDIDO_CREADO, *,
                 producer=None, consumer=None, registro=None,
                 max_reintentos=config.MAX_REINTENTOS, backoff_base_s=config.BACKOFF_BASE_S,
                 dormir=time.sleep, confirmar_offsets=True):
        self.servicio, self.group_id, self.topic = servicio, group_id, topic
        self.max_reintentos, self.backoff_base_s = max_reintentos, backoff_base_s
        self.confirmar_offsets = confirmar_offsets   # False: no hace commit => al reiniciar relee todo (replay)
        self._dormir = dormir
        self.log = logging.getLogger(f"kafka.{servicio}")
        self.producer = producer or EventProducer()
        self.registro = registro or RegistroProcesados()
        self._consumer = consumer       # si es None se crea dentro de run() (en el hilo que lo usará)
        self._parar = threading.Event()
        self._hilo = None

    # ---- lo que implementa cada servicio ----
    @abstractmethod
    def procesar(self, evento):
        """Lógica del servicio. Lanzar una excepción provoca un reintento."""

    def publicar_estado(self, id_pedido, tipo, estado, detalle=None):
        return self.producer.publicar_estado(id_pedido, tipo, estado, self.servicio, detalle)

    # ---- ciclo de vida ----
    @property
    def activo(self):
        return not self._parar.is_set()

    def detener(self):
        self._parar.set()

    def iniciar(self):
        """Hilo principal (scripts): Ctrl+C detiene de forma ordenada."""
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                signal.signal(sig, lambda *_: self.detener())
            except (ValueError, OSError):
                pass
        self.run()

    def iniciar_en_hilo(self):
        """Para servicios con servidor propio (FastAPI, WebSocket): consume en un hilo aparte."""
        self._hilo = threading.Thread(target=self.run, name=f"consumer-{self.servicio}", daemon=True)
        self._hilo.start()
        return self._hilo

    def _crear_consumer(self):
        return KafkaConsumer(
            self.topic,
            bootstrap_servers=config.BOOTSTRAP_SERVERS,
            group_id=self.group_id,
            enable_auto_commit=False,        # commit manual
            auto_offset_reset="earliest",
            session_timeout_ms=10000,        # sin latidos 10 s => se le quitan sus particiones
            heartbeat_interval_ms=3000,
            bootstrap_timeout_ms=5000,
            max_poll_records=10,
        )

    def run(self):
        while self.activo and self._consumer is None:      # espera a que Kafka esté disponible
            try:
                self._consumer = self._crear_consumer()
            except KafkaError as e:
                self.log.warning("Kafka no disponible (%r); reintento en 5 s", e)
                self._parar.wait(5)
        if not self.activo:
            return
        self.log.info("escuchando '%s' con group.id='%s'", self.topic, self.group_id)
        try:
            while self.activo:
                lotes = self._consumer.poll(timeout_ms=1000)
                for registros in lotes.values():
                    for registro in registros:
                        if not self.activo:
                            break
                        self._manejar_mensaje(registro)
        except Exception:
            self.log.exception("el consumidor terminó por un error inesperado")
        finally:
            try:
                self._consumer.close()
            finally:
                self.log.info("consumidor cerrado")

    # ---- manejo de un mensaje ----
    def _manejar_mensaje(self, rec):
        crudo = rec.value.decode("utf-8", errors="replace") if rec.value else ""
        try:
            evento = json.loads(crudo)
            validar_evento(self.topic, evento)
        except CombinacionInvalida as e:
            self.log.warning("evento ignorado: %s", e)
            self._confirmar(rec)
            return
        except ValueError as e:       # JSON roto o evento mal formado
            self.log.error("mensaje inválido (%s) -> DLQ", e)
            self._enviar_a_dlq(rec, crudo, f"mensaje inválido: {e}", 0)
            return

        clave = clave_idempotencia(self.topic, evento)
        if self.registro.ya_procesado(clave):
            self.log.info("duplicado ignorado: %s", clave)
            self._confirmar(rec)
            return

        ultimo_error = None
        for intento in range(1, self.max_reintentos + 1):
            try:
                self.procesar(evento)
            except Exception as e:
                ultimo_error = e
                self.log.warning("fallo en intento %d/%d: %r", intento, self.max_reintentos, e)
                if intento < self.max_reintentos:
                    self._dormir(self.backoff_base_s * 2 ** (intento - 1))
            else:
                self.registro.marcar(clave)
                self._confirmar(rec)
                return
        self.log.error("reintentos agotados para %s -> DLQ", clave)
        self._enviar_a_dlq(rec, crudo, repr(ultimo_error), self.max_reintentos)

    def _confirmar(self, rec):
        if not self.confirmar_offsets:
            return
        tp = TopicPartition(rec.topic, rec.partition)
        try:
            # el offset que se confirma es el del SIGUIENTE mensaje a leer
            self._consumer.commit({tp: OffsetAndMetadata(rec.offset + 1, "", -1)})
        except KafkaError as e:
            self.log.error("no se pudo confirmar el offset: %r (se reentregará y se deduplicará)", e)

    def _enviar_a_dlq(self, rec, crudo, motivo, intentos):
        valor = {"tipo": "pedido.dlq", "fecha": _ahora(), "servicio": self.servicio,
                 "motivo": motivo, "intentos": intentos, "topicOrigen": rec.topic,
                 "particion": rec.partition, "offset": rec.offset, "mensajeOriginal": crudo}
        clave = rec.key.decode("utf-8", errors="replace") if rec.key else None
        try:
            self.producer.publicar(config.TOPIC_DLQ, clave, valor)
        except KafkaNoDisponible as e:
            self.log.critical("no se pudo escribir en la DLQ (%s): me detengo SIN confirmar el offset "
                              "para no perder el mensaje", e)
            self.detener()
            return
        self._confirmar(rec)
