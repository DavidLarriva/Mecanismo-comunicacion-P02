import json
import logging
import threading

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

from errores import ErrorPublicacion

log = logging.getLogger("api.kafka")


class PublicadorKafka:
    def __init__(self, servidores, timeout_s):
        self._servidores = servidores
        self._timeout_s = timeout_s
        self._lock = threading.Lock()
        self._productor = None

    def publicar(self, topic, clave, valor):
        try:
            productor = self._obtener_productor()
            futuro = productor.send(topic, key=clave, value=valor)
            metadatos = futuro.get(timeout=self._timeout_s)
        except KafkaError as error:
            raise ErrorPublicacion(str(error)) from error
        log.info(
            "Publicado en %s clave=%s particion=%s offset=%s",
            topic, clave, metadatos.partition, metadatos.offset,
        )
        return metadatos

    def cerrar(self):
        with self._lock:
            if self._productor is not None:
                self._productor.close(timeout=2)
                self._productor = None

    def _obtener_productor(self):
        with self._lock:
            if self._productor is None:
                milisegundos = int(self._timeout_s * 1000)
                self._productor = KafkaProducer(
                    bootstrap_servers=self._servidores,
                    acks="all",
                    key_serializer=lambda clave: clave.encode("utf-8"),
                    value_serializer=lambda valor: json.dumps(valor, ensure_ascii=False).encode("utf-8"),
                    bootstrap_timeout_ms=milisegundos,
                    max_block_ms=milisegundos,
                    request_timeout_ms=milisegundos,
                    delivery_timeout_ms=milisegundos,
                )
                log.info("Productor Kafka conectado a %s", self._servidores)
            return self._productor


class ConsumidorEstados(threading.Thread):
    def __init__(self, servidores, topic, grupo, repositorio, espera_reintento_s):
        super().__init__(name="consumidor-pedido-estado", daemon=True)
        self._servidores = servidores
        self._topic = topic
        self._grupo = grupo
        self._repositorio = repositorio
        self._espera_reintento_s = espera_reintento_s
        self._detener = threading.Event()
        self._consumidor = None

    def detener(self):
        self._detener.set()
        self.join(timeout=10)

    def run(self):
        while not self._detener.is_set():
            try:
                if self._consumidor is None:
                    self._consumidor = self._crear_consumidor()
                registros = self._consumidor.poll(timeout_ms=1000)
                for mensajes in registros.values():
                    for mensaje in mensajes:
                        self._procesar(mensaje)
                if registros:
                    self._consumidor.commit()
            except KafkaError as error:
                log.warning("Consumidor de %s sin conexión (%s); reintento en %ss",
                            self._topic, error, self._espera_reintento_s)
                self._cerrar_consumidor()
                self._detener.wait(self._espera_reintento_s)
        self._cerrar_consumidor()

    def _crear_consumidor(self):
        consumidor = KafkaConsumer(
            self._topic,
            bootstrap_servers=self._servidores,
            group_id=self._grupo,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            bootstrap_timeout_ms=5000,
        )
        log.info("Consumidor conectado a %s (grupo %s)", self._topic, self._grupo)
        return consumidor

    def _procesar(self, mensaje):
        try:
            evento = json.loads(mensaje.value.decode("utf-8"))
        except (UnicodeDecodeError, ValueError):
            log.warning("Mensaje ilegible en %s offset=%s; se descarta", self._topic, mensaje.offset)
            return
        resultado = self._repositorio.aplicar_evento(evento)
        log.info(
            "Evento %s/%s del pedido %s: %s",
            evento.get("tipo"), evento.get("estado"), evento.get("idPedido"), resultado,
        )

    def _cerrar_consumidor(self):
        if self._consumidor is not None:
            try:
                self._consumidor.close()
            except KafkaError:
                pass
            self._consumidor = None
