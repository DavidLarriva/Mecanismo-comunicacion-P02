import json
import logging
import os
from threading import Event

from kafka import KafkaConsumer, KafkaProducer

from backend.caja.almacen_pagos import almacen_pagos


BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)


class KafkaCaja:
    def __init__(self) -> None:
        self.productor = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            acks="all",
            retries=3,
            key_serializer=lambda clave: (
                clave.encode("utf-8") if clave is not None else None
            ),
            value_serializer=lambda valor: json.dumps(
                valor,
                ensure_ascii=False,
            ).encode("utf-8"),
        )

    def publicar_estado_pago(self, evento: dict) -> None:
        resultado = self.productor.send(
            "pedido.estado",
            key=evento["idPedido"],
            value=evento,
        )
        resultado.get(timeout=5)

    def cerrar(self) -> None:
        self.productor.close()


def consumir_pedidos_creados(detener: Event) -> None:
    """Crea el cobro PENDIENTE cuando llega pedido.creado."""
    while not detener.is_set():
        consumidor = None

        try:
            consumidor = KafkaConsumer(
                "pedido.creado",
                bootstrap_servers=BOOTSTRAP_SERVERS,
                group_id="caja",
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                key_deserializer=lambda clave: (
                    clave.decode("utf-8") if clave else None
                ),
                value_deserializer=lambda valor: json.loads(
                    valor.decode("utf-8")
                ),
            )

            while not detener.is_set():
                mensajes_por_particion = consumidor.poll(timeout_ms=1000)

                for mensajes in mensajes_por_particion.values():
                    for mensaje in mensajes:
                        almacen_pagos.registrar_pedido(mensaje.value)
                        consumidor.commit()

        except Exception:
            logging.exception(
                "Error en el consumidor de pedido.creado; "
                "se intentará conectar de nuevo"
            )
            detener.wait(3)

        finally:
            if consumidor is not None:
                consumidor.close()