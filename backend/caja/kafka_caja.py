from backend.caja.almacen_pagos import almacen_pagos
from backend.kafka import config as kafka_config
from backend.kafka.consumer_base import ConsumerBase


class ConsumidorPedidosCaja(ConsumerBase):
    def __init__(self, **opciones) -> None:
        super().__init__(
            servicio="caja",
            group_id=kafka_config.GRUPOS_PEDIDO_CREADO["caja"],
            topic=kafka_config.TOPIC_PEDIDO_CREADO,
            **opciones,
        )
# INICIO: fragmento elaborado con apoyo de IA y revisado por Dome.
    def procesar(self, evento: dict) -> None:
        id_pedido = evento["idPedido"]

        # Si llega repetido, el almacén conserva el cobro existente.
        almacen_pagos.registrar_pedido(evento)

        evento_pendiente = almacen_pagos.obtener_evento_pendiente(id_pedido)
        if evento_pendiente is None:
            return

        # Usa el productor común de ConsumerBase; espera el ack de Kafka.
        self.producer.publicar(
            kafka_config.TOPIC_PEDIDO_ESTADO,
            id_pedido,
            evento_pendiente,
        )

        almacen_pagos.marcar_evento_pendiente_publicado(id_pedido)
# FIN: fragmento elaborado con apoyo de IA.