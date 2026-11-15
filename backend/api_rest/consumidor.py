from backend.kafka import config as kafka_config
from backend.kafka.consumer_base import ConsumerBase


class ConsumidorEstadosApi(ConsumerBase):
    def __init__(self, repositorio, grupo, **opciones):
        super().__init__("api", grupo, kafka_config.TOPIC_PEDIDO_ESTADO, **opciones)
        self.repositorio = repositorio

    def procesar(self, evento):
        resultado = self.repositorio.aplicar_evento(evento)
        self.log.info(
            "%s/%s del pedido %s: %s",
            evento["tipo"], evento["estado"], evento["idPedido"], resultado,
        )
        return resultado
