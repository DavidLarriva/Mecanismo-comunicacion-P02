import logging
import time
from datetime import datetime, timezone
from uuid import uuid4

import grpc

from backend.grpc.caja_client import ClienteCaja
from backend.kafka import config as kafka_config
from backend.kafka.consumer_base import ConsumerBase
from contracts.grpc import caja_pb2


ESTADOS_PAGO = {
    caja_pb2.PAGADO: "PAGADO",
    caja_pb2.PENDIENTE: "PENDIENTE",
    caja_pb2.NO_ENCONTRADO: "NO_ENCONTRADO",
}


class ServicioFacturacion(ConsumerBase):
    def __init__(self, **opciones) -> None:
        super().__init__(
            servicio="facturacion",
            group_id=kafka_config.GRUPOS_PEDIDO_ESTADO["facturacion"],
            topic=kafka_config.TOPIC_PEDIDO_ESTADO,
            **opciones,
        )

        self.cliente_caja = ClienteCaja()
        self.facturas_emitidas: dict[str, str] = {}
        self.eventos_publicados: dict[tuple[str, str], dict] = {}

    def _crear_evento(
        self,
        id_pedido: str,
        estado: str,
        detalle: dict,
    ) -> dict:
        clave = (id_pedido, estado)

        # Conserva el mismo idEvento y contenido si Kafka requiere reintentar.
        if clave not in self.eventos_publicados:
            fecha = (
                datetime.now(timezone.utc)
                .isoformat(timespec="seconds")
                .replace("+00:00", "Z")
            )
            self.eventos_publicados[clave] = {
                "idEvento": str(uuid4()),
                "idPedido": id_pedido,
                "tipo": "FACTURACION",
                "estado": estado,
                "origen": "facturacion",
                "detalle": detalle,
                "fecha": fecha,
            }

        return self.eventos_publicados[clave]

    def _publicar_evento(self, evento: dict) -> None:
        self.producer.publicar(
            kafka_config.TOPIC_PEDIDO_ESTADO,
            evento["idPedido"],
            evento,
        )
# INICIO: fragmento elaborado con apoyo de IA y revisado por Dome.
    def procesar(self, evento: dict) -> None:
        if evento.get("tipo") != "PAGO" or evento.get("estado") != "PAGADO":
            self.log.info(
                "evento ignorado: tipo=%s estado=%s",
                evento.get("tipo"),
                evento.get("estado"),
            )
            return

        id_pedido = evento.get("idPedido")
        if not id_pedido:
            self.log.warning("evento PAGO/PAGADO sin idPedido; se ignora")
            return

        ultimo_error = "NO_ENCONTRADO"

        for intento in range(1, 7):
            try:
                respuesta = self.cliente_caja.confirmar_pago(id_pedido)
                estado_pago = ESTADOS_PAGO.get(
                    respuesta.estado,
                    "ESTADO_DESCONOCIDO",
                )

                if estado_pago == "PAGADO":
                    id_factura = self.facturas_emitidas.setdefault(
                        id_pedido,
                        f"FAC-{uuid4().hex[:12].upper()}",
                    )
                    evento_factura = self._crear_evento(
                        id_pedido,
                        "FACTURADO",
                        {"idFactura": id_factura},
                    )
                    self._publicar_evento(evento_factura)
                    self.log.info("pedido %s FACTURADO", id_pedido)
                    return

                ultimo_error = estado_pago
                self.log.warning(
                    "intento %s/6 para %s: Caja respondió %s; se reintentará",
                    intento,
                    id_pedido,
                    estado_pago,
                )

            except grpc.RpcError as error:
                ultimo_error = error.code().name
                self.log.warning(
                    "intento %s/6 para %s: gRPC respondió %s; se reintentará",
                    intento,
                    id_pedido,
                    ultimo_error,
                )

            if intento < 6:
                time.sleep(10)

        evento_factura = self._crear_evento(
            id_pedido,
            "FACTURACION_PENDIENTE",
            {
                "intentos": 6,
                "ultimoError": ultimo_error,
            },
        )
        self._publicar_evento(evento_factura)
        self.log.info(
            "pedido %s FACTURACION_PENDIENTE tras 6 intentos",
            id_pedido,
        )
    # FIN: fragmento elaborado con apoyo de IA.


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)-18s %(message)s",
    )
    servicio = ServicioFacturacion()
    try:
        servicio.iniciar()
    finally:
        servicio.cliente_caja.cerrar()


if __name__ == "__main__":
    main()
