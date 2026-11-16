import json
import logging
import os
import time
from datetime import datetime, timezone
from uuid import uuid4

import grpc
from kafka import KafkaConsumer, KafkaProducer

from backend.grpc.caja_client import ClienteCaja
from contracts.grpc import caja_pb2


BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)

ESTADOS_PAGO = {
    caja_pb2.PAGADO: "PAGADO",
    caja_pb2.PENDIENTE: "PENDIENTE",
    caja_pb2.NO_ENCONTRADO: "NO_ENCONTRADO",
}


class ServicioFacturacion:
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

        self.cliente_caja = ClienteCaja()
        self.facturas_emitidas: dict[str, str] = {}
        self.pedidos_procesados: set[str] = set()
        self.ids_evento: dict[tuple[str, str], str] = {}

    def publicar_estado(self, evento: dict) -> None:
        resultado = self.productor.send(
            "pedido.estado",
            key=evento["idPedido"],
            value=evento,
        )
        resultado.get(timeout=5)

    def crear_evento(
        self,
        id_pedido: str,
        estado: str,
        detalle: dict,
    ) -> dict:
        clave = (id_pedido, estado)

        if clave not in self.ids_evento:
            self.ids_evento[clave] = str(uuid4())

        fecha = (
            datetime.now(timezone.utc)
            .isoformat(timespec="seconds")
            .replace("+00:00", "Z")
        )

        return {
            "idEvento": self.ids_evento[clave],
            "idPedido": id_pedido,
            "tipo": "FACTURACION",
            "estado": estado,
            "origen": "facturacion",
            "detalle": detalle,
            "fecha": fecha,
        }

    def procesar_pedido(self, pedido: dict) -> None:
        id_pedido = pedido.get("idPedido")

        if not id_pedido or id_pedido in self.pedidos_procesados:
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

                    evento = self.crear_evento(
                        id_pedido,
                        "FACTURADO",
                        {"idFactura": id_factura},
                    )
                    self.publicar_estado(evento)
                    self.pedidos_procesados.add(id_pedido)
                    return

                ultimo_error = estado_pago

            except grpc.RpcError as error:
                ultimo_error = error.code().name
                logging.warning(
                    "Intento %s para el pedido %s: %s",
                    intento,
                    id_pedido,
                    ultimo_error,
                )

            if intento < 6:
                time.sleep(10)

        evento = self.crear_evento(
            id_pedido,
            "FACTURACION_PENDIENTE",
            {
                "intentos": 6,
                "ultimoError": ultimo_error,
            },
        )
        self.publicar_estado(evento)
        self.pedidos_procesados.add(id_pedido)

    def ejecutar(self) -> None:
        consumidor = KafkaConsumer(
            "pedido.creado",
            bootstrap_servers=BOOTSTRAP_SERVERS,
            group_id="facturacion",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            value_deserializer=lambda valor: json.loads(
                valor.decode("utf-8")
            ),
        )

        try:
            for mensaje in consumidor:
                try:
                    self.procesar_pedido(mensaje.value)
                    consumidor.commit()
                except Exception:
                    logging.exception(
                        "No se pudo procesar el pedido; "
                        "se intentará nuevamente"
                    )
                    consumidor.seek(
                        mensaje.topic_partition,
                        mensaje.offset,
                    )
                    time.sleep(3)
        finally:
            consumidor.close()
            self.cliente_caja.cerrar()
            self.productor.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    ServicioFacturacion().ejecutar()