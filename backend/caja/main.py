from contextlib import asynccontextmanager
from typing import Literal

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from backend.caja.almacen_pagos import almacen_pagos
from backend.caja.kafka_caja import ConsumidorPedidosCaja
from backend.grpc.caja_server import iniciar_servidor
from backend.kafka import config as kafka_config
from backend.kafka.producer import EventProducer, KafkaNoDisponible


class SolicitudPago(BaseModel):
    metodo: Literal["EFECTIVO", "TARJETA"] = "EFECTIVO"


def respuesta_pago(pago: dict) -> dict:
    return {
        "idPedido": pago["idPedido"],
        "estado": pago["estado"],
        "monto": pago["monto"],
        "metodo": pago["metodo"],
        "idConfirmacion": pago["idConfirmacion"],
        "fechaPago": pago["fechaPago"],
    }


@asynccontextmanager
async def ciclo_de_vida(app: FastAPI):
    # El productor compartido es perezoso: no conecta hasta publicar.
    app.state.productor = EventProducer()
    app.state.servidor_grpc = iniciar_servidor()

    app.state.consumidor_pedidos = ConsumidorPedidosCaja()
    app.state.hilo_consumidor = (
        app.state.consumidor_pedidos.iniciar_en_hilo()
    )

    try:
        yield
    finally:
        app.state.consumidor_pedidos.detener()
        app.state.hilo_consumidor.join(timeout=3)
        app.state.servidor_grpc.stop(grace=0).wait()


app = FastAPI(
    title="Caja CampusBite",
    lifespan=ciclo_de_vida,
)


@app.exception_handler(RequestValidationError)
async def manejar_error_validacion(
    request: Request,
    exc: RequestValidationError,
):
    return JSONResponse(
        status_code=400,
        content={
            "error": "DATOS_INVALIDOS",
            "mensaje": "El método debe ser EFECTIVO o TARJETA.",
        },
    )

# INICIO: fragmento elaborado con apoyo de IA y revisado por Dome.
@app.post("/caja/pagos/{id_pedido}")
def registrar_pago(
    id_pedido: str,
    solicitud: SolicitudPago | None = None,
):
    metodo = solicitud.metodo if solicitud else "EFECTIVO"
    pago = almacen_pagos.registrar_pago(id_pedido, metodo)

    if pago is None:
        return JSONResponse(
            status_code=404,
            content={
                "error": "COBRO_NO_ENCONTRADO",
                "mensaje": "Caja todavía no tiene el cobro de ese pedido.",
            },
        )

    evento = almacen_pagos.obtener_evento_pago(id_pedido)

    if evento is not None:
        try:
            app.state.productor.publicar(
                kafka_config.TOPIC_PEDIDO_ESTADO,
                id_pedido,
                evento,
            )
        except KafkaNoDisponible:
            return JSONResponse(
                status_code=503,
                content={
                    "error": "SERVICIO_NO_DISPONIBLE",
                    "mensaje": (
                        "El pago quedó registrado; reintenta la solicitud "
                        "para volver a publicar su evento."
                    ),
                },
            )

        almacen_pagos.marcar_evento_pago_publicado(id_pedido)

    return respuesta_pago(pago)


@app.get("/caja/pagos/{id_pedido}")
def consultar_pago(id_pedido: str):
    pago = almacen_pagos.obtener_pago(id_pedido)

    if pago is None:
        return JSONResponse(
            status_code=404,
            content={
                "error": "COBRO_NO_ENCONTRADO",
                "mensaje": "Caja todavía no tiene el cobro de ese pedido.",
            },
        )

    return respuesta_pago(pago)
# FIN: fragmento elaborado con apoyo de IA.