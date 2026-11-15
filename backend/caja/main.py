from contextlib import asynccontextmanager
from datetime import datetime, timezone
from threading import Event, Thread
from typing import Literal

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from backend.caja.almacen_pagos import almacen_pagos
from backend.caja.kafka_caja import KafkaCaja, consumir_pedidos_creados
from backend.grpc.caja_server import iniciar_servidor


class SolicitudPago(BaseModel):
    metodo: Literal["EFECTIVO", "TARJETA"] = "EFECTIVO"


def respuesta_pago(pago: dict) -> dict:
    """Devuelve solo los campos públicos del contrato REST."""
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
    app.state.kafka = KafkaCaja()
    app.state.detener_consumidor = Event()
    app.state.servidor_grpc = iniciar_servidor()

    app.state.hilo_consumidor = Thread(
        target=consumir_pedidos_creados,
        args=(app.state.detener_consumidor,),
        daemon=True,
    )
    app.state.hilo_consumidor.start()

    try:
        yield
    finally:
        app.state.detener_consumidor.set()
        app.state.hilo_consumidor.join(timeout=3)
        app.state.servidor_grpc.stop(grace=0).wait()
        app.state.kafka.cerrar()


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

    if not pago["eventoPagoPublicado"]:
        evento = {
            "idEvento": pago["idEventoPago"],
            "idPedido": pago["idPedido"],
            "tipo": "PAGO",
            "estado": "PAGADO",
            "origen": "caja",
            "detalle": {
                "idConfirmacion": pago["idConfirmacion"],
                "monto": pago["monto"],
            },
            "fecha": (
                datetime.now(timezone.utc)
                .isoformat(timespec="seconds")
                .replace("+00:00", "Z")
            ),
        }

        try:
            app.state.kafka.publicar_estado_pago(evento)
            almacen_pagos.marcar_evento_pago_publicado(id_pedido)
        except Exception:
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