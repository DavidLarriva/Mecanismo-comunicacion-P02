import logging
from contextlib import asynccontextmanager
from typing import Annotated

import uvicorn
from fastapi import FastAPI, Header, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

import config
from errores import ErrorApi
from idempotencia import RegistroIdempotencia
from mensajeria import ConsumidorEstados, PublicadorKafka
from menu import MENU
from modelos import Error, Pedido, Producto, SolicitudPedido
from repositorio import RepositorioPedidos
from servicio import ServicioPedidos

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logging.getLogger("kafka").setLevel(logging.WARNING)

RESPUESTAS_ERROR = {
    400: {"model": Error},
    404: {"model": Error},
    409: {"model": Error},
    503: {"model": Error},
}


def respuesta_error(estado_http, codigo, mensaje):
    return JSONResponse(status_code=estado_http, content={"error": codigo, "mensaje": mensaje})


def crear_app(servicio, publicador=None, consumidor=None):
    @asynccontextmanager
    async def ciclo_de_vida(app):
        if consumidor is not None:
            consumidor.start()
        yield
        if consumidor is not None:
            consumidor.detener()
        if publicador is not None:
            publicador.cerrar()

    app = FastAPI(title="CampusBite - API de Pedidos", version="1.1", lifespan=ciclo_de_vida)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.exception_handler(ErrorApi)
    async def manejar_error_api(request: Request, error: ErrorApi):
        return respuesta_error(error.estado_http, error.codigo, error.mensaje)

    @app.exception_handler(RequestValidationError)
    async def manejar_validacion(request: Request, error: RequestValidationError):
        errores = error.errors()
        if any(detalle["loc"][:1] == ("header",) for detalle in errores):
            return respuesta_error(400, "FALTA_IDEMPOTENCY_KEY", "Falta la cabecera Idempotency-Key")
        detalle = errores[0]
        campo = ".".join(str(parte) for parte in detalle["loc"][1:]) or "cuerpo"
        return respuesta_error(400, "DATOS_INVALIDOS", f"{campo}: {detalle['msg']}")

    @app.get("/menu", response_model=list[Producto])
    def ver_menu():
        return servicio.menu()

    @app.post("/pedidos", status_code=201, response_model=Pedido,
              responses={200: {"model": Pedido}, **RESPUESTAS_ERROR})
    def crear_pedido(
        solicitud: SolicitudPedido,
        response: Response,
        idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=200)],
    ):
        pedido, es_nuevo = servicio.crear(idempotency_key, solicitud)
        if not es_nuevo:
            response.status_code = 200
        return pedido

    @app.get("/pedidos/{id_pedido}", response_model=Pedido, responses={404: {"model": Error}})
    def ver_pedido(id_pedido: str):
        return servicio.obtener(id_pedido)

    @app.patch("/pedidos/{id_pedido}/entregar", response_model=Pedido, responses=RESPUESTAS_ERROR)
    def entregar_pedido(id_pedido: str):
        return servicio.entregar(id_pedido)

    return app


repositorio = RepositorioPedidos()
publicador = PublicadorKafka(config.KAFKA_SERVIDORES, config.TIMEOUT_KAFKA_S)
consumidor = ConsumidorEstados(
    config.KAFKA_SERVIDORES,
    config.TOPIC_PEDIDO_ESTADO,
    config.GRUPO_CONSUMIDOR,
    repositorio,
    config.REINTENTO_CONSUMIDOR_S,
)
servicio = ServicioPedidos(
    repositorio,
    RegistroIdempotencia(config.VENCIMIENTO_CLAVE_S),
    publicador,
    MENU,
)
app = crear_app(servicio, publicador, consumidor)

if __name__ == "__main__":
    uvicorn.run(app, host=config.HOST, port=config.PUERTO)
