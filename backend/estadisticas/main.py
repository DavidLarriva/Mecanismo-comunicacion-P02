"""API REST de Estadísticas (contrato sección 4, puerto 8002).

Ejecutar desde la raíz del repo:  python -m backend.estadisticas.main
(o:  uvicorn backend.estadisticas.main:app --port 8002)
"""
import logging
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from backend.kafka import config

from .servicio import ConsumidorCreados, ConsumidorEstados, Estadisticas


def crear_app(stats=None, arrancar_consumidores=True):
    stats = stats or Estadisticas()
    consumidores = []

    @asynccontextmanager
    async def lifespan(app):
        if arrancar_consumidores:           # Kafka es bloqueante: un hilo por consumidor
            for c in (ConsumidorCreados(stats), ConsumidorEstados(stats)):
                c.iniciar_en_hilo()
                consumidores.append(c)
        yield
        for c in consumidores:
            c.detener()

    app = FastAPI(title="Estadísticas CampusBite", lifespan=lifespan)
    app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

    @app.exception_handler(StarletteHTTPException)
    async def _http(_, exc):   # formato de error del contrato
        return JSONResponse({"error": "NO_ENCONTRADO" if exc.status_code == 404 else "ERROR",
                             "mensaje": str(exc.detail)}, status_code=exc.status_code)

    @app.exception_handler(RequestValidationError)
    async def _validacion(_, exc):   # contrato: 422 -> 400 DATOS_INVALIDOS
        return JSONResponse({"error": "DATOS_INVALIDOS", "mensaje": "Datos inválidos"}, status_code=400)

    @app.get("/estadisticas")
    def obtener():
        return stats.resumen()

    return app


app = crear_app()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)-14s %(message)s")
    uvicorn.run(app, host="0.0.0.0", port=config.PUERTO_ESTADISTICAS)
