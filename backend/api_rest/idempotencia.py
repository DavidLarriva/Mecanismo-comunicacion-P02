import copy
import threading
import time
import uuid
from dataclasses import dataclass

EN_PROCESO = "EN_PROCESO"
COMPLETADA = "COMPLETADA"

NUEVA = "NUEVA"
EN_CURSO = "EN_CURSO"
REUTILIZADA = "REUTILIZADA"
REPETIDA = "REPETIDA"


@dataclass
class Entrada:
    estado: str
    huella: str
    token: str
    creada: float
    respuesta: dict | None = None


@dataclass
class Reserva:
    resultado: str
    token: str | None = None
    respuesta: dict | None = None


class RegistroIdempotencia:
    def __init__(self, vencimiento_s, reloj=time.monotonic):
        self._vencimiento_s = vencimiento_s
        self._reloj = reloj
        self._lock = threading.Lock()
        self._claves = {}

    def reservar(self, clave, huella):
        with self._lock:
            entrada = self._claves.get(clave)
            if entrada and entrada.estado == EN_PROCESO and self._vencida(entrada):
                del self._claves[clave]
                entrada = None
            if entrada is None:
                token = str(uuid.uuid4())
                self._claves[clave] = Entrada(EN_PROCESO, huella, token, self._reloj())
                return Reserva(NUEVA, token=token)
            if entrada.huella != huella:
                return Reserva(REUTILIZADA)
            if entrada.estado == EN_PROCESO:
                return Reserva(EN_CURSO)
            return Reserva(REPETIDA, respuesta=copy.deepcopy(entrada.respuesta))

    def completar(self, clave, token, respuesta):
        with self._lock:
            entrada = self._claves.get(clave)
            if entrada and entrada.token == token:
                entrada.estado = COMPLETADA
                entrada.respuesta = copy.deepcopy(respuesta)

    def liberar(self, clave, token):
        with self._lock:
            entrada = self._claves.get(clave)
            if entrada and entrada.token == token and entrada.estado == EN_PROCESO:
                del self._claves[clave]

    def estado(self, clave):
        with self._lock:
            entrada = self._claves.get(clave)
            return entrada.estado if entrada else None

    def _vencida(self, entrada):
        return self._reloj() - entrada.creada > self._vencimiento_s
