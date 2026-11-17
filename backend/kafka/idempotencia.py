# [IA]
# Revisado por: Carolina Fortmann
"""Registro en memoria de claves ya procesadas (deduplicación del consumidor).

LIMITACIÓN (documentar en el ADR): vive en memoria; se pierde al reiniciar el proceso.
"""
from collections import OrderedDict

from . import config


class RegistroProcesados:
    def __init__(self, max_items=config.MAX_REGISTRO_PROCESADOS):
        self._vistos = OrderedDict()
        self._max = max_items

    def ya_procesado(self, clave):
        return clave in self._vistos

    def marcar(self, clave):
        self._vistos[clave] = True
        self._vistos.move_to_end(clave)
        if len(self._vistos) > self._max:
            self._vistos.popitem(last=False)

    def __len__(self):
        return len(self._vistos)
