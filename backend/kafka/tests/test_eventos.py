# [IA]
# Revisado por: Carolina Fortmann
import re

import pytest

from .. import config
from ..eventos import (CombinacionInvalida, EventoInvalido, clave_idempotencia, clave_particion,
                       crear_evento_estado, crear_evento_pedido_creado, validar_evento)
from .fakes import pedido_api

CREADO, ESTADO = config.TOPIC_PEDIDO_CREADO, config.TOPIC_PEDIDO_ESTADO


def test_evento_creado_sigue_el_contrato():
    ev = crear_evento_pedido_creado(pedido_api("abc"))
    validar_evento(CREADO, ev)
    assert set(ev) == {"idEvento", "idPedido", "idEstudiante", "items", "total", "fecha"}
    assert all("subtotal" not in i for i in ev["items"])          # el evento no lleva subtotal
    assert ev["total"] == 450 and ev["fecha"] == "2026-11-13T15:04:05Z"
    assert clave_particion(ev) == "abc" == clave_idempotencia(CREADO, ev)


def test_total_que_no_cuadra_o_decimales_se_rechazan():
    p = pedido_api(); p["total"] = 999
    with pytest.raises(EventoInvalido):
        crear_evento_pedido_creado(p)
    p = pedido_api(); p["items"][0]["precioUnitario"] = 2.5       # dinero debe ser entero (centavos)
    with pytest.raises(EventoInvalido):
        crear_evento_pedido_creado(p)


def test_pedido_sin_campos_se_rechaza():
    with pytest.raises(EventoInvalido):
        crear_evento_pedido_creado({"idPedido": "x"})


def test_evento_estado_valido_y_su_clave_es_idEvento():
    ev = crear_evento_estado("abc", "PAGO", "PAGADO", "caja", {"idConfirmacion": "PAG-1", "monto": 450})
    validar_evento(ESTADO, ev)
    assert clave_idempotencia(ESTADO, ev) == ev["idEvento"]
    assert re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ", ev["fecha"])


@pytest.mark.parametrize("tipo,estado,origen", [
    ("PAGO", "LISTO", "caja"),                    # estado de otra dimensión
    ("PREPARACION", "LISTO", "caja"),             # origen sin autoridad
    ("ENTREGA", "ENTREGADO", "cocina"),
])
def test_combinaciones_invalidas_no_se_pueden_crear(tipo, estado, origen):
    with pytest.raises(CombinacionInvalida):
        crear_evento_estado("abc", tipo, estado, origen)


def test_al_consumir_combinacion_invalida_es_distinta_de_mal_formado():
    ev = crear_evento_estado("abc", "PREPARACION", "LISTO", "cocina")
    ev["origen"] = "caja"
    with pytest.raises(CombinacionInvalida):
        validar_evento(ESTADO, ev)
    del ev["idPedido"]
    with pytest.raises(EventoInvalido) as e:
        validar_evento(ESTADO, ev)
    assert not isinstance(e.value, CombinacionInvalida)
