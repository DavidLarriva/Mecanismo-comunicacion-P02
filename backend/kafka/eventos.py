"""Eventos según contratos.md v1.0 (secciones 1 y 5). JSON en camelCase, dinero en centavos.

pedido.creado  -> {idEvento, idPedido, idEstudiante, items[], total, fecha}
pedido.estado  -> {idEvento, idPedido, tipo, estado, origen, detalle, fecha}
El tipo de evento se reconoce por el TOPIC (el contrato no pone un campo 'tipo' en pedido.creado).
"""
import uuid
from datetime import datetime, timezone

from . import config

# (tipo, estado) -> origen permitido. Cualquier otra combinación se ignora (contrato, sección 1).
COMBINACIONES = {
    ("PREPARACION", "EN_PREPARACION"): "cocina",
    ("PREPARACION", "LISTO"): "cocina",
    ("PAGO", "PENDIENTE"): "caja",
    ("PAGO", "PAGADO"): "caja",
    ("FACTURACION", "FACTURADO"): "facturacion",
    ("FACTURACION", "FACTURACION_PENDIENTE"): "facturacion",
    ("ENTREGA", "ENTREGADO"): "api",
}


class EventoInvalido(ValueError):
    """El evento está mal formado (falta un campo, tipo incorrecto...). Va a la DLQ."""


class CombinacionInvalida(EventoInvalido):
    """Bien formado, pero (tipo, estado, origen) no es válido. El contrato dice: ignorar y registrar."""


def _ahora():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _es_entero(x):
    return isinstance(x, int) and not isinstance(x, bool)


def _texto(ev, campo):
    v = ev.get(campo)
    if not isinstance(v, str) or not v:
        raise EventoInvalido(f"falta el campo de texto '{campo}'")
    return v


# ---------------- pedido.creado ----------------
def validar_pedido_creado(ev):
    for campo in ("idEvento", "idPedido", "idEstudiante", "fecha"):
        _texto(ev, campo)
    items = ev.get("items")
    if not isinstance(items, list) or not items:
        raise EventoInvalido("items debe ser una lista no vacía")
    suma = 0
    for it in items:
        if not isinstance(it, dict):
            raise EventoInvalido("cada item debe ser un objeto")
        _texto(it, "idProducto")
        _texto(it, "nombre")
        if not _es_entero(it.get("cantidad")) or it["cantidad"] <= 0:
            raise EventoInvalido("cantidad debe ser un entero > 0")
        if not _es_entero(it.get("precioUnitario")) or it["precioUnitario"] < 0:
            raise EventoInvalido("precioUnitario debe ser un entero >= 0 (centavos)")
        suma += it["cantidad"] * it["precioUnitario"]
    if not _es_entero(ev.get("total")) or ev["total"] < 0:
        raise EventoInvalido("total debe ser un entero >= 0 (centavos)")
    if ev["total"] != suma:
        raise EventoInvalido(f"total ({ev['total']}) no coincide con la suma de items ({suma})")


def crear_evento_pedido_creado(pedido):
    """Construye el evento a partir del objeto pedido de la API (sección 2 del contrato)."""
    try:
        evento = {
            "idEvento": str(uuid.uuid4()),
            "idPedido": pedido["idPedido"],
            "idEstudiante": pedido["idEstudiante"],
            "items": [{"idProducto": i["idProducto"], "nombre": i["nombre"],
                       "cantidad": i["cantidad"], "precioUnitario": i["precioUnitario"]}
                      for i in pedido["items"]],
            "total": pedido["total"],
            "fecha": pedido.get("fechaCreacion") or _ahora(),
        }
    except (KeyError, TypeError) as e:
        raise EventoInvalido(f"el pedido no tiene la forma esperada: {e!r}") from e
    validar_pedido_creado(evento)
    return evento


# ---------------- pedido.estado ----------------
def validar_estado(ev):
    for campo in ("idEvento", "idPedido", "tipo", "estado", "origen", "fecha"):
        _texto(ev, campo)
    if not isinstance(ev.get("detalle"), dict):
        raise EventoInvalido("detalle debe ser un objeto")
    esperado = COMBINACIONES.get((ev["tipo"], ev["estado"]))
    if esperado is None or esperado != ev["origen"]:
        raise CombinacionInvalida(
            f"combinación no permitida: {ev['tipo']}/{ev['estado']} desde '{ev['origen']}'")


def crear_evento_estado(id_pedido, tipo, estado, origen, detalle=None):
    evento = {
        "idEvento": str(uuid.uuid4()),
        "idPedido": id_pedido,
        "tipo": tipo,
        "estado": estado,
        "origen": origen,
        "detalle": detalle or {},
        "fecha": _ahora(),
    }
    validar_estado(evento)
    return evento


# ---------------- comunes ----------------
def validar_evento(topic, ev):
    if not isinstance(ev, dict):
        raise EventoInvalido("el evento debe ser un objeto JSON")
    if topic == config.TOPIC_PEDIDO_CREADO:
        validar_pedido_creado(ev)
    elif topic == config.TOPIC_PEDIDO_ESTADO:
        validar_estado(ev)
    else:
        raise EventoInvalido(f"topic sin contrato: {topic}")


def clave_particion(evento):
    """Clave del mensaje Kafka = idPedido (contrato)."""
    return evento["idPedido"]


def clave_idempotencia(topic, evento):
    """Contrato: pedido.creado se deduplica por idPedido; pedido.estado por idEvento."""
    return evento["idPedido"] if topic == config.TOPIC_PEDIDO_CREADO else evento["idEvento"]
