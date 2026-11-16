# [IA]
# Revisado por: Carolina Fortmann
from fastapi.testclient import TestClient

from backend.cocina.main import Cocina
from backend.estadisticas.main import crear_app
from backend.estadisticas.servicio import Estadisticas
from ..eventos import crear_evento_estado, crear_evento_pedido_creado
from .fakes import FakeConsumer, FakeEventProducer, FakeRecord, pedido_api


# ---------------- Cocina ----------------
def cocina(prod, preparar=lambda s: None):
    return Cocina(demora_s=0, preparar=preparar, producer=prod, consumer=FakeConsumer(), dormir=lambda s: None)


def test_cocina_publica_en_preparacion_y_luego_listo():
    prod = FakeEventProducer()
    c = cocina(prod)
    c._manejar_mensaje(FakeRecord(crear_evento_pedido_creado(pedido_api("p1")), offset=0))
    estados = [(v["tipo"], v["estado"], v["origen"]) for _, _, v in prod.publicados]
    assert estados == [("PREPARACION", "EN_PREPARACION", "cocina"), ("PREPARACION", "LISTO", "cocina")]


def test_si_cocina_falla_a_mitad_no_repite_en_preparacion():
    prod, intentos = FakeEventProducer(), {"n": 0}
    def preparar(_):
        intentos["n"] += 1
        if intentos["n"] == 1:
            raise RuntimeError("falla en medio de la preparación")
    c = cocina(prod, preparar)
    c._manejar_mensaje(FakeRecord(crear_evento_pedido_creado(pedido_api("p1")), offset=0))
    estados = [v["estado"] for _, _, v in prod.publicados]
    assert estados == ["EN_PREPARACION", "LISTO"]      # un solo EN_PREPARACION pese al reintento


# ---------------- Estadísticas ----------------
def creado(id_pedido, total_items):
    p = pedido_api(id_pedido)
    return crear_evento_pedido_creado(p)


def test_estadisticas_cuenta_y_suma_sin_importar_el_orden():
    e = Estadisticas()
    ev1, ev2 = creado("p1", None), creado("p2", None)
    e.registrar_estado(crear_evento_estado("p1", "ENTREGA", "ENTREGADO", "api"))   # llega ANTES que creado
    e.registrar_creado(ev1)
    e.registrar_creado(ev1)                                                        # duplicado
    e.registrar_creado(ev2)
    e.registrar_estado(crear_evento_estado("p1", "PAGO", "PAGADO", "caja"))
    e.registrar_estado(crear_evento_estado("p2", "PREPARACION", "LISTO", "cocina"))  # no cuenta
    r = e.resumen()
    assert r["pedidosRecibidos"] == 2 and r["pedidosPagados"] == 1 and r["pedidosEntregados"] == 1
    assert r["ventasEntregadas"] == 450
    jugo = next(p for p in r["porProducto"] if p["idProducto"] == "JUG-01")
    assert jugo == {"idProducto": "JUG-01", "nombre": "Jugo de naranja", "cantidad": 4, "total": 400}


def test_endpoint_get_estadisticas_y_cors():
    stats = Estadisticas()
    stats.registrar_creado(creado("p1", None))
    cliente = TestClient(crear_app(stats, arrancar_consumidores=False))
    r = cliente.get("/estadisticas", headers={"Origin": "http://localhost:3000"})
    assert r.status_code == 200 and r.json()["pedidosRecibidos"] == 1
    assert set(r.json()) == {"pedidosRecibidos", "pedidosPagados", "pedidosEntregados",
                             "ventasEntregadas", "porProducto", "actualizado"}
    assert r.headers["access-control-allow-origin"] == "*"
    assert cliente.get("/no-existe").json()["error"] == "NO_ENCONTRADO"
