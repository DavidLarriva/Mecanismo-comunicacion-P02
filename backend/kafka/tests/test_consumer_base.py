# [IA]
# Revisado por: Carolina Fortmann
from ..consumer_base import ConsumerBase
from ..eventos import crear_evento_estado, crear_evento_pedido_creado
from .fakes import FakeConsumer, FakeEventProducer, FakeRecord, pedido_api


class ServicioDePrueba(ConsumerBase):
    def __init__(self, fallos_antes_de_exito=0, siempre_falla=False, topic="pedido.creado", **kw):
        self.llamadas, self._fallos, self._siempre, self.esperas = 0, fallos_antes_de_exito, siempre_falla, []
        super().__init__("prueba", "grupo-prueba", topic, dormir=self.esperas.append, **kw)

    def procesar(self, evento):
        self.llamadas += 1
        if self._siempre or self.llamadas <= self._fallos:
            raise RuntimeError("fallo simulado")


def armar(**kw):
    prod, cons = FakeEventProducer(falla=kw.pop("dlq_falla", False)), FakeConsumer()
    return ServicioDePrueba(producer=prod, consumer=cons, **kw), prod, cons


def rec(id_pedido="p1", offset=0):
    return FakeRecord(crear_evento_pedido_creado(pedido_api(id_pedido)), key=id_pedido, offset=offset)


def test_exito_procesa_y_confirma_el_offset_siguiente():
    s, prod, cons = armar()
    s._manejar_mensaje(rec(offset=5))
    assert s.llamadas == 1 and cons.commits == [6] and prod.publicados == []


def test_duplicado_de_pedido_creado_se_ignora_pero_se_confirma():
    s, _, cons = armar()
    s._manejar_mensaje(rec("p1", offset=1))
    s._manejar_mensaje(rec("p1", offset=2))
    assert s.llamadas == 1 and cons.commits == [2, 3]


def test_en_pedido_estado_se_deduplica_por_idEvento():
    s, _, cons = armar(topic="pedido.estado")
    ev = crear_evento_estado("p1", "PREPARACION", "LISTO", "cocina")
    otro = crear_evento_estado("p1", "PREPARACION", "LISTO", "cocina")   # mismo cambio, otro idEvento
    for o, e in enumerate([ev, ev, otro]):
        s._manejar_mensaje(FakeRecord(e, topic="pedido.estado", offset=o))
    assert s.llamadas == 2          # el repetido (mismo idEvento) se ignora; el de otro idEvento no


def test_reintenta_con_espera_creciente_y_termina_bien():
    s, prod, cons = armar(fallos_antes_de_exito=2)
    s._manejar_mensaje(rec(offset=3))
    assert s.llamadas == 3 and s.esperas == [1, 2] and cons.commits == [4] and prod.publicados == []


def test_reintentos_agotados_van_a_dlq_y_se_confirma():
    s, prod, cons = armar(siempre_falla=True)
    s._manejar_mensaje(rec("p9", offset=4))
    topic, key, valor = prod.publicados[0]
    assert s.llamadas == 3 and topic == "pedido.dlq" and key == "p9" and valor["intentos"] == 3
    assert cons.commits == [5]


def test_mensaje_mal_formado_va_directo_a_dlq():
    s, prod, cons = armar()
    s._manejar_mensaje(FakeRecord("esto no es json", offset=8))
    assert s.llamadas == 0 and prod.publicados[0][0] == "pedido.dlq" and cons.commits == [9]


def test_combinacion_de_estado_invalida_se_ignora_sin_dlq():
    s, prod, cons = armar(topic="pedido.estado")
    ev = crear_evento_estado("p1", "PREPARACION", "LISTO", "cocina")
    ev["origen"] = "caja"
    s._manejar_mensaje(FakeRecord(ev, topic="pedido.estado", offset=2))
    assert s.llamadas == 0 and prod.publicados == [] and cons.commits == [3]


def test_si_la_dlq_falla_no_se_confirma_y_se_detiene():
    s, _, cons = armar(siempre_falla=True, dlq_falla=True)
    s._manejar_mensaje(rec(offset=2))
    assert cons.commits == [] and s.activo is False


def test_sin_confirmar_offsets_nunca_hace_commit():
    s, _, cons = armar(confirmar_offsets=False)
    s._manejar_mensaje(rec(offset=1))
    assert s.llamadas == 1 and cons.commits == []


def test_bucle_run_procesa_y_cierra():
    s, _, cons = armar()
    cons.pendientes = [rec("a", 0), rec("b", 1)]
    original = cons.poll
    def poll(timeout_ms=0):
        r = original(timeout_ms)
        if not r:
            s.detener()
        return r
    cons.poll = poll
    s.run()
    assert s.llamadas == 2 and cons.commits == [1, 2] and cons.cerrado
