"""Pruebas unitarias del Notificador (sin Kafka): python -m pytest backend/websockets -q"""
# Generado con IA (bitácora #3, Claude), revisado y adaptado por Cinthy.
import asyncio
import json

from websockets.exceptions import ConnectionClosedError

from backend.websockets.servidor import Notificador


class ConexionFalsa:
    def __init__(self, nombre, rota=False):
        self.remote_address = nombre
        self.rota = rota
        self.enviados = []

    async def send(self, texto):
        if self.rota:
            raise ConnectionClosedError(None, None)
        self.enviados.append(json.loads(texto))


def evento(id_pedido="p1", estado="LISTO"):
    return {"idEvento": "e1", "idPedido": id_pedido, "tipo": "PREPARACION", "estado": estado,
            "origen": "cocina", "detalle": {}, "fecha": "2026-11-15T20:00:00Z"}


def correr(coro):
    return asyncio.run(coro)


def test_solo_recibe_quien_esta_suscrito_a_ese_pedido():
    n, a, b = Notificador(), ConexionFalsa("a"), ConexionFalsa("b")
    n.suscribir(a, "p1")
    n.suscribir(b, "p2")
    correr(n.notificar(evento("p1")))
    assert a.enviados == [{"tipo": "estado", "evento": evento("p1")}]   # sin modificar el evento
    assert b.enviados == []


def test_evento_sin_clientes_se_descarta():
    assert correr(Notificador().notificar(evento())) == 0


def test_conexion_rota_se_elimina_y_las_demas_reciben():
    n, rota, sana = Notificador(), ConexionFalsa("rota", rota=True), ConexionFalsa("sana")
    n.suscribir(rota, "p1")
    n.suscribir(sana, "p1")
    assert correr(n.notificar(evento())) == 1
    assert n.suscripciones["p1"] == {sana}


def test_mensaje_invalido_responde_error_sin_cerrar():
    n, a = Notificador(), ConexionFalsa("a")
    for crudo in ["no es json", "{}", '{"accion": "suscribir"}', '{"accion": "bailar", "idPedido": "p1"}']:
        correr(n._procesar_mensaje(a, crudo))
    assert [m["tipo"] for m in a.enviados] == ["error"] * 4
    correr(n._procesar_mensaje(a, '{"accion": "suscribir", "idPedido": "p1"}'))
    assert a.enviados[-1] == {"tipo": "suscrito", "idPedido": "p1"}   # la conexión sigue sirviendo


def test_desuscribir_y_quitar_conexion_limpian_el_diccionario():
    n, a = Notificador(), ConexionFalsa("a")
    n.suscribir(a, "p1")
    n.suscribir(a, "p2")
    n.desuscribir(a, "p1")
    assert "p1" not in n.suscripciones
    n.quitar_conexion(a)
    assert n.suscripciones == {}


def test_integracion_consumidor_en_hilo_hasta_cliente_websocket_real():
    """Kafka falso (fakes de Caro) -> consumidor en hilo -> loop asyncio -> cliente WebSocket real.
    Verifica: entrega al cliente, deduplicación por idEvento, combinación inválida ignorada y commit."""
    from websockets.asyncio.client import connect
    from websockets.asyncio.server import serve

    from backend.kafka.tests.fakes import FakeConsumer, FakeEventProducer, FakeRecord
    from backend.websockets.consumidor import ConsumidorNotificaciones

    valido = evento("p1", "LISTO")
    invalido = dict(valido, idEvento="e2", origen="caja")          # LISTO no lo publica caja
    registros = [FakeRecord(valido, topic="pedido.estado", offset=0),
                 FakeRecord(valido, topic="pedido.estado", offset=1),   # duplicado (mismo idEvento)
                 FakeRecord(invalido, topic="pedido.estado", offset=2)]

    async def escenario():
        n = Notificador()
        async with serve(n.atender, "localhost", 0) as servidor:
            puerto = servidor.sockets[0].getsockname()[1]
            async with connect(f"ws://localhost:{puerto}") as ws:
                await ws.send(json.dumps({"accion": "suscribir", "idPedido": "p1"}))
                assert json.loads(await ws.recv())["tipo"] == "suscrito"
                falso = FakeConsumer(registros)
                c = ConsumidorNotificaciones(n, asyncio.get_running_loop(),
                                             consumer=falso, producer=FakeEventProducer())
                c.iniciar_en_hilo()
                recibido = json.loads(await asyncio.wait_for(ws.recv(), 3))
                await asyncio.sleep(1.5)                            # deja que procese el resto
                c.detener()
                otro = None
                try:
                    otro = await asyncio.wait_for(ws.recv(), 0.5)
                except asyncio.TimeoutError:
                    pass
                return recibido, otro, falso.commits

    recibido, otro, commits = correr(escenario())
    assert recibido == {"tipo": "estado", "evento": valido}
    assert otro is None                                             # ni el duplicado ni el inválido
    assert commits == [1, 2, 3]                                     # commit de los 3 tras procesarlos
