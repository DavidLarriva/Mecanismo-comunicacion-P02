import threading
import time
import unittest
import uuid

from fastapi.testclient import TestClient

from errores import ErrorPublicacion
from idempotencia import RegistroIdempotencia
from main import crear_app
from menu import MENU
from modelos import SolicitudPedido
from repositorio import RepositorioPedidos
from servicio import ServicioPedidos


class PublicadorFalso:
    def __init__(self):
        self.mensajes = []
        self.fallar = False
        self.demora_s = 0
        self._lock = threading.Lock()

    def publicar(self, topic, clave, valor):
        if self.demora_s:
            time.sleep(self.demora_s)
        if self.fallar:
            raise ErrorPublicacion("Kafka no disponible")
        with self._lock:
            self.mensajes.append((topic, clave, valor))

    def cerrar(self):
        pass


SOLICITUD = {
    "idEstudiante": "0102030405",
    "items": [
        {"idProducto": "SAN-01", "cantidad": 1},
        {"idProducto": "JUG-01", "cantidad": 2},
    ],
}


def evento_estado(id_pedido, tipo, estado, origen, id_evento=None):
    return {
        "idEvento": id_evento or str(uuid.uuid4()),
        "idPedido": id_pedido,
        "tipo": tipo,
        "estado": estado,
        "origen": origen,
        "detalle": {},
        "fecha": "2026-11-13T15:05:00Z",
    }


class BaseApi(unittest.TestCase):
    def setUp(self):
        self.repositorio = RepositorioPedidos()
        self.publicador = PublicadorFalso()
        self.servicio = ServicioPedidos(self.repositorio, RegistroIdempotencia(30), self.publicador, MENU)
        self.cliente = TestClient(crear_app(self.servicio))

    def crear(self, clave="clave-1", cuerpo=None):
        return self.cliente.post("/pedidos", json=cuerpo or SOLICITUD, headers={"Idempotency-Key": clave})


class TestMenu(BaseApi):
    def test_menu(self):
        respuesta = self.cliente.get("/menu")
        self.assertEqual(respuesta.status_code, 200)
        self.assertEqual(len(respuesta.json()), len(MENU))


class TestCrearPedido(BaseApi):
    def test_crea_pedido_y_publica_evento(self):
        respuesta = self.crear()
        self.assertEqual(respuesta.status_code, 201)
        pedido = respuesta.json()
        self.assertEqual(pedido["total"], 450)
        self.assertEqual(pedido["estadoPreparacion"], "RECIBIDO")
        self.assertEqual(pedido["estadoPago"], "PENDIENTE")
        self.assertIsNone(pedido["estadoFacturacion"])
        self.assertFalse(pedido["entregado"])
        self.assertEqual(len(self.publicador.mensajes), 1)
        topic, clave, evento = self.publicador.mensajes[0]
        self.assertEqual(topic, "pedido.creado")
        self.assertEqual(clave, pedido["idPedido"])
        self.assertEqual(evento["total"], 450)
        self.assertNotIn("subtotal", evento["items"][0])

    def test_reintento_con_misma_clave_devuelve_el_mismo_pedido(self):
        primera = self.crear()
        segunda = self.crear()
        self.assertEqual(primera.status_code, 201)
        self.assertEqual(segunda.status_code, 200)
        self.assertEqual(primera.json(), segunda.json())
        self.assertEqual(len(self.publicador.mensajes), 1)

    def test_clave_reutilizada_con_otro_cuerpo(self):
        self.crear()
        otro = {"idEstudiante": "0102030405", "items": [{"idProducto": "CAF-01", "cantidad": 1}]}
        respuesta = self.crear(cuerpo=otro)
        self.assertEqual(respuesta.status_code, 409)
        self.assertEqual(respuesta.json()["error"], "CLAVE_REUTILIZADA")

    def test_falta_clave(self):
        respuesta = self.cliente.post("/pedidos", json=SOLICITUD)
        self.assertEqual(respuesta.status_code, 400)
        self.assertEqual(respuesta.json()["error"], "FALTA_IDEMPOTENCY_KEY")

    def test_clave_vacia(self):
        respuesta = self.crear(clave="   ")
        self.assertEqual(respuesta.status_code, 400)
        self.assertEqual(respuesta.json()["error"], "FALTA_IDEMPOTENCY_KEY")

    def test_datos_invalidos(self):
        casos = [
            {"idEstudiante": "0102030405", "items": []},
            {"idEstudiante": "0102030405", "items": [{"idProducto": "SAN-01", "cantidad": 0}]},
            {"idEstudiante": "0102030405", "items": [{"idProducto": "SAN-01", "cantidad": "2"}]},
            {"idEstudiante": "0102030405", "items": [{"idProducto": "NO-EXISTE", "cantidad": 1}]},
            {"idEstudiante": "0102030405", "items": [{"idProducto": "SAN-01", "cantidad": 1, "precioUnitario": 1}]},
            {"idEstudiante": "", "items": [{"idProducto": "SAN-01", "cantidad": 1}]},
            {"items": [{"idProducto": "SAN-01", "cantidad": 1}]},
        ]
        for indice, cuerpo in enumerate(casos):
            with self.subTest(caso=indice):
                respuesta = self.crear(clave=f"invalida-{indice}", cuerpo=cuerpo)
                self.assertEqual(respuesta.status_code, 400)
                self.assertEqual(respuesta.json()["error"], "DATOS_INVALIDOS")
        self.assertEqual(self.publicador.mensajes, [])

    def test_kafka_caido_responde_503_y_libera_la_clave(self):
        self.publicador.fallar = True
        respuesta = self.crear()
        self.assertEqual(respuesta.status_code, 503)
        self.assertEqual(respuesta.json()["error"], "SERVICIO_NO_DISPONIBLE")
        self.publicador.fallar = False
        reintento = self.crear()
        self.assertEqual(reintento.status_code, 201)
        self.assertEqual(len(self.publicador.mensajes), 1)

    def test_kafka_caido_no_deja_el_pedido_registrado(self):
        self.publicador.fallar = True
        self.crear()
        self.assertEqual(self.repositorio._pedidos, {})


class TestConcurrencia(unittest.TestCase):
    def test_dos_solicitudes_simultaneas_con_la_misma_clave(self):
        publicador = PublicadorFalso()
        publicador.demora_s = 0.5
        servicio = ServicioPedidos(RepositorioPedidos(), RegistroIdempotencia(30), publicador, MENU)
        solicitud = SolicitudPedido(**SOLICITUD)
        resultados = []

        def enviar():
            try:
                pedido, es_nuevo = servicio.crear("misma-clave", solicitud)
                resultados.append(("CREADO" if es_nuevo else "REPETIDO", pedido["idPedido"]))
            except Exception as error:
                resultados.append((getattr(error, "codigo", type(error).__name__), None))

        hilos = [threading.Thread(target=enviar) for _ in range(2)]
        for hilo in hilos:
            hilo.start()
        for hilo in hilos:
            hilo.join()

        self.assertEqual(sorted(r[0] for r in resultados), ["CREADO", "SOLICITUD_EN_CURSO"])
        self.assertEqual(len(publicador.mensajes), 1)


class TestIdempotencia(unittest.TestCase):
    def test_clave_en_proceso_vence(self):
        instante = [0.0]
        registro = RegistroIdempotencia(30, reloj=lambda: instante[0])
        primera = registro.reservar("c", "h")
        self.assertEqual(primera.resultado, "NUEVA")
        self.assertEqual(registro.reservar("c", "h").resultado, "EN_CURSO")
        instante[0] = 31.0
        segunda = registro.reservar("c", "h")
        self.assertEqual(segunda.resultado, "NUEVA")
        registro.liberar("c", primera.token)
        self.assertEqual(registro.estado("c"), "EN_PROCESO")


class TestConsultarYEstados(BaseApi):
    def test_pedido_inexistente(self):
        respuesta = self.cliente.get("/pedidos/no-existe")
        self.assertEqual(respuesta.status_code, 404)
        self.assertEqual(respuesta.json()["error"], "PEDIDO_NO_ENCONTRADO")

    def test_eventos_actualizan_el_pedido(self):
        id_pedido = self.crear().json()["idPedido"]
        self.repositorio.aplicar_evento(evento_estado(id_pedido, "PREPARACION", "EN_PREPARACION", "cocina"))
        self.repositorio.aplicar_evento(evento_estado(id_pedido, "PAGO", "PAGADO", "caja"))
        self.repositorio.aplicar_evento(evento_estado(id_pedido, "FACTURACION", "FACTURADO", "facturacion"))
        pedido = self.cliente.get(f"/pedidos/{id_pedido}").json()
        self.assertEqual(pedido["estadoPreparacion"], "EN_PREPARACION")
        self.assertEqual(pedido["estadoPago"], "PAGADO")
        self.assertEqual(pedido["estadoFacturacion"], "FACTURADO")

    def test_evento_duplicado_se_ignora(self):
        id_pedido = self.crear().json()["idPedido"]
        evento = evento_estado(id_pedido, "PREPARACION", "LISTO", "cocina")
        self.assertEqual(self.repositorio.aplicar_evento(evento), "APLICADO")
        self.assertEqual(self.repositorio.aplicar_evento(evento), "DUPLICADO")

    def test_combinacion_invalida_se_ignora(self):
        id_pedido = self.crear().json()["idPedido"]
        evento = evento_estado(id_pedido, "PREPARACION", "LISTO", "caja")
        self.assertEqual(self.repositorio.aplicar_evento(evento), "INVALIDO")
        self.assertEqual(self.cliente.get(f"/pedidos/{id_pedido}").json()["estadoPreparacion"], "RECIBIDO")

    def test_estado_no_retrocede(self):
        id_pedido = self.crear().json()["idPedido"]
        self.repositorio.aplicar_evento(evento_estado(id_pedido, "PREPARACION", "LISTO", "cocina"))
        resultado = self.repositorio.aplicar_evento(
            evento_estado(id_pedido, "PREPARACION", "EN_PREPARACION", "cocina"))
        self.assertEqual(resultado, "SIN_CAMBIO")
        self.assertEqual(self.cliente.get(f"/pedidos/{id_pedido}").json()["estadoPreparacion"], "LISTO")

    def test_evento_de_pedido_desconocido(self):
        evento = evento_estado(str(uuid.uuid4()), "PREPARACION", "LISTO", "cocina")
        self.assertEqual(self.repositorio.aplicar_evento(evento), "DESCONOCIDO")


class TestEntregar(BaseApi):
    def preparar(self, listo=True, pagado=True):
        id_pedido = self.crear().json()["idPedido"]
        if listo:
            self.repositorio.aplicar_evento(evento_estado(id_pedido, "PREPARACION", "LISTO", "cocina"))
        if pagado:
            self.repositorio.aplicar_evento(evento_estado(id_pedido, "PAGO", "PAGADO", "caja"))
        return id_pedido

    def test_no_listo(self):
        id_pedido = self.preparar(listo=False)
        respuesta = self.cliente.patch(f"/pedidos/{id_pedido}/entregar")
        self.assertEqual(respuesta.status_code, 409)
        self.assertEqual(respuesta.json()["error"], "PEDIDO_NO_LISTO")

    def test_no_pagado(self):
        id_pedido = self.preparar(pagado=False)
        respuesta = self.cliente.patch(f"/pedidos/{id_pedido}/entregar")
        self.assertEqual(respuesta.status_code, 409)
        self.assertEqual(respuesta.json()["error"], "PEDIDO_NO_PAGADO")

    def test_entrega_idempotente(self):
        id_pedido = self.preparar()
        primera = self.cliente.patch(f"/pedidos/{id_pedido}/entregar")
        segunda = self.cliente.patch(f"/pedidos/{id_pedido}/entregar")
        self.assertEqual(primera.status_code, 200)
        self.assertEqual(segunda.status_code, 200)
        self.assertTrue(segunda.json()["entregado"])
        entregas = [m for m in self.publicador.mensajes if m[0] == "pedido.estado"]
        self.assertEqual(len(entregas), 1)
        self.assertEqual(entregas[0][2]["estado"], "ENTREGADO")
        self.assertEqual(self.repositorio.aplicar_evento(entregas[0][2]), "DUPLICADO")

    def test_entrega_con_kafka_caido(self):
        id_pedido = self.preparar()
        self.publicador.fallar = True
        respuesta = self.cliente.patch(f"/pedidos/{id_pedido}/entregar")
        self.assertEqual(respuesta.status_code, 503)
        self.assertFalse(self.cliente.get(f"/pedidos/{id_pedido}").json()["entregado"])

    def test_entregar_inexistente(self):
        respuesta = self.cliente.patch("/pedidos/no-existe/entregar")
        self.assertEqual(respuesta.status_code, 404)


if __name__ == "__main__":
    unittest.main()
