"""Dobles de prueba: permiten ejecutar los tests SIN broker Kafka."""
import json
from types import SimpleNamespace

from kafka.errors import KafkaTimeoutError, KafkaError


class FakeRecord:
    """Imita kafka.consumer.fetcher.ConsumerRecord (atributos, no métodos)."""
    def __init__(self, valor, key="p1", topic="pedido.creado", partition=0, offset=0):
        self.value = valor if isinstance(valor, bytes) else (
            valor.encode() if isinstance(valor, str) else json.dumps(valor).encode())
        self.key = key.encode() if key else None
        self.topic, self.partition, self.offset = topic, partition, offset


class FakeFuture:
    def __init__(self, modo):
        self.modo = modo

    def get(self, timeout=None):
        if self.modo == "timeout":
            raise KafkaTimeoutError("sin respuesta")
        if self.modo == "error":
            raise KafkaError("broker rechazó")
        return SimpleNamespace(partition=2, offset=7)


class FakeKafkaProducer:
    """Imita KafkaProducer. modo: 'ok' | 'timeout' | 'error'."""
    def __init__(self, modo="ok"):
        self.modo, self.enviados = modo, []

    def send(self, topic, value=None, key=None):
        self.enviados.append((topic, key.decode() if key else None, json.loads(value)))
        return FakeFuture(self.modo)


class FakeEventProducer:
    """Imita EventProducer para los tests de los consumidores."""
    def __init__(self, falla=False):
        self.falla, self.publicados = falla, []

    def publicar(self, topic, key, valor):
        from ..producer import KafkaNoDisponible
        if self.falla:
            raise KafkaNoDisponible("simulado")
        self.publicados.append((topic, key, valor))
        return 0, 0

    def publicar_estado(self, id_pedido, tipo, estado, origen, detalle=None):
        return self.publicar("pedido.estado", id_pedido,
                             {"idPedido": id_pedido, "tipo": tipo, "estado": estado,
                              "origen": origen, "detalle": detalle or {}})


class FakeConsumer:
    def __init__(self, registros=()):
        self.pendientes, self.commits, self.cerrado = list(registros), [], False

    def poll(self, timeout_ms=0):
        if not self.pendientes:
            return {}
        r = self.pendientes.pop(0)
        return {(r.topic, r.partition): [r]}

    def commit(self, offsets=None):
        self.commits.extend(o.offset for o in offsets.values())   # = offset del mensaje + 1

    def close(self):
        self.cerrado = True


def pedido_api(id_pedido="p1"):
    """Objeto pedido tal como lo devuelve POST /pedidos (contrato, sección 2)."""
    return {"idPedido": id_pedido, "idEstudiante": "0102030405",
            "items": [{"idProducto": "SAN-01", "nombre": "Sanduche de pollo", "cantidad": 1,
                       "precioUnitario": 250, "subtotal": 250},
                      {"idProducto": "JUG-01", "nombre": "Jugo de naranja", "cantidad": 2,
                       "precioUnitario": 100, "subtotal": 200}],
            "total": 450, "estadoPreparacion": "RECIBIDO", "estadoPago": "PENDIENTE",
            "estadoFacturacion": None, "entregado": False, "fechaCreacion": "2026-11-13T15:04:05Z"}
