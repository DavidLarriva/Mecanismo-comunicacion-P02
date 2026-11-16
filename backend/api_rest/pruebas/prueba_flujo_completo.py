# Realizado con IA y revisado por David Larriva
import asyncio
import json
import time
import uuid

import httpx
from kafka import KafkaConsumer, TopicPartition
from websockets.asyncio.client import connect

from backend.kafka import config as kafka_config

API = "http://127.0.0.1:8000"
CAJA = "http://127.0.0.1:8001"
ESTADISTICAS = "http://127.0.0.1:8002"
WS = "ws://127.0.0.1:8765"
CUERPO = {
    "idEstudiante": "0102030405",
    "items": [
        {"idProducto": "SAN-01", "cantidad": 1},
        {"idProducto": "JUG-01", "cantidad": 2},
    ],
}
INICIO = time.monotonic()


def log(texto):
    print(f"+{time.monotonic() - INICIO:5.1f}s {texto}", flush=True)


def estado(id_pedido):
    p = httpx.get(f"{API}/pedidos/{id_pedido}").json()
    return p["estadoPreparacion"], p["estadoPago"], p["estadoFacturacion"], p["entregado"]


def resumen_estadisticas():
    d = httpx.get(f"{ESTADISTICAS}/estadisticas").json()
    claves = ("pedidosRecibidos", "pedidosPagados", "pedidosEntregados", "ventasEntregadas")
    return {k: d[k] for k in claves}


async def escuchar(ws, recibidos):
    async for crudo in ws:
        m = json.loads(crudo)
        if m["tipo"] == "estado":
            e = m["evento"]
            recibidos.append(e["estado"])
            log(f"WS <- {e['tipo']}/{e['estado']} ({e['origen']}) {e['detalle']}")


async def esperar(id_pedido, condicion, limite=40):
    fin = time.monotonic() + limite
    while time.monotonic() < fin:
        s = estado(id_pedido)
        if condicion(s):
            return s
        await asyncio.sleep(0.5)
    return estado(id_pedido)


def eventos_en_kafka(id_pedido):
    consumidor = KafkaConsumer(
        bootstrap_servers=kafka_config.BOOTSTRAP_SERVERS,
        enable_auto_commit=False,
        consumer_timeout_ms=3000,
    )
    for topic in (kafka_config.TOPIC_PEDIDO_CREADO, kafka_config.TOPIC_PEDIDO_ESTADO):
        particiones = [TopicPartition(topic, p) for p in consumidor.partitions_for_topic(topic)]
        consumidor.assign(particiones)
        consumidor.seek_to_beginning(*particiones)
        for m in consumidor:
            if m.key and m.key.decode() == id_pedido:
                e = json.loads(m.value)
                if topic == kafka_config.TOPIC_PEDIDO_CREADO:
                    descripcion = f"pedido creado, total={e.get('total')}"
                else:
                    descripcion = f"{e['tipo']}/{e['estado']} origen={e['origen']}"
                print(f"  {topic} particion={m.partition} offset={m.offset} {descripcion}")
    consumidor.close()


async def main():
    log(f"estadisticas antes: {resumen_estadisticas()}")
    r = httpx.post(f"{API}/pedidos", headers={"Idempotency-Key": str(uuid.uuid4())}, json=CUERPO)
    id_pedido = r.json()["idPedido"]
    log(f"POST /pedidos -> {r.status_code} {id_pedido} total={r.json()['total']}")
    recibidos = []
    async with connect(WS) as ws:
        await ws.send(json.dumps({"accion": "suscribir", "idPedido": id_pedido}))
        log(f"WS suscribir -> {json.loads(await ws.recv())['tipo']}")
        tarea = asyncio.create_task(escuchar(ws, recibidos))
        log(f"GET /pedidos -> {estado(id_pedido)}")
        r = httpx.patch(f"{API}/pedidos/{id_pedido}/entregar")
        log(f"PATCH entregar (antes de LISTO) -> {r.status_code} {r.json().get('error')}")
        log(f"esperando LISTO -> {await esperar(id_pedido, lambda s: s[0] == 'LISTO')}")
        r = httpx.patch(f"{API}/pedidos/{id_pedido}/entregar")
        log(f"PATCH entregar (sin pagar) -> {r.status_code} {r.json().get('error')}")
        cobro = httpx.get(f"{CAJA}/caja/pagos/{id_pedido}").json()
        log(f"GET /caja/pagos -> {cobro['estado']} monto={cobro['monto']}")
        r1 = httpx.post(f"{CAJA}/caja/pagos/{id_pedido}", json={"metodo": "TARJETA"})
        r2 = httpx.post(f"{CAJA}/caja/pagos/{id_pedido}", json={"metodo": "TARJETA"})
        log(f"POST /caja/pagos x2 -> {r1.status_code} {r1.json()['idConfirmacion']} / {r2.status_code} {r2.json()['idConfirmacion']}")
        r = httpx.post(f"{CAJA}/caja/pagos/no-existe", json={"metodo": "TARJETA"})
        log(f"POST /caja/pagos (pedido inexistente) -> {r.status_code} {r.json()['error']}")
        log(f"esperando FACTURADO -> {await esperar(id_pedido, lambda s: s[2] == 'FACTURADO')}")
        r1 = httpx.patch(f"{API}/pedidos/{id_pedido}/entregar")
        r2 = httpx.patch(f"{API}/pedidos/{id_pedido}/entregar")
        log(f"PATCH entregar x2 -> {r1.status_code} {r2.status_code} entregado={r2.json()['entregado']}")
        await asyncio.sleep(2)
        tarea.cancel()
    log(f"estados recibidos por WebSocket: {recibidos}")
    log(f"GET final -> {estado(id_pedido)}")
    log(f"estadisticas despues: {resumen_estadisticas()}")
    print("eventos del pedido en Kafka:")
    eventos_en_kafka(id_pedido)


if __name__ == "__main__":
    asyncio.run(main())
