"""Prueba WebSockets: cliente que se desconecta antes de LISTO y vuelve (docs/pruebas/prueba-websockets.md).

Necesita corriendo: Kafka, la API (8000), Cocina y el servidor de Notificaciones (8765).
Caja (8001) es opcional: si está, se paga y se entrega de verdad; si no, el evento futuro
se publica con el stub de Caro (simula a la API publicando ENTREGA/ENTREGADO).

  python -m backend.websockets.prueba_reconexion --espera 15
"""
# Generado con IA (bitácora #3, Claude), revisado y adaptado por Cinthy.
import argparse
import asyncio
import json
import time
import uuid

import httpx
from websockets.asyncio.client import connect

# (bitácora #4) 127.0.0.1 y no localhost: en Windows localhost puede ir a IPv6 (::1)
# y la API escucha solo en IPv4 (0.0.0.0), así que la conexión sería rechazada.
API = "http://127.0.0.1:8000"
CAJA = "http://127.0.0.1:8001"
WS = "ws://127.0.0.1:8765"
T0 = time.monotonic()


def log(texto):
    print(f"[{time.strftime('%H:%M:%S')} +{time.monotonic() - T0:5.1f}s] {texto}", flush=True)


async def suscribir_y_consultar(ws, id_pedido):
    """Orden del contrato: 1) suscribir por WebSocket, 2) GET por REST."""
    await ws.send(json.dumps({"accion": "suscribir", "idPedido": id_pedido}))
    log(f"→ WS suscribir: {json.loads(await ws.recv())}")
    r = httpx.get(f"{API}/pedidos/{id_pedido}")
    p = r.json()
    log(f"→ GET /pedidos → {r.status_code}  preparación={p['estadoPreparacion']} "
        f"pago={p['estadoPago']} facturación={p['estadoFacturacion']} entregado={p['entregado']}")
    return p


async def esperar_eventos(ws, segundos):
    recibidos = []
    fin = time.monotonic() + segundos
    while (resto := fin - time.monotonic()) > 0:
        try:
            msg = json.loads(await asyncio.wait_for(ws.recv(), resto))
        except asyncio.TimeoutError:
            break
        if msg["tipo"] == "estado":
            ev = msg["evento"]
            log(f"← WS push {ev['tipo']}/{ev['estado']} (origen {ev['origen']})")
            recibidos.append(ev["estado"])
    return recibidos


def provocar_evento_futuro(id_pedido):
    try:
        r = httpx.post(f"{CAJA}/caja/pagos/{id_pedido}", json={"metodo": "EFECTIVO"}, timeout=3)
        log(f"→ POST /caja/pagos → {r.status_code}")
        time.sleep(1)
        r = httpx.patch(f"{API}/pedidos/{id_pedido}/entregar")
        log(f"→ PATCH /entregar → {r.status_code} {r.json().get('error', '')}")
        if r.status_code == 200:
            return
    except httpx.HTTPError:
        log("Caja no está corriendo: se usa el stub de Caro para publicar ENTREGA/ENTREGADO")
    from backend.kafka.producer import publicar_estado
    publicar_estado(id_pedido, "ENTREGA", "ENTREGADO", "api")
    log("→ stub publicó ENTREGA/ENTREGADO en pedido.estado")


async def main(espera):
    # 1. Crear el pedido por REST
    r = httpx.post(f"{API}/pedidos", headers={"Idempotency-Key": str(uuid.uuid4())},
                   json={"idEstudiante": "0102030405", "items": [{"idProducto": "SAN-01", "cantidad": 1}]})
    id_pedido = r.json()["idPedido"]
    log(f"1. POST /pedidos → {r.status_code} idPedido={id_pedido}")

    # 2. Conectar, suscribirse y consultar; recibir EN_PREPARACION
    async with connect(WS) as ws:
        log("2. WebSocket abierto (101 Switching Protocols)")
        await suscribir_y_consultar(ws, id_pedido)
        await esperar_eventos(ws, 2)
        log("3. El cliente se DESCONECTA antes de que cocina termine")
    # 3. Desconectado: cocina publica LISTO y nadie lo recibe
    log(f"   ...esperando {espera:.0f} s sin conexión (cocina publica LISTO mientras tanto)...")
    await asyncio.sleep(espera)

    # 4. Volver: suscribir y luego GET -> recupera LISTO por REST
    async with connect(WS) as ws:
        log("4. El cliente VUELVE: nuevo WebSocket")
        p = await suscribir_y_consultar(ws, id_pedido)
        recuperado = p["estadoPreparacion"] == "LISTO"
        log(f"   Estado recuperado por GET: {p['estadoPreparacion']} -> {'OK' if recuperado else 'NO'}")
        # 5. Un evento posterior sí llega por WebSocket
        log("5. Se provoca un evento posterior")
        provocar_evento_futuro(id_pedido)
        recibidos = await esperar_eventos(ws, 6)
    llego = "ENTREGADO" in recibidos
    log(f"RESULTADO: estado recuperado por REST={'sí' if recuperado else 'no'}; "
        f"ENTREGADO recibido por WebSocket={'sí' if llego else 'no'}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--espera", type=float, default=15, help="segundos desconectado (> demora de cocina)")
    asyncio.run(main(ap.parse_args().espera))
