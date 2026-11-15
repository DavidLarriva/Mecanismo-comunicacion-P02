"""STUB de la API de Pedidos (David): publica pedidos con la MISMA función que usará la API.

  python -m backend.kafka.stubs.simulador_api --n 5 --intervalo 1
  python -m backend.kafka.stubs.simulador_api --n 1 --duplicar        # mismo pedido dos veces
  python -m backend.kafka.stubs.simulador_api --entregar <idPedido>   # publica ENTREGA/ENTREGADO
"""
import argparse
import logging
import random
import time
import uuid
from datetime import datetime, timezone

from ..producer import KafkaNoDisponible, publicar_estado, publicar_pedido_creado

MENU = [("SAN-01", "Sanduche de pollo", 250), ("JUG-01", "Jugo de naranja", 100),
        ("CAF-01", "Café americano", 150)]


def pedido_falso(id_estudiante):
    """Mismo formato que la respuesta de POST /pedidos (contrato, sección 2)."""
    items = []
    for id_prod, nombre, precio in random.sample(MENU, k=random.randint(1, 3)):
        c = random.randint(1, 3)
        items.append({"idProducto": id_prod, "nombre": nombre, "cantidad": c,
                      "precioUnitario": precio, "subtotal": c * precio})
    return {"idPedido": str(uuid.uuid4()), "idEstudiante": id_estudiante, "items": items,
            "total": sum(i["subtotal"] for i in items), "estadoPreparacion": "RECIBIDO",
            "estadoPago": "PENDIENTE", "estadoFacturacion": None, "entregado": False,
            "fechaCreacion": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}


def enviar(pedido):
    try:
        particion, offset = publicar_pedido_creado(pedido)
        print(f"201 simulado | {pedido['idPedido']} total={pedido['total']} -> p{particion} o{offset}")
    except KafkaNoDisponible as e:
        print(f"503 simulado | {pedido['idPedido'][:8]} NO publicado: {e}")


def main():
    logging.basicConfig(level=logging.WARNING)
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=3)
    ap.add_argument("--intervalo", type=float, default=1.0)
    ap.add_argument("--estudiante", default="0102030405")
    ap.add_argument("--duplicar", action="store_true", help="publica cada pedido dos veces")
    ap.add_argument("--entregar", nargs="+", metavar="ID", help="publica ENTREGADO para esos pedidos")
    a = ap.parse_args()
    if a.entregar:
        for id_pedido in a.entregar:
            publicar_estado(id_pedido, "ENTREGA", "ENTREGADO", "api")
            print("ENTREGADO publicado:", id_pedido)
        return
    for _ in range(a.n):
        pedido = pedido_falso(a.estudiante)
        enviar(pedido)
        if a.duplicar:
            enviar(pedido)
        time.sleep(a.intervalo)


if __name__ == "__main__":
    main()
