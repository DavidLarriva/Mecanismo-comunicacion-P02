"""Modo simulado (Fase 2): prueba el WebSocket sin Kafka ni los otros servicios.

Cuando un cliente se suscribe a un pedido por primera vez, se genera la secuencia de eventos
que en el sistema real publicarían Cocina, Caja, Facturación y la API, con el formato EXACTO
de 'pedido.estado' (contrato, sección 5). Se pasan por la misma función notificar() que usa
Kafka, así que el cliente no nota la diferencia.

  python -m backend.websockets.servidor --simulado --demora 4
"""
# Generado con IA (bitácora #1, Claude), revisado y adaptado por Cinthy.
import asyncio
import logging
import uuid
from datetime import datetime, timezone

log = logging.getLogger("notificaciones.simulador")

# (tipo, estado, origen, detalle) en el orden del flujo integrado (contrato, sección 8)
SECUENCIA = [
    ("PREPARACION", "EN_PREPARACION", "cocina", {}),
    ("PAGO", "PAGADO", "caja", {"idConfirmacion": "PAG-SIM", "monto": 450}),
    ("FACTURACION", "FACTURADO", "facturacion", {"idFactura": "FAC-SIM"}),
    ("PREPARACION", "LISTO", "cocina", {}),
    ("ENTREGA", "ENTREGADO", "api", {}),
]


def evento_falso(id_pedido, tipo, estado, origen, detalle):
    return {
        "idEvento": str(uuid.uuid4()),
        "idPedido": id_pedido,
        "tipo": tipo,
        "estado": estado,
        "origen": origen,
        "detalle": detalle,
        "fecha": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


class SimuladorEstados:
    def __init__(self, notificador, demora_s=4.0):
        self.notificador = notificador
        self.demora_s = demora_s
        self.simulados = set()       # pedidos que ya tienen su secuencia en marcha
        self.tareas = set()          # referencias a las tareas para que no las borre el GC

    def iniciar(self, id_pedido):
        """Se llama en cada suscripción. Una sola secuencia por pedido, aunque el cliente
        se reconecte (así se puede probar la desconexión también en modo simulado)."""
        if id_pedido in self.simulados:
            return
        self.simulados.add(id_pedido)
        tarea = asyncio.create_task(self._secuencia(id_pedido))
        self.tareas.add(tarea)
        tarea.add_done_callback(self.tareas.discard)

    async def _secuencia(self, id_pedido):
        log.info("simulando estados de %s cada %.0f s", id_pedido[:8], self.demora_s)
        for tipo, estado, origen, detalle in SECUENCIA:
            await asyncio.sleep(self.demora_s)
            await self.notificador.notificar(evento_falso(id_pedido, tipo, estado, origen, detalle))
