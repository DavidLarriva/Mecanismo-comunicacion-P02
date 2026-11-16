"""Notificaciones (Cinthy, Integrante 4): servidor WebSocket en ws://localhost:8765.

Contrato: docs/contratos.md v1.0, sección 7.

  Cliente -> servidor:  {"accion": "suscribir",   "idPedido": "..."}
                        {"accion": "desuscribir", "idPedido": "..."}
  Servidor -> cliente:  {"tipo": "suscrito", "idPedido": "..."}
                        {"tipo": "estado",   "evento": { ...evento de pedido.estado sin modificar... }}
                        {"tipo": "error",    "mensaje": "..."}

Ejecutar desde la raíz del repo:
  python -m backend.websockets.servidor              # con Kafka (consume pedido.estado)
  python -m backend.websockets.servidor --simulado   # sin Kafka: genera eventos falsos
"""
# Generado con IA (bitácora #1, Claude), revisado y adaptado por Cinthy.
import argparse
import asyncio
import json
import logging
import os

from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

log = logging.getLogger("notificaciones")

PUERTO = int(os.getenv("PUERTO_NOTIFICACIONES", "8765"))


class Notificador:
    """Guarda quién está suscrito a qué pedido y les reenvía los eventos (server push)."""

    def __init__(self, al_suscribir=None):
        # idPedido -> conjunto de conexiones abiertas suscritas a ese pedido
        self.suscripciones: dict[str, set] = {}
        self.al_suscribir = al_suscribir        # solo lo usa el modo simulado

    # ---------- suscripciones ----------
    def suscribir(self, conexion, id_pedido):
        self.suscripciones.setdefault(id_pedido, set()).add(conexion)
        if self.al_suscribir:
            self.al_suscribir(id_pedido)

    def desuscribir(self, conexion, id_pedido):
        conexiones = self.suscripciones.get(id_pedido)
        if conexiones is None:
            return
        conexiones.discard(conexion)
        if not conexiones:                      # no dejar llaves vacías en el diccionario
            del self.suscripciones[id_pedido]

    def quitar_conexion(self, conexion):
        """Se llama cuando una conexión se cierra o falla: la saca de todos los pedidos."""
        for id_pedido in list(self.suscripciones):
            self.desuscribir(conexion, id_pedido)

    # ---------- envío ----------
    async def notificar(self, evento):
        """Envía el evento solo a las conexiones suscritas a evento['idPedido'].

        Si nadie está conectado, el aviso se descarta (el WebSocket es comunicación
        transitoria). El estado no se pierde: vive en la API y el cliente lo pide con GET.
        """
        id_pedido = evento["idPedido"]
        conexiones = list(self.suscripciones.get(id_pedido, ()))
        if not conexiones:
            log.info("sin clientes para %s: %s/%s descartado",
                     id_pedido[:8], evento["tipo"], evento["estado"])
            return 0
        mensaje = json.dumps({"tipo": "estado", "evento": evento}, ensure_ascii=False)
        enviados = 0
        for conexion in conexiones:
            try:
                await conexion.send(mensaje)
                enviados += 1
            except ConnectionClosed:
                # La conexión murió entre la suscripción y el envío: se quita y se sigue
                log.warning("envío fallido a %s: conexión cerrada, se elimina", conexion.remote_address)
                self.quitar_conexion(conexion)
        log.info("%s/%s de %s -> %d cliente(s)",
                 evento["tipo"], evento["estado"], id_pedido[:8], enviados)
        return enviados

    # ---------- una conexión ----------
    async def atender(self, conexion):
        """Handler de websockets: se ejecuta una vez por cada cliente conectado."""
        log.info("cliente conectado %s", conexion.remote_address)
        try:
            async for crudo in conexion:
                await self._procesar_mensaje(conexion, crudo)
        except ConnectionClosed:
            pass                                 # cierre sin handshake (p. ej. se cayó el Wi-Fi)
        finally:
            self.quitar_conexion(conexion)
            log.info("cliente desconectado %s (pedidos con clientes: %d)",
                     conexion.remote_address, len(self.suscripciones))

    async def _procesar_mensaje(self, conexion, crudo):
        try:
            mensaje = json.loads(crudo)
            accion = mensaje["accion"]
            id_pedido = mensaje["idPedido"]
            if not isinstance(id_pedido, str) or not id_pedido:
                raise ValueError("idPedido vacío")
        except (ValueError, TypeError, KeyError):
            # Mensaje inválido: se avisa, pero NO se cierra la conexión (contrato)
            await self._enviar_error(conexion, "Mensaje inválido: se espera {accion, idPedido}")
            return

        if accion == "suscribir":
            self.suscribir(conexion, id_pedido)
            log.info("%s suscrito a %s", conexion.remote_address, id_pedido[:8])
            await conexion.send(json.dumps({"tipo": "suscrito", "idPedido": id_pedido}))
        elif accion == "desuscribir":
            self.desuscribir(conexion, id_pedido)
            log.info("%s desuscrito de %s", conexion.remote_address, id_pedido[:8])
        else:
            await self._enviar_error(conexion, f"Acción desconocida: {accion}")

    @staticmethod
    async def _enviar_error(conexion, texto):
        await conexion.send(json.dumps({"tipo": "error", "mensaje": texto}, ensure_ascii=False))


async def main(simulado=False, demora=4.0):
    notificador = Notificador()

    if simulado:
        from .simulador import SimuladorEstados
        simulador = SimuladorEstados(notificador, demora)
        notificador.al_suscribir = simulador.iniciar   # secuencia falsa al suscribirse
    else:
        from .consumidor import ConsumidorNotificaciones
        consumidor = ConsumidorNotificaciones(notificador, asyncio.get_running_loop())
        consumidor.iniciar_en_hilo()             # kafka-python es bloqueante: va en otro hilo

    # ping_interval=20: cada 20 s se envía un ping; si no hay pong, la conexión se da por muerta
    async with serve(notificador.atender, None, PUERTO, ping_interval=20, ping_timeout=20):
        log.info("servidor WebSocket en ws://localhost:%d (%s)", PUERTO,
                 "modo simulado" if simulado else "consumiendo pedido.estado de Kafka")
        await asyncio.Future()                   # corre hasta Ctrl+C


def ejecutar():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)-22s %(message)s")
    logging.getLogger("kafka").setLevel(logging.WARNING)
    logging.getLogger("websockets").setLevel(logging.WARNING)
    ap = argparse.ArgumentParser(description="Servidor WebSocket de notificaciones de CampusBite")
    ap.add_argument("--simulado", action="store_true", help="sin Kafka: genera eventos falsos")
    ap.add_argument("--demora", type=float, default=4.0, help="segundos entre eventos simulados")
    args = ap.parse_args()
    try:
        asyncio.run(main(args.simulado, args.demora))
    except KeyboardInterrupt:
        log.info("servidor detenido")


if __name__ == "__main__":
    ejecutar()
