import hashlib
import json
import logging
import threading
import uuid
from datetime import datetime, timezone

import config
import idempotencia
from errores import ErrorApi, ErrorPublicacion

log = logging.getLogger("api.servicio")


def ahora_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def huella_solicitud(solicitud):
    contenido = json.dumps(solicitud.model_dump(), sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(contenido.encode("utf-8")).hexdigest()


class ServicioPedidos:
    def __init__(self, repositorio, registro, publicador, menu):
        self._repositorio = repositorio
        self._registro = registro
        self._publicador = publicador
        self._menu = menu
        self._lock_entregas = threading.Lock()

    def menu(self):
        return list(self._menu.values())

    def crear(self, clave, solicitud):
        clave = (clave or "").strip()
        if not clave:
            raise ErrorApi(400, "FALTA_IDEMPOTENCY_KEY", "Falta la cabecera Idempotency-Key")
        items = self._armar_items(solicitud)

        reserva = self._registro.reservar(clave, huella_solicitud(solicitud))
        if reserva.resultado == idempotencia.REPETIDA:
            log.info("Reintento con clave %s: se devuelve el pedido %s", clave, reserva.respuesta["idPedido"])
            return reserva.respuesta, False
        if reserva.resultado == idempotencia.EN_CURSO:
            log.info("Clave %s en proceso: solicitud rechazada", clave)
            raise ErrorApi(409, "SOLICITUD_EN_CURSO", "Ya hay una solicitud con esta Idempotency-Key en proceso")
        if reserva.resultado == idempotencia.REUTILIZADA:
            log.info("Clave %s reutilizada con otro contenido", clave)
            raise ErrorApi(409, "CLAVE_REUTILIZADA", "La Idempotency-Key ya se usó con un pedido distinto")

        pedido = {
            "idPedido": str(uuid.uuid4()),
            "idEstudiante": solicitud.idEstudiante,
            "items": items,
            "total": sum(item["subtotal"] for item in items),
            "estadoPreparacion": "RECIBIDO",
            "estadoPago": "PENDIENTE",
            "estadoFacturacion": None,
            "entregado": False,
            "fechaCreacion": ahora_iso(),
        }
        evento = {
            "idEvento": str(uuid.uuid4()),
            "idPedido": pedido["idPedido"],
            "idEstudiante": pedido["idEstudiante"],
            "items": [
                {k: item[k] for k in ("idProducto", "nombre", "cantidad", "precioUnitario")}
                for item in items
            ],
            "total": pedido["total"],
            "fecha": pedido["fechaCreacion"],
        }

        self._repositorio.guardar(pedido)
        try:
            self._publicador.publicar(config.TOPIC_PEDIDO_CREADO, pedido["idPedido"], evento)
        except ErrorPublicacion as error:
            self._repositorio.eliminar(pedido["idPedido"])
            self._registro.liberar(clave, reserva.token)
            log.warning("Kafka no confirmó el pedido %s: %s", pedido["idPedido"], error)
            raise ErrorApi(503, "SERVICIO_NO_DISPONIBLE", "No se pudo registrar el pedido, intente de nuevo")
        except Exception:
            self._repositorio.eliminar(pedido["idPedido"])
            self._registro.liberar(clave, reserva.token)
            raise

        self._registro.completar(clave, reserva.token, pedido)
        log.info("Pedido %s creado con clave %s", pedido["idPedido"], clave)
        return pedido, True

    def obtener(self, id_pedido):
        pedido = self._repositorio.obtener(id_pedido)
        if pedido is None:
            raise ErrorApi(404, "PEDIDO_NO_ENCONTRADO", f"No existe el pedido {id_pedido}")
        return pedido

    def entregar(self, id_pedido):
        with self._lock_entregas:
            pedido = self.obtener(id_pedido)
            if pedido["entregado"]:
                log.info("Pedido %s ya estaba entregado", id_pedido)
                return pedido
            if pedido["estadoPreparacion"] != "LISTO":
                raise ErrorApi(409, "PEDIDO_NO_LISTO", f"El pedido está {pedido['estadoPreparacion']}")
            if pedido["estadoPago"] != "PAGADO":
                raise ErrorApi(409, "PEDIDO_NO_PAGADO", f"El pago del pedido está {pedido['estadoPago']}")

            evento = {
                "idEvento": str(uuid.uuid4()),
                "idPedido": id_pedido,
                "tipo": "ENTREGA",
                "estado": "ENTREGADO",
                "origen": "api",
                "detalle": {},
                "fecha": ahora_iso(),
            }
            try:
                self._publicador.publicar(config.TOPIC_PEDIDO_ESTADO, id_pedido, evento)
            except ErrorPublicacion as error:
                log.warning("Kafka no confirmó la entrega del pedido %s: %s", id_pedido, error)
                raise ErrorApi(503, "SERVICIO_NO_DISPONIBLE", "No se pudo registrar la entrega, intente de nuevo")

            log.info("Pedido %s entregado", id_pedido)
            return self._repositorio.marcar_entregado(id_pedido, evento["idEvento"])

    def _armar_items(self, solicitud):
        items = []
        for item in solicitud.items:
            producto = self._menu.get(item.idProducto)
            if producto is None:
                raise ErrorApi(400, "DATOS_INVALIDOS", f"El producto {item.idProducto} no está en el menú")
            items.append({
                "idProducto": producto["idProducto"],
                "nombre": producto["nombre"],
                "cantidad": item.cantidad,
                "precioUnitario": producto["precio"],
                "subtotal": producto["precio"] * item.cantidad,
            })
        return items
