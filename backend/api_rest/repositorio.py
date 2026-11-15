import copy
import threading

COMBINACIONES_VALIDAS = {
    ("PREPARACION", "EN_PREPARACION", "cocina"),
    ("PREPARACION", "LISTO", "cocina"),
    ("PAGO", "PENDIENTE", "caja"),
    ("PAGO", "PAGADO", "caja"),
    ("FACTURACION", "FACTURADO", "facturacion"),
    ("FACTURACION", "FACTURACION_PENDIENTE", "facturacion"),
    ("ENTREGA", "ENTREGADO", "api"),
}

ORDEN = {
    "PREPARACION": ("estadoPreparacion", {"RECIBIDO": 0, "EN_PREPARACION": 1, "LISTO": 2}),
    "PAGO": ("estadoPago", {"PENDIENTE": 0, "PAGADO": 1}),
    "FACTURACION": ("estadoFacturacion", {None: 0, "FACTURACION_PENDIENTE": 1, "FACTURADO": 2}),
}

APLICADO = "APLICADO"
DUPLICADO = "DUPLICADO"
INVALIDO = "INVALIDO"
DESCONOCIDO = "DESCONOCIDO"
SIN_CAMBIO = "SIN_CAMBIO"


class RepositorioPedidos:
    def __init__(self):
        self._lock = threading.Lock()
        self._pedidos = {}
        self._eventos_procesados = set()

    def guardar(self, pedido):
        with self._lock:
            self._pedidos[pedido["idPedido"]] = copy.deepcopy(pedido)

    def eliminar(self, id_pedido):
        with self._lock:
            self._pedidos.pop(id_pedido, None)

    def obtener(self, id_pedido):
        with self._lock:
            pedido = self._pedidos.get(id_pedido)
            return copy.deepcopy(pedido) if pedido else None

    def marcar_entregado(self, id_pedido):
        with self._lock:
            pedido = self._pedidos.get(id_pedido)
            if pedido:
                pedido["entregado"] = True
            return copy.deepcopy(pedido) if pedido else None

    def aplicar_evento(self, evento):
        if not isinstance(evento, dict):
            return INVALIDO
        id_evento = evento.get("idEvento")
        id_pedido = evento.get("idPedido")
        combinacion = (evento.get("tipo"), evento.get("estado"), evento.get("origen"))
        if not id_evento or not id_pedido or combinacion not in COMBINACIONES_VALIDAS:
            return INVALIDO
        with self._lock:
            if id_evento in self._eventos_procesados:
                return DUPLICADO
            self._eventos_procesados.add(id_evento)
            pedido = self._pedidos.get(id_pedido)
            if pedido is None:
                return DESCONOCIDO
            tipo, estado, _ = combinacion
            if tipo == "ENTREGA":
                if pedido["entregado"]:
                    return SIN_CAMBIO
                pedido["entregado"] = True
                return APLICADO
            campo, orden = ORDEN[tipo]
            if orden[estado] <= orden[pedido[campo]]:
                return SIN_CAMBIO
            pedido[campo] = estado
            return APLICADO
