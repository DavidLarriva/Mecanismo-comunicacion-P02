"""Lógica de Estadísticas (contrato sección 4) y sus dos consumidores.

- pedido.creado -> pedidosRecibidos y porProducto
- pedido.estado -> pedidosPagados (PAGO/PAGADO) y pedidosEntregados (ENTREGA/ENTREGADO)
- ventasEntregadas = suma del total de los pedidos entregados (se calcula al consultar, así no
  importa si el evento de estado llega antes que el de creación: son topics distintos).

Los consumidores NO confirman offsets: al reiniciar el servicio releen los topics desde el
principio y reconstruyen las estadísticas (replay de Kafka), ya que los datos viven en memoria.
"""
import threading
from datetime import datetime, timezone

from backend.kafka import config
from backend.kafka.consumer_base import ConsumerBase


class Estadisticas:
    def __init__(self):
        self._lock = threading.Lock()    # los consumidores (hilos) y FastAPI acceden a la vez
        self._totales = {}               # idPedido -> total (centavos)
        self._pagados = set()
        self._entregados = set()
        self._productos = {}             # idProducto -> {nombre, cantidad, total}
        self._actualizado = None

    def _tocar(self):
        self._actualizado = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    def registrar_creado(self, ev):
        with self._lock:
            if ev["idPedido"] in self._totales:
                return
            self._totales[ev["idPedido"]] = ev["total"]
            for it in ev["items"]:
                p = self._productos.setdefault(it["idProducto"],
                                               {"idProducto": it["idProducto"], "nombre": it["nombre"],
                                                "cantidad": 0, "total": 0})
                p["cantidad"] += it["cantidad"]
                p["total"] += it["cantidad"] * it["precioUnitario"]
            self._tocar()

    def registrar_estado(self, ev):
        with self._lock:
            if (ev["tipo"], ev["estado"]) == ("PAGO", "PAGADO"):
                self._pagados.add(ev["idPedido"])
            elif (ev["tipo"], ev["estado"]) == ("ENTREGA", "ENTREGADO"):
                self._entregados.add(ev["idPedido"])
            else:
                return
            self._tocar()

    def resumen(self):
        with self._lock:
            return {
                "pedidosRecibidos": len(self._totales),
                "pedidosPagados": len(self._pagados),
                "pedidosEntregados": len(self._entregados),
                "ventasEntregadas": sum(self._totales[i] for i in self._entregados if i in self._totales),
                "porProducto": sorted((dict(p) for p in self._productos.values()),
                                      key=lambda p: (-p["total"], p["idProducto"])),
                "actualizado": self._actualizado,
            }


class ConsumidorCreados(ConsumerBase):
    def __init__(self, stats, **kw):
        super().__init__("estadisticas", config.GRUPOS_PEDIDO_CREADO["estadisticas"],
                         config.TOPIC_PEDIDO_CREADO, confirmar_offsets=False, **kw)
        self.stats = stats

    def procesar(self, evento):
        self.stats.registrar_creado(evento)


class ConsumidorEstados(ConsumerBase):
    def __init__(self, stats, **kw):
        super().__init__("estadisticas", config.GRUPOS_PEDIDO_ESTADO["estadisticas"],
                         config.TOPIC_PEDIDO_ESTADO, confirmar_offsets=False, **kw)
        self.stats = stats

    def procesar(self, evento):
        self.stats.registrar_estado(evento)
