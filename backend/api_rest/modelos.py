# Realizado con IA y revisado por David Larriva
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, StrictInt


class ItemSolicitud(BaseModel):
    model_config = ConfigDict(extra="forbid")

    idProducto: str = Field(min_length=1)
    cantidad: StrictInt = Field(ge=1)


class SolicitudPedido(BaseModel):
    model_config = ConfigDict(extra="forbid")

    idEstudiante: str = Field(min_length=1, max_length=20)
    items: list[ItemSolicitud] = Field(min_length=1)


class Producto(BaseModel):
    idProducto: str
    nombre: str
    precio: int


class ItemPedido(BaseModel):
    idProducto: str
    nombre: str
    cantidad: int
    precioUnitario: int
    subtotal: int


class Pedido(BaseModel):
    idPedido: str
    idEstudiante: str
    items: list[ItemPedido]
    total: int
    estadoPreparacion: Literal["RECIBIDO", "EN_PREPARACION", "LISTO"]
    estadoPago: Literal["PENDIENTE", "PAGADO"]
    estadoFacturacion: Literal["FACTURADO", "FACTURACION_PENDIENTE"] | None
    entregado: bool
    fechaCreacion: str


class Error(BaseModel):
    error: str
    mensaje: str
