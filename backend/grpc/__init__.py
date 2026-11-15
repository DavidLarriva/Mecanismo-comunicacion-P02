from concurrent import futures
from typing import Protocol

import grpc

from contracts.grpc import caja_pb2, caja_pb2_grpc


class Pago(Protocol):
    estado: str
    monto: int
    id_confirmacion: str | None


class PagosCaja(Protocol):
    def confirmar_para_facturacion(
        self,
        id_pedido: str,
    ) -> tuple[Pago | None, bool]:
        """Devuelve el pago y si ya se había confirmado para facturar."""
        ...


class ServicioCaja(caja_pb2_grpc.CajaServicer):
    def __init__(self, pagos: PagosCaja) -> None:
        self.pagos = pagos

    def ConfirmarPago(self, request, context):
        id_pedido = request.id_pedido.strip()

        if not id_pedido:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "id_pedido es obligatorio",
            )

        pago, repetida = self.pagos.confirmar_para_facturacion(id_pedido)

        if pago is None:
            return caja_pb2.ConfirmarPagoResponse(
                id_pedido=id_pedido,
                estado=caja_pb2.NO_ENCONTRADO,
            )

        if pago.estado != "PAGADO":
            return caja_pb2.ConfirmarPagoResponse(
                id_pedido=id_pedido,
                estado=caja_pb2.PENDIENTE,
                monto=pago.monto,
            )

        return caja_pb2.ConfirmarPagoResponse(
            id_pedido=id_pedido,
            estado=caja_pb2.PAGADO,
            id_confirmacion=pago.id_confirmacion or "",
            monto=pago.monto,
            repetida=repetida,
        )


def iniciar_servidor(
    pagos: PagosCaja,
    host: str = "127.0.0.1",
    puerto: int = 50051,
) -> grpc.Server:
    servidor = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10)
    )
    caja_pb2_grpc.add_CajaServicer_to_server(
        ServicioCaja(pagos),
        servidor,
    )

    puerto_abierto = servidor.add_insecure_port(
        f"{host}:{puerto}"
    )
    if puerto_abierto == 0:
        raise RuntimeError(f"No se pudo abrir el puerto {puerto}")

    servidor.start()
    return servidor