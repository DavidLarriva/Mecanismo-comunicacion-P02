from concurrent.futures import ThreadPoolExecutor

import grpc

from backend.caja.almacen_pagos import almacen_pagos
from contracts.grpc import caja_pb2, caja_pb2_grpc


class ServicioCaja(caja_pb2_grpc.CajaServicer):
    def ConfirmarPago(self, request, context):
        pago, repetida = almacen_pagos.confirmar_para_facturacion(
            request.id_pedido
        )

        if pago is None:
            return caja_pb2.ConfirmarPagoResponse(
                id_pedido=request.id_pedido,
                estado=caja_pb2.NO_ENCONTRADO,
            )

        if pago["estado"] == "PENDIENTE":
            return caja_pb2.ConfirmarPagoResponse(
                id_pedido=request.id_pedido,
                estado=caja_pb2.PENDIENTE,
                monto=pago["monto"],
            )

        if pago["estado"] == "PAGADO":
            return caja_pb2.ConfirmarPagoResponse(
                id_pedido=request.id_pedido,
                estado=caja_pb2.PAGADO,
                id_confirmacion=pago["idConfirmacion"],
                monto=pago["monto"],
                repetida=repetida,
            )

        context.abort(
            grpc.StatusCode.INTERNAL,
            "El pago tiene un estado no reconocido",
        )


def iniciar_servidor():
    servidor = grpc.server(ThreadPoolExecutor(max_workers=10))
    caja_pb2_grpc.add_CajaServicer_to_server(
        ServicioCaja(),
        servidor,
    )

    puerto = servidor.add_insecure_port("[::]:50051")
    if puerto == 0:
        raise RuntimeError("No se pudo abrir el puerto gRPC 50051")

    servidor.start()
    print("Servidor gRPC de Caja escuchando en el puerto 50051")
    return servidor