import grpc

from contracts.grpc import caja_pb2, caja_pb2_grpc


class ClienteCaja:
    def __init__(self, destino: str = "localhost:50051") -> None:
        self.canal = grpc.insecure_channel(destino)
        self.stub = caja_pb2_grpc.CajaStub(self.canal)

    def confirmar_pago(self, id_pedido: str):
        solicitud = caja_pb2.ConfirmarPagoRequest(
            id_pedido=id_pedido
        )
        return self.stub.ConfirmarPago(
            solicitud,
            timeout=2,
        )

    def cerrar(self) -> None:
        self.canal.close()