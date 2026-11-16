class ErrorApi(Exception):
    def __init__(self, estado_http, codigo, mensaje):
        super().__init__(mensaje)
        self.estado_http = estado_http
        self.codigo = codigo
        self.mensaje = mensaje
