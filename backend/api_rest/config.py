import os

GRUPO_CONSUMIDOR = os.getenv("KAFKA_GRUPO_API", "api")
VENCIMIENTO_CLAVE_S = float(os.getenv("VENCIMIENTO_CLAVE_S", "30"))
HOST = os.getenv("API_HOST", "0.0.0.0")
PUERTO = int(os.getenv("API_PUERTO", "8000"))
