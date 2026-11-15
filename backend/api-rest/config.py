import os

KAFKA_SERVIDORES = os.getenv("KAFKA_SERVIDORES", "localhost:9092")
TOPIC_PEDIDO_CREADO = "pedido.creado"
TOPIC_PEDIDO_ESTADO = "pedido.estado"
GRUPO_CONSUMIDOR = os.getenv("KAFKA_GRUPO", "api")
TIMEOUT_KAFKA_S = float(os.getenv("TIMEOUT_KAFKA_S", "5"))
REINTENTO_CONSUMIDOR_S = float(os.getenv("REINTENTO_CONSUMIDOR_S", "5"))
VENCIMIENTO_CLAVE_S = float(os.getenv("VENCIMIENTO_CLAVE_S", "30"))
HOST = os.getenv("API_HOST", "0.0.0.0")
PUERTO = int(os.getenv("API_PUERTO", "8000"))
