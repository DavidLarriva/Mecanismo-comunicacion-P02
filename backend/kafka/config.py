# [IA]
# Revisado por: Carolina Fortmann
"""Configuración central de la capa Kafka (según contratos.md v1.0, sección 5)."""
import os

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# --- Topics (nombre -> particiones). Los dos primeros vienen del contrato. ---
TOPIC_PEDIDO_CREADO = "pedido.creado"
TOPIC_PEDIDO_ESTADO = "pedido.estado"
# PROPUESTA (no está en el contrato v1.0; hay que registrarla en la sección 9):
TOPIC_DLQ = "pedido.dlq"

TOPICS = {TOPIC_PEDIDO_CREADO: 3, TOPIC_PEDIDO_ESTADO: 3, TOPIC_DLQ: 1}

# --- Consumer groups (contrato): un grupo distinto por servicio => todos reciben todo ---
GRUPOS_PEDIDO_CREADO = {"cocina": "cocina", "caja": "caja", "estadisticas": "estadisticas"}
GRUPOS_PEDIDO_ESTADO = {"api": "api", "notificaciones": "notificaciones",
                        "estadisticas": "estadisticas", "facturacion": "facturacion"}
GRUPO_MONITOR = "monitor-estados"   # solo para el stub de pruebas (no es del contrato)

# --- Productor: la API espera la confirmación 5 s (contrato) ---
PRODUCER_TIMEOUT_S = float(os.getenv("KAFKA_PRODUCER_TIMEOUT_S", "5"))

# --- Consumidores ---
MAX_REINTENTOS = int(os.getenv("KAFKA_MAX_REINTENTOS", "3"))
BACKOFF_BASE_S = float(os.getenv("KAFKA_BACKOFF_BASE_S", "1"))   # esperas: 1 s, 2 s, 4 s...
MAX_REGISTRO_PROCESADOS = 10_000

# --- Servicios propios (contrato: Estadísticas en 8002) ---
PUERTO_ESTADISTICAS = int(os.getenv("PUERTO_ESTADISTICAS", "8002"))
COCINA_DEMORA_S = float(os.getenv("COCINA_DEMORA_S", "10"))   # tiempo simulado de preparación
