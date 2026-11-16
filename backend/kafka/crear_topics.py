# [IA]
# Revisado por: Carolina Fortmann
"""Crea los topics de config.py con las particiones del contrato (seguro de repetir).

Uso (desde la raíz del repo):  python -m backend.kafka.crear_topics
"""
import sys
import time

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError

from . import config


def conectar(intentos=10):
    for i in range(1, intentos + 1):
        try:
            return KafkaAdminClient(bootstrap_servers=config.BOOTSTRAP_SERVERS, bootstrap_timeout_ms=5000)
        except KafkaError:
            print(f"Esperando a Kafka en {config.BOOTSTRAP_SERVERS}... ({i}/{intentos})")
            time.sleep(2)
    return None


def main():
    admin = conectar()
    if admin is None:
        print("No se pudo conectar con Kafka. ¿Está corriendo el contenedor? (docker ps)")
        return 1
    try:
        for nombre, particiones in config.TOPICS.items():
            try:
                admin.create_topics([NewTopic(nombre, num_partitions=particiones, replication_factor=1)])
                print(f"creado:    {nombre} ({particiones} particiones)")
            except TopicAlreadyExistsError:
                print(f"ya existe: {nombre}")
    finally:
        admin.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
