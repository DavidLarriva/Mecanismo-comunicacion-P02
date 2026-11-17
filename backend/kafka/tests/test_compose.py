# [IA]
# Revisado por: Carolina Fortmann
"""Vigila que infra/docker-compose.yml cree los mismos topics que backend/kafka/config.py."""
import re
from pathlib import Path

from .. import config

COMPOSE = Path(__file__).resolve().parents[3] / "infra" / "docker-compose.yml"


def test_compose_crea_los_topics_de_config_con_sus_particiones():
    texto = COMPOSE.read_text(encoding="utf-8")
    for topic, particiones in config.TOPICS.items():
        patron = rf"--topic {re.escape(topic)} --partitions {particiones} "
        assert re.search(patron, texto), f"el compose no crea '{topic}' con {particiones} particiones"


def test_compose_tiene_healthcheck_y_espera_a_kafka():
    texto = COMPOSE.read_text(encoding="utf-8")
    assert "healthcheck:" in texto and "condition: service_healthy" in texto
