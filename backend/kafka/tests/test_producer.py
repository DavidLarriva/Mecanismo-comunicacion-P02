import pytest

from ..producer import EventProducer, KafkaNoDisponible
from .fakes import FakeKafkaProducer, pedido_api


def test_publica_y_devuelve_particion_y_offset():
    fake = FakeKafkaProducer("ok")
    assert EventProducer(producer=fake).publicar_pedido_creado(pedido_api("x1")) == (2, 7)
    topic, key, valor = fake.enviados[0]
    assert (topic, key) == ("pedido.creado", "x1") and valor["idPedido"] == "x1"


@pytest.mark.parametrize("modo", ["timeout", "error"])
def test_sin_confirmacion_lanza_kafka_no_disponible(modo):
    with pytest.raises(KafkaNoDisponible):
        EventProducer(producer=FakeKafkaProducer(modo)).publicar_pedido_creado(pedido_api())


def test_publicar_estado_usa_topic_estado_y_clave_idPedido():
    fake = FakeKafkaProducer("ok")
    EventProducer(producer=fake).publicar_estado("x1", "PREPARACION", "LISTO", "cocina")
    assert fake.enviados[0][:2] == ("pedido.estado", "x1")


def test_pedido_invalido_ni_siquiera_se_envia():
    fake = FakeKafkaProducer("ok")
    with pytest.raises(ValueError):
        EventProducer(producer=fake).publicar_pedido_creado({"idPedido": "x"})
    assert fake.enviados == []


def test_sin_broker_real_falla_rapido_y_controlado():
    """Usa el KafkaProducer REAL contra un puerto cerrado."""
    import time
    prod = EventProducer(bootstrap_servers="localhost:1", timeout_s=1)
    t = time.time()
    with pytest.raises(KafkaNoDisponible):
        prod.publicar_pedido_creado(pedido_api())
    assert time.time() - t < 5
