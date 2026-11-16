# Realizado con IA y revisado por David Larriva
import argparse
import json
import threading
import time
from collections import Counter

import httpx
from kafka import KafkaConsumer, TopicPartition

from backend.kafka import config as kafka_config

CUERPO = {
    "idEstudiante": "0102030405",
    "items": [
        {"idProducto": "SAN-01", "cantidad": 1},
        {"idProducto": "JUG-01", "cantidad": 2},
    ],
}
OTRO_CUERPO = {
    "idEstudiante": "0102030405",
    "items": [{"idProducto": "CAF-01", "cantidad": 1}],
}


def enviar(cliente, clave, cuerpo=CUERPO):
    cabeceras = {"Idempotency-Key": clave} if clave is not None else {}
    inicio = time.perf_counter()
    respuesta = cliente.post("/pedidos", json=cuerpo, headers=cabeceras)
    milisegundos = (time.perf_counter() - inicio) * 1000
    datos = respuesta.json()
    return respuesta.status_code, datos.get("idPedido") or datos.get("error"), milisegundos


def mostrar(etiqueta, resultado):
    estado, detalle, milisegundos = resultado
    print(f"  {etiqueta:<34} HTTP {estado}  {detalle}  ({milisegundos:.0f} ms)")


def contar_eventos(ids_pedido, servidores):
    consumidor = KafkaConsumer(
        bootstrap_servers=servidores,
        group_id=None,
        enable_auto_commit=False,
        bootstrap_timeout_ms=5000,
    )
    particiones = [
        TopicPartition(kafka_config.TOPIC_PEDIDO_CREADO, p)
        for p in consumidor.partitions_for_topic(kafka_config.TOPIC_PEDIDO_CREADO)
    ]
    consumidor.assign(particiones)
    consumidor.seek_to_beginning(*particiones)
    finales = consumidor.end_offsets(particiones)
    conteo = Counter()
    while any(consumidor.position(tp) < finales[tp] for tp in particiones):
        for registros in consumidor.poll(timeout_ms=1000).values():
            for registro in registros:
                evento = json.loads(registro.value.decode("utf-8"))
                if evento.get("idPedido") in ids_pedido:
                    conteo[evento["idPedido"]] += 1
    consumidor.close()
    return conteo


def principal():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://localhost:8000")
    parser.add_argument("--kafka", default=kafka_config.BOOTSTRAP_SERVERS)
    parser.add_argument("--concurrentes", type=int, default=5)
    argumentos = parser.parse_args()

    prefijo = time.strftime("prueba-%H%M%S")
    ids_creados = set()

    with httpx.Client(base_url=argumentos.url, timeout=15) as cliente:
        print("Caso 1: la misma solicitud enviada dos veces (reintento del cliente)")
        clave = f"{prefijo}-secuencial"
        primera = enviar(cliente, clave)
        segunda = enviar(cliente, clave)
        mostrar("primer envío", primera)
        mostrar("reintento con la misma clave", segunda)
        ids_creados.update({primera[1], segunda[1]})
        print(f"  mismo idPedido en ambas respuestas: {primera[1] == segunda[1]}")

        print(f"\nCaso 2: {argumentos.concurrentes} envíos simultáneos con la misma clave")
        clave = f"{prefijo}-concurrente"
        barrera = threading.Barrier(argumentos.concurrentes)
        resultados = []

        def tarea():
            barrera.wait()
            resultados.append(enviar(cliente, clave))

        hilos = [threading.Thread(target=tarea) for _ in range(argumentos.concurrentes)]
        for hilo in hilos:
            hilo.start()
        for hilo in hilos:
            hilo.join()
        for indice, resultado in enumerate(sorted(resultados, key=lambda r: r[2]), start=1):
            mostrar(f"envío {indice}", resultado)
        exitosos = {r[1] for r in resultados if r[0] in (200, 201)}
        ids_creados.update(exitosos)
        print(f"  códigos: {dict(Counter(r[0] for r in resultados))}")
        print(f"  idPedido distintos entre las respuestas exitosas: {len(exitosos)}")

        print("\nCaso 3: la misma clave con un pedido distinto")
        mostrar("cuerpo distinto, clave del caso 1", enviar(cliente, f"{prefijo}-secuencial", OTRO_CUERPO))

        print("\nCaso 4: solicitud sin Idempotency-Key")
        mostrar("sin cabecera", enviar(cliente, None))

    print("\nVerificación en Kafka (topic pedido.creado)")
    conteo = contar_eventos(ids_creados, argumentos.kafka)
    for id_pedido in sorted(ids_creados):
        print(f"  {id_pedido}: {conteo[id_pedido]} evento(s)")
    print(f"  pedidos creados: {len(ids_creados)}  eventos publicados: {sum(conteo.values())}")


if __name__ == "__main__":
    principal()
