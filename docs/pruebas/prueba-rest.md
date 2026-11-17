# Prueba REST: pedido duplicado por reintento

- **Mecanismo:** REST, en la API de Pedidos (`POST /pedidos`)
- **Escenarios de calidad relacionados:**
  - **2. Confiabilidad e idempotencia**, que es el principal. Si el cliente reintenta la misma solicitud no se pueden crear dos pedidos con la misma clave.
  - **1. Disponibilidad.** Si Kafka no está disponible cuando se crea un pedido, la API no puede decir que lo registró.
- **Responsable:** Integrante 1 (David)
- **Fecha de ejecución:** 2026-11-15
- **Entorno:** macOS, Python 3.14.5, Kafka `apache/kafka:3.8.1` en Docker en modo KRaft, `pedido.creado` con 3 particiones y la API en `localhost:8000`.

## Objetivo

Comprobar que el sistema registra **un solo pedido** y publica **un solo evento** `pedido.creado` aunque el cliente mande la misma solicitud varias veces, por un reintento, un doble clic o una red inestable. También ver que, si Kafka se cae, la API responde de forma controlada y el reintento no duplica el pedido.

## Diseño de la prueba

El cliente envía la cabecera `Idempotency-Key` y la API guarda cada clave con un lock en un registro en memoria. La clave puede estar en tres estados.

| Estado de la clave | Qué responde la API a otra solicitud con la misma clave |
|---|---|
| `EN_PROCESO`, mientras la primera solicitud espera la confirmación de Kafka | `409 SOLICITUD_EN_CURSO` |
| `COMPLETADA` | `200` con la respuesta guardada, sin crear otro pedido |
| `LIBERADA`, cuando Kafka falló y se respondió `503` | Se procesa otra vez **con el mismo `idPedido`** |

Si la clave llega con otro cuerpo, detectado con un hash SHA-256, la respuesta es `409 CLAVE_REUTILIZADA`.

La prueba tuvo dos partes.

1. **Casos automáticos** con `backend/api_rest/pruebas/prueba_idempotencia.py`, que envía las solicitudes y después **lee el topic `pedido.creado` desde el inicio** para contar los eventos por pedido. Esa es la evidencia de que hay un solo pedido, porque es lo que ven cocina, caja, facturación y estadísticas.
2. **Kafka caído**, a mano. Se detiene Kafka con `docker stop kafka`, se mandan dos `POST` con la misma clave, se levanta con `docker start kafka` y se reintenta.

## Cómo reproducirla

Desde la raíz del repositorio:

```bash
docker compose -f infra/docker-compose.yml up -d
```
```bash
.venv/bin/python -m backend.api_rest.main
```
En otra terminal:
```bash
.venv/bin/python -m backend.api_rest.pruebas.prueba_idempotencia
```
Para la parte de Kafka caído:
```bash
docker stop kafka
```
```bash
curl -s -X POST localhost:8000/pedidos -H 'Content-Type: application/json' -H 'Idempotency-Key: caida-1' -d '{"idEstudiante":"0102030405","items":[{"idProducto":"EMP-01","cantidad":3}]}'
```
Se repite el mismo comando antes de que pasen 5 s y luego se levanta Kafka:
```bash
docker start kafka
```
Al final se vuelve a enviar el mismo `curl`.

## Resultado obtenido (real)

Las salidas completas están en `docs/pruebas/evidencias/rest/`.

### Parte 1: casos automáticos

| Caso | Solicitudes | Resultado |
|---|---|---|
| 1. Reintento secuencial | La misma solicitud con la misma clave, dos veces | `201` en 132 ms y luego `200` en 1 ms, con **el mismo `idPedido`** `c3137324-…` |
| 2. Envíos simultáneos | 5 hilos liberados al mismo tiempo con la misma clave | Un `201` y cuatro `409 SOLICITUD_EN_CURSO`, con **un solo `idPedido`** (`3669ef50-…`) |
| 3. Clave reutilizada | La clave del caso 1 con otro pedido | `409 CLAVE_REUTILIZADA` |
| 4. Sin clave | Sin la cabecera `Idempotency-Key` | `400 FALTA_IDEMPOTENCY_KEY` |

Al leer `pedido.creado` desde el inicio quedó esto.

```
3669ef50-e2b3-495e-85d7-343a824b1e9c: 1 evento(s)
c3137324-70e3-4402-bcec-08d883fda45c: 1 evento(s)
pedidos creados: 2  eventos publicados: 2
```

**7 solicitudes de creación** con dos claves dejaron **2 pedidos y 2 eventos**.

El caso 2 se repitió tres veces más y salió lo mismo, `{409: 4, 201: 1}`. En una ejecución anterior, antes de agregar la reutilización del `idPedido` (que no toca la concurrencia), el reparto fue `{201: 1, 200: 3, 409: 1}`. Lo explicamos en el análisis.

### Parte 2: Kafka caído

| Paso | Resultado |
|---|---|
| `docker stop kafka` | Broker detenido |
| Envío A con la clave `caida-191309` | `503 SERVICIO_NO_DISPONIBLE` en **5,00 s** |
| Envío B con la misma clave, 1 s después de A | `409 SOLICITUD_EN_CURSO` en **1,3 ms**, porque A seguía esperando a Kafka |
| `GET /menu` con Kafka caído | `200` en 1,2 ms, la API sigue respondiendo |
| `docker start kafka` y reintento con la misma clave | `201` en 25 ms, con `idPedido` **`52a1efde-…`** |

Extracto del log de la API:

```
19:13:10,961 api.servicio  Clave caida-191309 en proceso: solicitud rechazada
19:13:14,969 api.servicio  Kafka no confirmó el pedido 52a1efde-b4f2-43a2-b6d4-464b0004c273: ... KafkaTimeoutError('Timeout after waiting for 5.0 secs.')
19:13:21,308 api.servicio  Pedido 52a1efde-b4f2-43a2-b6d4-464b0004c273 creado con clave caida-191309
```

El intento que falló y el reintento usaron **el mismo `idPedido`**, y ese pedido tiene **1 evento** en `pedido.creado`.

## Análisis (conceptos del Capítulo 2)

**Comunicación síncrona y reintentos.** REST es síncrono y el cliente se queda esperando la respuesta. Si no llega por un timeout o una caída de red, no sabe si su pedido se registró y lo lógico es reintentar. Esa es la semántica **al menos una vez** del lado del cliente, que por sí sola duplicaría pedidos.

**Idempotencia en el servidor.** Con la clave el servidor reconoce el reintento y descarta el duplicado, que es la semántica **a lo sumo una vez** del Capítulo 2. Combinadas, el cliente reintenta y el servidor filtra, y cada pedido se crea **exactamente una vez**. Los casos 1 y 2 lo muestran, con varias solicitudes y un solo evento.

**Concurrencia y el estado `EN_PROCESO`.** Sin un estado intermedio, dos solicitudes que llegan juntas podrían ver que la clave no existe antes de que alguna la guarde, y las dos publicarían. Por eso la clave se reserva con un lock **antes** de publicar. Que el caso 2 diera cuatro `409` en unas ejecuciones y un `409` con tres `200` en otra no es un error, depende de cuándo llega cada solicitud. Las que llegan mientras la primera espera a Kafka, unos 4 ms, reciben `409`, y las que llegan después reciben `200` con la respuesta guardada. En los dos casos hay un solo pedido.

**Dependencia temporal de Kafka.** La API espera la confirmación del broker con `acks='all'` antes de responder `201`, así nunca dice que registró un pedido que no está en Kafka. Con el broker caído eso cuesta 5 s de espera y un `503`, pero la API no se cae. `GET /menu` siguió respondiendo en 1 ms y la API se reconectó sola. Lo preferimos a aceptar el pedido y publicarlo después porque, con datos en memoria, ese pedido se perdería si la API se reinicia.

**Ambigüedad del timeout y reutilización del `idPedido`.** Que Kafka no confirme en 5 s no prueba que el mensaje no se escribió, puede que solo se haya perdido la confirmación. Si el reintento generara otro `idPedido`, los consumidores verían dos pedidos. Por eso la clave liberada conserva su `idPedido` y, si el primer mensaje sí había llegado, los consumidores lo descartan porque deduplican `pedido.creado` por `idPedido` (contrato, sección 5). La parte 2 lo confirma con el mismo `idPedido` en ambos intentos y un solo evento.

## Conclusión

Se cumple el escenario 2, porque **ningún reintento ni envío simultáneo creó un segundo pedido**. También el escenario 1, ya que con Kafka caído la API respondió `503` a tiempo, siguió atendiendo y el reintento funcionó sin duplicar.

## Limitaciones

- **Registro de claves en memoria.** Si la API se reinicia se pierden las claves y un reintento posterior crearía otro pedido. Se resolvería guardándolas en Redis o en una base de datos con índice único.
- **Una sola instancia.** El lock vive dentro del proceso, así que con varias réplicas el registro de claves tendría que ser compartido.
- **Costo de la espera.** Con Kafka caído cada creación tarda 5 s en responder `503`.
