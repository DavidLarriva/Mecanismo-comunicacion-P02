# Prueba REST: pedido duplicado por reintento

- **Mecanismo:** REST (API de Pedidos, `POST /pedidos`)
- **Escenarios de calidad relacionados:**
  - **2. Confiabilidad / idempotencia** (principal): si el cliente reintenta la misma solicitud, no se crean dos pedidos con la misma clave.
  - **1. Disponibilidad:** si Kafka no está disponible al crear un pedido, la API no afirma falsamente que lo registró.
- **Responsable:** Integrante 1 (David)
- **Fecha de ejecución:** 2026-11-15
- **Entorno:** macOS, Python 3.14.5, Kafka `apache/kafka:3.8.1` en Docker (modo KRaft, `pedido.creado` con 3 particiones), API en `localhost:8000`.

## Objetivo

Comprobar que, aunque el cliente envíe la misma solicitud varias veces (por un reintento tras un timeout, un doble clic o una red inestable), el sistema registra **un solo pedido** y publica **un solo evento** `pedido.creado`. Además, comprobar que ante una caída de Kafka la API responde de forma controlada y que el reintento posterior no duplica el pedido.

## Diseño de la prueba

El cliente envía la cabecera `Idempotency-Key`. La API guarda cada clave con un lock en un registro en memoria, con tres estados posibles:

| Estado de la clave | Qué responde la API a otra solicitud con la misma clave |
|---|---|
| `EN_PROCESO` (la primera solicitud espera la confirmación de Kafka) | `409 SOLICITUD_EN_CURSO` |
| `COMPLETADA` | `200` con la respuesta guardada, sin crear otro pedido |
| `LIBERADA` (Kafka falló y se respondió `503`) | Se procesa de nuevo **reutilizando el mismo `idPedido`** |

Si la clave llega con un cuerpo distinto (se compara un hash SHA-256 del cuerpo), la respuesta es `409 CLAVE_REUTILIZADA`.

Se ejecutaron dos partes:

1. **Casos automáticos** con el script `backend/api_rest/pruebas/prueba_idempotencia.py`, que envía las solicitudes y luego **lee el topic `pedido.creado` desde el inicio** para contar cuántos eventos hay por pedido. Esa es la evidencia de que existe un solo pedido: lo que vieron cocina, caja, facturación y estadísticas.
2. **Kafka caído**, a mano con `docker stop kafka`, dos `POST` con la misma clave, `docker start kafka` y un reintento.

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
(repetir el mismo comando antes de que pasen 5 s, luego)
```bash
docker start kafka
```
y volver a enviar el mismo `curl`.

## Resultado obtenido (real)

Salidas completas en `docs/pruebas/evidencias/rest/`.

### Parte 1: casos automáticos

| Caso | Solicitudes | Resultado |
|---|---|---|
| 1. Reintento secuencial | Misma solicitud y misma clave, dos veces | `201` (132 ms) y luego `200` (1 ms), **el mismo `idPedido`** `c3137324-…` |
| 2. Envíos simultáneos | 5 hilos liberados a la vez con la misma clave | 1 × `201` y 4 × `409 SOLICITUD_EN_CURSO`; **1 solo `idPedido`** (`3669ef50-…`) |
| 3. Clave reutilizada | La clave del caso 1 con otro pedido | `409 CLAVE_REUTILIZADA` |
| 4. Sin clave | Sin cabecera `Idempotency-Key` | `400 FALTA_IDEMPOTENCY_KEY` |

Verificación en Kafka, leyendo `pedido.creado` desde el inicio:

```
3669ef50-e2b3-495e-85d7-343a824b1e9c: 1 evento(s)
c3137324-70e3-4402-bcec-08d883fda45c: 1 evento(s)
pedidos creados: 2  eventos publicados: 2
```

**7 solicitudes de creación** con dos claves produjeron **2 pedidos y 2 eventos**.

El caso 2 se repitió tres veces más con la API en marcha y el resultado fue el mismo: `{409: 4, 201: 1}`. En una ejecución anterior del mismo script (antes de agregar la reutilización del `idPedido`, que no cambia la lógica de concurrencia) el reparto fue distinto: `{201: 1, 200: 3, 409: 1}`. Ese resultado se explica en la sección siguiente.

### Parte 2: Kafka caído

| Paso | Resultado |
|---|---|
| `docker stop kafka` | Broker detenido |
| Envío A con la clave `caida-191309` | `503 SERVICIO_NO_DISPONIBLE` en **5,00 s** |
| Envío B, misma clave, 1 s después de A | `409 SOLICITUD_EN_CURSO` en **1,3 ms** (A seguía esperando a Kafka) |
| `GET /menu` con Kafka caído | `200` en 1,2 ms: la API sigue respondiendo |
| `docker start kafka` y reintento con la misma clave | `201` en 25 ms, `idPedido` **`52a1efde-…`** |

Log de la API (extracto):

```
19:13:10,961 api.servicio  Clave caida-191309 en proceso: solicitud rechazada
19:13:14,969 api.servicio  Kafka no confirmó el pedido 52a1efde-b4f2-43a2-b6d4-464b0004c273: ... KafkaTimeoutError('Timeout after waiting for 5.0 secs.')
19:13:21,308 api.servicio  Pedido 52a1efde-b4f2-43a2-b6d4-464b0004c273 creado con clave caida-191309
```

El intento fallido y el reintento usaron **el mismo `idPedido`**, y en `pedido.creado` ese pedido tiene **1 evento**.

## Análisis (conceptos del Capítulo 2)

**Comunicación síncrona y reintentos.** REST es síncrono: el cliente queda bloqueado esperando la respuesta. Si la respuesta no llega (timeout, red caída), el cliente no sabe si el pedido se registró, y lo razonable es reintentar. Eso es la semántica **al menos una vez** del lado del cliente, que por sí sola duplicaría pedidos.

**Idempotencia del servidor: "a lo sumo una vez".** La clave de idempotencia permite al servidor reconocer el reintento y descartar el duplicado, que es la semántica **a lo sumo una vez** del Capítulo 2. Combinadas (el cliente reintenta y el servidor filtra), el efecto sobre el registro es que cada pedido queda creado **exactamente una vez**. Así lo muestran los casos 1 y 2: varias solicitudes, un evento.

**Concurrencia y el estado `EN_PROCESO`.** Sin el estado intermedio, dos solicitudes que llegan juntas pasarían la verificación "¿existe la clave?" antes de que cualquiera la guarde, y las dos publicarían. Por eso la clave se reserva bajo un lock **antes** de publicar en Kafka. El resultado distinto entre ejecuciones del caso 2 (`4 × 409` en unas, `1 × 409 + 3 × 200` en otra) no es un error: depende de en qué momento llega cada solicitud.
- Las que llegan mientras la primera espera la confirmación de Kafka (unos 4 ms) reciben `409`.
- Las que llegan después reciben `200` con la respuesta guardada.

En ambos casos el invariante se cumple: un solo pedido.

**Acoplamiento temporal con Kafka (disponibilidad).** La API espera la confirmación del broker (`acks='all'`) antes de responder `201`. Así nunca dice "registrado" sin que el evento esté en Kafka. Con el broker caído paga ese acoplamiento con 5 s de espera y un `503`, pero no se cae: `GET /menu` siguió respondiendo en 1 ms, y la API se reconectó sola cuando Kafka volvió. Se eligió esto frente a aceptar el pedido y publicarlo después porque, con datos en memoria, un pedido aceptado y no publicado se perdería al reiniciar la API.

**Ambigüedad del timeout y reutilización del `idPedido`.** Que Kafka no confirme en 5 s no prueba que el mensaje no se escribió: pudo escribirse y perderse solo la confirmación. Si el reintento generara un `idPedido` nuevo, los consumidores verían dos pedidos distintos. Por eso la clave liberada conserva su `idPedido`, y el reintento publica con el mismo. Si el primer mensaje sí llegó, los consumidores lo descartan, porque deduplican `pedido.creado` por `idPedido` (contrato, sección 5). La parte 2 lo confirma: mismo `idPedido` en el intento fallido y en el reintento, y un solo evento.

## Conclusión

Se cumple el escenario 2: **ningún reintento ni envío simultáneo creó un segundo pedido.** También se cumple el escenario 1: con Kafka caído, la API respondió `503` en el tiempo acordado, siguió atendiendo otras solicitudes y el reintento posterior funcionó sin duplicar.

## Limitaciones

- **Registro de claves en memoria.** Si la API se reinicia, se pierden las claves, y un reintento posterior al reinicio crearía un pedido nuevo. Con persistencia (por ejemplo Redis o una base de datos con índice único sobre la clave) se resolvería.
- **Una sola instancia de la API.** El lock es local al proceso. Con varias réplicas detrás de un balanceador, el registro de claves tendría que ser compartido.
- **Costo de la espera.** Mientras Kafka está caído, cada creación tarda 5 s en responder `503`.
