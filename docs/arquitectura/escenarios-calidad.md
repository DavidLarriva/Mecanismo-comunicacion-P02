# Escenarios de atributos de calidad

- **Responsable del documento:** Integrante 1 (David)
- **Formato:** igual que en la Práctica 01, cada escenario tiene fuente, estímulo, artefacto, entorno, respuesta y medida de la respuesta.

## Priorización

Como en el árbol de utilidad, calificamos cada escenario por su **importancia** para la cafetería, es decir qué tan grave es que falle, y por la **dificultad** técnica de lograrlo. Primero van los que tienen las dos altas, porque son los que justifican los mecanismos elegidos.

| Prioridad | Atributo | Escenario resumido | Importancia | Dificultad | Mecanismo que lo resuelve |
|---|---|---|---|---|---|
| 1 | Disponibilidad | Kafka no está disponible cuando un estudiante crea un pedido | Alta | Alta | REST + Kafka |
| 2 | Confiabilidad (idempotencia) | El cliente reintenta el mismo pedido | Alta | Alta | REST |
| 3 | Resiliencia | Caja no responde cuando facturación consulta el pago | Alta | Media | gRPC |
| 4 | Rendimiento | La API no espera a cocina, caja ni facturación | Media | Media | Kafka |
| 5 | Recuperación del cliente | El estudiante pierde la conexión WebSocket | Media | Media | WebSocket + REST |
| 6 | Interoperabilidad | Un servicio publica un evento que no cumple el contrato | Media | Baja | Kafka + contrato |

**Por qué este orden.**
- **1 y 2.** Sin ellos el sistema miente o cobra dos veces. Un pedido registrado que nadie cocina, o uno creado dos veces con dos cobros en caja, afecta directamente al estudiante y a la cafetería.
- **3.** Si facturación se queda bloqueada se detiene el flujo interno.
- **4 y 5.** Afectan la experiencia del usuario, pero no la exactitud de los datos.
- **6.** Es la base para que encaje el trabajo de los cuatro, y es menos difícil porque se resuelve con un contrato compartido y validación.

---

## Escenario 1: Disponibilidad ante la caída de Kafka

| Parte | Descripción |
|---|---|
| Fuente | Falla de infraestructura, el broker Kafka se detiene |
| Estímulo | Un estudiante envía `POST /pedidos` mientras Kafka no responde |
| Artefacto | API de Pedidos (REST) y su publicación en `pedido.creado` |
| Entorno | Operación normal en hora pico, con los demás servicios funcionando |
| Respuesta | La API **no** dice que el pedido quedó registrado. Espera la confirmación de Kafka y si no llega responde `503 SERVICIO_NO_DISPONIBLE`, descarta el pedido y libera la clave para que el cliente reintente. El resto de la API sigue atendiendo y se reconecta sola cuando Kafka vuelve |
| Medida de la respuesta | (a) 0 pedidos respondidos con `201` sin evento en Kafka. (b) Respuesta `503` en ≤ 5 s. (c) Los demás endpoints responden normalmente durante la caída. (d) El primer reintento después de recuperar Kafka responde `201` sin reiniciar la API |
| Verificación | Parte 2 de `docs/pruebas/prueba-rest.md`. Dio `503` en 5,00 s, `GET /menu` respondió en 1,2 ms durante la caída y el reintento dio `201` en 25 ms con el mismo `idPedido` y un solo evento |

## Escenario 2: Confiabilidad ante reintentos (idempotencia)

| Parte | Descripción |
|---|---|
| Fuente | El cliente web o móvil del estudiante |
| Estímulo | La misma solicitud llega varias veces por un reintento tras un timeout, un doble clic o envíos casi simultáneos con la misma `Idempotency-Key` |
| Artefacto | API de Pedidos, en su registro de claves de idempotencia |
| Entorno | Operación normal, con una red inestable del lado del cliente |
| Respuesta | Se crea **un solo** pedido y se publica **un solo** `pedido.creado`. Un reintento recibe la misma respuesta con `200`, y una solicitud que llega mientras la primera sigue en proceso recibe `409 SOLICITUD_EN_CURSO` |
| Medida de la respuesta | 1 evento en `pedido.creado` por cada `Idempotency-Key`, sin importar cuántas veces se envíe, y 0 pedidos duplicados |
| Verificación | Parte 1 de `docs/pruebas/prueba-rest.md`. 7 solicitudes con 2 claves dejaron 2 pedidos y 2 eventos, y 5 envíos simultáneos dejaron un solo pedido |

## Escenario 3: Resiliencia cuando Caja no responde

| Parte | Descripción |
|---|---|
| Fuente | Un servicio interno, Caja, que es el servidor gRPC |
| Estímulo | Facturación llama a `ConfirmarPago` y Caja está detenida o tarda más de lo permitido |
| Artefacto | Facturación como cliente gRPC y Caja como servidor gRPC |
| Entorno | Operación normal, con Caja caída o sobrecargada |
| Respuesta | Facturación corta la llamada por deadline y no se queda bloqueada. Reintenta según la política acordada y, si se acaban los intentos, publica `FACTURACION_PENDIENTE`. Cocina, notificaciones y la API siguen funcionando |
| Medida de la respuesta | Cada llamada termina en ≤ 2 s, con un máximo de 6 intentos cada 10 s. 0 facturas emitidas sin pago confirmado y 0 facturas duplicadas por pedido |
| Verificación | `docs/pruebas/prueba-grpc.md` (Integrante 3) |

## Escenario 4: Rendimiento al recibir pedidos

| Parte | Descripción |
|---|---|
| Fuente | Estudiantes en hora pico |
| Estímulo | Llegan pedidos nuevos mientras cocina, caja o facturación están ocupados o detenidos |
| Artefacto | API de Pedidos y Kafka (`pedido.creado`) |
| Entorno | Carga normal o alta, con uno o más consumidores detenidos por un tiempo |
| Respuesta | La API responde apenas Kafka confirma el evento, sin esperar a ningún servicio interno. Los eventos quedan en Kafka y cada servicio los procesa a su ritmo, incluso después de reiniciar |
| Medida de la respuesta | `POST /pedidos` responde `201` aunque los consumidores estén detenidos, y no se pierde ningún evento cuando un consumidor se reinicia |
| Verificación | `docs/pruebas/prueba-kafka.md` (Integrante 2). Con Cocina **detenida** la API respondió `201` y el pedido quedó guardado en Kafka. Al reiniciar, Cocina lo procesó y publicó `EN_PREPARACION` y `LISTO` en los offsets 8 y 9, sin perder el evento |

## Escenario 5: Recuperación del cliente tras una desconexión

| Parte | Descripción |
|---|---|
| Fuente | El navegador o el celular del estudiante |
| Estímulo | La conexión WebSocket se pierde mientras el pedido se está preparando, por ejemplo por un cambio de red o porque se cerró la pestaña |
| Artefacto | Servidor de notificaciones (WebSocket) y API de Pedidos (`GET /pedidos/{id}`) |
| Entorno | El pedido sigue avanzando en el backend mientras el cliente está desconectado |
| Respuesta | El backend sigue procesando sin depender del cliente. Al volver, el cliente abre el WebSocket y se suscribe, **después** consulta el estado por REST y desde ahí recibe los eventos nuevos |
| Medida de la respuesta | El cliente intenta reconectarse cada 3 s. Al reconectarse muestra el estado correcto, el mismo que devuelve `GET`, y no pierde ningún cambio de estado posterior |
| Verificación | `docs/pruebas/prueba-websockets.md` (Integrante 4) |

## Escenario 6: Interoperabilidad entre servicios

| Parte | Descripción |
|---|---|
| Fuente | Cualquier servicio que publica eventos, como la API, cocina, caja o facturación |
| Estímulo | Se publica en `pedido.estado` un evento mal formado o con una combinación de tipo, estado y origen que no está permitida, por ejemplo `LISTO` publicado por caja |
| Artefacto | Consumidores de `pedido.estado`, que son la API, notificaciones, facturación y estadísticas |
| Entorno | Integración entre servicios hechos por personas distintas |
| Respuesta | El consumidor no aplica el evento y lo anota en el log, o lo manda a `pedido.dlq` si está mal formado. Sigue con los demás sin detenerse ni dañar el estado del pedido |
| Medida de la respuesta | 0 estados cambiados por eventos inválidos, el consumidor sigue con los siguientes mensajes y todos los eventos que publica la API cumplen `contracts/pedido.schema.json` |
| Verificación | Pruebas automatizadas en `backend/api_rest/tests`, que revisan que se ignore una combinación inválida y que un estado no retroceda, y en `backend/kafka/tests/test_eventos.py` (Integrante 2). Además, el 16/11 validamos contra `contracts/pedido.schema.json` los 74 eventos reales de Kafka, 17 de `pedido.creado` y 57 de `pedido.estado` publicados por la API, Cocina, Caja y Facturación, y todos lo cumplen |

---

## Relación entre escenarios y pruebas

| Escenario | Prueba de comportamiento | Responsable |
|---|---|---|
| 1. Disponibilidad | `prueba-rest.md`, parte 2 (Kafka caído) | Integrante 1 |
| 2. Confiabilidad | `prueba-rest.md`, parte 1 (duplicados) | Integrante 1 |
| 3. Resiliencia | `prueba-grpc.md` (Caja detenida o lenta) | Integrante 3 |
| 4. Rendimiento | `prueba-kafka.md` (consumidor detenido) | Integrante 2 |
| 5. Recuperación del cliente | `prueba-websockets.md` (desconexión) | Integrante 4 |
| 6. Interoperabilidad | Pruebas automatizadas de eventos | Integrantes 1 y 2 |
