# Escenarios de atributos de calidad

- **Responsable del documento:** Integrante 1 (David)
- **Formato:** cada escenario se describe con fuente, estímulo, artefacto, entorno, respuesta y medida de la respuesta, como en la Práctica 01.

## Priorización

Cada escenario se calificó en dos dimensiones, al estilo del árbol de utilidad:
- **Importancia** para la cafetería: qué tan grave es para el negocio que falle.
- **Dificultad** técnica para lograrlo.

Se priorizaron primero los que tienen alta importancia y alta dificultad, porque son los que justifican los mecanismos elegidos.

| Prioridad | Atributo | Escenario resumido | Importancia | Dificultad | Mecanismo que lo resuelve |
|---|---|---|---|---|---|
| 1 | Disponibilidad | Kafka no está disponible cuando un estudiante crea un pedido | Alta | Alta | REST + Kafka |
| 2 | Confiabilidad (idempotencia) | El cliente reintenta el mismo pedido | Alta | Alta | REST |
| 3 | Resiliencia | Caja no responde cuando facturación consulta el pago | Alta | Media | gRPC |
| 4 | Rendimiento | La API no espera a cocina, caja ni facturación | Media | Media | Kafka |
| 5 | Recuperación del cliente | El estudiante pierde la conexión WebSocket | Media | Media | WebSocket + REST |
| 6 | Interoperabilidad | Un servicio publica un evento que no cumple el contrato | Media | Baja | Kafka + contrato |

**Por qué este orden.**
- **1 y 2:** sin ellos el sistema miente o duplica cobros. Un pedido "registrado" que nadie cocina, o un pedido creado dos veces (dos cobros pendientes en caja), afecta directamente al estudiante y a la cafetería.
- **3:** una facturación bloqueada detendría el flujo interno.
- **4 y 5:** afectan la experiencia, pero no la exactitud de los datos.
- **6:** es la base de la integración entre los cuatro integrantes. Es menos difícil porque se resuelve con un contrato compartido y validación.

---

## Escenario 1: Disponibilidad ante la caída de Kafka

| Parte | Descripción |
|---|---|
| Fuente | Falla de infraestructura: el broker Kafka se detiene |
| Estímulo | Un estudiante envía `POST /pedidos` mientras Kafka no responde |
| Artefacto | API de Pedidos (REST) y su publicación en `pedido.creado` |
| Entorno | Operación normal en hora pico; los demás servicios siguen en marcha |
| Respuesta | La API **no** responde "registrado": espera la confirmación de Kafka, y si no llega, responde `503 SERVICIO_NO_DISPONIBLE`, descarta el pedido y libera la clave para que el cliente reintente. El resto de la API sigue atendiendo, y cuando Kafka vuelve la API se reconecta sola |
| Medida de la respuesta | (a) 0 pedidos respondidos con `201` sin evento en Kafka; (b) respuesta `503` en ≤ 5 s; (c) los demás endpoints responden con normalidad durante la caída; (d) el primer reintento después de recuperar Kafka responde `201` sin reiniciar la API |
| Verificación | `docs/pruebas/prueba-rest.md`, parte 2: `503` en 5,00 s, `GET /menu` en 1,2 ms durante la caída, y reintento `201` en 25 ms con el mismo `idPedido` y un solo evento |

## Escenario 2: Confiabilidad ante reintentos (idempotencia)

| Parte | Descripción |
|---|---|
| Fuente | El cliente web o móvil del estudiante |
| Estímulo | La misma solicitud de pedido llega varias veces: un reintento tras un timeout, un doble clic, o varios envíos casi simultáneos con la misma `Idempotency-Key` |
| Artefacto | API de Pedidos: registro de claves de idempotencia |
| Entorno | Operación normal, con red inestable del lado del cliente |
| Respuesta | Se crea **un solo** pedido y se publica **un solo** `pedido.creado`. Un reintento recibe la misma respuesta (`200`), y una solicitud que llega mientras la primera está en proceso recibe `409 SOLICITUD_EN_CURSO` |
| Medida de la respuesta | 1 evento en `pedido.creado` por cada `Idempotency-Key`, sin importar cuántas veces se envíe; 0 pedidos duplicados |
| Verificación | `docs/pruebas/prueba-rest.md`, parte 1: 7 solicitudes con 2 claves produjeron 2 pedidos y 2 eventos, y 5 envíos simultáneos produjeron un solo pedido |

## Escenario 3: Resiliencia cuando Caja no responde

| Parte | Descripción |
|---|---|
| Fuente | Servicio interno: Caja (servidor gRPC) |
| Estímulo | Facturación llama a `ConfirmarPago` y Caja está detenida o tarda más de lo permitido |
| Artefacto | Facturación (cliente gRPC) y Caja (servidor gRPC) |
| Entorno | Operación normal; Caja caída o sobrecargada |
| Respuesta | Facturación corta la llamada por deadline, no se queda bloqueada, reintenta según la política acordada y, si se agotan los intentos, publica `FACTURACION_PENDIENTE`. El resto del flujo (cocina, notificaciones, API) no se ve afectado |
| Medida de la respuesta | Cada llamada termina en ≤ 2 s; como máximo 6 intentos cada 10 s; 0 facturas emitidas sin pago confirmado; 0 facturas duplicadas por pedido |
| Verificación | `docs/pruebas/prueba-grpc.md` (Integrante 3) |

## Escenario 4: Rendimiento al recibir pedidos

| Parte | Descripción |
|---|---|
| Fuente | Estudiantes en hora pico |
| Estímulo | Llegan pedidos nuevos mientras cocina, caja o facturación están ocupados o detenidos |
| Artefacto | API de Pedidos y Kafka (`pedido.creado`) |
| Entorno | Carga normal o alta; uno o más consumidores detenidos temporalmente |
| Respuesta | La API responde en cuanto Kafka confirma el evento, sin esperar a ningún servicio interno. Los eventos quedan guardados en Kafka y cada servicio los procesa a su ritmo, incluso después de volver a arrancar |
| Medida de la respuesta | `POST /pedidos` responde en < 200 ms aunque no haya ningún consumidor en marcha; 0 eventos perdidos cuando un consumidor se reinicia |
| Verificación | `docs/pruebas/prueba-rest.md`: con cocina, caja y facturación **detenidas**, la API respondió `201` en 132 ms en el primer pedido (incluye conectar el productor) y en 5 ms en los siguientes. El procesamiento posterior de los eventos está en `docs/pruebas/prueba-kafka.md` (Integrante 2) |

## Escenario 5: Recuperación del cliente tras una desconexión

| Parte | Descripción |
|---|---|
| Fuente | El navegador o celular del estudiante |
| Estímulo | La conexión WebSocket se pierde (cambio de red, pestaña cerrada) mientras el pedido se está preparando |
| Artefacto | Servidor de notificaciones (WebSocket) y API de Pedidos (`GET /pedidos/{id}`) |
| Entorno | El pedido avanza en el backend mientras el cliente está desconectado |
| Respuesta | El backend sigue procesando sin depender del cliente. Al volver, el cliente abre primero el WebSocket y se suscribe, y **después** consulta por REST el estado vigente; desde ese momento recibe los eventos nuevos |
| Medida de la respuesta | El cliente intenta reconectarse cada 3 s; tras reconectar muestra el estado correcto (el mismo que devuelve `GET`); 0 cambios de estado perdidos después de la reconexión |
| Verificación | `docs/pruebas/prueba-websockets.md` (Integrante 4) |

## Escenario 6: Interoperabilidad entre servicios

| Parte | Descripción |
|---|---|
| Fuente | Cualquier servicio productor (API, cocina, caja, facturación) |
| Estímulo | Se publica en `pedido.estado` un evento mal formado o con una combinación tipo/estado/origen no permitida (por ejemplo `LISTO` publicado por caja) |
| Artefacto | Consumidores de `pedido.estado` (API, notificaciones, estadísticas) |
| Entorno | Integración entre servicios desarrollados por personas distintas |
| Respuesta | El consumidor no aplica el evento, lo registra en el log (o lo envía a `pedido.dlq` si está mal formado) y sigue procesando los demás, sin detenerse ni corromper el estado del pedido |
| Medida de la respuesta | 0 estados modificados por eventos inválidos; el consumidor sigue procesando los siguientes mensajes; todos los eventos que publica la API cumplen `contracts/pedido.schema.json` |
| Verificación | Pruebas automatizadas: `backend/api_rest/tests` (combinación inválida ignorada, estado que no retrocede) y `backend/kafka/tests/test_eventos.py` (Integrante 2) |

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
