# ADR-004: WebSockets para notificar el estado del pedido al estudiante

- **Estado:** Aceptado
- **Fecha:** 2026-11-15
- **Responsable:** Cinthy (Integrante 4)
- **Escenario de calidad relacionado:** recuperación del cliente (tras perder la conexión, recupera el estado por REST y recibe los eventos futuros)

## Contexto

El estudiante quiere saber **en el momento** en que su pedido cambia de estado, sobre todo cuando está `LISTO`, para no esperar en la fila. Los cambios los producen otros servicios (Cocina, Caja, Facturación y la API) y se publican de forma asíncrona en el topic `pedido.estado` de Kafka.

El cliente es una página web, y un navegador **no puede** conectarse a Kafka. Hace falta un canal entre el backend y el navegador que:

- permita que **el servidor tome la iniciativa** (server push), porque el estudiante no sabe cuándo cambiará el estado;
- tenga baja latencia y bajo costo por mensaje;
- funcione en cualquier navegador sin librerías especiales;
- tolere que el estudiante se desconecte (Wi-Fi del campus, pestaña cerrada) sin perder el estado.

## Decisión

Un servicio **Notificaciones** con un servidor **WebSocket** (`ws://localhost:8765`, Python con `asyncio` + `websockets`) que:

1. Consume `pedido.estado` con el consumer group propio `notificaciones`. El consumidor hereda de `ConsumerBase` (ADR-002) y corre en un hilo aparte, porque `kafka-python` es bloqueante.
2. Mantiene `idPedido → conjunto de conexiones` y reenvía cada evento **sin modificarlo**, solo a las conexiones suscritas a ese pedido.
3. Protocolo de mensajes (contrato, sección 7): `suscribir` / `desuscribir` del cliente; `suscrito` / `estado` / `error` del servidor.
4. Limpia las conexiones cerradas o que fallan al enviar, sin detenerse. Usa `ping_interval=20` para detectar conexiones muertas.
5. **No guarda estado de pedidos.** Si nadie está conectado, el aviso se descarta. La fuente de verdad es la API (ADR-001): el cliente, al conectarse o reconectarse, **primero se suscribe y después hace `GET /pedidos/{id}`**, y reintenta la conexión cada 3 s si se cae.

## Alternativas consideradas

| Alternativa | Por qué no |
|---|---|
| **Polling con REST** (`GET` cada N segundos) | Si N es chico, genera muchas peticiones inútiles (cada una con conexión y cabeceras HTTP completas); si N es grande, el aviso llega tarde. HTTP es iniciado por el cliente: no tiene server push nativo |
| **Server-Sent Events (SSE)** | Sí hace push del servidor, pero solo en un sentido. Para suscribirse a otro pedido habría que abrir otra conexión o usar REST aparte. WebSocket permite hacerlo por el mismo canal |
| **gRPC server streaming** | No es nativo en el navegador (requiere gRPC-Web y un proxy). gRPC queda para la llamada interna Facturación → Caja (ADR-003) |
| **Sockets TCP** | Muy bajo nivel: no hay handshake HTTP, el navegador no los puede abrir y habría que inventar el entramado de mensajes |
| **Que el navegador lea Kafka directamente** | Imposible desde un navegador, y expondría el broker interno a los clientes |

## Justificación (conceptos del Capítulo 2)

- **Canal full-duplex sobre una conexión TCP persistente.** Tras el handshake (`GET` con `Upgrade: websocket` → `101 Switching Protocols`) la conexión deja de ser HTTP y queda abierta. Tanto el cliente (suscribirse) como el servidor (avisar) pueden enviar en cualquier momento.
- **Server push nativo.** Es justo lo que necesita una notificación: el servidor avisa cuando ocurre el cambio, sin que el cliente pregunte.
- **Bajo overhead y latencia muy baja.** Las cabeceras solo se envían en el handshake; cada aviso es un mensaje pequeño. En la tabla comparativa de la unidad, WebSocket aparece como la mejor opción para "push y tiempo real" con clientes web.
- **Asíncrono para el cliente.** El estudiante no queda bloqueado esperando; recibe el resultado como un evento. Esto encaja con Kafka, que también es asíncrono: el WebSocket es la "última milla" del mismo flujo de eventos.
- **Separar aviso y estado.** El WebSocket es comunicación **transitoria** (si el receptor no está, el mensaje se pierde). La persistencia la dan Kafka (el evento queda en el log) y la API (el estado vigente). Por eso combinamos WebSocket para el aviso inmediato con REST para recuperar el estado.

## Consecuencias

**Positivas**
- El estudiante ve el cambio de estado al instante, sin recargar ni hacer polling.
- Notificaciones queda desacoplado de los demás servicios: solo conoce el contrato de `pedido.estado`. Si se cae, el resto del sistema sigue funcionando y, al volver, Kafka le entrega lo pendiente desde su offset.
- El servidor no tiene estado de negocio, así que reiniciarlo no corrompe nada.

**Negativas y riesgos (y cómo se manejan)**
- **Conexiones que se caen.** El cliente reintenta cada 3 s y repite suscribir y luego `GET`; el servidor limpia las conexiones muertas (ping cada 20 s).
- **Carrera entre el `GET` y los eventos.** Se resuelve con el orden "primero WebSocket, después `GET`". Además, la página solo deja avanzar los estados, para que un `GET` desactualizado no retroceda lo recibido.
- **Avisos perdidos mientras no hay conexión.** Es aceptable porque el estado se recupera por REST; no se reenvía el historial de avisos.
- **Conexiones abiertas consumen recursos** (memoria y descriptores por cliente). Para una cafetería es poco; a gran escala habría que repartir clientes entre varias instancias, y cada una necesitaría su propio consumer group o un canal compartido.
- **Sin autenticación.** Cualquiera que conozca un `idPedido` (UUID difícil de adivinar) puede suscribirse a él. Es una limitación conocida del prototipo.
- **Estado en memoria.** La deduplicación por `idEvento` se pierde al reiniciar; un evento repetido se reenviaría al cliente, que lo ignora porque los estados solo avanzan.

## Validación (lo que pasó al implementar)

- **Reutilización de la capa Kafka común.** El consumidor de Notificaciones hereda de `ConsumerBase` (ADR-002), igual que la API. Así tiene el mismo commit manual, la misma deduplicación por `idEvento` y la misma validación de combinaciones `tipo/estado/origen` que el resto de servicios.
- **Prueba con el sistema real** (Kafka, API, Cocina y Notificaciones, `docs/pruebas/prueba-websockets.md`): el cliente se desconectó antes de `LISTO`; al volver, el `GET` devolvió `LISTO` y el evento posterior `ENTREGADO` llegó por WebSocket. Se cumple la medida del Escenario 5.
- **Hallazgo inesperado: `localhost` en Windows.** En Windows, `localhost` se resolvía primero a IPv6 (`::1`). La API escucha solo en IPv4 (`0.0.0.0`), así que la página y el script recibían `WinError 10061`, mientras que el WebSocket sí conectaba porque la librería `websockets` escucha en IPv4 e IPv6. Se usa `127.0.0.1` en el cliente y en el script de prueba.
- **Orden de llegada entre `GET` y eventos.** En la prueba real, el primer `GET` ya devolvió `EN_PREPARACION` y luego llegó el mismo estado por push. La regla "cada estado solo avanza" evita que esto confunda o haga retroceder la pantalla.
