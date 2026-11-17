# ADR-004: WebSockets para notificar el estado del pedido al estudiante

- **Estado:** Aceptado
- **Fecha:** 2026-11-15
- **Responsable:** Cinthya Ramón
- **Escenario de calidad relacionado:** Recuperación del cliente. Si pierde la conexión, puede consultar nuevamente el estado del pedido mediante REST y continuar recibiendo las notificaciones de los siguientes cambios.

## Contexto

El estudiante necesita conocer los cambios de estado de su pedido sin tener que estar consultando constantemente la página. Esto es especialmente importante cuando el pedido pasa a `LISTO`, ya que así puede acercarse a retirarlo sin tener que esperar en la fila.

Los cambios de estado se generan en distintos servicios del sistema, como Cocina, Caja, Facturación y la API, y se publican de forma asíncrona en el topic `pedido.estado` de Kafka.

Como el cliente es una página web, el navegador no puede consumir directamente los mensajes de Kafka. Por esta razón, se necesita un mecanismo que permita comunicar los eventos desde el backend hasta el estudiante. Para elegirlo se tuvieron en cuenta los siguientes aspectos:

- El servidor debe poder enviar notificaciones cuando ocurra un cambio, sin esperar a que el estudiante realice una consulta.
- La comunicación debe tener poca latencia y evitar un consumo innecesario de recursos.
- Debe funcionar directamente desde los navegadores.
- Si el estudiante pierde la conexión, cierra la pestaña o cambia de red, debe poder recuperar el estado actual de su pedido cuando vuelva a conectarse.

## Decisión

Se decidió implementar un servicio independiente llamado **Notificaciones**, utilizando WebSockets para comunicar los cambios de estado al navegador.

El servidor WebSocket se implementó en Python, utilizando `asyncio` y la librería `websockets`, y escucha en `ws://localhost:8765`.

Su funcionamiento es el siguiente:

1. Consume los eventos del topic `pedido.estado` mediante un consumer group propio llamado `notificaciones`. Para mantener la compatibilidad con la implementación común de Kafka, el consumidor hereda de `ConsumerBase` (ADR-002). Como `kafka-python` es una librería bloqueante, el consumidor se ejecuta en un hilo independiente del servidor WebSocket.
2. Mantiene una relación entre cada `idPedido` y las conexiones que están suscritas a ese pedido.
3. Cuando recibe un evento, lo envía únicamente a los clientes que están suscritos al pedido correspondiente. El mensaje se reenvía sin modificar su contenido.
4. Utiliza un protocolo sencillo de mensajes. El cliente puede enviar `suscribir` y `desuscribir`, mientras que el servidor responde con `suscrito`, `estado` o `error`.
5. Cuando una conexión se cierra o falla al enviar un mensaje, se elimina para evitar mantener conexiones que ya no están disponibles. También se utiliza `ping_interval=20` para detectar conexiones que dejaron de responder.

El servicio no almacena los estados de los pedidos ni conserva un historial de notificaciones. Si no hay ningún cliente conectado cuando ocurre un cambio, el aviso no se envía posteriormente.

Para evitar que esto represente un problema, se mantiene la API como fuente de verdad del estado del pedido (ADR-001). Cuando el estudiante abre la página o recupera la conexión, el cliente primero se suscribe al WebSocket y después realiza un `GET /pedidos/{id}`. De esta manera, obtiene el estado actual mediante REST y queda preparado para recibir los siguientes cambios por WebSocket.

Si la conexión se pierde, el cliente intenta conectarse nuevamente cada 3 segundos.

## Alternativas consideradas

| Alternativa                                  | Motivo por el que no fue seleccionada                                                                                                                                                                                                                                                         |
| -------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Polling con REST** (`GET` cada N segundos) | Requiere que el navegador consulte periódicamente el estado del pedido. Si las consultas son muy frecuentes, se generan peticiones innecesarias; si son muy espaciadas, el estudiante puede tardar en enterarse de que su pedido está listo.                                                  |
| **Server-Sent Events (SSE)**                 | Permite que el servidor envíe eventos al navegador, pero la comunicación es en un solo sentido. Para gestionar las suscripciones a diferentes pedidos se necesitaría complementar la solución con otras peticiones REST. WebSocket permite realizar ambas operaciones por una misma conexión. |
| **gRPC server streaming**                    | No funciona directamente en los navegadores y requiere gRPC-Web y un proxy adicional. Se mantiene gRPC para la comunicación interna entre Facturación y Caja, como se estableció en el ADR-003.                                                                                               |
| **Sockets TCP**                              | Son una alternativa de bajo nivel y no pueden utilizarse directamente desde el navegador. Además, sería necesario implementar por cuenta propia el protocolo de comunicación.                                                                                                                 |
| **Conexión directa del navegador a Kafka**   | No es posible consumir Kafka directamente desde un navegador. Además, exponer el broker a los clientes representaría un riesgo de seguridad.                                                                                                                                   |

## Justificación

La elección de WebSockets responde principalmente a la necesidad de notificar los cambios de estado sin que el estudiante tenga que actualizar la página o realizar consultas constantemente.

- **Comunicación bidireccional:** WebSocket mantiene una conexión TCP abierta y **full-duplex**, que permite enviar y recibir mensajes en cualquier momento. La conexión comienza con una solicitud HTTP que utiliza `Upgrade: websocket`; si el servidor acepta el cambio, responde con `101 Switching Protocols`. A partir de ese momento, ambos extremos pueden comunicarse mediante mensajes WebSocket.
- **Notificaciones en tiempo real:** El servidor puede enviar un aviso apenas recibe un evento de Kafka (**server push**). Esto resulta útil cuando el pedido cambia a `LISTO`, porque el estudiante no necesita consultar manualmente si ya puede retirarlo.
- **Menor cantidad de datos por mensaje:** Después de establecer la conexión, no es necesario enviar nuevamente todas las cabeceras HTTP para cada notificación. Esto reduce el tráfico y permite enviar mensajes pequeños con poca latencia.
- **Comunicación asíncrona:** El estudiante puede seguir utilizando la página mientras espera los cambios de su pedido. No necesita mantener una consulta activa ni esperar una respuesta para continuar.
- **Complemento de Kafka y REST:** WebSocket permite llevar las notificaciones hasta el navegador, pero no reemplaza los mecanismos de almacenamiento del sistema. WebSocket es comunicación **transitoria**: si el estudiante no está conectado, el aviso se pierde. Kafka es **persistente** y mantiene los eventos, y la API proporciona el estado actual del pedido. Así, cada tecnología cumple una función diferente.

## Consecuencias

### Positivas

- El estudiante recibe los cambios de estado sin tener que recargar la página ni realizar consultas periódicas.
- El servicio Notificaciones funciona de manera independiente de los demás servicios. Solo necesita conocer el formato de los eventos publicados en `pedido.estado`.
- Si el servicio se detiene, los demás componentes pueden continuar trabajando. Cuando vuelve a funcionar, puede retomar el consumo de Kafka desde el último offset confirmado.
- Al no almacenar información de los pedidos, un reinicio del servicio no afecta los datos del negocio.
- La combinación de WebSocket y REST permite que el estudiante recupere el estado de su pedido incluso después de perder la conexión.

### Negativas y riesgos

- **Pérdida de conexión:** El estudiante puede desconectarse por problemas de Wi-Fi o al cerrar la pestaña. Para manejarlo, el cliente intenta reconectarse cada 3 segundos, vuelve a suscribirse y consulta el estado mediante REST. El servidor también utiliza un ping cada 20 segundos para detectar conexiones que ya no responden.
- **Posibles diferencias entre el GET y los eventos:** Puede ocurrir que el estado obtenido mediante REST no coincida con el último evento recibido por WebSocket, debido al orden en que llegan los mensajes. Para reducir este problema, primero se establece la suscripción y después se consulta la API. Además, la página solo permite avanzar entre estados, evitando que una respuesta anterior haga retroceder el pedido.
- **Notificaciones no recibidas mientras el cliente está desconectado:** Los avisos no se almacenan para enviarlos posteriormente. Esto se considera aceptable porque el estudiante puede consultar el estado actualizado mediante REST cuando vuelva a conectarse.
- **Consumo de recursos por conexiones abiertas:** Cada cliente conectado utiliza memoria y descriptores de conexión. Para el alcance actual de una cafetería, este consumo es manejable. Sin embargo, si el sistema crece, será necesario distribuir las conexiones entre varias instancias del servicio y definir cómo compartir los eventos entre ellas.
- **Falta de autenticación:** En esta versión, cualquier persona que conozca un `idPedido` puede intentar suscribirse a sus notificaciones. Se considera una limitación del prototipo, aunque el uso de UUID dificulta adivinar los identificadores.
- **Deduplicación en memoria:** La deduplicación mediante `idEvento` se pierde cuando el servicio se reinicia. Por lo tanto, un evento repetido podría volver a enviarse. Para evitar que esto afecte la interfaz, el cliente ignora los cambios que no representan un avance en el estado del pedido.

## Validación (implementación y pruebas)

Durante la implementación se realizaron pruebas para comprobar que el servicio pudiera comunicarse con Kafka y que el estudiante pudiera recuperar la información de su pedido después de una desconexión.

- **Uso de la implementación común de Kafka:** El consumidor de Notificaciones hereda de `ConsumerBase` (ADR-002), al igual que el consumidor de la API. Esto permite reutilizar la lógica de commit manual, deduplicación mediante `idEvento` y validación de las combinaciones `tipo/estado/origen` de los eventos.
- **Prueba con los servicios funcionando en conjunto:** Se realizó una prueba utilizando Kafka, la API, Cocina y Notificaciones. El procedimiento y los resultados se encuentran documentados en `docs/pruebas/prueba-websockets.md`. Durante la prueba, el cliente se desconectó antes de que el pedido llegara a `LISTO`. Al volver a conectarse, la consulta REST devolvió el estado `LISTO` y posteriormente recibió por WebSocket el evento correspondiente a `ENTREGADO`. Con esto se comprobó el comportamiento esperado en el Escenario 5.
- **Problema encontrado con localhost en Windows:** Durante las pruebas se detectó que Windows resolvía `localhost` primero como una dirección IPv6 (`::1`), mientras que la API estaba escuchando únicamente en IPv4 (`0.0.0.0`). Esto provocaba el error `WinError 10061` al intentar conectarse desde la página y el script de pruebas. El servidor WebSocket sí aceptaba la conexión porque la librería `websockets` escuchaba tanto en IPv4 como en IPv6. Para evitar el problema, se cambió la dirección utilizada por el cliente y el script de pruebas a `127.0.0.1`.
- **Orden de llegada de REST y WebSocket:** En una de las pruebas, la primera consulta REST ya devolvió `EN_PREPARACION` y después llegó una notificación con ese mismo estado. Esto permitió comprobar que podían recibirse datos repetidos por ambos mecanismos. La regla implementada en la página, que permite únicamente avanzar entre estados, evita que estos mensajes provoquen inconsistencias en la visualización.
