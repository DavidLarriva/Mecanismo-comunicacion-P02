# ADR-005: Selección de mecanismos y arquitectura conjunta

- **Estado:** Propuesto
- **Fecha:** 2026-11-16
- **Responsable:** Todos
- **Escenarios de calidad relacionados:** los seis (`docs/arquitectura/escenarios-calidad.md`)

## Contexto

La consigna obliga a usar REST para la API con la que los estudiantes crean y consultan pedidos. El resto de los mecanismos se elige entre Sockets TCP, RPC/RMI, gRPC, Kafka y WebSockets, con al menos uno asíncrono. Todos deben formar parte del mismo flujo.

El enunciado es para grupos de tres y pide tres mecanismos. El grupo tiene cuatro integrantes, y el PDF de división del trabajo asigna un mecanismo a cada uno. Por eso se eligieron **tres** además de REST.

El sistema tiene cuatro tipos de comunicación con necesidades distintas:

1. **Estudiante → sistema.** Crear y consultar el pedido desde un navegador o un celular, con respuesta inmediata.
2. **API → servicios internos.** Un pedido nuevo tiene que llegar a Cocina, Caja, Facturación y Estadísticas sin que la API los espere, aunque alguno esté detenido.
3. **Facturación → Caja.** Antes de emitir la factura, Facturación necesita saber, en el momento y de la fuente autorizada, si el pago está confirmado.
4. **Sistema → estudiante.** El estudiante quiere enterarse en el momento en que su pedido está listo, sin quedarse consultando.

## Decisión

| Comunicación | Mecanismo | Tipo | ADR |
|---|---|---|---|
| Estudiante y cajero → API de Pedidos; cajero → Caja; administración → Estadísticas | REST | Síncrono | ADR-001 |
| API y servicios → servicios (`pedido.creado`, `pedido.estado`) | Kafka | Asíncrono, persistente | ADR-002 |
| Facturación → Caja (`ConfirmarPago`) | gRPC unario | Síncrono, con deadline de 2 s | ADR-003 |
| Notificaciones → estudiante | WebSocket | Asíncrono para el cliente (server push) | ADR-004 |

Reglas de la arquitectura conjunta, fijadas en `docs/contratos.md`:

- **Kafka es el único canal de cambios de estado.** Ningún servicio consulta a otro para enterarse de un estado. Lo lee de `pedido.estado` con su propio consumer group.
- **Cada estado tiene un solo dueño:**
  - Preparación: Cocina.
  - Pago: Caja.
  - Facturación: Facturación.
  - Recepción y entrega: la API.
- **gRPC solo donde hace falta una respuesta.** Es una sola llamada interna. Todo lo demás entre servicios va por Kafka.
- **El WebSocket solo avisa.** La fuente de verdad del estado es la API (`GET /pedidos/{id}`). El cliente se suscribe primero y después consulta.
- **Contrato común:** el mismo formato de evento (`contracts/pedido.schema.json`), la clave `idPedido` en todos los mensajes y el `.proto` versionado en `contracts/grpc/`.

## Alternativas consideradas

| Alternativa | Por qué no se eligió |
|---|---|
| **Sockets TCP entre servicios** | Su implementación exigiría desarrollar manualmente el protocolo de comunicación, incluyendo la estructura de los mensajes, su delimitación, los mecanismos de reconexión y los reintentos. Además, al depender de una conexión directa, los mensajes no quedarían almacenados si el servicio receptor estuviera desconectado. Esto no permitiría cumplir el escenario 4. |
| **REST también entre servicios** | Implicaría que la API realizara solicitudes directas a Cocina, Caja, Facturación y Estadísticas, dependiendo de que cada servicio estuviera disponible para completar la operación. Si alguno dejara de responder, podría afectar el registro del pedido. Esta dependencia contradice el objetivo de procesamiento independiente establecido en el escenario 4. |
| **Kafka para todo, incluida la consulta a Caja** | No sería la alternativa más adecuada para `ConfirmarPago`, porque Facturación necesita recibir una respuesta de Caja antes de continuar con la emisión de la factura. Implementarlo mediante Kafka requeriría manejar una solicitud y una respuesta en topics diferentes, además de relacionarlas, lo que agregaría complejidad innecesaria para una operación que requiere una respuesta inmediata. |
| **Kafka para todo, incluida la consulta a Caja** | `ConfirmarPago` es una pregunta que necesita respuesta antes de continuar. Con Kafka habría que publicar la pregunta, esperar la respuesta en otro topic y relacionarlas, y eso es construir un RPC encima de una cola. |
| **gRPC entre todos los servicios** | Volvería síncrono lo que hoy es asíncrono. Cada servicio tendría que conocer a los demás y estar disponible al mismo tiempo, y se perdería la persistencia de los eventos. El PDF de división recomienda limitar gRPC a un caso interno. |
| **Consultar el estado con `GET` cada pocos segundos (polling) en lugar de WebSocket** | Genera tráfico constante aunque el pedido no cambie, y el aviso llega con el retraso del intervalo. El WebSocket empuja el cambio en cuanto ocurre, con una sola conexión. |
| **Solo dos mecanismos además de REST (Kafka y WebSocket)** | Cumpliría la consigna para tres personas, pero dejaría a un integrante sin mecanismo propio. Facturación tendría que fiarse del evento de pago sin confirmarlo con Caja. |

## Justificación

La elección sigue la guía de selección del capítulo 2 (sección 8.1), aplicada a cada comunicación:

- **REST para el cliente:** es compatible con cualquier navegador y la latencia es tolerable. El estudiante necesita una respuesta síncrona: su `idPedido`, o el error si algo falla.
- **Kafka entre servicios:** se utiliza para distribuir eventos sin establecer una comunicación directa entre el servicio que los genera y los componentes que los reciben. La API puede publicar un pedido sin conocer qué servicios lo procesarán ni esperar a que finalicen sus operaciones. Si alguno de los consumidores se encuentra detenido, los mensajes pueden permanecer almacenados en Kafka y ser procesados cuando vuelva a estar disponible. Esto favorece la separación entre componentes, permite incorporar nuevos consumidores sin modificar la API y contribuye a la tolerancia a fallos.
- **gRPC para Facturación → Caja:** es una llamada interna con contrato bien definido y baja latencia. El deadline de 2 s evita que Facturación se quede bloqueada si Caja no responde (escenario 3). El campo `repetida` da la semántica "a lo sumo una vez" a la confirmación.
- **WebSocket para el aviso:** es para tiempo real con clientes web. La conexión queda abierta y el servidor toma la iniciativa (escenario 5).

Así, dos mecanismos son síncronos (REST y gRPC) y dos son asíncronos (Kafka y WebSocket). Lo síncrono queda donde alguien necesita una respuesta para continuar, y lo asíncrono donde basta con que la información llegue.

La prueba de integración (`docs/pruebas/integracion.md`) mostró los cuatro mecanismos en un mismo pedido:

- La API respondió en 0,1 s mientras Cocina tardaba 5 s.
- Los siete eventos del pedido quedaron en orden en la misma partición, publicados por cuatro servicios distintos.
- El estudiante recibió los seis cambios de estado por WebSocket.

## Consecuencias

**Positivas**
- Cada integrante es dueño de un mecanismo y de sus servicios. La integración depende del contrato y no de acuerdos informales.
- Un servicio interno caído no detiene la creación de pedidos: los eventos esperan en Kafka.
- Agregar un consumidor nuevo es crear otro consumer group, sin tocar la API.

**Negativas y riesgos aceptados**
- **Más piezas en marcha.** Para el flujo completo hay que levantar Kafka y seis servicios. Lo resuelven el `docker-compose` y el README, pero la demostración es más frágil que un sistema monolítico.
- **Kafka es un punto único.** Con el broker caído no se crean pedidos (`503`, escenario 1). Se aceptó porque, con datos en memoria, aceptar un pedido sin publicarlo podría perderlo.
- **Consistencia eventual.** El `GET` y las notificaciones van unos milisegundos detrás de lo que publicó cada servicio.
- **Alcance mayor que el enunciado.** El grupo usa cuatro mecanismos en lugar de tres. Hay que confirmar con el docente que acepta este alcance.
