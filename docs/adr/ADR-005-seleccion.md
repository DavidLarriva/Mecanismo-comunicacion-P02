# ADR-005: Selección de mecanismos y arquitectura conjunta

- **Estado:** Propuesto
- **Fecha:** 2026-11-16
- **Responsable:** Todos
- **Escenarios de calidad relacionados:** los seis (`docs/arquitectura/escenarios-calidad.md`)

## Contexto

La consigna exige REST para la API con la que los estudiantes crean y consultan sus pedidos. Los demás mecanismos se eligen entre Sockets TCP, RPC/RMI, gRPC, Kafka y WebSockets, y al menos uno tiene que ser asíncrono. Todos deben ser parte del mismo flujo.

El enunciado está pensado para grupos de tres y pide tres mecanismos. Nuestro grupo tiene cuatro integrantes y el PDF de división del trabajo le asigna un mecanismo a cada uno, por eso elegimos **tres** además de REST.

En el sistema hay cuatro tipos de comunicación y cada uno necesita algo distinto.

1. **Estudiante → sistema.** Crear y consultar el pedido desde un navegador o un celular y recibir respuesta enseguida.
2. **API → servicios internos.** Un pedido nuevo tiene que llegar a Cocina, Caja, Facturación y Estadísticas sin que la API los espere, aunque alguno esté detenido.
3. **Facturación → Caja.** Antes de emitir la factura, Facturación necesita saber en ese momento, y por la fuente autorizada, si el pago está confirmado.
4. **Sistema → estudiante.** El estudiante quiere saber apenas su pedido está listo, sin tener que estar consultando.

## Decisión

| Comunicación | Mecanismo | Tipo | ADR |
|---|---|---|---|
| Estudiante y cajero → API de Pedidos, cajero → Caja, administración → Estadísticas | REST | Síncrono | ADR-001 |
| API y servicios → servicios (`pedido.creado`, `pedido.estado`) | Kafka | Asíncrono, persistente | ADR-002 |
| Facturación → Caja (`ConfirmarPago`) | gRPC unario | Síncrono, con deadline de 2 s | ADR-003 |
| Notificaciones → estudiante | WebSocket | Asíncrono para el cliente, con server push | ADR-004 |

La arquitectura conjunta sigue estas reglas, que están en `docs/contratos.md`.

- **Kafka es el único canal para los cambios de estado.** Ningún servicio le pregunta a otro por un estado, lo lee de `pedido.estado` con su propio consumer group.
- **Cada estado tiene un solo dueño.** Cocina maneja la preparación, Caja el pago, Facturación la facturación y la API la recepción y la entrega.
- **gRPC solo donde hace falta una respuesta.** Es una única llamada interna y todo lo demás entre servicios va por Kafka.
- **El WebSocket solo avisa.** El estado real lo tiene la API en `GET /pedidos/{id}`. El cliente primero se suscribe y después consulta.
- **Contrato común.** Todos usan el mismo formato de evento (`contracts/pedido.schema.json`), la clave `idPedido` en todos los mensajes y el `.proto` versionado en `contracts/grpc/`.

## Alternativas consideradas

| Alternativa | Por qué no se eligió |
|---|---|
| **Sockets TCP entre servicios** | Su implementación exigiría desarrollar manualmente el protocolo de comunicación, incluyendo la estructura de los mensajes, su delimitación, los mecanismos de reconexión y los reintentos. Además, al depender de una conexión directa, los mensajes no quedarían almacenados si el servicio receptor estuviera desconectado. Esto no permitiría cumplir el escenario 4. |
| **REST también entre servicios** | Implicaría que la API realizara solicitudes directas a Cocina, Caja, Facturación y Estadísticas, dependiendo de que cada servicio estuviera disponible para completar la operación. Si alguno dejara de responder, podría afectar el registro del pedido. Esta dependencia contradice el objetivo de procesamiento independiente establecido en el escenario 4. |
| **Kafka para todo, incluida la consulta a Caja** | No sería la alternativa más adecuada para `ConfirmarPago`, porque Facturación necesita recibir una respuesta de Caja antes de continuar con la emisión de la factura. Implementarlo mediante Kafka requeriría manejar una solicitud y una respuesta en topics diferentes, además de relacionarlas, lo que agregaría complejidad innecesaria para una operación que requiere una respuesta inmediata. |
| **gRPC entre todos los servicios** | Haría que las comunicaciones internas dependieran de respuestas directas y de que los servicios involucrados estuvieran disponibles al mismo tiempo. También reemplazaría el intercambio asíncrono y la persistencia que aporta Kafka. La guía de división reserva gRPC para una comunicación interna específica. |
| **RPC/RMI clásico** | También permitiría hacer llamadas remotas, pero el grupo eligió gRPC para Facturación–Caja porque `caja.proto` define los mensajes y permite generar el código del cliente y del servidor. |
| **Consultar el estado con `GET` cada pocos segundos (polling) en lugar de WebSocket** | Genera tráfico constante aunque el pedido no cambie, y el aviso llega con el retraso del intervalo. El WebSocket empuja el cambio en cuanto ocurre, con una sola conexión. |
| **Solo dos mecanismos además de REST (Kafka y WebSocket)** | Cumpliría la consigna para tres personas, pero un integrante se quedaría sin mecanismo propio. Además Facturación tendría que confiar en el evento de pago sin confirmarlo con Caja. |

## Justificación

Para elegir seguimos la guía de selección del capítulo 2 (sección 8.1) en cada comunicación.

- **REST para el cliente:** funciona en cualquier navegador y su latencia es aceptable. El estudiante necesita una respuesta síncrona con su `idPedido`, o con el error si algo falla.
- **Kafka entre servicios:** se utiliza para distribuir eventos sin establecer una comunicación directa entre el servicio que los genera y los componentes que los reciben. La API puede publicar un pedido sin conocer qué servicios lo procesarán ni esperar a que finalicen sus operaciones. Si alguno de los consumidores se encuentra detenido, los mensajes pueden permanecer almacenados en Kafka y ser procesados cuando vuelva a estar disponible. Esto favorece la separación entre componentes, permite incorporar nuevos consumidores sin modificar la API y contribuye a la tolerancia a fallos.
- **gRPC para Facturación → Caja:** se usa en esta consulta porque Facturación necesita confirmar el pago con Caja antes de emitir la factura. El contrato tipado define lo que intercambian ambos servicios y el deadline de 2 s limita la espera si Caja no responde (escenario 3). La respuesta incluye `repetida`, que permite reconocer una consulta ya procesada sin generar otra confirmación.
- **WebSocket para el aviso:** es para tiempo real con clientes web. La conexión queda abierta y el servidor toma la iniciativa (escenario 5).

Al final quedaron dos mecanismos síncronos, REST y gRPC, y dos asíncronos, Kafka y WebSocket. Usamos lo síncrono donde alguien necesita una respuesta para seguir, y lo asíncrono donde basta con que la información llegue.

En la prueba de integración (`docs/pruebas/integracion.md`) se vieron los cuatro mecanismos en un mismo pedido. La API respondió en 0,1 s mientras Cocina tardaba 5 s, los siete eventos del pedido quedaron en orden en la misma partición aunque los publicaron cuatro servicios distintos, y el estudiante recibió los seis cambios de estado por WebSocket.

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
