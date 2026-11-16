# Pruebas de gRPC y comunicación Caja-Facturación

- **Escenario de calidad relacionado:** Escenario 3, resiliencia cuando Caja no responde (`docs/arquitectura/escenarios-calidad.md`). Medida: cada llamada termina en ≤ 2 s, como máximo 6 intentos cada 10 s, 0 facturas sin pago confirmado y 0 facturas duplicadas por pedido.
- **Responsable:** Integrante 3 (Domenica Uyunkar)
- **Mecanismo:** gRPC (`contracts/grpc/caja.proto`), con Kafka para el evento que activa a Facturación

## Prueba 1: servicio gRPC no disponible

- **Escenario:** el cliente llama a un puerto sin servidor gRPC disponible.
- **Configuración:** timeout del cliente de 2 segundos.
- **Pedido usado:** `a8b4c921-5f73-4f82-9ec4-b83202a8ad01`.
- **Ejecución:** se llamó a Caja usando `localhost:50052`, puerto sin servidor escuchando.
- **Resultado obtenido:** gRPC respondió `DEADLINE_EXCEEDED`; el detalle fue `Deadline Exceeded`.
- **Tiempo observado:** 2,02 segundos.
- **Resultado esperado:** la llamada termina al alcanzar el límite, sin esperar indefinidamente.
- **Conclusión:** el timeout de 2 segundos del cliente se aplicó correctamente.

## Prueba 2: confirmación de pago y facturación

- **Pedido usado:** `a8b4c921-5f73-4f82-9ec4-b83202a8ad01`.
- **Resultado de Caja:** el pago pasó a `PAGADO`; confirmación `PAG-3F40D3BBC3CA`.
- **Resultado de Facturación:** Kafka recibió `FACTURACION/FACTURADO`; factura `FAC-04CAAF4A013D`.
- **Eventos observados:** primero `PAGO/PAGADO` y luego `FACTURACION/FACTURADO`.
- **Conclusión:** Facturación consultó a Caja por gRPC y publicó la factura después de confirmar el pago.

## Prueba 3: reintento idempotente de pago

- **Pedido usado:** `a8b4c921-5f73-4f82-9ec4-b83202a8ad01`.
- **Ejecución:** se repitió el `POST /caja/pagos/{id_pedido}` con el mismo pedido.
- **Resultado obtenido:** Caja devolvió la misma confirmación `PAG-3F40D3BBC3CA`.
- **Eventos observados:** Kafka contenía un solo evento `PAGO/PAGADO` para ese pedido.
- **Conclusión:** repetir el pago no generó una segunda confirmación ni un segundo evento de pago.

## Prueba 4: Facturación procesa un pago tardío reproducido desde Kafka

- **Pedido usado:** `a8538411-75eb-4164-8c56-7e53b34efa90`.
- **Resultado inicial:** antes del cambio del consumidor, Facturación publicó `FACTURACION_PENDIENTE` tras seis intentos, porque el pago todavía estaba `PENDIENTE`.
- **Pago:** Caja publicó después el evento `PAGO/PAGADO`, con confirmación `PAG-412BC93D4D9F`.
- **Cambio aplicado:** Facturación se suscribió a `pedido.estado` y procesa los eventos `PAGO/PAGADO`.
- **Resultado observado:** al reiniciar Facturación, el consumidor leyó el evento de pago guardado en Kafka y publicó `FACTURADO`, con factura `FAC-6242528FFF56`.
- **Conclusión:** Facturación pudo procesar el evento de pago reproducido desde Kafka y emitir la factura. Esta prueba también mostró que, si se agotan los reintentos antes del pago, el evento de pago debe procesarse para completar la facturación.

## Explicación (conceptos del Cap. 2)

- **Llamada síncrona y deadline (pruebas 1 y 2).** `ConfirmarPago` es una llamada gRPC unaria: Facturación envía una solicitud y se bloquea hasta recibir la respuesta. Es la forma correcta para este caso, porque Facturación no puede emitir la factura sin saber si el pago está confirmado. El riesgo de una llamada síncrona es el acoplamiento temporal: si Caja no está, el cliente podría quedarse esperando. El deadline de 2 s lo limita. En la prueba 1, la llamada a un puerto sin servidor terminó en 2,02 s con `DEADLINE_EXCEEDED`, en lugar de bloquear Facturación. Según el sistema, una conexión rechazada también puede terminar antes con `UNAVAILABLE`. Facturación trata los dos códigos igual: lo registra y reintenta.
- **Contrato tipado con Protocol Buffers.** La solicitud y la respuesta están definidas en `caja.proto`, y el cliente y el servidor usan código generado (`caja_pb2`, `caja_pb2_grpc`). Por eso las dos partes no pueden diferir en el nombre de un campo ni en el tipo de un estado (`PAGADO`, `PENDIENTE`, `NO_ENCONTRADO`). Con REST y JSON, ese error aparecería recién al ejecutar.
- **Semánticas de falla (prueba 3).** Facturación reintenta cuando la llamada falla. Esa es la semántica "al menos una vez", y significa que Caja puede recibir la misma consulta más de una vez. Como confirmar un pago para facturar tiene efecto, Caja descarta los duplicados: desde la segunda consulta devuelve la misma confirmación con `repetida = true` (`almacen_pagos.confirmar_para_facturacion`). Esa es la semántica "a lo sumo una vez". La prueba 3 comprobó la misma regla en el `POST` de pago por REST: repetirlo devolvió otra vez `PAG-3F40D3BBC3CA` y Kafka tuvo un solo `PAGO/PAGADO`. Combinadas, las dos semánticas dan una sola factura por pedido.
- **Resultado inesperado: reintentos que se agotan antes del pago (prueba 4).** Cuando Facturación se activaba con `pedido.creado`, empezaba a preguntar antes de que el estudiante pagara. Los 6 intentos cada 10 s cubren poco más de un minuto. Si el pago llegaba después, el pedido quedaba en `FACTURACION_PENDIENTE` aunque ya estuviera pagado. El problema no era gRPC, sino usar una llamada síncrona para esperar un evento que llega cuando el estudiante quiere. La corrección combinó los dos mecanismos:
  - Kafka, que es asíncrono y persistente, avisa cuándo ocurrió el pago (`PAGO/PAGADO` en `pedido.estado`).
  - gRPC, que es síncrono, solo confirma con Caja, la autoridad sobre el pago, antes de emitir la factura.

  Como Kafka guarda el evento, Facturación lo procesó aunque al publicarse no estuviera en marcha (desacoplamiento temporal). El cambio quedó registrado en el contrato v1.3 y en el ADR-003.
- **Lo que no cubren estas pruebas.** No se probó una Caja lenta (que responda después de los 2 s), sino un puerto sin servidor. El efecto en el cliente es el mismo, porque el deadline corta la llamada en los dos casos. Tampoco se probó qué pasa con un evento de pago que llega mientras Facturación procesa los reintentos de otro pedido: como el consumidor es secuencial, ese evento espera hasta unos 62 s (ADR-003).
