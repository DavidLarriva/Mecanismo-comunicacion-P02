# Pruebas de gRPC y comunicación Caja-Facturación

- **Escenario de calidad:** Escenario 3, resiliencia ante la falta de respuesta de Caja, documentado en `docs/arquitectura/escenarios-calidad.md`. Los criterios son limitar cada llamada a 2 segundos, realizar como máximo 6 intentos separados por 10 segundos y evitar facturar sin pago confirmado o duplicar facturas para un pedido.
- **Responsable:** Integrante 3, Domenica Uyunkar.
- **Mecanismos:** Facturación recibe por Kafka el evento `PAGO/PAGADO` en `pedido.estado` y consulta el estado del pago con Caja mediante gRPC, usando `ConfirmarPago` definido en `contracts/grpc/caja.proto`.

## Prueba 1: servidor gRPC no disponible

- **Escenario:** el cliente intenta comunicarse con un puerto donde no hay un servidor gRPC escuchando.
- **Configuración:** timeout del cliente de 2 segundos.
- **Pedido usado:** `a8b4c921-5f73-4f82-9ec4-b83202a8ad01`.
- **Ejecución:** se llamó a Caja mediante `localhost:50052`.
- **Resultado obtenido:** gRPC respondió `DEADLINE_EXCEEDED`, con el detalle `Deadline Exceeded`.
- **Tiempo observado:** 2,02 segundos.
- **Resultado esperado:** que la llamada termine al alcanzar el límite configurado.
- **Conclusión:** el timeout evitó que el cliente esperara indefinidamente.

## Prueba 2: confirmación del pago y facturación

- **Pedido usado:** `a8b4c921-5f73-4f82-9ec4-b83202a8ad01`.
- **Resultado de Caja:** el pago quedó en `PAGADO`, con la confirmación `PAG-3F40D3BBC3CA`.
- **Resultado de Facturación:** Kafka recibió `FACTURACION/FACTURADO`, con la factura `FAC-04CAAF4A013D`.
- **Eventos observados:** primero `PAGO/PAGADO` y después `FACTURACION/FACTURADO`.
- **Conclusión:** Facturación consultó a Caja mediante gRPC y emitió la factura después de confirmar el pago.

## Prueba 3: reintento idempotente del pago

- **Pedido usado:** `a8b4c921-5f73-4f82-9ec4-b83202a8ad01`.
- **Ejecución:** se repitió el `POST /caja/pagos/{id_pedido}` con el mismo pedido. También se llamó dos veces a `ClienteCaja.confirmar_pago` para comprobar la respuesta de Caja.
- **Resultado obtenido:** ambas llamadas gRPC devolvieron `PAG-3F40D3BBC3CA`; la segunda indicó `repetida = true`. El `POST` repetido también devolvió la misma confirmación.
- **Eventos observados:** Kafka contenía un solo evento `PAGO/PAGADO` para ese pedido.
- **Conclusión:** en esta prueba, repetir la solicitud conservó la confirmación original y no produjo un segundo evento de pago.

## Prueba 4: Facturación procesa un pago tardío reproducido desde Kafka

- **Pedido usado:** `a8538411-75eb-4164-8c56-7e53b34efa90`.
- **Resultado inicial:** antes del cambio del consumidor, Facturación publicó `FACTURACION_PENDIENTE` tras seis intentos porque el pago seguía en `PENDIENTE`.
- **Pago posterior:** Caja publicó `PAGO/PAGADO` con la confirmación `PAG-412BC93D4D9F`.
- **Cambio aplicado:** Facturación pasó a consumir `pedido.estado` y a actuar cuando recibe `PAGO/PAGADO`.
- **Resultado observado:** al reiniciar Facturación, el consumidor leyó el evento guardado en Kafka y publicó `FACTURADO`, con la factura `FAC-6242528FFF56`.
- **Conclusión:** el evento conservado en Kafka permitió completar la factura después del pago tardío. La prueba muestra que Facturación debe procesar el evento de pago aunque los intentos iniciales hayan terminado antes.

## Explicación (conceptos del Capítulo 2)

- **Llamada síncrona y timeout (pruebas 1 y 2).** `ConfirmarPago` es una llamada gRPC unaria: Facturación envía una solicitud y espera la respuesta de Caja. Esa respuesta es necesaria para decidir si se puede emitir la factura. La espera crea una dependencia temporal con Caja; el timeout de 2 segundos pone un límite. En la prueba 1, la llamada a un puerto sin servidor terminó en 2,02 segundos con `DEADLINE_EXCEEDED`. Según cómo falle la conexión, también puede aparecer `UNAVAILABLE`; Facturación registra el error y reintenta.

- **Contrato tipado con Protocol Buffers.** `caja.proto` define la solicitud, la respuesta y los estados que comparten el cliente y el servidor. El código generado (`caja_pb2` y `caja_pb2_grpc`) ayuda a mantener los mismos campos y valores, como `PAGADO`, `PENDIENTE` y `NO_ENCONTRADO`, en ambos extremos.

- **Reintentos e idempotencia (prueba 3).** Si una solicitud se repite, Caja debe evitar crear otra confirmación para el mismo pago. En la comprobación gRPC, la segunda llamada devolvió la confirmación anterior y marcó `repetida = true`. Al repetir el `POST` de pago por REST también se recibió `PAG-3F40D3BBC3CA`, y Kafka mostró un solo evento `PAGO/PAGADO` para el pedido probado.

- **Pago tardío y comunicación asíncrona (prueba 4).** Cuando Facturación se activaba al recibir `pedido.creado`, podía iniciar sus seis intentos antes de que el estudiante pagara. Si esos intentos terminaban primero, el sistema publicaba `FACTURACION_PENDIENTE`. El cambio hizo que Facturación actuara al recibir `PAGO/PAGADO` en `pedido.estado`: Kafka avisa que ocurrió el pago y gRPC permite consultar a Caja, que mantiene el estado del cobro.

- **Persistencia y reproducción de eventos.** Kafka conservó el evento de pago mientras Facturación no lo procesaba. Después del reinicio, el consumidor pudo leerlo y publicar `FACTURADO`. Esto muestra cómo la persistencia de mensajes permite que productor y consumidor no tengan que estar activos al mismo tiempo.

- **Límites observados.** La prueba de indisponibilidad usó un puerto sin servidor; no simuló una Caja lenta que responda después de 2 segundos. Tampoco midió qué ocurre cuando llega otro evento de pago mientras Facturación procesa los reintentos de un pedido distinto. Como el consumidor trabaja secuencialmente, ese evento podría esperar alrededor de 62 segundos si las seis llamadas agotan el timeout y se realizan las cinco esperas de 10 segundos.