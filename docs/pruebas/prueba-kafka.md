# Prueba Kafka: consumidor no responde

- **Escenario de calidad relacionado:** Escenario 4, rendimiento al recibir pedidos (`docs/arquitectura/escenarios-calidad.md`). Medida: la API responde sin esperar a ningún servicio interno y hay 0 eventos perdidos cuando un consumidor se reinicia.
- **Responsable:** Integrante 2 (Carolina Fortmann)

## Ejecución

1. Se inició el broker Kafka y se verificó que los servicios necesarios estuvieran disponibles.

2. Se inició la API REST y se comprobó su funcionamiento mediante Swagger.

3. Se detuvo temporalmente el consumidor de Cocina, manteniendo Kafka y la API REST activos.

4. Se envió un pedido mediante el endpoint POST /pedidos, utilizando una clave de idempotencia.

5. Se reinició el consumidor de Cocina mediante el comando python -m backend.cocina.main --demora 5.

6. Se observaron los mensajes generados durante el procesamiento del pedido y las publicaciones de cambios de estado en el topic pedido.estado.

## Resultado obtenido (real)

Después de reiniciar el consumidor de Cocina, se observaron los siguientes registros:

Se publicó un evento en pedido.estado, partición 0, offset 8, y Cocina registró el estado EN_PREPARACION.

Posteriormente, se publicó otro evento en pedido.estado, partición 0, offset 9, y Cocina registró el estado LISTO.

Ambos registros corresponden a la misma clave de pedido: 3742ad17-0722-48e6-b67d-e054ee096c24.

Los resultados muestran que el consumidor volvió a procesar un pedido y publicó sus cambios de estado en Kafka después de su reinicio. El identificador coincide con la respuesta de Swagger del pedido enviado durante la interrupción: `201` con `idPedido` `3742ad17-0722-48e6-b67d-e054ee096c24` (captura `03-pedido-publicado.png`), el mismo que procesó Cocina al volver (captura `04-cocina-recuperada.png`). La API respondió `201` aunque Cocina estaba detenida, y el pedido no se perdió.

Evidencias en `docs/pruebas/evidencias/kafka/`:
- `01-kafka-activo.png`: broker en marcha.
- `02-cocina-detenida.png`: Cocina detenida.
- `03-pedido-publicado.png`: el pedido creado por Swagger.
- `04-cocina-recuperada.png`: Cocina procesando el pedido al volver.

## Explicación (conceptos del Cap. 2)

Kafka permite la comunicación asíncrona entre productores y consumidores. Esto significa que la API puede publicar un evento sin esperar a que el servicio de Cocina termine de procesarlo.

Cuando un consumidor se detiene, Kafka conserva los mensajes publicados en el topic de acuerdo con su configuración de almacenamiento y retención. Cuando el consumidor vuelve a estar disponible, puede continuar leyendo los mensajes pendientes desde la posición de consumo correspondiente.

En esta prueba se observó que Cocina retomó el procesamiento y publicó dos cambios de estado: EN_PREPARACION y LISTO. Esto demuestra el funcionamiento de la comunicación asíncrona y la recuperación del consumidor en el escenario ejecutado.

**Por qué no se perdió el pedido.** Cocina pertenece al consumer group `cocina`, y Kafka guarda para cada grupo el offset hasta el que ya procesó. El consumidor hace commit del offset solo después de procesar el mensaje (commit manual, `enable_auto_commit=False`). Mientras Cocina estuvo detenida, el pedido quedó en `pedido.creado` después del último offset confirmado del grupo, y al reiniciar el consumidor continuó desde ahí. Este es el desacoplamiento temporal del capítulo 2: productor y consumidor no necesitan estar activos al mismo tiempo, porque la comunicación es persistente.

**Al menos una vez.** Como el commit se hace después de procesar, si Cocina se cayera entre procesar y confirmar, el mensaje se volvería a leer al reiniciar. Por eso Kafka entrega "al menos una vez", y los consumidores descartan los pedidos repetidos por `idPedido` (contrato, sección 5).

**Orden.** Los dos cambios de estado quedaron en la partición 0, con offsets 8 y 9, porque se publican con la clave `idPedido`. Kafka garantiza el orden dentro de una partición, así que cualquier consumidor lee `EN_PREPARACION` antes que `LISTO`.
