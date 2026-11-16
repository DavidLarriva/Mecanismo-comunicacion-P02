# Prueba Kafka: consumidor no responde

- **Escenario de calidad relacionado:** Resiliencia y confiabilidad.
- **Responsable:** Integrante 2

## Ejecución

Se inició el broker Kafka y se verificó que los servicios necesarios estuvieran disponibles.

Se inició la API REST y se comprobó su funcionamiento mediante Swagger.

Se detuvo temporalmente el consumidor de Cocina, manteniendo Kafka y la API REST activos.

Se envió un pedido mediante el endpoint POST /pedidos, utilizando una clave de idempotencia.

Se reinició el consumidor de Cocina mediante el comando python -m backend.cocina.main --demora 5.

Se observaron los mensajes generados durante el procesamiento del pedido y las publicaciones de cambios de estado en el topic pedido.estado.

## Resultado obtenido (real)

Después de reiniciar el consumidor de Cocina, se observaron los siguientes registros:

Se publicó un evento en pedido.estado, partición 0, offset 8, y Cocina registró el estado EN_PREPARACION.

Posteriormente, se publicó otro evento en pedido.estado, partición 0, offset 9, y Cocina registró el estado LISTO.

Ambos registros corresponden a la misma clave de pedido: 3742ad17-0722-48e6-b67d-e054ee096c24.

Los resultados muestran que el consumidor volvió a procesar un pedido y publicó sus cambios de estado en Kafka después de su reinicio. Se debe contrastar el identificador con la respuesta de Swagger para confirmar que corresponde al pedido enviado durante la interrupción.

## Explicación (conceptos del Cap. 2)

Kafka permite la comunicación asíncrona entre productores y consumidores. Esto significa que la API puede publicar un evento sin esperar a que el servicio de Cocina termine de procesarlo.

Cuando un consumidor se detiene, Kafka conserva los mensajes publicados en el topic de acuerdo con su configuración de almacenamiento y retención. Cuando el consumidor vuelve a estar disponible, puede continuar leyendo los mensajes pendientes desde la posición de consumo correspondiente.

En esta prueba se observó que Cocina retomó el procesamiento y publicó dos cambios de estado: EN_PREPARACION y LISTO. Esto demuestra el funcionamiento de la comunicación asíncrona y la recuperación del consumidor en el escenario ejecutado.