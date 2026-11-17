# Prueba Kafka: recuperación del consumidor de Cocina

- **Escenario de calidad relacionado:** 4. Rendimiento.
- **Responsable:** Integrante 2 (Carolina Fortmann)

## Ejecución

1. Se inició el broker Kafka y se comprobó que estuviera disponible junto con los servicios necesarios para realizar la prueba.

2. Se levantó la API REST y se verificó su funcionamiento mediante Swagger.

3. Se interrumpió temporalmente la ejecución del consumidor de Cocina, mientras Kafka y la API REST permanecieron activos.

4. Durante la interrupción del consumidor, se registró un pedido mediante el endpoint `POST /pedidos`, utilizando una clave de idempotencia.

5. Una vez registrado el pedido, se volvió a iniciar el consumidor de Cocina mediante el comando:

   ```powershell
   python -m backend.cocina.main --demora 5
   ```

6. Se revisaron los registros de ejecución para identificar el procesamiento del pedido y las publicaciones de sus cambios de estado en el topic `pedido.estado`.

## Resultado obtenido (real)

Después de iniciar nuevamente el consumidor de Cocina, se observaron dos publicaciones en el topic `pedido.estado`:

- En la partición 0, offset 8, se publicó el cambio de estado y Cocina registró el pedido como `EN_PREPARACION`.
- En la partición 0, offset 9, se publicó el siguiente cambio y Cocina registró el pedido como `LISTO`.

Los dos registros presentan la misma clave de pedido: `3742ad17-0722-48e6-b67d-e054ee096c24`.

Al comparar este identificador con el campo `idPedido` de la respuesta obtenida en Swagger, se confirmó que ambos corresponden al mismo pedido enviado mientras el consumidor estaba detenido.

Por lo tanto, se comprobó que, después de reiniciar Cocina, el consumidor pudo recuperar el procesamiento del pedido y publicar sus cambios de estado en Kafka.

## Evidencias

Las capturas de la ejecución están en `docs/pruebas/evidencias/kafka/`.

1. `01-kafka-activo.png`: el broker Kafka en marcha.
2. `02-cocina-detenida.png`: el consumidor de Cocina detenido.
3. `03-pedido-publicado.png`: la respuesta de Swagger a `POST /pedidos`, con el `idPedido` generado.
4. `04-cocina-recuperada.png`: los registros de Cocina al volver, con los estados `EN_PREPARACION` y `LISTO`, la partición 0 y los offsets 8 y 9.

## Explicación (conceptos del Capítulo 2)

**Por qué no se perdió el pedido**

Kafka almacena los mensajes publicados en sus topics de acuerdo con la configuración de retención y almacenamiento del broker. Por esta razón, detener un consumidor no implica que los mensajes pendientes desaparezcan automáticamente. Cuando Cocina volvió a ejecutarse, pudo continuar con la lectura de los mensajes que todavía debía procesar.

**Entrega al menos una vez**

El comportamiento observado es compatible con una estrategia de entrega al menos una vez (*at-least-once*). Esto significa que un mensaje puede volver a entregarse si el consumidor no ha confirmado correctamente su procesamiento. Por ello, los consumidores deben contemplar mecanismos de idempotencia para evitar que una entrega repetida genere efectos duplicados.

**Orden de procesamiento**

Kafka conserva el orden de los mensajes dentro de una misma partición. En esta ejecución se observaron los offsets 8 y 9 en la partición 0, correspondientes a los cambios de estado `EN_PREPARACION` y `LISTO`, respectivamente. Esto permitió verificar la secuencia de procesamiento del pedido durante la prueba.