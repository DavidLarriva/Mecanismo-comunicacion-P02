# ADR-003: gRPC para confirmar pagos

- **Estado:** Aceptado
- **Responsable:** Integrante 3 (Domenica Uyunkar)

## Contexto

Facturación se activa cuando recibe en el topic `pedido.estado` un evento `PAGO/PAGADO`. Entonces consulta a Caja mediante `ConfirmarPago(idPedido)` por gRPC; Caja es la fuente del estado del pago. La llamada tiene un timeout de 2 segundos. Si Caja responde `PENDIENTE` o `NO_ENCONTRADO`, o la llamada falla por timeout, Facturación reintenta cada 10 segundos, hasta seis intentos, y luego publica `FACTURACION_PENDIENTE`.

Este diseño evita que Facturación agote sus reintentos mientras el pedido todavía espera el pago. Como el consumidor procesa los eventos de forma secuencial, los reintentos pueden retrasar otros eventos: el tiempo máximo aproximado es de 62 segundos si las seis llamadas alcanzan el timeout.

## Decisión

Facturación consumirá el topic `pedido.estado` y actuará cuando reciba un evento
`PAGO/PAGADO`. Entonces consultará a Caja mediante `Caja.ConfirmarPago`, enviando
`id_pedido`. La respuesta indicará `PAGADO`, `PENDIENTE` o `NO_ENCONTRADO`.
El cliente usará un timeout de 2 segundos y realizará hasta seis intentos
separados por esperas de 10 segundos.

## Alternativas consideradas

- REST: también permitiría hacer esta consulta y configurar un tiempo de espera. Elegimos gRPC para Facturación y Caja porque `caja.proto` define los datos enviados y recibidos mediante un contrato tipado, y los stubs generados facilitan implementar la comunicación entre ambos servicios. El deadline de 2 segundos limita cuánto espera Facturación la respuesta.
- Kafka: distribuye mensajes de forma asíncrona, pero Facturación necesita
  la respuesta de Caja para decidir si factura.
- Sockets TCP: exigirían definir y mantener el protocolo manualmente.

## Justificación

gRPC ofrece una interfaz tipada con Protocol Buffers y una respuesta
sincrónica. El timeout evita que Facturación espere indefinidamente.

## Consecuencias

- Caja debe mantener disponible el servidor gRPC en el puerto 50051.
- Facturación debe manejar `DEADLINE_EXCEEDED` y `UNAVAILABLE`.
- Una llamada repetida debe devolver la misma confirmación de pago.
- El servidor gRPC debe consultar el estado de pago real de Caja.
- Facturación realiza hasta seis consultas gRPC con esperas de 10 segundos entre intentos. Como el procesamiento es secuencial dentro del consumidor, los pedidos posteriores pueden esperar alrededor de un minuto mientras se procesa uno pendiente. Si cada llamada agota el timeout de 2 segundos, el tiempo total puede ser algo mayor.
- Se acepta esta espera para la demostración. En una versión de producción se podría mover cada pedido a una cola de trabajo para que un pedido pendiente no retrase la facturación de los demás.