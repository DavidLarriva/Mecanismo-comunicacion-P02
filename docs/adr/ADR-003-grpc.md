# ADR-003: gRPC para confirmar pagos

- **Estado:** Propuesto
- **Responsable:** Integrante 3

## Contexto

Facturación necesita consultar a Caja si un pedido está pagado
antes de emitir su factura. La llamada requiere una respuesta inmediata.

## Decisión

Facturación llamará a `Caja.ConfirmarPago` mediante gRPC.
La solicitud enviará `id_pedido`; la respuesta indicará
`PAGADO`, `PENDIENTE` o `NO_ENCONTRADO`.
El cliente usará un timeout de dos segundos.

## Alternativas consideradas

- REST: se podría usar, pero el grupo reserva gRPC para esta llamada interna.
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