# Pruebas de gRPC y comunicación Caja-Facturación

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