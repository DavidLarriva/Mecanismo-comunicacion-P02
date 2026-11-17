# Prueba de integración: flujo completo

- **Escenario de calidad relacionado:** Escenario 6, interoperabilidad entre servicios (`docs/arquitectura/escenarios-calidad.md`). También comprueba con el sistema completo que la API responde sin esperar a los servicios internos (escenario 4) y que repetir un pago o una entrega no los duplica (escenario 2).
- **Responsable:** Todos (coordina Integrante 4)
- **Mecanismos:** REST, Kafka, gRPC y WebSocket en un mismo pedido

## Hipótesis

Un pedido creado en la API debería pasar por los cuatro mecanismos según el flujo del contrato (`docs/contratos.md`, sección 8). La API lo publica en `pedido.creado`, que consumen Cocina, Caja y Estadísticas. Los cambios de estado van por `pedido.estado`, donde Facturación se activa con `PAGO/PAGADO` y confirma el pago con Caja por gRPC, y el estudiante recibe cada cambio por WebSocket.

Si todos respetamos el contrato, los eventos del pedido quedan en la misma partición y en orden, y la entrega solo se acepta con el pedido `LISTO` y `PAGADO`.

## Ejecución

La prueba se hizo el 16/11/2026 en una sola máquina, con Kafka en Docker (`infra/docker-compose.yml`) y los seis servicios en sus terminales.

| Servicio | Comando |
|---|---|
| API de Pedidos | `python -m backend.api_rest.main` |
| Cocina | `python -m backend.cocina.main --demora 5` |
| Estadísticas | `python -m backend.estadisticas.main` |
| Caja (REST y gRPC) | `python -m uvicorn backend.caja.main:app --port 8001` |
| Facturación | `python -m backend.facturacion.main` |
| Notificaciones | `python -m backend.websockets.servidor` |

Con todo levantado se ejecutó el script de la prueba.

```bash
python -m backend.api_rest.pruebas.prueba_flujo_completo
```

El script hace lo que harían el estudiante y el cajero. Crea un pedido de 1 sánduche y 2 jugos por 450 centavos, se suscribe por WebSocket y lo consulta con `GET`. Luego intenta entregarlo antes de que esté listo y antes de pagarlo, consulta el cobro en Caja, paga dos veces, intenta pagar un pedido inexistente y entrega dos veces. Al final lee de Kafka los eventos del pedido y compara las estadísticas.

## Resultado obtenido (real)

La salida completa está en `docs/pruebas/evidencias/integracion/flujo-completo.txt`. El pedido de la prueba fue `0751c38d-1a4b-4de6-bd6a-dc0117913d76`.

| Momento | Acción | Mecanismo | Resultado |
|---|---|---|---|
| +0,1 s | `POST /pedidos` | REST | `201`, total 450 |
| +0,1 s | Suscripción y `GET /pedidos/{id}` | WebSocket, REST | `suscrito`, y el pedido ya estaba `EN_PREPARACION` / `PENDIENTE` |
| +0,1 s | `PATCH /entregar` antes de `LISTO` | REST | `409 PEDIDO_NO_LISTO` |
| +0,1 s | Cocina y Caja leen `pedido.creado` | Kafka | Llegan por WebSocket `EN_PREPARACION` y `PAGO/PENDIENTE` |
| +5,1 s | Cocina termina | Kafka, WebSocket | Llega `LISTO` |
| +5,3 s | `PATCH /entregar` sin pagar | REST | `409 PEDIDO_NO_PAGADO` |
| +5,4 s | `GET /caja/pagos/{id}` | REST | `PENDIENTE`, monto 450 |
| +5,4 s | `POST /caja/pagos/{id}` dos veces | REST | Las dos responden `200` con la misma confirmación `PAG-C2F6C3C45D19` |
| +5,4 s | Pago de un pedido que no existe | REST | `404 COBRO_NO_ENCONTRADO` |
| +5,4 s | Facturación lee `PAGO/PAGADO` y llama a `ConfirmarPago` | Kafka, gRPC | `FACTURADO` con la factura `FAC-103F52009CA9` |
| +5,5 s | `PATCH /entregar` dos veces | REST | `200` y `200`, con `entregado = true` |

Por WebSocket llegaron los seis cambios de estado en este orden: `EN_PREPARACION`, `PENDIENTE`, `LISTO`, `PAGADO`, `FACTURADO` y `ENTREGADO`.

En Kafka el pedido tiene un evento en `pedido.creado` y seis en `pedido.estado`, todos en la **partición 0**.

| Topic | Offset | Evento | Publicado por |
|---|---|---|---|
| `pedido.creado` | 7 | Pedido creado, total 450 | API |
| `pedido.estado` | 27 | `PREPARACION/EN_PREPARACION` | Cocina |
| `pedido.estado` | 28 | `PAGO/PENDIENTE` | Caja |
| `pedido.estado` | 29 | `PREPARACION/LISTO` | Cocina |
| `pedido.estado` | 30 | `PAGO/PAGADO` | Caja |
| `pedido.estado` | 31 | `FACTURACION/FACTURADO` | Facturación |
| `pedido.estado` | 32 | `ENTREGA/ENTREGADO` | API |

Aunque el pago y la entrega se hicieron dos veces, hay **un solo** `PAGADO`, un solo `FACTURADO` y un solo `ENTREGADO`.

En Estadísticas los pedidos recibidos pasaron de 18 a 19, los pagados de 3 a 4 y los entregados de 3 a 4, y las ventas entregadas de 1350 a 1800, justo los 450 del pedido.

**Algo que parece raro en la salida.** En la evidencia las líneas `WS <- PAGADO`, `FACTURADO` y `ENTREGADO` salen después de la entrega, pero no llegaron tarde. El script escucha el WebSocket en una tarea de `asyncio` que solo corre cuando el resto le cede el control, y en esos pasos las llamadas HTTP son bloqueantes. Los mensajes ya estaban en el socket y se imprimieron al terminar esas llamadas. En Kafka el orden es correcto, con los offsets 30, 31 y 32.

Ese mismo día la prueba ya se había corrido con otro pedido (`3ff2d977-…`, offsets 15 a 20) y el resultado fue igual.

## Explicación (conceptos del Cap. 2)

**Síncrono y asíncrono en un mismo flujo.** El estudiante y el cajero usan REST, que es síncrono, porque necesitan saber al momento si el pedido se creó o si la entrega se rechazó. Entre servicios la comunicación es asíncrona con Kafka, y por eso la API respondió `201` en 0,1 s aunque Cocina tardó 5 s. Es el desacoplamiento temporal del modelo de mensajería.

**Comunicación persistente.** Kafka guarda cada evento en el log del topic y cada servicio lo lee con su propio consumer group (cocina, caja, estadísticas, facturación, api y notificaciones). Así un mismo evento les llega a todos sin que el productor sepa quiénes son, que es el desacoplamiento lógico.

**Orden por partición.** Todos publican con la clave `idPedido` y Kafka manda los mensajes con la misma clave a la misma partición, que es donde garantiza el orden. Por eso los siete eventos quedaron en la partición 0 en el orden en que pasaron (offsets 27 a 32), aunque los publicaron cuatro servicios distintos.

**gRPC unario cuando se necesita una respuesta.** Facturación no puede facturar sin saber si el pago está confirmado, así que le pregunta a Caja con una llamada síncrona y tipada, `ConfirmarPago`, definida en `caja.proto`. Con Kafka habría que armar una respuesta asíncrona para lo que es solo una consulta puntual entre dos servicios.

**A lo sumo una vez en operaciones con efectos.** El cliente puede reintentar un pago o una entrega, que es la semántica al menos una vez, y el servidor descarta los duplicados. El segundo pago devolvió la misma confirmación sin publicar otro evento y la segunda entrega respondió `200` sin otro `ENTREGADO`, así que el efecto ocurrió una sola vez.

**WebSocket para server push.** El estudiante no tuvo que preguntar. El servidor de notificaciones le mandó cada cambio por la conexión abierta apenas lo leyó de `pedido.estado`, y como la suscripción se hizo **antes** del `GET` no se perdió nada entre las dos acciones.

**Interoperabilidad por contrato.** Cuatro personas programamos los seis servicios por separado y el flujo funcionó sin ajustes, porque todos usamos los mismos topics, la misma clave, el mismo formato de evento (`contracts/pedido.schema.json`) y el mismo `.proto`.
