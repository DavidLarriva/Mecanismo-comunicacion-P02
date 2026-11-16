# Prueba de integración: flujo completo

- **Escenario de calidad relacionado:** Escenario 6, interoperabilidad entre servicios (`docs/arquitectura/escenarios-calidad.md`). Además comprueba, con el sistema completo, la medida del escenario 4 (la API responde sin esperar a los servicios internos) y la regla del escenario 2 aplicada a Caja y a la entrega (repetir una operación no la duplica).
- **Responsable:** Todos (coordina Integrante 4)
- **Mecanismos:** REST, Kafka, gRPC y WebSocket, en un mismo pedido

## Hipótesis

Un pedido creado en la API recorre los cuatro mecanismos siguiendo el flujo del contrato (`docs/contratos.md`, sección 8):

1. La API lo publica en `pedido.creado`. Cocina, Caja y Estadísticas lo consumen.
2. Los cambios de estado viajan por `pedido.estado`. Facturación se activa con `PAGO/PAGADO` y confirma el pago con Caja por gRPC.
3. El estudiante recibe cada cambio por WebSocket, sin consultar.

Si los cuatro integrantes respetan el contrato, todos los eventos del pedido deben quedar en la misma partición y en orden. La entrega solo debe aceptarse cuando el pedido está `LISTO` y `PAGADO`.

## Ejecución

Fecha: 16/11/2026, en una sola máquina con Kafka en Docker (`infra/docker-compose.yml`) y los seis servicios, cada uno en su terminal:

| Servicio | Comando |
|---|---|
| API de Pedidos | `python -m backend.api_rest.main` |
| Cocina | `python -m backend.cocina.main --demora 5` |
| Estadísticas | `python -m backend.estadisticas.main` |
| Caja (REST y gRPC) | `python -m uvicorn backend.caja.main:app --port 8001` |
| Facturación | `python -m backend.facturacion.main` |
| Notificaciones | `python -m backend.websockets.servidor` |

Con todo en marcha se ejecutó:

```bash
python -m backend.api_rest.pruebas.prueba_flujo_completo
```

El script hace lo que harían el estudiante y el cajero:

1. Crea un pedido: 1 sánduche y 2 jugos, total 450 centavos.
2. Se suscribe por WebSocket y consulta el pedido con `GET`.
3. Intenta entregarlo antes de que esté listo y otra vez antes de pagarlo.
4. Consulta el cobro en Caja y paga dos veces el mismo pedido.
5. Intenta pagar un pedido que no existe.
6. Entrega el pedido dos veces.

Al final lee desde Kafka todos los eventos de ese pedido y compara las estadísticas de antes y después.

## Resultado obtenido (real)

Salida completa: `docs/pruebas/evidencias/integracion/flujo-completo.txt`. Pedido `0751c38d-1a4b-4de6-bd6a-dc0117913d76`.

| Momento | Acción | Mecanismo | Resultado |
|---|---|---|---|
| +0,1 s | `POST /pedidos` | REST | `201`, total 450 |
| +0,1 s | Suscripción y `GET /pedidos/{id}` | WebSocket, REST | `suscrito`; el pedido ya estaba `EN_PREPARACION` / `PENDIENTE` |
| +0,1 s | `PATCH /entregar` antes de `LISTO` | REST | `409 PEDIDO_NO_LISTO` |
| +0,1 s | Cocina y Caja leen `pedido.creado` | Kafka | Llegan por WebSocket `EN_PREPARACION` y `PAGO/PENDIENTE` |
| +5,1 s | Cocina termina | Kafka, WebSocket | Llega `LISTO` |
| +5,3 s | `PATCH /entregar` sin pagar | REST | `409 PEDIDO_NO_PAGADO` |
| +5,4 s | `GET /caja/pagos/{id}` | REST | `PENDIENTE`, monto 450 |
| +5,4 s | `POST /caja/pagos/{id}` dos veces | REST | Las dos devuelven `200` con la misma confirmación `PAG-C2F6C3C45D19` |
| +5,4 s | Pago de un pedido inexistente | REST | `404 COBRO_NO_ENCONTRADO` |
| +5,4 s | Facturación lee `PAGO/PAGADO` y llama a `ConfirmarPago` | Kafka, gRPC | `FACTURADO`, factura `FAC-103F52009CA9` |
| +5,5 s | `PATCH /entregar` dos veces | REST | `200` y `200`, `entregado = true` |

**Por WebSocket** llegaron los seis cambios de estado, en este orden: `EN_PREPARACION`, `PENDIENTE`, `LISTO`, `PAGADO`, `FACTURADO` y `ENTREGADO`.

**En Kafka** el pedido tiene 1 evento en `pedido.creado` y 6 en `pedido.estado`, todos en la **partición 0**:

| Topic | Offset | Evento | Publicado por |
|---|---|---|---|
| `pedido.creado` | 7 | Pedido creado, total 450 | API |
| `pedido.estado` | 27 | `PREPARACION/EN_PREPARACION` | Cocina |
| `pedido.estado` | 28 | `PAGO/PENDIENTE` | Caja |
| `pedido.estado` | 29 | `PREPARACION/LISTO` | Cocina |
| `pedido.estado` | 30 | `PAGO/PAGADO` | Caja |
| `pedido.estado` | 31 | `FACTURACION/FACTURADO` | Facturación |
| `pedido.estado` | 32 | `ENTREGA/ENTREGADO` | API |

Aunque el pago y la entrega se repitieron, hay **un solo** `PAGADO`, un solo `FACTURADO` y un solo `ENTREGADO`.

**Estadísticas** pasó de 18 a 19 pedidos recibidos, de 3 a 4 pagados y de 3 a 4 entregados. Las ventas entregadas subieron de 1350 a 1800, es decir, los 450 del pedido.

**Algo que parece raro en la salida.** En el archivo de evidencia, las líneas `WS <- PAGADO`, `FACTURADO` y `ENTREGADO` aparecen después de la entrega. No significa que hayan llegado tarde. El script escucha el WebSocket en una tarea de `asyncio` que solo se ejecuta cuando el resto del script cede el control, y las llamadas HTTP de esos pasos son bloqueantes. Los mensajes ya estaban en el socket y se imprimieron al terminar esas llamadas. En Kafka el orden es el correcto (offsets 30, 31 y 32).

La misma prueba se había ejecutado ese mismo día con otro pedido (`3ff2d977-…`, offsets 15 a 20) y dio el mismo resultado.

## Explicación (conceptos del Cap. 2)

- **Síncrono y asíncrono en un mismo flujo.** El estudiante y el cajero usan REST, que es síncrono: esperan la respuesta y necesitan saber de inmediato si el pedido se creó o si la entrega se rechazó. Entre servicios, la comunicación es asíncrona con Kafka: la API respondió `201` en 0,1 s aunque Cocina tardó 5 s en terminar, porque no la espera. Este es el desacoplamiento temporal del modelo de mensajería.
- **Comunicación persistente.** Kafka guarda cada evento en el log del topic. Cada servicio lo lee con su propio consumer group (cocina, caja, estadísticas, facturación, api y notificaciones), así que un mismo evento llega a todos sin que el productor sepa quiénes son. Es el desacoplamiento lógico.
- **Orden por partición.** Todos los servicios publican con la clave `idPedido`. Kafka envía los mensajes con la misma clave a la misma partición y garantiza el orden solo dentro de una partición. Por eso los siete eventos quedaron en la partición 0, en el orden en que ocurrieron (offsets 27 a 32), aunque los publicaron cuatro servicios distintos.
- **gRPC unario para una pregunta que necesita respuesta.** Facturación no puede facturar sin saber si el pago está confirmado, así que le pregunta a Caja con una llamada síncrona tipada (`ConfirmarPago`, definida en `caja.proto`). Usar Kafka para esto obligaría a armar una respuesta asíncrona para algo que es una consulta puntual entre dos servicios internos.
- **Semántica "a lo sumo una vez" en operaciones con efectos.** Pagar y entregar son operaciones con efectos secundarios. El cliente puede reintentarlas (al menos una vez), pero el servidor descarta los duplicados: el segundo pago devolvió la misma confirmación sin publicar otro evento, y la segunda entrega respondió `200` sin publicar otro `ENTREGADO`. Las dos semánticas combinadas hacen que el efecto ocurra una sola vez.
- **WebSocket para server push.** El estudiante no preguntó por los cambios: el servidor de notificaciones los empujó por la conexión abierta en cuanto los leyó de `pedido.estado`. La suscripción se hizo **antes** del `GET`, así que ningún cambio quedó entre los dos.
- **Interoperabilidad por contrato.** Cuatro personas programaron los seis servicios por separado. El flujo funcionó sin ajustes porque todos usan los mismos nombres de topics, la misma clave, el mismo formato de evento (`contracts/pedido.schema.json`) y el mismo `.proto`.
