# Escenarios de atributos de calidad

Formato: Estímulo · Entorno · Respuesta · Medida de respuesta.

| Prio | Atributo | Escenario inicial |
|---|---|---|
| 1 | Disponibilidad | Si Kafka no está disponible al crear un pedido, la API no afirma falsamente que publicó el evento (se rechaza o queda pendiente para reintento; por definir). |
| 2 | Confiabilidad / idempotencia | Si el cliente reintenta la misma solicitud, no se crean dos pedidos con la misma clave. |
| 3 | Resiliencia | Si el destino gRPC no responde, el llamador termina por timeout y maneja el error sin bloquearse indefinidamente. |
| 4 | Rendimiento | Kafka distribuye el pedido sin esperar a que todos los servicios internos finalicen. |
| 5 | Recuperación del cliente | Tras perder el WebSocket, el cliente recupera el estado vigente por REST y recibe eventos futuros. |

## Detalle
_TODO (Integrante 1): desarrollar cada escenario con estímulo, entorno, respuesta y medida._
