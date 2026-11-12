# CampusBite — Sistema distribuido para la cafetería universitaria

Práctica 02 · Mecanismos de comunicación en Sistemas Distribuidos (UPS).

Pedidos desde web/móvil que recorren cocina, caja y facturación, con notificación en tiempo real y estadísticas de ventas.

## Mecanismos (por confirmar en el grupo)
| Mecanismo | Tipo | Uso |
|---|---|---|
| REST | Síncrono | Cliente ↔ API de pedidos |
| Kafka | Asíncrono | API → servicios internos (cocina, caja, facturación) |
| gRPC | Síncrono | Un único caso interno que requiere respuesta inmediata (por definir) |
| WebSockets | Persistente / bidireccional | Notificaciones de estado al cliente |

> Nota: la consigna base pide REST + 2 mecanismos; gRPC es un cuarto mecanismo adicional (confirmar alcance con el docente).

## Estructura
- `backend/` — servicios (api-rest, kafka, cocina, caja, facturacion, grpc, websockets, estadisticas)
- `contracts/` — contrato JSON del pedido y `.proto` de gRPC
- `infra/` — docker-compose (Kafka)
- `docs/arquitectura/` — C4, escenarios de calidad, mapa de microservicios
- `docs/adr/` — decisiones de arquitectura
- `docs/pruebas/` — pruebas de comportamiento (resultados reales)
- `docs/bitacora-ia.md` — bitácora de uso de IA

## Requisitos
_TODO: lenguaje, versiones, Kafka (Docker), etc._

## Cómo ejecutar
_TODO (Integrante 4)_

## Cómo probar
_TODO_

## Equipo
| Integrante | Responsabilidad |
|---|---|
| 1 | REST y contrato del pedido |
| 2 | Kafka y mensajería asíncrona |
| 3 | gRPC y comunicación interna |
| 4 | WebSockets y notificaciones |
