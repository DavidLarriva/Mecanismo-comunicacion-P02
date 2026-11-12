# CampusBite — Sistema distribuido para la cafetería universitaria

Práctica 02 · Mecanismos de comunicación en Sistemas Distribuidos (UPS).

Pedidos desde web/móvil que recorren cocina, caja y facturación, con notificación en tiempo real y estadísticas de ventas.

## Mecanismos (por confirmar en el grupo)
| Mecanismo | Tipo | Uso |
|---|---|---|
| REST | Síncrono | Cliente ↔ API de pedidos |
| Kafka | Asíncrono | API → servicios internos |
| WebSockets | Asíncrono | Notificaciones al cliente |

## Estructura
- `backend/` — servicios (api-rest, cocina, caja, facturacion, kafka, websockets, estadisticas)
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
| 1 | Arquitectura y API REST |
| 2 | Kafka y mensajería asíncrona |
| 3 | WebSockets y notificaciones |
| 4 | Servicios internos, integración y estadísticas |
