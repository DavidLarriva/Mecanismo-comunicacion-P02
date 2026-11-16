# CampusBite — Sistema distribuido para la cafetería universitaria

Práctica 02 · Mecanismos de comunicación en Sistemas Distribuidos (UPS).

Los estudiantes hacen pedidos desde la web o el celular. Cada pedido llega a cocina, caja y facturación, el estudiante recibe una notificación cuando está listo, y la administración ve las estadísticas de ventas.

## Mecanismos de comunicación

| Mecanismo | Tipo | Dónde se usa | Decisión |
|---|---|---|---|
| REST | Síncrono | Cliente → API de Pedidos; cajero → API y Caja; administración → Estadísticas | `docs/adr/ADR-001-rest.md` |
| Kafka | Asíncrono | API → cocina, caja, facturación y estadísticas (`pedido.creado`); cambios de estado entre servicios (`pedido.estado`) | `docs/adr/ADR-002-kafka.md` |
| gRPC | Síncrono | Facturación → Caja (`ConfirmarPago`), antes de emitir la factura | `docs/adr/ADR-003-grpc.md` |
| WebSocket | Asíncrono (conexión persistente) | Notificaciones → cliente (cambios de estado y pedido listo) | `docs/adr/ADR-004-websockets.md` |

La consigna pide REST y dos mecanismos más, y el grupo implementa cuatro. La justificación de la selección está en `docs/adr/ADR-005-seleccion.md`.

## Estructura

| Carpeta | Contenido |
|---|---|
| `backend/api_rest/` | API de Pedidos (REST, puerto 8000) |
| `backend/kafka/` | Capa Kafka común: productor, consumidor base, eventos y simuladores |
| `backend/cocina/` | Cocina (consume `pedido.creado`, publica preparación) |
| `backend/caja/` | Caja (REST 8001 y servidor gRPC 50051) |
| `backend/facturacion/` | Facturación (cliente gRPC) |
| `backend/grpc/` | Cliente y servidor gRPC de Caja |
| `backend/websockets/` | Servidor de notificaciones (WebSocket 8765) |
| `backend/estadisticas/` | Estadísticas de ventas (REST 8002) |
| `contracts/` | `pedido.schema.json` y `grpc/caja.proto` |
| `infra/` | `docker-compose.yml` de Kafka |
| `docs/contratos.md` | Contrato de comunicación entre servicios |
| `docs/arquitectura/` | C4 nivel 1, escenarios de calidad y mapa de microservicios |
| `docs/adr/` | Decisiones de arquitectura |
| `docs/pruebas/` | Pruebas de comportamiento con resultados reales y sus evidencias |
| `docs/bitacora-ia.md` | Bitácora de uso de IA |

## Requisitos

- Python 3.12 o superior (probado con 3.14.5)
- Docker Desktop, para Kafka (imagen `apache/kafka:3.8.1`)
- Las dependencias de `requirements.txt` (versiones fijadas)

## Instalación (una sola vez)

Desde la raíz del repositorio.

macOS / Linux:
```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

Windows (PowerShell):
```powershell
py -m venv .venv
.venv\Scripts\pip install -r requirements.txt
```

## Cómo ejecutar

Todos los servicios se ejecutan **desde la raíz del repositorio**, cada uno en su propia terminal. En Windows, reemplazar `.venv/bin/python` por `.venv\Scripts\python`.

1. **Kafka.** Crea los topics `pedido.creado`, `pedido.estado` y `pedido.dlq`:
   ```bash
   docker compose -f infra/docker-compose.yml up -d
   ```
2. **API de Pedidos** (`http://localhost:8000`, Swagger en `/docs`):
   ```bash
   .venv/bin/python -m backend.api_rest.main
   ```
3. **Cocina:**
   ```bash
   .venv/bin/python -m backend.cocina.main --demora 5
   ```
4. **Estadísticas** (`http://localhost:8002`):
   ```bash
   .venv/bin/python -m backend.estadisticas.main
   ```
5. **Caja, Facturación y Notificaciones:** _pendiente (Integrantes 3 y 4)._

Para apagar Kafka:
```bash
docker compose -f infra/docker-compose.yml down
```

Cada servicio tiene más detalle en su propia carpeta. Para la API: `backend/api_rest/README.md`.

## Cómo probar

| Mecanismo | Prueba | Cómo ejecutarla |
|---|---|---|
| REST | Pedido duplicado y Kafka caído (`docs/pruebas/prueba-rest.md`) | `.venv/bin/python -m backend.api_rest.pruebas.prueba_idempotencia` |
| Kafka | `docs/pruebas/prueba-kafka.md` | _Integrante 2_ |
| gRPC | `docs/pruebas/prueba-grpc.md` | _Integrante 3_ |
| WebSocket | `docs/pruebas/prueba-websockets.md` | _Integrante 4_ |
| Integración | `docs/pruebas/integracion.md` | _Todos_ |

Pruebas automatizadas de la API (no necesitan Kafka):
```bash
.venv/bin/python -m unittest discover -s backend/api_rest/tests -t .
```

## Equipo

| Integrante | Responsabilidad | Servicios |
|---|---|---|
| 1 · David | REST y contrato del pedido | API de Pedidos, contrato, arquitectura |
| 2 · Caro | Kafka y mensajería asíncrona | Kafka, Cocina, Estadísticas |
| 3 · Dome | gRPC y comunicación interna | Caja, Facturación |
| 4 · Cinthy | WebSockets y notificaciones | Notificaciones, cliente web |
