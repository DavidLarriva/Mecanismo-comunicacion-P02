# CampusBite, sistema distribuido para la cafetería universitaria

Práctica 02 · Mecanismos de comunicación en Sistemas Distribuidos (UPS).

Con CampusBite los estudiantes hacen sus pedidos desde la web o el celular. Cada pedido llega a cocina, caja y facturación, el estudiante recibe un aviso cuando su pedido está listo y la administración puede ver las estadísticas de ventas.

## Mecanismos de comunicación

| Mecanismo | Tipo | Dónde se usa | Decisión |
|---|---|---|---|
| REST | Síncrono | Cliente → API de Pedidos, cajero → API y Caja, administración → Estadísticas | `docs/adr/ADR-001-rest.md` |
| Kafka | Asíncrono | API → cocina, caja y estadísticas con `pedido.creado`. Los cambios de estado viajan entre servicios por `pedido.estado`, que también activa a facturación | `docs/adr/ADR-002-kafka.md` |
| gRPC | Síncrono | Facturación → Caja (`ConfirmarPago`) cuando recibe `PAGO/PAGADO`, antes de emitir la factura | `docs/adr/ADR-003-grpc.md` |
| WebSocket | Asíncrono, con conexión persistente | Notificaciones → cliente, para los cambios de estado y el aviso de pedido listo | `docs/adr/ADR-004-websockets.md` |

La consigna pide REST y dos mecanismos más, y nosotros implementamos cuatro. Por qué elegimos estos se explica en `docs/adr/ADR-005-seleccion.md`.

## Estructura

| Carpeta | Contenido |
|---|---|
| `backend/api_rest/` | API de Pedidos (REST, puerto 8000) |
| `backend/kafka/` | Capa Kafka común: productor, consumidor base, eventos y simuladores |
| `backend/cocina/` | Cocina, que consume `pedido.creado` y publica la preparación |
| `backend/caja/` | Caja, con REST en el 8001 y servidor gRPC en el 50051 |
| `backend/facturacion/` | Facturación, que es el cliente gRPC |
| `backend/grpc/` | Cliente y servidor gRPC de Caja |
| `backend/websockets/` | Servidor de notificaciones por WebSocket en el 8765 |
| `backend/estadisticas/` | Estadísticas de ventas por REST en el 8002 |
| `contracts/` | `pedido.schema.json` y `grpc/caja.proto` |
| `infra/` | `docker-compose.yml` de Kafka |
| `docs/contratos.md` | Contrato de comunicación entre servicios |
| `docs/arquitectura/` | C4 nivel 1, escenarios de calidad y mapa de microservicios |
| `docs/adr/` | Decisiones de arquitectura |
| `docs/pruebas/` | Pruebas de comportamiento con resultados reales y sus evidencias |
| `docs/bitacora-ia.md` | Bitácora de uso de IA |

## Requisitos

- Python 3.12 o superior. Lo probamos con 3.14.5
- Docker Desktop para Kafka, con la imagen `apache/kafka:3.8.1`
- Las dependencias de `requirements.txt`, que tienen las versiones fijadas

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

Todos los servicios se ejecutan **desde la raíz del repositorio**, cada uno en su propia terminal. En Windows hay que cambiar `.venv/bin/python` por `.venv\Scripts\python`.

1. **Kafka.** Al levantarlo se crean los topics `pedido.creado`, `pedido.estado` y `pedido.dlq`.
   ```bash
   docker compose -f infra/docker-compose.yml up -d
   ```
2. **API de Pedidos** en `http://localhost:8000`, con Swagger en `/docs`.
   ```bash
   .venv/bin/python -m backend.api_rest.main
   ```
3. **Cocina.**
   ```bash
   .venv/bin/python -m backend.cocina.main --demora 5
   ```
4. **Estadísticas** en `http://localhost:8002`.
   ```bash
   .venv/bin/python -m backend.estadisticas.main
   ```
5. **Caja**, con REST en `http://localhost:8001` y gRPC en el puerto 50051.
   ```bash
   .venv/bin/python -m uvicorn backend.caja.main:app --port 8001
   ```
6. **Facturación.**
   ```bash
   .venv/bin/python -m backend.facturacion.main
   ```
7. **Notificaciones** en `ws://localhost:8765`.
   ```bash
   .venv/bin/python -m backend.websockets.servidor
   ```
8. **Cliente web.** Se abre `backend/websockets/cliente/index.html` en el navegador.

Cuando arranca, cada consumidor lee los eventos que tiene pendientes en Kafka. Si quedaron eventos de ejecuciones anteriores, Cocina y Facturación tardan un poco en ponerse al día. Para empezar con Kafka vacío basta con apagarlo y volver a levantarlo, porque el contenedor no guarda los datos en un volumen.

Para apagar Kafka:
```bash
docker compose -f infra/docker-compose.yml down
```

Hay más detalle en las carpetas de cada servicio, en `backend/api_rest/README.md` y `backend/websockets/README.md`.

## Cómo probar

| Mecanismo | Prueba | Cómo ejecutarla |
|---|---|---|
| REST | Pedido duplicado y Kafka caído (`docs/pruebas/prueba-rest.md`) | `.venv/bin/python -m backend.api_rest.pruebas.prueba_idempotencia` |
| Kafka | Consumidor de Cocina detenido (`docs/pruebas/prueba-kafka.md`) | Se detiene Cocina, se crea un pedido y se vuelve a levantar |
| gRPC | Caja no disponible y pago repetido (`docs/pruebas/prueba-grpc.md`) | Los pasos están en el documento de la prueba |
| WebSocket | Cliente desconectado y reconectado (`docs/pruebas/prueba-websockets.md`) | `.venv/bin/python -m backend.websockets.prueba_reconexion --espera 15` |
| Integración | Flujo completo con los cuatro mecanismos (`docs/pruebas/integracion.md`) | Con Kafka y los seis servicios en marcha: `.venv/bin/python -m backend.api_rest.pruebas.prueba_flujo_completo` |

Las pruebas automatizadas de todo el repositorio no necesitan Kafka ni los servicios en marcha.
```bash
.venv/bin/python -m pytest -q
```

## Equipo

| Integrante | Responsabilidad | Servicios |
|---|---|---|
| 1 · David | REST y contrato del pedido | API de Pedidos, contrato, arquitectura |
| 2 · Caro | Kafka y mensajería asíncrona | Kafka, Cocina, Estadísticas |
| 3 · Dome | gRPC y comunicación interna | Caja, Facturación |
| 4 · Cinthy | WebSockets y notificaciones | Notificaciones, cliente web |
