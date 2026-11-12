# Contratos de CampusBite

Versión 1.0 — contrato de comunicación entre servicios, basado en el flujo integrado (pasos 1 a 10).
Ningún integrante cambia por su cuenta un endpoint, un topic, un evento, el `.proto` o un estado: cualquier cambio se acuerda en grupo y se registra en la sección 9.

---

## 0. Convenciones generales

| Tema | Acuerdo |
|---|---|
| Formato | JSON en UTF-8; nombres de campos en `camelCase` (en el `.proto`, `snake_case`, como manda la convención de Protobuf) |
| Identificadores | `idPedido`: UUID v4 generado por la API. `idEstudiante`: texto (cédula o código). `idEvento`: UUID v4 generado por quien publica |
| Fechas | ISO 8601 en UTC, por ejemplo `2026-11-13T15:04:05Z` |
| Dinero | Entero en **centavos** de dólar (`250` = $2,50). Se evita `float` para no tener errores de redondeo. Solo la interfaz lo muestra con decimales |
| Errores REST | `{"error": "CODIGO", "mensaje": "texto legible"}` en todos los servicios |
| Validación | Los errores de validación de FastAPI (`422` por defecto) se convierten a `400 DATOS_INVALIDOS` |
| CORS | Las APIs REST habilitan CORS para que el cliente web pueda llamarlas desde el navegador |
| Datos | En memoria. Si un servicio se reinicia, pierde su estado (limitación documentada) |

### Tecnologías

| Uso | Librería |
|---|---|
| Lenguaje | Python 3.12 o superior (probado en 3.14), en un `.venv`. Las versiones exactas están en `requirements.txt` |
| REST | `fastapi` + `uvicorn` |
| Kafka | `kafka-python` 3.0 (la 2.0.2 falla en Python 3.12+). Los consumidores corren en un hilo aparte porque la librería es bloqueante |
| gRPC | `grpcio` + `grpcio-tools` |
| WebSocket | `websockets` |
| Kafka (broker) | Docker Compose, modo KRaft, en `infra/docker-compose.yml` |

### Servicios, responsables y puertos

| Servicio | Responsable | Puerto | Mecanismos que usa |
|---|---|---|---|
| API de Pedidos | David | 8000 | REST (servidor), Kafka (productor y consumidor) |
| Caja: REST del cajero | Dome | 8001 | REST (servidor), Kafka (productor y consumidor) |
| Caja: servidor gRPC | Dome | 50051 | gRPC (servidor) |
| Facturación | Dome | — | Kafka (productor y consumidor), gRPC (cliente) |
| Cocina | Caro | — | Kafka (productor y consumidor) |
| Estadísticas | Caro | 8002 | REST (servidor), Kafka (consumidor) |
| Notificaciones | Cinthy | 8765 | WebSocket (servidor), Kafka (consumidor) |
| Kafka | Caro | 9092 | — |

---

## 1. Estados

El estado de un pedido tiene cuatro dimensiones independientes. Cada una tiene **un solo servicio con autoridad** para cambiarla.

| Tipo | Valores | Quién lo cambia | ¿Se publica en `pedido.estado`? |
|---|---|---|---|
| `PREPARACION` | `RECIBIDO` → `EN_PREPARACION` → `LISTO` | API (`RECIBIDO`), Cocina (los otros) | `RECIBIDO` no (está implícito en `pedido.creado`); los otros sí |
| `PAGO` | `PENDIENTE` → `PAGADO` | Caja | Sí |
| `FACTURACION` | `FACTURADO` o `FACTURACION_PENDIENTE` | Facturación | Sí |
| `ENTREGA` | `ENTREGADO` | API (por acción del cajero) | Sí |

Combinaciones válidas en un evento `pedido.estado` (cualquier otra se ignora y se registra en el log):

| `tipo` | `estado` | `origen` |
|---|---|---|
| `PREPARACION` | `EN_PREPARACION`, `LISTO` | `cocina` |
| `PAGO` | `PENDIENTE`, `PAGADO` | `caja` |
| `FACTURACION` | `FACTURADO`, `FACTURACION_PENDIENTE` | `facturacion` |
| `ENTREGA` | `ENTREGADO` | `api` |

Decisiones:
- **Cocina prepara sin esperar el pago.** En una cafetería es razonable y evita encadenar cocina con caja.
- **Solo se entrega un pedido `LISTO` y `PAGADO`.**

---

## 2. REST: API de Pedidos (David, puerto 8000)

### Menú (fijo, en memoria en la API)

El cliente no envía precios: la API toma el nombre y el precio del menú y calcula el total. Así el cliente no puede manipularlo.

`GET /menu` → `200`
```json
[
  { "idProducto": "SAN-01", "nombre": "Sanduche de pollo", "precio": 250 },
  { "idProducto": "JUG-01", "nombre": "Jugo de naranja", "precio": 100 },
  { "idProducto": "CAF-01", "nombre": "Café americano", "precio": 150 }
]
```

### `POST /pedidos`

Cabecera obligatoria: `Idempotency-Key: <texto único generado por el cliente, por ejemplo un UUID>`

Cuerpo:
```json
{
  "idEstudiante": "0102030405",
  "items": [
    { "idProducto": "SAN-01", "cantidad": 1 },
    { "idProducto": "JUG-01", "cantidad": 2 }
  ]
}
```

Respuesta (`201`, o `200` si es un reintento):
```json
{
  "idPedido": "7f3c2a1e-9b4d-4c1a-8e2f-1a2b3c4d5e6f",
  "idEstudiante": "0102030405",
  "items": [
    { "idProducto": "SAN-01", "nombre": "Sanduche de pollo", "cantidad": 1, "precioUnitario": 250, "subtotal": 250 },
    { "idProducto": "JUG-01", "nombre": "Jugo de naranja", "cantidad": 2, "precioUnitario": 100, "subtotal": 200 }
  ],
  "total": 450,
  "estadoPreparacion": "RECIBIDO",
  "estadoPago": "PENDIENTE",
  "estadoFacturacion": null,
  "entregado": false,
  "fechaCreacion": "2026-11-13T15:04:05Z"
}
```

| Código | Cuándo | `error` |
|---|---|---|
| `201` | Pedido nuevo y Kafka confirmó la publicación | — |
| `200` | La clave ya está `COMPLETADA` con el mismo cuerpo: se devuelve la respuesta guardada, sin crear otro pedido | — |
| `400` | Falta la cabecera | `FALTA_IDEMPOTENCY_KEY` |
| `400` | Lista vacía, cantidad ≤ 0, producto que no está en el menú, campos faltantes | `DATOS_INVALIDOS` |
| `409` | La clave está `EN_PROCESO` (otra solicitud igual en curso) | `SOLICITUD_EN_CURSO` |
| `409` | La clave ya se usó con un cuerpo distinto | `CLAVE_REUTILIZADA` |
| `503` | Kafka no confirmó en 5 s. La clave se borra y el pedido no queda registrado | `SERVICIO_NO_DISPONIBLE` |

Registro de claves (en memoria, protegido con un lock):
```
{ clave: { estado: EN_PROCESO | COMPLETADA, hashCuerpo, respuesta, creada } }
```
Reglas:
1. Con el lock tomado: si la clave no existe, se guarda como `EN_PROCESO`; si existe, se responde según la tabla.
2. Se publica en Kafka esperando confirmación (fuera del lock).
3. Si Kafka confirma: se guarda el pedido, la clave pasa a `COMPLETADA` con su respuesta, y se responde `201`.
4. Si Kafka falla: se borra la clave y se responde `503`.
5. Una clave `EN_PROCESO` por más de 30 s se considera vencida y se libera.

### `GET /pedidos/{idPedido}`
- `200` con el objeto de arriba, actualizado con los eventos de `pedido.estado`.
- `404` `PEDIDO_NO_ENCONTRADO`.

### `PATCH /pedidos/{idPedido}/entregar`
Sin cuerpo. Lo usa el cajero en el mostrador.

| Código | Cuándo | `error` |
|---|---|---|
| `200` | Estaba `LISTO` y `PAGADO`: queda entregado y publica `ENTREGA/ENTREGADO` | — |
| `200` | Ya estaba entregado: responde igual, sin publicar otra vez | — |
| `404` | No existe | `PEDIDO_NO_ENCONTRADO` |
| `409` | No está `LISTO` | `PEDIDO_NO_LISTO` |
| `409` | Está `LISTO` pero no `PAGADO` | `PEDIDO_NO_PAGADO` |
| `503` | Kafka no confirmó la publicación; el pedido no cambia | `SERVICIO_NO_DISPONIBLE` |

---

## 3. REST: Caja (Dome, puerto 8001)

### `POST /caja/pagos/{idPedido}`
Lo usa el cajero cuando el estudiante paga en el mostrador. Cuerpo opcional: `{ "metodo": "EFECTIVO" | "TARJETA" }` (por defecto `EFECTIVO`).

Caja solo conoce los cobros que registró al leer `pedido.creado`; no consulta a la API.

| Código | Cuándo | `error` |
|---|---|---|
| `200` | El cobro pasa de `PENDIENTE` a `PAGADO`; genera `idConfirmacion` y publica `PAGO/PAGADO` | — |
| `200` | Ya estaba `PAGADO`: devuelve lo mismo, sin volver a contar ni publicar | — |
| `404` | Caja aún no tiene ese cobro (todavía no leyó `pedido.creado`, o el pedido no existe) | `COBRO_NO_ENCONTRADO` |

Respuesta:
```json
{
  "idPedido": "7f3c2a1e-...",
  "estado": "PAGADO",
  "monto": 450,
  "metodo": "EFECTIVO",
  "idConfirmacion": "PAG-000123",
  "fechaPago": "2026-11-13T15:07:30Z"
}
```

`GET /caja/pagos/{idPedido}` → `200` con el mismo objeto (estado `PENDIENTE` o `PAGADO`), o `404 COBRO_NO_ENCONTRADO`.

---

## 4. REST: Estadísticas (Caro, puerto 8002)

### `GET /estadisticas`
```json
{
  "pedidosRecibidos": 42,
  "pedidosPagados": 39,
  "pedidosEntregados": 37,
  "ventasEntregadas": 12850,
  "porProducto": [
    { "idProducto": "JUG-01", "nombre": "Jugo de naranja", "cantidad": 30, "total": 3000 }
  ],
  "actualizado": "2026-11-13T16:00:00Z"
}
```
- `pedidosRecibidos` y `porProducto` salen de `pedido.creado`.
- `pedidosPagados` y `pedidosEntregados` salen de `pedido.estado`.
- `ventasEntregadas` suma el total de los pedidos con `ENTREGA/ENTREGADO`.

---

## 5. Kafka (Caro, puerto 9092)

| Topic | Particiones | Clave del mensaje | Productores | Consumer groups |
|---|---|---|---|---|
| `pedido.creado` | 3 | `idPedido` | API | `cocina`, `caja`, `facturacion`, `estadisticas` |
| `pedido.estado` | 3 | `idPedido` | API, Cocina, Caja, Facturación | `api`, `notificaciones`, `estadisticas` |

Configuración:
- **Topics:** se crean explícitamente con 3 particiones (por script o en el `docker-compose`). No se confía en la creación automática, que usaría 1 partición.
- **Serialización:** clave = `idPedido` en UTF-8; valor = JSON en UTF-8.
- **Productor:** `acks='all'`; se espera la confirmación con `future.get(timeout=5)`.
- **Consumidor:** `auto_offset_reset='earliest'`, `enable_auto_commit=False`, y `commit()` **después** de procesar el mensaje. Si el servicio se cae a mitad, el mensaje se vuelve a leer.
- **Orden:** como la clave es `idPedido`, todos los eventos de un pedido van a la misma partición y se leen en el orden en que se escribieron.

### Evento `pedido.creado`
```json
{
  "idEvento": "c1d2e3f4-...",
  "idPedido": "7f3c2a1e-...",
  "idEstudiante": "0102030405",
  "items": [
    { "idProducto": "SAN-01", "nombre": "Sanduche de pollo", "cantidad": 1, "precioUnitario": 250 },
    { "idProducto": "JUG-01", "nombre": "Jugo de naranja", "cantidad": 2, "precioUnitario": 100 }
  ],
  "total": 450,
  "fecha": "2026-11-13T15:04:05Z"
}
```
La `Idempotency-Key` no viaja en el evento: es un detalle interno de la API.

### Evento `pedido.estado`
```json
{
  "idEvento": "a9b8c7d6-...",
  "idPedido": "7f3c2a1e-...",
  "tipo": "PREPARACION",
  "estado": "EN_PREPARACION",
  "origen": "cocina",
  "detalle": {},
  "fecha": "2026-11-13T15:05:00Z"
}
```

Contenido de `detalle` según el estado:

| Estado | `detalle` |
|---|---|
| `PAGADO` | `{ "idConfirmacion": "PAG-000123", "monto": 450 }` |
| `FACTURADO` | `{ "idFactura": "FAC-000045" }` |
| `FACTURACION_PENDIENTE` | `{ "intentos": 6, "ultimoError": "PENDIENTE" }` |
| Los demás | `{}` |

### Deduplicación (Kafka entrega "al menos una vez")
- `pedido.creado`: cada consumidor guarda los `idPedido` ya procesados e ignora los repetidos.
- `pedido.estado`: cada consumidor guarda los `idEvento` ya procesados e ignora los repetidos.

---

## 6. gRPC: `contracts/grpc/caja.proto` (Dome, puerto 50051)

```proto
syntax = "proto3";

package campusbite.caja;

service Caja {
  // Facturación pregunta si el pedido ya está pagado antes de facturar.
  // Idempotente: repetir la llamada devuelve el mismo id_confirmacion
  // y la confirmación para facturar se cuenta una sola vez.
  rpc ConfirmarPago (ConfirmarPagoRequest) returns (ConfirmarPagoResponse);
}

message ConfirmarPagoRequest {
  string id_pedido = 1;
}

enum EstadoPago {
  ESTADO_PAGO_DESCONOCIDO = 0;  // valor por defecto obligatorio en proto3
  PAGADO = 1;
  PENDIENTE = 2;
  NO_ENCONTRADO = 3;
}

message ConfirmarPagoResponse {
  string id_pedido = 1;
  EstadoPago estado = 2;
  string id_confirmacion = 3;  // solo si PAGADO
  int64 monto = 4;             // en centavos
  bool repetida = 5;           // true si esta confirmación ya se había entregado antes
}
```

Reglas del servidor (Caja):
- La primera vez que responde `PAGADO` para un pedido, marca ese pago como "confirmado para facturar".
- Si la llamada llega otra vez, responde lo mismo con `repetida = true` y no vuelve a contarla. Es la semántica "a lo sumo una vez".

Reglas del cliente (Facturación):
- Deadline de 2 s por llamada (`timeout=2`).
- `DEADLINE_EXCEEDED` o `UNAVAILABLE` (errores de transporte), `PENDIENTE` o `NO_ENCONTRADO` (respuestas de negocio) → reintenta cada 10 s, máximo 6 intentos. Después publica `FACTURACION/FACTURACION_PENDIENTE`.
- `PAGADO` → emite una sola factura por pedido (guarda los `idPedido` facturados) y publica `FACTURACION/FACTURADO`.

Los stubs de Python se generan con:
```bash
python -m grpc_tools.protoc -I contracts/grpc --python_out=<destino> --grpc_python_out=<destino> contracts/grpc/caja.proto
```

---

## 7. WebSocket: Notificaciones (Cinthy, `ws://localhost:8765`)

Cliente → servidor:
```json
{ "accion": "suscribir", "idPedido": "7f3c2a1e-..." }
```
```json
{ "accion": "desuscribir", "idPedido": "7f3c2a1e-..." }
```

Servidor → cliente:
```json
{ "tipo": "suscrito", "idPedido": "7f3c2a1e-..." }
```
```json
{ "tipo": "estado", "evento": { "idEvento": "...", "idPedido": "7f3c2a1e-...", "tipo": "PREPARACION", "estado": "LISTO", "origen": "cocina", "detalle": {}, "fecha": "..." } }
```
```json
{ "tipo": "error", "mensaje": "Mensaje inválido" }
```

El servidor reenvía el evento de `pedido.estado` sin modificarlo, dentro de `evento`, para no tener un segundo formato.

Reglas del servidor:
- Guarda `idPedido → conjunto de conexiones` y envía a cada conexión solo los eventos de los pedidos a los que se suscribió.
- Un mensaje inválido responde `error`, pero no cierra la conexión.
- Si una conexión se cierra o falla al enviar, se elimina sin detener el servidor.
- Usa el ping de la librería (`ping_interval=20`) para detectar conexiones muertas.
- Los eventos de pedidos sin conexión abierta se descartan. El estado vive en el backend y el cliente lo recupera con `GET`.

Reglas del cliente:
1. Abre el WebSocket y se suscribe.
2. **Después** hace `GET /pedidos/{idPedido}` para obtener el estado vigente.
3. Si la conexión se cae, reintenta cada 3 s y, al reconectar, repite los pasos 1 y 2.

Ese orden evita perder un evento que llegue entre la consulta y la suscripción.

---

## 8. Resumen del flujo con los contratos

| Paso | Origen → destino | Mecanismo | Contrato |
|---|---|---|---|
| 1 | Cliente → API | REST | `POST /pedidos` |
| 2 | API → Kafka | Kafka | `pedido.creado` |
| 3 | Cliente → Notificaciones, luego → API | WebSocket, REST | `suscribir`, `GET /pedidos/{id}` |
| 4 | Kafka → Cocina, Caja, Facturación, Estadísticas | Kafka | `pedido.creado` |
| 5 | Cocina → Kafka → API, Notificaciones | Kafka, WebSocket | `PREPARACION/EN_PREPARACION` |
| 6 | Cajero → Caja | REST | `POST /caja/pagos/{id}` → `PAGO/PAGADO` |
| 7 | Facturación → Caja | gRPC | `ConfirmarPago` → `FACTURACION/FACTURADO` |
| 8 | Cocina → Kafka → API, Notificaciones | Kafka, WebSocket | `PREPARACION/LISTO` |
| 9 | Cajero → API | REST | `PATCH /pedidos/{id}/entregar` → `ENTREGA/ENTREGADO` |
| 10 | Cliente se reconecta | WebSocket, REST | `suscribir` y luego `GET` |

---

## 9. Registro de cambios

| Versión | Fecha | Cambio | Acordado por |
|---|---|---|---|
| 1.0 | 2026-11-12 | Versión inicial | _pendiente_ |
| 1.1 | 2026-11-12 | Python 3.12 o superior y versiones fijadas en `requirements.txt`; se agrega `contracts/pedido.schema.json` | _pendiente_ |
