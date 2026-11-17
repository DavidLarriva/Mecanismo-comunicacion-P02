# Contratos de CampusBite

Versión 1.3 del contrato de comunicación entre los servicios.

---

## 0. Convenciones generales

| Tema | Acuerdo |
|---|---|
| Formato | JSON en UTF-8 con los nombres de campos en `camelCase`. En el `.proto` se usa `snake_case`, que es la convención de Protobuf |
| Identificadores | `idPedido` es un UUID v4 que genera la API. `idEstudiante` es texto, la cédula o el código. `idEvento` es un UUID v4 que genera quien publica |
| Fechas | ISO 8601 en UTC, por ejemplo `2026-11-13T15:04:05Z` |
| Dinero | Entero en **centavos** de dólar, por ejemplo `250` es $2,50. No usamos `float` para evitar errores de redondeo. Solo la interfaz lo muestra con decimales |
| Errores REST | `{"error": "CODIGO", "mensaje": "texto legible"}` en todos los servicios |
| Validación | Los errores de validación de FastAPI, que por defecto son `422`, se convierten a `400 DATOS_INVALIDOS` |
| CORS | Las APIs REST tienen CORS habilitado para que el cliente web las pueda llamar desde el navegador |
| Datos | En memoria. Si un servicio se reinicia pierde su estado, y es una limitación que ya está documentada |

### Tecnologías

| Uso | Librería |
|---|---|
| Lenguaje | Python 3.12 o superior dentro de un `.venv`. Lo probamos en 3.14 y las versiones exactas están en `requirements.txt` |
| REST | `fastapi` + `uvicorn` |
| Kafka | `kafka-python` 3.0, porque la 2.0.2 falla desde Python 3.12. Los consumidores corren en un hilo aparte porque la librería es bloqueante |
| gRPC | `grpcio` + `grpcio-tools` |
| WebSocket | `websockets` |
| Kafka (broker) | Docker Compose en modo KRaft, definido en `infra/docker-compose.yml` |

### Servicios, responsables y puertos

| Servicio | Responsable | Puerto | Mecanismos que usa |
|---|---|---|---|
| API de Pedidos | David | 8000 | REST (servidor), Kafka (productor y consumidor) |
| Caja: REST del cajero | Dome | 8001 | REST (servidor), Kafka (productor y consumidor) |
| Caja: servidor gRPC | Dome | 50051 | gRPC (servidor) |
| Facturación | Dome | No tiene | Kafka (productor y consumidor), gRPC (cliente) |
| Cocina | Caro | No tiene | Kafka (productor y consumidor) |
| Estadísticas | Caro | 8002 | REST (servidor), Kafka (consumidor) |
| Notificaciones | Cinthy | 8765 | WebSocket (servidor), Kafka (consumidor) |
| Kafka | Caro | 9092 | Es el broker |

---

## 1. Estados

El estado de un pedido tiene cuatro partes independientes, y cada una tiene **un solo servicio con autoridad** para cambiarla.

| Tipo | Valores | Quién lo cambia | ¿Se publica en `pedido.estado`? |
|---|---|---|---|
| `PREPARACION` | `RECIBIDO` → `EN_PREPARACION` → `LISTO` | La API pone `RECIBIDO` y Cocina los otros dos | `RECIBIDO` no, porque ya va implícito en `pedido.creado`. Los otros sí |
| `PAGO` | `PENDIENTE` → `PAGADO` | Caja | Sí |
| `FACTURACION` | `FACTURADO` o `FACTURACION_PENDIENTE` | Facturación | Sí |
| `ENTREGA` | `ENTREGADO` | La API, cuando el cajero entrega | Sí |

Estas son las combinaciones válidas en un evento `pedido.estado`. Cualquier otra se ignora y se anota en el log.

| `tipo` | `estado` | `origen` |
|---|---|---|
| `PREPARACION` | `EN_PREPARACION`, `LISTO` | `cocina` |
| `PAGO` | `PENDIENTE`, `PAGADO` | `caja` |
| `FACTURACION` | `FACTURADO`, `FACTURACION_PENDIENTE` | `facturacion` |
| `ENTREGA` | `ENTREGADO` | `api` |

Decisiones que tomamos sobre los estados.
- **Cocina prepara sin esperar el pago.** En una cafetería tiene sentido y así cocina no depende de caja.
- **Solo se entrega un pedido que está `LISTO` y `PAGADO`.**

---

## 2. REST: API de Pedidos (David, puerto 8000)

### Menú (fijo y en memoria dentro de la API)

El cliente no manda precios. La API toma el nombre y el precio del menú y calcula el total, así el cliente no lo puede manipular.

`GET /menu` → `200`
```json
[
  { "idProducto": "SAN-01", "nombre": "Sanduche de pollo", "precio": 250 },
  { "idProducto": "EMP-01", "nombre": "Empanada de queso", "precio": 120 },
  { "idProducto": "JUG-01", "nombre": "Jugo de naranja", "precio": 100 },
  { "idProducto": "CAF-01", "nombre": "Café americano", "precio": 150 },
  { "idProducto": "AGU-01", "nombre": "Agua sin gas", "precio": 80 }
]
```

### `POST /pedidos`

La cabecera `Idempotency-Key` es obligatoria. Su valor es un texto único que genera el cliente, por ejemplo un UUID.

El cuerpo es así.
```json
{
  "idEstudiante": "0102030405",
  "items": [
    { "idProducto": "SAN-01", "cantidad": 1 },
    { "idProducto": "JUG-01", "cantidad": 2 }
  ]
}
```

La respuesta es `201`, o `200` si se trata de un reintento.
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
| `201` | Pedido nuevo y Kafka confirmó la publicación | No aplica |
| `200` | La clave ya está `COMPLETADA` con el mismo cuerpo. Se devuelve la respuesta guardada y no se crea otro pedido | No aplica |
| `400` | Falta la cabecera | `FALTA_IDEMPOTENCY_KEY` |
| `400` | Lista vacía, cantidad ≤ 0, un producto que no está en el menú o campos que faltan | `DATOS_INVALIDOS` |
| `409` | La clave está `EN_PROCESO` porque otra solicitud igual sigue en curso | `SOLICITUD_EN_CURSO` |
| `409` | La clave ya se usó con un cuerpo distinto | `CLAVE_REUTILIZADA` |
| `503` | Kafka no confirmó en 5 s. La clave queda `LIBERADA` y el pedido no se registra | `SERVICIO_NO_DISPONIBLE` |

El registro de claves está en memoria y protegido con un lock.
```
{ clave: { estado: EN_PROCESO | COMPLETADA | LIBERADA, hashCuerpo, idPedido, respuesta, creada } }
```
Así se usa el registro.
1. Con el lock tomado, si la clave no existe se guarda como `EN_PROCESO`. Si ya existe se responde según la tabla.
2. Fuera del lock se publica en Kafka y se espera la confirmación.
3. Si Kafka confirma, se guarda el pedido, la clave pasa a `COMPLETADA` con su respuesta y se responde `201`.
4. Si Kafka falla, la clave pasa a `LIBERADA` y se responde `503`. Un reintento con la misma clave y el mismo cuerpo se procesa otra vez **con el mismo `idPedido`**, así que si Kafka sí había escrito el primer mensaje los consumidores lo descartan por `idPedido`.
5. Si una clave lleva más de 30 s en `EN_PROCESO` se da por vencida y se libera.

### `GET /pedidos/{idPedido}`
- `200` con el mismo objeto de arriba, actualizado con los eventos de `pedido.estado`.
- `404` `PEDIDO_NO_ENCONTRADO` si no existe.

### `PATCH /pedidos/{idPedido}/entregar`
No lleva cuerpo. Lo usa el cajero en el mostrador.

| Código | Cuándo | `error` |
|---|---|---|
| `200` | Estaba `LISTO` y `PAGADO`, así que queda entregado y se publica `ENTREGA/ENTREGADO` | No aplica |
| `200` | Ya estaba entregado. Responde lo mismo y no vuelve a publicar | No aplica |
| `404` | No existe | `PEDIDO_NO_ENCONTRADO` |
| `409` | No está `LISTO` | `PEDIDO_NO_LISTO` |
| `409` | Está `LISTO` pero no `PAGADO` | `PEDIDO_NO_PAGADO` |
| `503` | Kafka no confirmó la publicación y el pedido no cambia | `SERVICIO_NO_DISPONIBLE` |

---

## 3. REST: Caja (Dome, puerto 8001)

### `POST /caja/pagos/{idPedido}`
Lo usa el cajero cuando el estudiante paga en el mostrador. El cuerpo es opcional, `{ "metodo": "EFECTIVO" | "TARJETA" }`, y si no se manda se toma `EFECTIVO`.

Caja solo conoce los cobros que registró al leer `pedido.creado` y no le consulta nada a la API.

| Código | Cuándo | `error` |
|---|---|---|
| `200` | El cobro pasa de `PENDIENTE` a `PAGADO`, se genera el `idConfirmacion` y se publica `PAGO/PAGADO` | No aplica |
| `200` | Ya estaba `PAGADO`. Devuelve lo mismo sin volver a contarlo ni publicarlo | No aplica |
| `404` | Caja todavía no tiene ese cobro, porque aún no leyó `pedido.creado` o porque el pedido no existe | `COBRO_NO_ENCONTRADO` |
| `503` | El pago quedó registrado pero Kafka no confirmó `PAGO/PAGADO`. En el reintento Caja publica el mismo evento, con el mismo `idEvento`, y devuelve la misma confirmación | `SERVICIO_NO_DISPONIBLE` |

La respuesta tiene esta forma.
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

`GET /caja/pagos/{idPedido}` responde `200` con el mismo objeto, en estado `PENDIENTE` o `PAGADO`, o `404 COBRO_NO_ENCONTRADO`.

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
`pedidosRecibidos` y `porProducto` se calculan con `pedido.creado`, y `pedidosPagados` y `pedidosEntregados` con `pedido.estado`. `ventasEntregadas` es la suma del total de los pedidos que tienen `ENTREGA/ENTREGADO`.

---

## 5. Kafka (Caro, puerto 9092)

| Topic | Particiones | Clave del mensaje | Productores | Consumer groups |
|---|---|---|---|---|
| `pedido.creado` | 3 | `idPedido` | API | `cocina`, `caja`, `estadisticas` |
| `pedido.estado` | 3 | `idPedido` | API, Cocina, Caja, Facturación | `api`, `notificaciones`, `facturacion`, `estadisticas` |

Facturación no consume `pedido.creado`. Se activa con el evento `PAGO/PAGADO` de `pedido.estado` e ignora los demás (sección 6).

Configuración acordada.
- **Topics.** Se crean a propósito con 3 particiones, con un script o en el `docker-compose`. No dependemos de la creación automática porque usaría 1 partición.
- **Serialización.** La clave es el `idPedido` en UTF-8 y el valor es JSON en UTF-8.
- **Productor.** Usa `acks='all'` y espera la confirmación con `future.get(timeout=5)`.
- **Consumidor.** Usa `auto_offset_reset='earliest'` y `enable_auto_commit=False`, y hace `commit()` **después** de procesar el mensaje. Si el servicio se cae a la mitad, el mensaje se vuelve a leer.
- **Orden.** Como la clave es `idPedido`, todos los eventos de un pedido van a la misma partición y se leen en el orden en que se escribieron.

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
La `Idempotency-Key` no va en el evento porque es un detalle interno de la API.

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

El contenido de `detalle` depende del estado.

| Estado | `detalle` |
|---|---|
| `PAGADO` | `{ "idConfirmacion": "PAG-000123", "monto": 450 }` |
| `FACTURADO` | `{ "idFactura": "FAC-000045" }` |
| `FACTURACION_PENDIENTE` | `{ "intentos": 6, "ultimoError": "PENDIENTE" }` |
| Los demás | `{}` |

### Deduplicación (Kafka entrega "al menos una vez")

En `pedido.creado` cada consumidor guarda los `idPedido` que ya procesó e ignora los repetidos. En `pedido.estado` hace lo mismo con los `idEvento`.

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

Reglas del servidor, que es Caja.
- La primera vez que responde `PAGADO` para un pedido marca ese pago como confirmado para facturar.
- Si la llamada llega de nuevo responde lo mismo con `repetida = true` y no la vuelve a contar. Esa es la semántica "a lo sumo una vez".

Reglas del cliente, que es Facturación.
- Llama a `ConfirmarPago` cuando lee un evento `PAGO/PAGADO` en `pedido.estado`. El evento solo avisa, y la confirmación la da Caja porque es la autoridad sobre el pago.
- Cada llamada tiene un deadline de 2 s (`timeout=2`).
- Si recibe un error de transporte como `DEADLINE_EXCEEDED` o `UNAVAILABLE`, o una respuesta de negocio como `PENDIENTE` o `NO_ENCONTRADO`, reintenta cada 10 s hasta 6 veces. Después publica `FACTURACION/FACTURACION_PENDIENTE`.
- Si recibe `PAGADO` emite una sola factura por pedido, para eso guarda los `idPedido` facturados, y publica `FACTURACION/FACTURADO`.

Los stubs de Python se generan con este comando.
```bash
python -m grpc_tools.protoc -I contracts/grpc --python_out=<destino> --grpc_python_out=<destino> contracts/grpc/caja.proto
```

---

## 7. WebSocket: Notificaciones (Cinthy, `ws://localhost:8765`)

Mensajes del cliente al servidor:
```json
{ "accion": "suscribir", "idPedido": "7f3c2a1e-..." }
```
```json
{ "accion": "desuscribir", "idPedido": "7f3c2a1e-..." }
```

Mensajes del servidor al cliente:
```json
{ "tipo": "suscrito", "idPedido": "7f3c2a1e-..." }
```
```json
{ "tipo": "estado", "evento": { "idEvento": "...", "idPedido": "7f3c2a1e-...", "tipo": "PREPARACION", "estado": "LISTO", "origen": "cocina", "detalle": {}, "fecha": "..." } }
```
```json
{ "tipo": "error", "mensaje": "Mensaje inválido" }
```

El servidor reenvía el evento de `pedido.estado` tal como llega, dentro de `evento`, para no tener que manejar un segundo formato.

Reglas del servidor.
- Guarda para cada `idPedido` el conjunto de conexiones suscritas y a cada conexión le manda solo los eventos de sus pedidos.
- Si recibe un mensaje inválido responde `error`, pero no cierra la conexión.
- Si una conexión se cierra o falla al enviar, la quita sin detener el servidor.
- Usa el ping de la librería (`ping_interval=20`) para detectar conexiones muertas.
- Si un pedido no tiene ninguna conexión abierta, sus eventos se descartan. El estado sigue en el backend y el cliente lo recupera con `GET`.

Reglas del cliente.
1. Abre el WebSocket y se suscribe.
2. **Después** hace `GET /pedidos/{idPedido}` para conocer el estado actual.
3. Si la conexión se cae reintenta cada 3 s, y cuando se reconecta repite los pasos 1 y 2.

Con ese orden no se pierde un evento que llegue entre la consulta y la suscripción.

---

## 8. Resumen del flujo con los contratos

| Paso | Origen → destino | Mecanismo | Contrato |
|---|---|---|---|
| 1 | Cliente → API | REST | `POST /pedidos` |
| 2 | API → Kafka | Kafka | `pedido.creado` |
| 3 | Cliente → Notificaciones, luego → API | WebSocket, REST | `suscribir`, `GET /pedidos/{id}` |
| 4 | Kafka → Cocina, Caja, Estadísticas | Kafka | `pedido.creado`, y Caja publica `PAGO/PENDIENTE` |
| 5 | Cocina → Kafka → API, Notificaciones | Kafka, WebSocket | `PREPARACION/EN_PREPARACION` |
| 6 | Cajero → Caja | REST | `POST /caja/pagos/{id}` → `PAGO/PAGADO` |
| 7 | Kafka → Facturación → Caja | Kafka, gRPC | `PAGO/PAGADO` → `ConfirmarPago` → `FACTURACION/FACTURADO` |
| 8 | Cocina → Kafka → API, Notificaciones | Kafka, WebSocket | `PREPARACION/LISTO` |
| 9 | Cajero → API | REST | `PATCH /pedidos/{id}/entregar` → `ENTREGA/ENTREGADO` |
| 10 | Cliente se reconecta | WebSocket, REST | `suscribir` y luego `GET` |

---
