# API de Pedidos (REST)

- **Responsable:** Integrante 1 (David)
- **Puerto:** 8000
- **Mecanismo:** REST síncrono con el cliente y el cajero. Usa Kafka, a través de la capa común `backend/kafka`, para publicar `pedido.creado` y `ENTREGADO` y para consumir `pedido.estado`.
- **Documentos relacionados:**
  - Contrato en la sección 2 de `docs/contratos.md`
  - Decisión en `docs/adr/ADR-001-rest.md`
  - Prueba en `docs/pruebas/prueba-rest.md`

## Qué hace

1. **Recibe pedidos** con `POST /pedidos`. Valida los datos, toma los precios del menú y calcula el total en centavos.
2. **Evita duplicados con la cabecera `Idempotency-Key`.**
   - Si se reintenta con la misma clave devuelve el mismo pedido con `200`.
   - Si la primera solicitud sigue en proceso responde `409 SOLICITUD_EN_CURSO`.
   - Si la clave se usa con otro pedido responde `409 CLAVE_REUTILIZADA`.
3. **Publica `pedido.creado` en Kafka** y responde `201` recién cuando Kafka confirma. Si no hay confirmación en 5 s responde `503` y no registra el pedido. Un reintento posterior con la misma clave conserva el mismo `idPedido`.
4. **Lleva el estado de cada pedido** leyendo `pedido.estado` con el grupo `api`. Así `GET /pedidos/{id}` muestra lo que hicieron cocina, caja y facturación.
5. **Registra la entrega** con `PATCH /pedidos/{id}/entregar`, solo si el pedido está `LISTO` y `PAGADO`, y una sola vez.

## Archivos

| Archivo | Contenido |
|---|---|
| `main.py` | Aplicación FastAPI con las rutas, la conversión de errores al formato del contrato, CORS y el arranque y parada del consumidor |
| `servicio.py` | Reglas de negocio para crear, consultar y entregar pedidos |
| `idempotencia.py` | Registro de claves en `EN_PROCESO`, `COMPLETADA` o `LIBERADA`, con lock y vencimiento |
| `repositorio.py` | Pedidos en memoria. Aplica los eventos de `pedido.estado` y descarta los duplicados, las combinaciones inválidas y los estados que retroceden |
| `consumidor.py` | Consumidor de `pedido.estado`, basado en `ConsumerBase` de la capa Kafka común |
| `modelos.py` | Modelos de entrada y salida con Pydantic |
| `menu.py` | Menú fijo con precios en centavos |
| `config.py` | Configuración por variables de entorno |
| `tests/test_api.py` | Pruebas automatizadas que usan un productor falso en lugar de Kafka |
| `pruebas/prueba_idempotencia.py` | Prueba de comportamiento contra la API y Kafka reales |
| `pruebas/prueba_flujo_completo.py` | Prueba de integración donde un pedido recorre REST, Kafka, gRPC y WebSocket |

## Requisitos

- Python 3.12 o superior. Lo probamos con 3.14.5
- Docker Desktop para Kafka
- Las dependencias del `requirements.txt` de la raíz

## Instalación (se hace una vez, desde la raíz del repositorio)

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

## Ejecutar

Todos los comandos se ejecutan **desde la raíz del repositorio**.

1. Se levanta Kafka. El servicio `kafka-init` crea los topics con 3 particiones.
   ```bash
   docker compose -f infra/docker-compose.yml up -d
   ```
2. Se levanta la API.
   ```bash
   .venv/bin/python -m backend.api_rest.main
   ```
   En Windows el comando es `.venv\Scripts\python -m backend.api_rest.main`

La API queda en `http://localhost:8000` y la documentación interactiva de Swagger en `http://localhost:8000/docs`. Desde Swagger se pueden probar todos los endpoints sin escribir comandos, en cualquier sistema operativo.

La API arranca aunque Kafka esté apagado. El consumidor intenta conectarse cada 5 s y `POST /pedidos` responde `503` hasta que Kafka vuelva.

### Variables de entorno (opcionales)

| Variable | Valor por defecto | Uso |
|---|---|---|
| `API_PUERTO` | `8000` | Puerto de la API |
| `API_HOST` | `0.0.0.0` | Interfaz de red |
| `VENCIMIENTO_CLAVE_S` | `30` | Segundos después de los cuales una clave `EN_PROCESO` se da por abandonada |
| `KAFKA_GRUPO_API` | `api` | Consumer group para `pedido.estado` |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Dirección de Kafka, definida en la capa común |
| `KAFKA_PRODUCER_TIMEOUT_S` | `5` | Tiempo máximo de espera de la confirmación de Kafka, definido en la capa común |

## Endpoints

| Método y ruta | Éxito | Errores |
|---|---|---|
| `GET /menu` | `200` | Ninguno |
| `POST /pedidos` con la cabecera `Idempotency-Key` | `201` al crear, `200` en un reintento | `400`, `409`, `503` |
| `GET /pedidos/{idPedido}` | `200` | `404` |
| `PATCH /pedidos/{idPedido}/entregar` | `200` | `404`, `409`, `503` |

Todos los errores tienen la forma `{"error": "CODIGO", "mensaje": "..."}`. Los códigos y en qué casos aparecen están en la sección 2 de `docs/contratos.md`.

## Probar a mano con `curl` (macOS / Linux)

Para ver el menú:
```bash
curl -s localhost:8000/menu
```

Para crear un pedido:
```bash
curl -s -X POST localhost:8000/pedidos -H 'Content-Type: application/json' -H 'Idempotency-Key: demo-001' -d '{"idEstudiante":"0102030405","items":[{"idProducto":"SAN-01","cantidad":1},{"idProducto":"JUG-01","cantidad":2}]}'
```

Si se repite **exactamente** el mismo comando, devuelve el mismo `idPedido` con código `200`. Para ver el código HTTP se puede agregar `-w ' HTTP %{http_code}\n'`.

Para consultar el pedido, cambiando `<idPedido>` por el que devolvió la API:
```bash
curl -s localhost:8000/pedidos/<idPedido>
```

Para entregarlo. Responde `409` hasta que cocina lo marque como `LISTO` y caja como `PAGADO`.
```bash
curl -s -X PATCH localhost:8000/pedidos/<idPedido>/entregar
```

Si se quiere ver el pedido avanzar sin levantar los demás servicios, basta con la cocina de Caro, que lo pasa a `EN_PREPARACION` y después a `LISTO`.
```bash
.venv/bin/python -m backend.cocina.main --demora 5
```

## Pruebas

**Pruebas automatizadas.** No necesitan Kafka ni la API en marcha.
```bash
.venv/bin/python -m unittest discover -s backend/api_rest/tests -t .
```
Revisan la creación, los reintentos, la clave reutilizada, la falta de clave, los datos inválidos, Kafka caído, las solicitudes simultáneas, el vencimiento de claves, la reutilización del `idPedido`, cómo se aplican los eventos (duplicados, combinaciones inválidas y estados que no retroceden) y la entrega.

**Prueba de comportamiento.** Necesita Kafka y la API en marcha.
```bash
.venv/bin/python -m backend.api_rest.pruebas.prueba_idempotencia
```
Manda la misma solicitud dos veces, 5 solicitudes simultáneas con la misma clave, una clave reutilizada con otro pedido y una solicitud sin clave. Al final **lee el topic `pedido.creado` desde el inicio** y muestra cuántos eventos hay por pedido, que tiene que ser exactamente uno. Los resultados y su análisis están en `docs/pruebas/prueba-rest.md`.

**Prueba de integración.** Necesita Kafka y los seis servicios en marcha, como se explica en el README de la raíz.
```bash
.venv/bin/python -m backend.api_rest.pruebas.prueba_flujo_completo
```
Crea un pedido, se suscribe por WebSocket, intenta entregarlo antes de tiempo, lo paga dos veces en Caja y lo entrega dos veces. Al final muestra los eventos del pedido en Kafka y compara las estadísticas de antes y después. Los resultados están en `docs/pruebas/integracion.md`.

**Kafka caído.**
```bash
docker stop kafka
```
Si se envía un `POST /pedidos` responde `503` en unos 5 s, mientras `GET /menu` sigue respondiendo. Después se levanta Kafka otra vez.
```bash
docker start kafka
```
Al reenviar el mismo `POST` con la misma clave responde `201` con el mismo `idPedido` del intento que falló.

## Limitaciones conocidas

- Los pedidos y las claves de idempotencia están en memoria, así que se pierden si la API se reinicia.
- El registro de claves y su lock solo existen dentro del proceso, por eso la API funciona como una sola instancia.
- Mientras Kafka está caído cada creación de pedido tarda 5 s en responder `503`.
