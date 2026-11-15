# ADR-002: Kafka

# Guía de inicio: Docker (Kafka) y entorno Python

Sistema distribuido de pedidos de la cafetería (CampusBite). Esta guía deja el entorno listo y
permite ver funcionar la capa Kafka sin necesitar los demás servicios ya que usamos STUBS.
Todos los comandos se ejecutan **desde la raíz del repositorio** (la carpeta que contiene `backend/`, `infra/` y `docs/`).

## 0. Requisitos
| Herramienta | Versión | Comprobar |
|---|---|---|
| Docker Desktop (abierto y en ejecución) | reciente, con Compose v2 | `docker --version` y `docker compose version` |
| Python | 3.12 (el del contrato) | `python --version` |
| Git | cualquiera | `git --version` |

Puerto **9092** libre (Kafka). Los servicios usan además 8000, 8001, 8002, 8765 y 50051 (ver sección 7).

## 1. Levantar Kafka con Docker
```bash
docker compose -f infra/docker-compose.yml up -d
```
Se crean dos contenedores:
- `kafka`: el broker (modo KRaft, sin Zookeeper) en `localhost:9092`, con *healthcheck*.
- `kafka-init`: espera a que Kafka esté listo, **crea los topics** y termina.

Verificar (esperar 20–40 s la primera vez; la descarga de la imagen puede tardar más):
```bash
docker compose -f infra/docker-compose.yml ps -a
docker compose -f infra/docker-compose.yml logs kafka-init
```
Resultado esperado:
- `ps -a`: `kafka` en **healthy** y `kafka-init` en **Exited (0)**. (Hay que usar `-a`: `ps` solo no muestra los contenedores terminados.)
- `logs kafka-init`: termina con `Topics listos:` y la lista `pedido.creado`, `pedido.dlq`, `pedido.estado`.

Comprobar particiones de un topic:
```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic pedido.creado
```
Debe mostrar `PartitionCount: 3`.

| Topic | Particiones | Uso |
|---|---|---|
| `pedido.creado` | 3 | La API publica cada pedido nuevo |
| `pedido.estado` | 3 | Cocina, Caja, Facturación y API publican cambios de estado |
| `pedido.dlq` | 1 | Mensajes que fallaron tras 3 reintentos o llegaron mal formados |

## 2. Crear el entorno virtual e instalar dependencias
**En la terminal:**
```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```
Si se bloquea bloquea el script (`la ejecución de scripts está deshabilitada`):
```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.venv\Scripts\Activate.ps1
```
**Windows (CMD):** `.venv\Scripts\activate.bat`  
**macOS / Linux:** `source .venv/bin/activate`

Con el entorno activo (el prompt muestra `(.venv)`):
```bash
python -m pip install --upgrade pip
pip install -r backend/kafka/requirements.txt
python -c "import kafka; print(kafka.__version__)"      # debe imprimir 3.0.13
```
Para salir del entorno: `deactivate`.

> No instale el paquete `kafka` ni `kafka-python` de otra versión: la carpeta `backend/kafka/` comparte nombre con la librería.
> Por eso hay que ejecutar siempre desde la raíz y con `python -m ...`.
> Lo mismo aplica a `backend/websockets/` y la librería `websockets`.

## 3. Ejecutar las pruebas (no necesitan Kafka)
```bash
python -m pytest backend/kafka/tests -q
```
Esperado: `30 passed` (pueden aparecer avisos de librerías; se ignoran).

## 4. Demostración del flujo con Kafka real
Abrir **una terminal por línea** (cada una con el `.venv` activado y en la raíz del repo):
```bash
python -m backend.kafka.stubs.monitor_estados                    # 1. muestra lo que llega a pedido.estado
python -m backend.cocina.main --demora 3                         # 2. Cocina (real); 3 s de preparación
python -m backend.kafka.stubs.servicio_stub caja                 # 3. STUB de Caja
python -m backend.kafka.stubs.servicio_stub facturacion          # 4. STUB de Facturación
python -m backend.estadisticas.main                              # 5. API REST de Estadísticas (puerto 8002)
python -m backend.kafka.stubs.simulador_api --n 3 --intervalo 1  # 6. STUB de la API: publica 3 pedidos
```
Qué debe verse:
- Terminal 1 (monitor), por cada pedido: `PREPARACION/EN_PREPARACION`, `PAGO/PAGADO`, `FACTURACION/FACTURADO` y, 3 s después, `PREPARACION/LISTO`.
- Estadísticas: `curl.exe http://localhost:8002/estadisticas` (en PowerShell se escribe `curl.exe`; en macOS/Linux `curl`; o abrir la URL en el navegador) → `pedidosRecibidos: 3`, `pedidosPagados: 3`.
- Entregar un pedido (copiar un `idPedido` impreso por el simulador):
  `python -m backend.kafka.stubs.simulador_api --entregar <idPedido>` → suben `pedidosEntregados` y `ventasEntregadas`.

> Cuando existan los servicios reales (Caja, Facturación, API, Notificaciones), **no ejecutar el stub del mismo servicio**:
> comparten *consumer group* y se repartirían los pedidos.

Otras pruebas útiles:
```bash
python -m backend.kafka.stubs.simulador_api --n 1 --duplicar          # mismo pedido dos veces: el 2.º se ignora
python -m backend.kafka.stubs.servicio_stub caja --fallar-todo        # fuerza reintentos (1 s, 2 s) y envío a la DLQ
```

## 5. Comandos de inspección de Kafka
```bash
# Topics
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
# Grupos de consumidores y su atraso (LAG = mensajes pendientes)
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group cocina
# Leer mensajes de un topic (Ctrl+C para salir)
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic pedido.creado --from-beginning --property print.key=true
# Ver la cola de errores (DLQ)
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic pedido.dlq --from-beginning
```
Los *consumer groups* del contrato: `cocina`, `caja`, `facturacion`, `estadisticas` (sobre `pedido.creado`) y
`api`, `notificaciones`, `estadisticas` (sobre `pedido.estado`).

## 6. Cómo usar la capa Kafka desde otro servicio
```python
from backend.kafka.producer import publicar_pedido_creado, publicar_estado, KafkaNoDisponible

try:
    publicar_pedido_creado(pedido)       # bloquea hasta 5 s esperando la confirmación del broker
except KafkaNoDisponible:
    ...                                  # responder 503 SERVICIO_NO_DISPONIBLE y NO registrar el pedido

publicar_estado(id_pedido, "PAGO", "PAGADO", "caja", {"idConfirmacion": "PAG-000123", "monto": 450})

from backend.kafka.consumer_base import ConsumerBase
class MiServicio(ConsumerBase):
    def procesar(self, evento): ...      # lanzar una excepción = reintento (1 s, 2 s); al 3.º fallo, a la DLQ
MiServicio("api", "api", "pedido.estado").iniciar_en_hilo()   # consume en un hilo aparte (FastAPI, WebSocket)
```
El formato de los eventos y los estados válidos están en `contratos.md`.

## 7. Apagar y reiniciar
| Acción | Comando | Efecto |
|---|---|---|
| Pausar | `docker compose -f infra/docker-compose.yml stop` | Conserva topics y mensajes |
| Reanudar | `docker compose -f infra/docker-compose.yml start` | Continúa donde estaba |
| Apagar y borrar | `docker compose -f infra/docker-compose.yml down` | **Borra los eventos** (no hay volumen); los topics se recrean con `up -d` |
| Ver registros | `docker compose -f infra/docker-compose.yml logs kafka` | Para diagnosticar |

## 8. Problemas frecuentes
| Síntoma | Causa y solución |
|---|---|
| `Cannot connect to the Docker daemon` / `error during connect` | Docker Desktop no está abierto. Abrirlo y esperar a que indique "running". |
| `port is already allocated` (9092) | Otro programa usa el puerto. Windows: `netstat -ano \| findstr :9092`; macOS/Linux: `lsof -i :9092`. |
| `no such service: kafka-init` | Se está usando una versión antigua de `infra/docker-compose.yml`. Hacer `git pull` y repetir `down` + `up -d`. |
| `kafka` queda `unhealthy` | `docker compose -f infra/docker-compose.yml logs kafka`. Suele resolverse con `down` y `up -d`. |
| `kafka-init` en `Exited (1)` | `docker compose -f infra/docker-compose.yml logs kafka-init`. Repetir `up -d`. |
| `ModuleNotFoundError: No module named 'backend'` | Se ejecutó fuera de la raíz o sin `python -m`. Volver a la raíz y usar `python -m paquete.modulo`. |
| `KafkaTimeoutError: Unable to bootstrap` / `KafkaNoDisponible` | Kafka no está encendido o no es alcanzable en `localhost:9092` (`ps -a`). Es el comportamiento esperado si se apaga Kafka: la API respondería 503. |
| `UnknownTopicOrPartition` | Los topics no existen. Repetir `up -d` o ejecutar `python -m backend.kafka.crear_topics`. |
| `pip install` falla o versiones raras | Comprobar `python --version` (3.12) y que el `.venv` esté activado. |
| PowerShell: scripts deshabilitados | `Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass` (solo vale para esa ventana). |
| Un consumidor no recibe nada | Comprobar el grupo con `kafka-consumer-groups.sh --describe`; si dos procesos comparten grupo, se reparten los mensajes. |

## 9. Limitaciones conocidas
- Datos y deduplicación en memoria: al reiniciar un servicio se pierde su estado (Estadísticas lo reconstruye releyendo Kafka).
- Un solo broker con `replication_factor=1`: `down` o la pérdida del contenedor borra los eventos.
- `kafka-python` no ofrece productor idempotente: los duplicados se controlan en los consumidores.
- Si el productor da *timeout*, el mensaje podría haberse escrito igualmente.
- Cocina prepara un pedido a la vez; los reintentos bloquean su partición mientras duran.

