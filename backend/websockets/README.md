# Notificaciones (WebSocket) — Cinthy, Integrante 4

Servidor WebSocket en `ws://localhost:8765` que avisa al estudiante, en tiempo real, cada cambio de estado de **su** pedido. Contrato: `docs/contratos.md`, sección 7.

## Archivos

| Archivo | Qué hace |
|---|---|
| `servidor.py` | Servidor WebSocket (`asyncio` + `websockets`). Clase `Notificador`: suscripciones `idPedido → conexiones`, `notificar(evento)`, limpieza de conexiones caídas |
| `consumidor.py` | Consumidor de `pedido.estado` (grupo `notificaciones`). Hereda de `ConsumerBase` de Caro y corre en un **hilo aparte** |
| `simulador.py` | Modo `--simulado`: genera eventos falsos con el formato exacto de `pedido.estado`, sin Kafka |
| `cliente/index.html` | Página del estudiante: menú, crear pedido, WebSocket → `GET`, reconexión cada 3 s |
| `prueba_reconexion.py` | Ejecuta la prueba de desconexión y reconexión, y deja evidencia con hora |
| `tests/test_servidor.py` | Pruebas unitarias y una de integración con el Kafka falso de Caro |

## Ejecutar (desde la raíz del repo)

```bash
pip install -r requirements.txt

# A) Sin Kafka ni otros servicios (para probar solo el WebSocket)
python -m backend.websockets.servidor --simulado --demora 4

# B) Con el sistema real
docker compose -f infra/docker-compose.yml up -d     # Kafka + topics
python -m backend.api_rest.main                      # API (David), puerto 8000
python -m backend.cocina.main --demora 10            # Cocina (Caro)
python -m backend.websockets.servidor                # Notificaciones, puerto 8765
```

Para la página, abre `backend/websockets/cliente/index.html` con doble clic en el navegador. Si la API o el WebSocket están en otra máquina, usa por ejemplo `index.html?api=http://192.168.1.10:8000&ws=ws://192.168.1.10:8765`.

Pruebas:
```bash
python -m pytest backend/websockets -q
python -m backend.websockets.prueba_reconexion --espera 15
```

## Cómo funciona

1. El navegador abre `ws://localhost:8765`. Es un `GET` HTTP con `Upgrade: websocket`; el servidor responde **`101 Switching Protocols`** y desde ahí la conexión queda abierta y es **full-duplex**.
2. El cliente envía `{"accion": "suscribir", "idPedido": "..."}` y el servidor lo guarda en `suscripciones[idPedido]`.
3. El consumidor de Kafka (en su hilo) recibe un evento de `pedido.estado` y lo pasa al loop de `asyncio` con `asyncio.run_coroutine_threadsafe(notificar(evento), loop)`. Espera a que termine el envío y recién ahí se hace el commit del offset.
4. `notificar` hace **server push**: envía `{"tipo": "estado", "evento": <evento sin modificar>}` solo a las conexiones de ese pedido.
5. Si nadie está suscrito, el aviso se descarta: el WebSocket es comunicación **transitoria**. El estado no se pierde, porque vive en la API y el cliente lo recupera con `GET` al volver.
6. El cliente, al conectarse o reconectarse, **primero se suscribe y después hace `GET`**. Así, un evento que ocurra en medio llega por una de las dos vías.
7. Como el `GET` y los eventos pueden llegar en cualquier orden, la página solo deja **avanzar** cada estado (`RECIBIDO → EN_PREPARACION → LISTO`, `PENDIENTE → PAGADO`). Un `GET` viejo no hace retroceder lo que ya llegó por WebSocket.
