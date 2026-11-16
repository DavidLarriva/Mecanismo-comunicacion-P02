# Bitácora de uso de IA

Los fragmentos de código desarrollados con asistencia de inteligencia artificial deben identificarse mediante el comentario `# [IA]` o una marca equivalente, según el lenguaje utilizado. Cada archivo debe incluir el nombre de quien realizó su revisión.

| Integrante y fecha | Herramienta | Consulta | Respuesta (resumen) | Acción del grupo | Validación |
|---|---|---|---|---|---|
| David / 12-nov-2026 | Claude Code | Leer la consigna y el PDF de división y proponer la estructura del repositorio | Propuso las carpetas `backend/`, `docs/`, `contracts/` e `infra/`, las plantillas de ADR y pruebas, el `.gitignore` y un README inicial | Adopté la estructura y la reorganicé cuando el grupo pasó a 4 mecanismos (ADR-003 gRPC, ADR-004 WebSocket, ADR-005 selección) | Comparada con la estructura sugerida en el PDF de división |
| David / 12-nov-2026 | Claude Code | Revisar si el flujo del PDF de división era razonable | Señaló huecos: cómo llegan los cambios de estado al WebSocket y a la API (propuso el topic `pedido.estado`), qué pasa si Kafka cae, un caso concreto para gRPC, estadísticas y quién es dueño de cada estado | Propuse agregar el cobro pendiente en Caja entre los pasos 4 y 5 para que `ConfirmarPago` consulte algo real. Pedí además controlar la concurrencia de la clave, ordenar por partición con `idPedido`, abrir el WebSocket antes del `GET`, limitar los reintentos y exigir `LISTO` para entregar. Con eso definí el flujo v2 (pasos 1 a 10) | Discutido y aprobado por el grupo antes de programar |
| David / 12-nov-2026 | Claude Code | Revisar la propuesta de contrato del grupo y proponer uno definitivo | Propuso dinero en centavos, menú fijo en la API, `409 CLAVE_REUTILIZADA`, commit manual en Kafka, topics con 3 particiones y el campo `repetida` en el `.proto` | Decidí qué se incorporaba y lo escribí en `docs/contratos.md` v1.0. Las decisiones pendientes (precios, cobro inexistente, entregar solo si está pagado) las tomé con el grupo | Validado con `contracts/pedido.schema.json` y 14 casos de prueba |
| David / 12-nov-2026 | Claude Code | Qué versión de Python y de librerías usar | Recomendó Python 3.12 por un problema de `kafka-python` 2.0.2 | **No aplicaba.** Lo probé y todo funciona en Python 3.14 con `kafka-python` 3.0. Fijé las versiones en `requirements.txt` y cambié el contrato a "3.12 o superior" (v1.1) | Instalación limpia verificada con `pip check` |
| David / 13-nov-2026 | Claude Code | Generar la API REST a partir del contrato | Generó la primera versión de la API en FastAPI: registro de claves con lock, estados `EN_PROCESO`/`COMPLETADA`, `503` si Kafka no confirma, consumidor de `pedido.estado`, entrega y 22 pruebas | Revisé archivo por archivo, la ejecuté y decidí los cambios de las entradas siguientes | 22 pruebas automatizadas y prueba manual sin Kafka (`503` en 5 s) |
| David / 15-nov-2026 | Claude Code | Revisar la integración de la API con la capa Kafka de Caro | Señaló que la API tenía su propio productor y consumidor (dos implementaciones de Kafka) y que la carpeta `api-rest` con guion no se podía importar como paquete | Decidí usar `EventProducer` y `ConsumerBase` de Caro, eliminar el productor propio y renombrar la carpeta a `api_rest` | Flujo real API → Kafka → cocina → API, con los eventos en orden en la misma partición |
| David / 15-nov-2026 | Claude Code | Diseñar la prueba de pedido duplicado con evidencia real | Propuso un script que envía reintentos y solicitudes simultáneas y luego cuenta los eventos en `pedido.creado` | La ejecuté con Kafka real, analicé los resultados y los documenté en `prueba-rest.md` | Resultados reales guardados en `docs/pruebas/evidencias/rest/` |
| David / 15-nov-2026 | Claude Code | Revisar los resultados de la prueba con Kafka caído | Detectó que, si Kafka escribe el mensaje pero se pierde la confirmación, el reintento creaba otro `idPedido` y los consumidores no podían deduplicar | Aprobé el cambio: tras un `503` la clave queda `LIBERADA` y conserva el `idPedido`. Lo registré en el contrato v1.2 | 2 pruebas nuevas y prueba real: el mismo `idPedido` en el intento fallido y en el reintento, con 1 solo evento |
| David / 15-nov-2026 | Claude Code | Guía para redactar el ADR-001, el C4, los escenarios de calidad y el mapa de microservicios | Propuso la estructura y una redacción con los datos medidos en las pruebas | Revisé que cada número coincidiera con las pruebas reales y corregí el mapa, que atribuía la capa Kafka común a servicios que todavía no la usaban | Diagramas Mermaid comprobados al dibujarlos |
| David / 16-nov-2026 | Claude Code | Guía para el README de la API y el README raíz | Propuso las secciones de requisitos, instalación, ejecución y pruebas | Revisé el contenido y ejecuté cada comando antes de dejarlo | Entorno limpio con `pip check`, pruebas, API, Swagger y Estadísticas funcionando con los comandos documentados |
| David / 16-nov-2026 | Claude Code | Revisión del código de todo `main` después de integrar a los cuatro integrantes | Ejecutó las 61 pruebas y el flujo completo con los 6 servicios. Detectó que Facturación ya consumía `pedido.estado` y no `pedido.creado` como decía el contrato, y que un merge había borrado entradas de esta bitácora | Decidí adoptar el diseño de Dome en lugar de pedirle que volviera al contrato, porque corrige un error real (un pago tardío dejaba el pedido sin factura). Actualicé el contrato a v1.3, el mapa y el README, y avisé al grupo, incluido lo de las entradas borradas de la bitácora | Flujo completo: un evento de cada tipo en la misma partición y en orden, y los 6 estados recibidos por WebSocket |
| Caro / 12-nov-2026 | ChatGPT | Analizar la guía del proyecto y la división de actividades entre los cuatro integrantes, identificando las responsabilidades del Integrante 2 y sus dependencias. | Se propuso una planificación para la implementación de Kafka, incluyendo los topics, los consumer groups, la política de reintentos, la DLQ y las dependencias con los demás integrantes. | Se utilizó como orientación inicial para organizar las tareas de la capa Kafka y coordinar las dependencias con los demás servicios. | Se contrastó la propuesta con la guía del proyecto y la práctica 01. |
| Caro / 12-nov-2026 | Claude | Generar una implementación independiente de la capa de comunicación Kafka. | Se generó una primera versión utilizando `confluent-kafka` y un formato de eventos propio. | Se descartó esta implementación porque no coincidía con el contrato de comunicación establecido por el grupo. | Se comparó el código generado con el contrato documentado en `contratos.md`. |
| Caro / 12-nov-2026 | Claude | Adaptar la implementación al contrato v1.0 y desarrollar los servicios de Cocina y Estadísticas. | Se propuso una nueva implementación con `kafka-python`, eventos en camelCase, valores monetarios en centavos, un consumidor base y los servicios de Cocina y Estadísticas. | Se tomó como base para adaptar la capa Kafka y desarrollar los consumidores correspondientes.. | Se ejecutaron 30 pruebas con dobles de prueba. La ejecución con Kafka real se verificó por separado. . |
| Caro / 13-nov-2026 | Claude | Revisar las observaciones de David sobre la configuración de Docker Compose, especialmente la creación de topics y el healthcheck. | Se propuso incorporar el servicio `kafka-init` para crear los topics y configurar un healthcheck para Kafka. | Se incorporaron los ajustes necesarios en la configuración de Docker Compose. | Se revisó el estado de los contenedores con `docker compose ps -a` y los registros de `kafka-init`. |
| Caro / 13-nov-2026 | ChatGPT | Consultar cómo solucionar un error al levantar Docker Compose en Windows. | Se sugirió reemplazar `$$T` por rutas completas y agregar un bucle de espera en `kafka-init`. | Se aceptó la propuesta para corregir la configuración y permitir que el servicio de inicialización esperara a Kafka. | Se comprobó mediante la ejecución de Docker Compose en Windows. |
| Caro / 15-nov-2026 | Claude | Organizar los commits. | Se generó una propuesta de división de commits para mantener un orden limpio. | Se utilizaron los documentos como apoyo para organizar el trabajo. | Se realizó una revisión manual de los documentos. |
| Caro / 15-nov-2026 | Claude | Analizar `cambios_para_el_equipo.txt` y organizar las actividades pendientes correspondientes a la implementación de Kafka. | Se prepararon orientaciones para el ADR-002, la prueba de falla del consumidor, la actualización de documentación y la lista de pendientes. | Se utilizaron las orientaciones para revisar y completar la documentación de Kafka y registrar los resultados de las pruebas realizadas. | Se contrastaron las indicaciones con los archivos del proyecto, los escenarios de calidad y los resultados observados durante la ejecución. |
| #1 · Cinthy · 15/11/2026 | Claude (Anthropic) | Cómo implementar el servidor WebSocket de Notificaciones según el contrato (sección 7), reutilizando `ConsumerBase` de Caro, y cómo pasar eventos de un consumidor bloqueante (`kafka-python`) al loop de `asyncio` | Propuso `servidor.py` (clase `Notificador`), `consumidor.py` con `asyncio.run_coroutine_threadsafe` en un hilo aparte y `simulador.py` para probar sin Kafka | Se incorporaron los tres archivos, marcados en el código. Se cambió la primera versión del simulador (reemplazaba un método en tiempo de ejecución) por un parámetro `al_suscribir`, más simple de explicar | 6 pruebas con `pytest` (incluida una de integración con el Kafka falso de Caro) y ejecución real en modo simulado con desconexión a mitad de la secuencia |
| #2 · Cinthy · 15/11/2026 | Claude (Anthropic) | Página del cliente: crear pedido con `Idempotency-Key`, abrir WebSocket y luego `GET`, reconexión cada 3 s | Propuso `cliente/index.html` y señaló una carrera: un `GET` desactualizado puede hacer retroceder un estado ya recibido por push | Se incorporó y se agregó la regla "cada estado solo avanza" | Probada en Chromium: creación, push, caída simulada, vuelta y reconexión automática tras reiniciar el servidor |
| #3 · Cinthy · 15/11/2026 | Claude (Anthropic) | Script de la prueba de desconexión/reconexión y borrador del ADR-004 | Propuso `prueba_reconexion.py`, `tests/test_servidor.py`, ADR-004 y la guía de la prueba | Se revisaron y adaptaron al contrato v1.0 (eventos en `{"tipo":"estado","evento":...}`, centavos) | Resultado real de la prueba con el sistema completo registrado en `docs/pruebas/prueba-websockets.md` |
| #4 · Cinthy · 15/11/2026 | Claude (Anthropic) | La página y `prueba_reconexion.py` fallaban con `WinError 10061` ("No se pudo cargar el menú") aunque la API estaba corriendo y el WebSocket sí conectaba | Explicó que en Windows `localhost` se resuelve primero a IPv6 (`::1`) y la API escucha solo en IPv4 (`0.0.0.0`); propuso usar `127.0.0.1` en el cliente y en el script | Se cambió a `127.0.0.1` en `cliente/index.html` y `prueba_reconexion.py`, con un comentario que explica el motivo | Se repitió la prueba con Kafka, API y Cocina reales: estado recuperado por REST y `ENTREGADO` recibido por WebSocket (`docs/pruebas/evidencias/websockets/reconexion.txt`) |

## Código con asistencia de IA: Integrante 1 (David)

Generados con Claude Code a partir del contrato; revisados, probados y ajustados por David:
- `backend/api_rest/*` (incluye `tests/` y `pruebas/`)
- `contracts/pedido.schema.json`

## Código con asistencia de IA: Integrante 2 (Caro)

Los siguientes archivos o grupos de archivos recibieron asistencia de herramientas de IA durante su desarrollo o revisión:

- `infra/docker-compose.yml`
- `backend/kafka/*`
- `backend/cocina/main.py`
- `backend/estadisticas/*`
- `backend/kafka/stubs/*`
- `backend/kafka/tests/*`

## Código con asistencia de IA: Integrante 4 (Cinthy)

Generados con Claude y marcados en el código con `# Generado con IA (bitácora #N)`; revisados, probados y ajustados por Cinthya:
- `backend/websockets/*` (servidor, consumidor, simulador, cliente web, prueba de reconexión y tests)
- `docs/adr/ADR-004-websockets.md` y `docs/pruebas/prueba-websockets.md`

## Reflexión final

- **Carolina:**

Durante el desarrollo de la capa Kafka utilicé herramientas de inteligencia artificial como apoyo para generar código, resolver errores y organizar la documentación. Sin embargo, algunas respuestas iniciales no coincidieron con la arquitectura ni con los acuerdos establecidos por el grupo, por lo que fue necesario revisarlas y contrastarlas con el código y los contratos del proyecto.

Uno de los primeros inconvenientes fue que la IA propuso utilizar `confluent-kafka` y un formato de eventos propio. Esta solución no correspondía con el contrato de comunicación del grupo, que establecía el uso de `kafka-python` y una estructura específica para los eventos. Por esta razón, descarté esa primera implementación y adapté el código a los acuerdos del proyecto.

También se presentaron diferencias en la configuración de Docker Compose. La primera propuesta no creaba automáticamente los topics de Kafka, por lo que fue necesario incorporar el servicio `kafka-init`. Además, descubrí que utilizar únicamente `docker compose ps` no permitía visualizar este contenedor después de finalizar su ejecución; para comprobarlo correctamente debía utilizar `docker compose ps -a`.

Otro caso ocurrió al configurar el tiempo de espera del productor Kafka. La IA sugirió un comportamiento del constructor de `KafkaProducer` que no coincidía con el observado durante las pruebas: al no encontrar un broker, la inicialización podía tardar aproximadamente 30 segundos. Fue necesario revisar la configuración y ajustar el parámetro `bootstrap_timeout_ms` para cumplir con el límite de espera de 5 segundos establecido en el contrato.

Asimismo, las pruebas generadas inicialmente utilizaban un broker simulado. Aunque fueron útiles para comprobar la lógica de los componentes, no demostraban por sí solas el funcionamiento de la aplicación con una instancia real de Kafka. Por ello, fue necesario realizar pruebas adicionales con Kafka ejecutándose mediante Docker Compose.

Finalmente, algunas recomendaciones sobre el uso de Zookeeper y la configuración de `ADVERTISED_LISTENERS` no correspondían a nuestra implementación, ya que utilizamos KRaft y los servicios Python se ejecutaban fuera de Docker.

En conclusión, la IA fue una herramienta de apoyo para acelerar el desarrollo y facilitar la resolución de problemas, pero sus respuestas no siempre fueron compatibles con las decisiones técnicas del equipo. Por ello, fue necesario verificar cada propuesta con los contratos, la arquitectura, la documentación y las pruebas reales antes de incorporarla al proyecto.

- **David:**

Usé Claude Code como apoyo durante todo el trabajo: para revisar el código, como guía para ordenar las tareas y la documentación, y para generar la primera versión de la API a partir del contrato. Las decisiones las tomé yo: qué se incorporaba, qué se descartaba y cómo se integraba con el trabajo de mis compañeros. Cada propuesta la verifiqué ejecutándola y contrastándola con el contrato y con las pruebas reales.

Hubo varios casos en que la IA se equivocó o no aplicaba a nuestro diseño:

1. **Versión de Python.** Recomendó Python 3.12 por una incompatibilidad de una versión antigua de `kafka-python`. Al probarlo, todo funcionaba en Python 3.14 con la versión actual de la librería, así que no seguí esa restricción y lo corregí en el contrato.
2. **Dos implementaciones de Kafka.** La primera versión de la API tenía su propio productor y consumidor, aunque el reparto decía que eso se coordinaba con el Integrante 2. Funcionaba por separado, pero duplicaba el mecanismo. Al revisar el trabajo de Caro decidí usar su capa común.
3. **Nombre de la carpeta.** Se mantuvo `api-rest`, con guion, sin advertir que Python no puede importar ese nombre como paquete, cuando el resto del equipo ejecuta los servicios con `python -m backend.<servicio>`.
4. **Un caso que no vio en el diseño.** La primera versión borraba la clave de idempotencia tras un `503`, sin considerar que Kafka pudo haber escrito el mensaje aunque la confirmación no llegara. Lo detectamos al analizar la prueba real, no al diseñar, y por eso agregué el estado `LIBERADA`.
5. **Lo que la IA no conoce.** No sabe lo que hacen los compañeros hasta que está en el repositorio, ni las decisiones del grupo. Por ejemplo, el cambio de Facturación apareció recién al revisar `main`, y la decisión de adoptarlo y actualizar el contrato fue mía.

Mi conclusión es que la IA acelera el trabajo y sirve para encontrar casos que se escapan, pero sus respuestas hay que comprobarlas siempre con el sistema funcionando. Varias de sus propuestas eran correctas en general, pero no para nuestra arquitectura.

- **Cinthya:**

Usé Claude para generar el servidor WebSocket, el consumidor de `pedido.estado`, el simulador, la página del cliente, el script de la prueba y la revisión del ADR-004. Cada archivo está marcado con `# Generado con IA (bitácora #N)` y lo revisé para aprovar que es lo que se hacia y así tammbién poder explicarlo.

**Asunción sobre resolución de nombres (`localhost` vs `127.0.0.1` en Windows):** El código generado por la IA asumió que `localhost` se comportaría de forma idéntica en cualquier sistema operativo. En Windows, `localhost` se resuelve primero a la dirección de loopback IPv6 (`::1`). Dado que la API REST (FastAPI / Uvicorn) escuchaba en `0.0.0.0` (solo IPv4), las llamadas HTTP desde la página y los clientes fallaban con `WinError 10061` ("No se pudo cargar el menú" / conexión rechazada), mientras que el servidor WebSocket sí funcionaba porque `websockets` enlaza en dual-stack (IPv4 e IPv6). Se debió intervenir manualmente para parametrizar y forzar `127.0.0.1` en `index.html` y `prueba_reconexion.py`, garantizando compatibilidad IPv4 estricta.

Otro punto: la IA trabajó al principio con un borrador de contratos que tenía el dinero en dólares con decimales, mientras que el contrato v1.0 del repositorio usa centavos y otro formato para los mensajes del WebSocket. Hubo que adaptarla a la versión del repositorio. Además, la IA no pudo probar con Kafka real en su entorno; la prueba con el sistema completo la ejecuté yo.

Conclusión: la IA aceleró el desarrollo, pero sus suposiciones sobre el entorno solo se pueden validar ejecutando el sistema real en las máquinas del equipo.
