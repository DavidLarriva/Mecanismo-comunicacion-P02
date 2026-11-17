# Bitácora de uso de IA

Los fragmentos de código desarrollados con asistencia de inteligencia artificial deben identificarse mediante el comentario `# [IA]` o una marca equivalente, según el lenguaje utilizado. Cada archivo debe incluir el nombre de quien realizó su revisión.

| Integrante y fecha | Herramienta | Consulta | Respuesta (resumen) | Acción del grupo | Validación |
|---|---|---|---|---|---|
| David / 12-nov-2026 | Claude Code | Leer la consigna y el PDF de división y proponer la estructura del repositorio | Propuso las carpetas `backend/`, `docs/`, `contracts/` e `infra/`, las plantillas de ADR y pruebas, el `.gitignore` y un README inicial | Adopté la estructura y la reorganicé cuando el grupo pasó a 4 mecanismos (ADR-003 gRPC, ADR-004 WebSocket, ADR-005 selección) | Comparada con la estructura sugerida en el PDF de división |
| David / 12-nov-2026 | Claude Code | Revisar si el flujo del PDF de división era razonable | Señaló huecos: cómo llegan los cambios de estado al WebSocket y a la API (propuso el topic `pedido.estado`), qué pasa si Kafka cae, un caso concreto para gRPC, estadísticas y quién es dueño de cada estado | Propuse agregar el cobro pendiente en Caja entre los pasos 4 y 5 para que `ConfirmarPago` consulte algo real. Pedí además controlar la concurrencia de la clave, ordenar por partición con `idPedido`, abrir el WebSocket antes del `GET`, limitar los reintentos y exigir `LISTO` para entregar. Con eso definí el flujo v2 (pasos 1 a 10) | Discutido y aprobado por el grupo antes de programar |
| David / 12-nov-2026 | Claude Code | Revisar la propuesta de contrato del grupo y proponer uno definitivo | Propuso dinero en centavos, menú fijo en la API, `409 CLAVE_REUTILIZADA`, commit manual en Kafka, topics con 3 particiones y el campo `repetida` en el `.proto` | Decidí qué se incorporaba y lo escribí en `docs/contratos.md` v1.0. Las decisiones pendientes (precios, cobro inexistente, entregar solo si está pagado) las tomé con el grupo | Validado con `contracts/pedido.schema.json` y 14 casos de prueba |
| David / 12-nov-2026 | Claude Code | Qué versión de Python y de librerías usar | Recomendó Python 3.12 por un problema de `kafka-python` 2.0.2 | **No aplicaba.** Lo probé y todo funciona en Python 3.14 con `kafka-python` 3.0. Fijé las versiones en `requirements.txt` y cambié el contrato a "3.12 o superior" (v1.1) | Instalación limpia verificada con `pip check` |
| David / 13-nov-2026 | Claude Code | Generar la API REST a partir del contrato | Generó la primera versión de la API en FastAPI: registro de claves con lock, estados `EN_PROCESO`/`COMPLETADA`, `503` si Kafka no confirma, consumidor de `pedido.estado`, entrega y 22 pruebas | Revisé archivo por archivo, la ejecuté y decidí los cambios de las entradas siguientes | 22 pruebas automatizadas y prueba manual sin Kafka (`503` en 5 s) |
| David / 15-nov-2026 | Claude Code | Revisar la integración de la API con la capa Kafka de Carolina | Señaló que la API tenía su propio productor y consumidor (dos implementaciones de Kafka) y que la carpeta `api-rest` con guion no se podía importar como paquete | Decidí usar `EventProducer` y `ConsumerBase` de Carolina, eliminar el productor propio y renombrar la carpeta a `api_rest` | Flujo real API → Kafka → cocina → API, con los eventos en orden en la misma partición |
| David / 15-nov-2026 | Claude Code | Diseñar la prueba de pedido duplicado con evidencia real | Propuso un script que envía reintentos y solicitudes simultáneas y luego cuenta los eventos en `pedido.creado` | La ejecuté con Kafka real, analicé los resultados y los documenté en `prueba-rest.md` | Resultados reales guardados en `docs/pruebas/evidencias/rest/` |
| David / 15-nov-2026 | Claude Code | Revisar los resultados de la prueba con Kafka caído | Detectó que, si Kafka escribe el mensaje pero se pierde la confirmación, el reintento creaba otro `idPedido` y los consumidores no podían deduplicar | Aprobé el cambio: tras un `503` la clave queda `LIBERADA` y conserva el `idPedido`. Lo registré en el contrato v1.2 | 2 pruebas nuevas y prueba real: el mismo `idPedido` en el intento fallido y en el reintento, con 1 solo evento |
| David / 15-nov-2026 | Claude Code | Guía para redactar el ADR-001, el C4, los escenarios de calidad y el mapa de microservicios | Propuso la estructura y una redacción con los datos medidos en las pruebas | Revisé que cada número coincidiera con las pruebas reales y corregí el mapa, que atribuía la capa Kafka común a servicios que todavía no la usaban | Diagramas Mermaid comprobados al dibujarlos |
| David / 16-nov-2026 | Claude Code | Guía para el README de la API y el README raíz | Propuso las secciones de requisitos, instalación, ejecución y pruebas | Revisé el contenido y ejecuté cada comando antes de dejarlo | Entorno limpio con `pip check`, pruebas, API, Swagger y Estadísticas funcionando con los comandos documentados |
| David / 16-nov-2026 | Claude Code | Revisión del código de todo `main` después de integrar a los cuatro integrantes | Ejecutó las 61 pruebas y el flujo completo con los 6 servicios. Detectó que Facturación ya consumía `pedido.estado` y no `pedido.creado` como decía el contrato, y que un merge había borrado entradas de esta bitácora | Decidí adoptar el diseño de Doménica en lugar de pedirle que volviera al contrato, porque corrige un error real (un pago tardío dejaba el pedido sin factura). Actualicé el contrato a v1.3, el mapa y el README, y avisé al grupo, incluido lo de las entradas borradas de la bitácora | Flujo completo: un evento de cada tipo en la misma partición y en orden, y los 6 estados recibidos por WebSocket |
| David / 16-nov-2026 | Claude Code | Ejecutar y documentar la prueba de integración con los seis servicios | Generó `backend/api_rest/pruebas/prueba_flujo_completo.py`, lo ejecutó con Kafka y los seis servicios reales y redactó `docs/pruebas/integracion.md` con los resultados y la explicación con conceptos del capítulo 2 | Revisé que cada dato del documento coincidiera con la salida guardada. La salida mostraba algunos mensajes del WebSocket después de la entrega; se explicó en el documento como un efecto de cómo el script imprime, no como un retraso, porque en Kafka el orden era correcto | Salida real en `docs/pruebas/evidencias/integracion/flujo-completo.txt`: 6 estados por WebSocket y 7 eventos en orden en la partición 0 |
| David / 16-nov-2026 | Claude Code | Redactar lo que faltaba del grupo: ADR-005, explicación de `prueba-grpc.md` y `prueba-kafka.md`, y limpieza de `config.py`, del stub y de `requirements.txt` | Redactó el ADR-005 a partir de las decisiones ya tomadas por el grupo (contrato, ADR-001 a ADR-004) y de la guía de selección del capítulo 2. Agregó a las pruebas de gRPC y Kafka el escenario relacionado y la explicación con conceptos del capítulo. Comprobó en las capturas de Carolina que el pedido de Swagger era el mismo que procesó Cocina | El ADR-005 queda en estado Propuesto hasta que el grupo lo revise y lo apruebe. Los textos de las pruebas de Doménica y Carolina se agregaron a sus documentos sin cambiar sus resultados, para que cada una los revise | Las 61 pruebas automatizadas siguen pasando después de los cambios en `config.py` y el stub |
| Carolina / 12-nov-2026 | ChatGPT | Analizar la guía del proyecto y la división de actividades entre los cuatro integrantes, identificando las responsabilidades del Integrante 2 y sus dependencias. | Se propuso una planificación para la implementación de Kafka, incluyendo los topics, los consumer groups, la política de reintentos, la DLQ y las dependencias con los demás integrantes. | Se utilizó como orientación inicial para organizar las tareas de la capa Kafka y coordinar las dependencias con los demás servicios. | Se contrastó la propuesta con la guía del proyecto y la práctica 01. |
| Carolina / 12-nov-2026 | Claude | Generar una implementación independiente de la capa de comunicación Kafka. | Se generó una primera versión utilizando `confluent-kafka` y un formato de eventos propio. | Se descartó esta implementación porque no coincidía con el contrato de comunicación establecido por el grupo. | Se comparó el código generado con el contrato documentado en `contratos.md`. |
| Carolina / 12-nov-2026 | Claude | Adaptar la implementación al contrato v1.0 y desarrollar los servicios de Cocina y Estadísticas. | Se propuso una nueva implementación con `kafka-python`, eventos en camelCase, valores monetarios en centavos, un consumidor base y los servicios de Cocina y Estadísticas. | Se tomó como base para adaptar la capa Kafka y desarrollar los consumidores correspondientes.. | Se ejecutaron 30 pruebas con dobles de prueba. La ejecución con Kafka real se verificó por separado. . |
| Carolina / 13-nov-2026 | Claude | Revisar las observaciones de David sobre la configuración de Docker Compose, especialmente la creación de topics y el healthcheck. | Se propuso incorporar el servicio `kafka-init` para crear los topics y configurar un healthcheck para Kafka. | Se incorporaron los ajustes necesarios en la configuración de Docker Compose. | Se revisó el estado de los contenedores con `docker compose ps -a` y los registros de `kafka-init`. |
| Carolina / 13-nov-2026 | ChatGPT | Consultar cómo solucionar un error al levantar Docker Compose en Windows. | Se sugirió reemplazar `$$T` por rutas completas y agregar un bucle de espera en `kafka-init`. | Se aceptó la propuesta para corregir la configuración y permitir que el servicio de inicialización esperara a Kafka. | Se comprobó mediante la ejecución de Docker Compose en Windows. |
| Carolina / 15-nov-2026 | Claude | Organizar los commits. | Se generó una propuesta de división de commits para mantener un orden limpio. | Se utilizaron los documentos como apoyo para organizar el trabajo. | Se realizó una revisión manual de los documentos. |
| Carolina / 15-nov-2026 | Claude | Analizar `cambios_para_el_equipo.txt` y organizar las actividades pendientes correspondientes a la implementación de Kafka. | Se prepararon orientaciones para el ADR-002, la prueba de falla del consumidor, la actualización de documentación y la lista de pendientes. | Se utilizaron las orientaciones para revisar y completar la documentación de Kafka y registrar los resultados de las pruebas realizadas. | Se contrastaron las indicaciones con los archivos del proyecto, los escenarios de calidad y los resultados observados durante la ejecución. |
| #1 · Cinthya · 15/11/2026 | Claude (Anthropic) | Cómo implementar el servidor WebSocket de Notificaciones según el contrato (sección 7), reutilizando `ConsumerBase` de Carolina, y cómo pasar eventos de un consumidor bloqueante (`kafka-python`) al loop de `asyncio` | Propuso `servidor.py` (clase `Notificador`), `consumidor.py` con `asyncio.run_coroutine_threadsafe` en un hilo aparte y `simulador.py` para probar sin Kafka | Se incorporaron los tres archivos, marcados en el código. Se cambió la primera versión del simulador (reemplazaba un método en tiempo de ejecución) por un parámetro `al_suscribir`, más simple de explicar | 6 pruebas con `pytest` (incluida una de integración con el Kafka falso de Carolina) y ejecución real en modo simulado con desconexión a mitad de la secuencia |
| #2 · Cinthya · 15/11/2026 | Claude (Anthropic) | Página del cliente: crear pedido con `Idempotency-Key`, abrir WebSocket y luego `GET`, reconexión cada 3 s | Propuso `cliente/index.html` y señaló una carrera: un `GET` desactualizado puede hacer retroceder un estado ya recibido por push | Se incorporó y se agregó la regla "cada estado solo avanza" | Probada en Chromium: creación, push, caída simulada, vuelta y reconexión automática tras reiniciar el servidor |
| #3 · Cinthya · 15/11/2026 | Claude (Anthropic) | Script de la prueba de desconexión/reconexión y borrador del ADR-004 | Propuso `prueba_reconexion.py`, `tests/test_servidor.py`, ADR-004 y la guía de la prueba | Se revisaron y adaptaron al contrato v1.0 (eventos en `{"tipo":"estado","evento":...}`, centavos) | Resultado real de la prueba con el sistema completo registrado en `docs/pruebas/prueba-websockets.md` |
| #4 · Cinthya · 15/11/2026 | Claude (Anthropic) | La página y `prueba_reconexion.py` fallaban con `WinError 10061` ("No se pudo cargar el menú") aunque la API estaba corriendo y el WebSocket sí conectaba | Explicó que en Windows `localhost` se resuelve primero a IPv6 (`::1`) y la API escucha solo en IPv4 (`0.0.0.0`); propuso usar `127.0.0.1` en el cliente y en el script | Se cambió a `127.0.0.1` en `cliente/index.html` y `prueba_reconexion.py`, con un comentario que explica el motivo | Se repitió la prueba con Kafka, API y Cocina reales: estado recuperado por REST y `ENTREGADO` recibido por WebSocket (`docs/pruebas/evidencias/websockets/reconexion.txt`) |
| Domenica Uyunkar / 13-nov-2026 | ChatGPT | Diseñar el contrato gRPC para que Facturación consulte a Caja mediante `ConfirmarPago`. | Se propuso un servicio unario con mensajes tipados, estados de pago y un timeout de 2 segundos. | Creé `caja.proto`, el cliente y el servidor gRPC conectados al almacén de pagos de Caja. | Al llamar a un puerto sin servidor, el cliente obtuvo `DEADLINE_EXCEEDED` en 2,02 segundos. |
| Domenica Uyunkar / 15-nov-2026 | ChatGPT | Integrar Caja y Facturación con la capa Kafka compartida y conservar la idempotencia del pago. | Se indicó reutilizar `EventProducer` y `ConsumerBase`, y conservar la misma confirmación al repetir el pago. | Adapté el almacén de pagos y Caja para publicar los estados del pago; conecté la consulta gRPC de Facturación. | El reintento devolvió el mismo `idConfirmacion`; en Kafka se observó el evento `PAGO/PAGADO`. |
| Domenica Uyunkar / 16-nov-2026 | ChatGPT | Investigar por qué Facturación agotaba los reintentos antes de que se registrara un pago tardío. | La revisión identificó que iniciar los intentos al recibir `pedido.creado` podía publicar `FACTURACION_PENDIENTE` antes del pago. Se recomendó activar Facturación con `PAGO/PAGADO` en `pedido.estado`. | Cambié el consumidor de Facturación y actualicé el grupo, el ADR-003 y el documento de prueba. | Tras reiniciar el consumidor, Kafka reprodujo el pago guardado y Facturación publicó `FACTURADO` para el pedido probado. |

## Código con asistencia de IA: Integrante 1 (David Larriva)

Generados con Claude Code a partir del contrato y marcados en el código con `# Realizado con IA y revisado por David Larriva`; revisados, probados y ajustados por David:
- `backend/api_rest/*` (incluye `tests/` y `pruebas/`)
- `contracts/pedido.schema.json`

## Código con asistencia de IA: Integrante 2 (Carolina Fortmann)

Los siguientes archivos o grupos de archivos recibieron asistencia de herramientas de IA durante su desarrollo o revisión:

- `infra/docker-compose.yml`
- `backend/kafka/*`
- `backend/cocina/main.py`
- `backend/estadisticas/*`
- `backend/kafka/stubs/*`
- `backend/kafka/tests/*`

## Código con asistencia de IA: Integrante 3 (Domenica Uyunkar)

Revisados y ajustados por Domenica Uyunkar:

- `contracts/grpc/caja.proto` y los stubs gRPC generados.
- `backend/grpc/caja_client.py` y `backend/grpc/caja_server.py`.
- `backend/caja/almacen_pagos.py`, `backend/caja/kafka_caja.py` y `backend/caja/main.py`.
- `backend/facturacion/main.py`.
- El cambio de Facturación en `backend/kafka/config.py`.

## Código con asistencia de IA: Integrante 4 (Cinthya Ramón)

Generados con Claude y marcados en el código con `# Generado con IA (bitácora #N)`; revisados, probados y ajustados por Cinthya:
- `backend/websockets/*` (servidor, consumidor, simulador, cliente web, prueba de reconexión y tests)
- `docs/adr/ADR-004-websockets.md` y `docs/pruebas/prueba-websockets.md`

## Reflexión final

- **Carolina Fortmann:**

Durante el desarrollo de la capa Kafka utilicé herramientas de inteligencia artificial como apoyo para generar código, resolver errores y organizar la documentación. Sin embargo, algunas respuestas iniciales no coincidieron con la arquitectura ni con los acuerdos establecidos por el grupo, por lo que fue necesario revisarlas y contrastarlas con el código y los contratos del proyecto.

Uno de los primeros inconvenientes fue que la IA propuso utilizar `confluent-kafka` y un formato de eventos propio. Esta solución no correspondía con el contrato de comunicación del grupo, que establecía el uso de `kafka-python` y una estructura específica para los eventos. Por esta razón, descarté esa primera implementación y adapté el código a los acuerdos del proyecto.

También se presentaron diferencias en la configuración de Docker Compose. La primera propuesta no creaba automáticamente los topics de Kafka, por lo que fue necesario incorporar el servicio `kafka-init`. Además, descubrí que utilizar únicamente `docker compose ps` no permitía visualizar este contenedor después de finalizar su ejecución; para comprobarlo correctamente debía utilizar `docker compose ps -a`.

Otro caso ocurrió al configurar el tiempo de espera del productor Kafka. La IA sugirió un comportamiento del constructor de `KafkaProducer` que no coincidía con el observado durante las pruebas: al no encontrar un broker, la inicialización podía tardar aproximadamente 30 segundos. Fue necesario revisar la configuración y ajustar el parámetro `bootstrap_timeout_ms` para cumplir con el límite de espera de 5 segundos establecido en el contrato.

Asimismo, las pruebas generadas inicialmente utilizaban un broker simulado. Aunque fueron útiles para comprobar la lógica de los componentes, no demostraban por sí solas el funcionamiento de la aplicación con una instancia real de Kafka. Por ello, fue necesario realizar pruebas adicionales con Kafka ejecutándose mediante Docker Compose.

Finalmente, algunas recomendaciones sobre el uso de Zookeeper y la configuración de `ADVERTISED_LISTENERS` no correspondían a nuestra implementación, ya que utilizamos KRaft y los servicios Python se ejecutaban fuera de Docker.

En conclusión, la IA fue una herramienta de apoyo para acelerar el desarrollo y facilitar la resolución de problemas, pero sus respuestas no siempre fueron compatibles con las decisiones técnicas del equipo. Por ello, fue necesario verificar cada propuesta con los contratos, la arquitectura, la documentación y las pruebas reales antes de incorporarla al proyecto.

- **David Larriva:**

Durante el trabajo utilicé Claude Code como apoyo para revisar código, organizar tareas, documentar y generar una primera versión de la API a partir del contrato. Sin embargo, las decisiones sobre qué utilizar y cómo integrarlo con el trabajo del equipo las tomé yo, comprobando las propuestas mediante pruebas reales.

También encontré varios errores y limitaciones en sus recomendaciones:

1. **Versión de Python:** recomendó usar Python 3.12 por problemas con una versión antigua de `kafka-python`, pero al probarlo comprobé que funcionaba con Python 3.14 y la librería actual, así que corregí esa restricción en el contrato.
2. **Implementación de Kafka:** inicialmente generó un productor y consumidor propios para la API, duplicando el trabajo del Integrante 2. Al revisar lo que había desarrollado Carolina decidí utilizar su implementación común.
3. **Nombre de la carpeta:** mantuvo `api-rest` sin considerar que el guion genera problemas al importar el paquete en Python mediante `python -m backend.<servicio>`.
4. **Manejo de idempotencia:** la primera versión eliminaba la clave después de un error `503`, sin considerar que Kafka podía haber recibido el mensaje. Esto lo detectamos durante las pruebas y por eso agregué el estado `LIBERADA`.
5. **Contexto del equipo:** la IA no conocía los cambios de mis compañeros ni las decisiones que tomábamos. Por ejemplo, el cambio en Facturación lo encontré al revisar `main` y decidí incorporarlo y actualizar el contrato.

En conclusión, la IA me ayudó a avanzar más rápido y revisar aspectos que podían pasar desapercibidos, pero también comprobé que no todas sus sugerencias se adaptaban a nuestro proyecto. Por eso fue necesario probar cada cambio y revisar que realmente funcionara con la arquitectura del equipo.

- **Cinthya Ramón:**

Usé Claude para generar el servidor WebSocket, el consumidor de `pedido.estado`, el simulador, la página del cliente, el script de la prueba y la revisión del ADR-004. Cada archivo está marcado con `# Generado con IA (bitácora #N)` y lo revisé para aprovar que es lo que se hacia y así tammbién poder explicarlo.

**Asunción sobre resolución de nombres (`localhost` vs `127.0.0.1` en Windows):** El código generado por la IA asumió que `localhost` se comportaría de forma idéntica en cualquier sistema operativo. En Windows, `localhost` se resuelve primero a la dirección de loopback IPv6 (`::1`). Dado que la API REST (FastAPI / Uvicorn) escuchaba en `0.0.0.0` (solo IPv4), las llamadas HTTP desde la página y los clientes fallaban con `WinError 10061` ("No se pudo cargar el menú" / conexión rechazada), mientras que el servidor WebSocket sí funcionaba porque `websockets` enlaza en dual-stack (IPv4 e IPv6). Se debió intervenir manualmente para parametrizar y forzar `127.0.0.1` en `index.html` y `prueba_reconexion.py`, garantizando compatibilidad IPv4 estricta.

Otro punto: la IA trabajó al principio con un borrador de contratos que tenía el dinero en dólares con decimales, mientras que el contrato v1.0 del repositorio usa centavos y otro formato para los mensajes del WebSocket. Hubo que adaptarla a la versión del repositorio. Además, la IA no pudo probar con Kafka real en su entorno; la prueba con el sistema completo la ejecuté yo.

Conclusión: la IA aceleró el desarrollo, pero sus suposiciones sobre el entorno solo se pueden validar ejecutando el sistema real en las máquinas del equipo.

- **Domenica Uyunkar:**

Usé ChatGPT como apoyo para diseñar la comunicación gRPC entre Facturación y Caja, conectar el servidor gRPC con el almacén de pagos y adaptar Facturación a la capa Kafka del grupo. Las propuestas me sirvieron para avanzar con el contrato `.proto`, el cliente, el servidor y los reintentos, pero tuve que revisarlas y corregir detalles de integración e indentación antes de ejecutar el código.

La prueba de pago tardío mostró un problema importante: si Facturación iniciaba sus seis intentos al recibir `pedido.creado`, podía publicar `FACTURACION_PENDIENTE` antes de que el estudiante pagara. Cambié el disparador para que Facturación actuara ante `PAGO/PAGADO` en `pedido.estado`. Después comprobé en Kafka que el evento guardado se procesó y que se publicó `FACTURADO`.

También verifiqué el timeout gRPC: una llamada a un puerto sin servidor terminó con `DEADLINE_EXCEEDED` en 2,02 segundos. En la prueba de idempotencia, repetir la consulta devolvió el mismo identificador de confirmación. Aprendí a validar las sugerencias con el contrato, los logs y los resultados reales; el código sugerido por la IA necesitó correcciones antes de poder usarlo.