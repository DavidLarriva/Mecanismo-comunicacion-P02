# Bitácora de uso de IA

Marcar en el código los fragmentos con asistencia de IA (p. ej. comentario `# [IA]`).

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

## Código con asistencia de IA: Integrante 1 (David)

Generados con Claude Code a partir del contrato; revisados, probados y ajustados por David:
- `backend/api_rest/*` (incluye `tests/` y `pruebas/`)
- `contracts/pedido.schema.json`

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
