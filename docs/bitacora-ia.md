# Bitácora de uso de IA

Marcar en el código los fragmentos con asistencia de IA (p. ej. comentario `# [IA]`).

# Bitácora de uso de IA

Los fragmentos de código desarrollados con asistencia de inteligencia artificial deben identificarse mediante el comentario `# [IA]` o una marca equivalente, según el lenguaje utilizado. Cada archivo debe incluir el nombre de quien realizó su revisión.

| Integrante y fecha | Herramienta | Consulta | Respuesta (resumen) | Acción del grupo | Validación |
|---|---|---|---|---|---|
| Caro / 12-nov-2026 | ChatGPT | Analizar la guía del proyecto y la división de actividades entre los cuatro integrantes, identificando las responsabilidades del Integrante 2 y sus dependencias. | Se propuso una planificación para la implementación de Kafka, incluyendo los topics, los consumer groups, la política de reintentos, la DLQ y las dependencias con los demás integrantes. | Se utilizó como orientación inicial para organizar las tareas de la capa Kafka y coordinar las dependencias con los demás servicios. | Se contrastó la propuesta con la guía del proyecto y la práctica 01. |
| Caro / 12-nov-2026 | Claude | Generar una implementación independiente de la capa de comunicación Kafka. | Se generó una primera versión utilizando `confluent-kafka` y un formato de eventos propio. | Se descartó esta implementación porque no coincidía con el contrato de comunicación establecido por el grupo. | Se comparó el código generado con el contrato documentado en `contratos.md`. |
| Caro / 12-nov-2026 | Claude | Adaptar la implementación al contrato v1.0 y desarrollar los servicios de Cocina y Estadísticas. | Se propuso una nueva implementación con `kafka-python`, eventos en camelCase, valores monetarios en centavos, un consumidor base y los servicios de Cocina y Estadísticas. | Se tomó como base para adaptar la capa Kafka y desarrollar los consumidores correspondientes.. | Se ejecutaron 30 pruebas con dobles de prueba. La ejecución con Kafka real se verificó por separado. . |
| Caro / 13-nov-2026 | Claude | Revisar las observaciones de David sobre la configuración de Docker Compose, especialmente la creación de topics y el healthcheck. | Se propuso incorporar el servicio `kafka-init` para crear los topics y configurar un healthcheck para Kafka. | Se incorporaron los ajustes necesarios en la configuración de Docker Compose. | Se revisó el estado de los contenedores con `docker compose ps -a` y los registros de `kafka-init`. |
| Caro / 13-nov-2026 | ChatGPT | Consultar cómo solucionar un error al levantar Docker Compose en Windows. | Se sugirió reemplazar `$$T` por rutas completas y agregar un bucle de espera en `kafka-init`. | Se aceptó la propuesta para corregir la configuración y permitir que el servicio de inicialización esperara a Kafka. | Se comprobó mediante la ejecución de Docker Compose en Windows. |
| Caro / 15-nov-2026 | Claude | Organizar los commits. | Se generó una propuesta de división de commits para mantener un orden limpio. | Se utilizaron los documentos como apoyo para organizar el trabajo. | Se realizó una revisión manual de los documentos. |
| Caro / 15-nov-2026 | Claude | Analizar `cambios_para_el_equipo.txt` y organizar las actividades pendientes correspondientes a la implementación de Kafka. | Se prepararon orientaciones para el ADR-002, la prueba de falla del consumidor, la actualización de documentación y la lista de pendientes. | Se utilizaron las orientaciones para revisar y completar la documentación de Kafka y registrar los resultados de las pruebas realizadas. | Se contrastaron las indicaciones con los archivos del proyecto, los escenarios de calidad y los resultados observados durante la ejecución. |

## Código con asistencia de IA

Los siguientes archivos o grupos de archivos recibieron asistencia de herramientas de IA durante su desarrollo o revisión:

- `infra/docker-compose.yml`
- `backend/kafka/*`
- `backend/cocina/main.py`
- `backend/estadisticas/*`
- `backend/kafka/stubs/*`
- `backend/kafka/tests/*`


## Reflexión final

- **Carolina:**

Durante el desarrollo de la capa Kafka utilicé herramientas de inteligencia artificial como apoyo para generar código, resolver errores y organizar la documentación. Sin embargo, algunas respuestas iniciales no coincidieron con la arquitectura ni con los acuerdos establecidos por el grupo, por lo que fue necesario revisarlas y contrastarlas con el código y los contratos del proyecto.

Uno de los primeros inconvenientes fue que la IA propuso utilizar `confluent-kafka` y un formato de eventos propio. Esta solución no correspondía con el contrato de comunicación del grupo, que establecía el uso de `kafka-python` y una estructura específica para los eventos. Por esta razón, descarté esa primera implementación y adapté el código a los acuerdos del proyecto.

También se presentaron diferencias en la configuración de Docker Compose. La primera propuesta no creaba automáticamente los topics de Kafka, por lo que fue necesario incorporar el servicio `kafka-init`. Además, descubrí que utilizar únicamente `docker compose ps` no permitía visualizar este contenedor después de finalizar su ejecución; para comprobarlo correctamente debía utilizar `docker compose ps -a`.

Otro caso ocurrió al configurar el tiempo de espera del productor Kafka. La IA sugirió un comportamiento del constructor de `KafkaProducer` que no coincidía con el observado durante las pruebas: al no encontrar un broker, la inicialización podía tardar aproximadamente 30 segundos. Fue necesario revisar la configuración y ajustar el parámetro `bootstrap_timeout_ms` para cumplir con el límite de espera de 5 segundos establecido en el contrato.

Asimismo, las pruebas generadas inicialmente utilizaban un broker simulado. Aunque fueron útiles para comprobar la lógica de los componentes, no demostraban por sí solas el funcionamiento de la aplicación con una instancia real de Kafka. Por ello, fue necesario realizar pruebas adicionales con Kafka ejecutándose mediante Docker Compose.

Finalmente, algunas recomendaciones sobre el uso de Zookeeper y la configuración de `ADVERTISED_LISTENERS` no correspondían a nuestra implementación, ya que utilizamos KRaft y los servicios Python se ejecutaban fuera de Docker.

En conclusión, la IA fue una herramienta de apoyo para acelerar el desarrollo y facilitar la resolución de problemas, pero sus respuestas no siempre fueron compatibles con las decisiones técnicas del equipo. Por ello, fue necesario verificar cada propuesta con los contratos, la arquitectura, la documentación y las pruebas reales antes de incorporarla al proyecto.
