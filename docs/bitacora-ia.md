# Bitácora de uso de IA

Marcar en el código los fragmentos con asistencia de IA (p. ej. comentario `# [IA]`).

# Bitácora de uso de IA

Los fragmentos de código desarrollados con asistencia de inteligencia artificial deben identificarse mediante el comentario `# [IA]` o una marca equivalente, según el lenguaje utilizado. Cada archivo debe incluir el nombre de quien realizó su revisión.

| Integrante y fecha | Herramienta | Consulta | Respuesta (resumen) | Acción del grupo | Validación |
|---|---|---|---|---|---|

## Reflexión final

- **Carolina:**

Durante el desarrollo de la capa Kafka utilicé herramientas de inteligencia artificial como apoyo para generar código, resolver errores y organizar la documentación. Sin embargo, algunas respuestas iniciales no coincidieron con la arquitectura ni con los acuerdos establecidos por el grupo, por lo que fue necesario revisarlas y contrastarlas con el código y los contratos del proyecto.

Uno de los primeros inconvenientes fue que la IA propuso utilizar `confluent-kafka` y un formato de eventos propio. Esta solución no correspondía con el contrato de comunicación del grupo, que establecía el uso de `kafka-python` y una estructura específica para los eventos. Por esta razón, descarté esa primera implementación y adapté el código a los acuerdos del proyecto.

También se presentaron diferencias en la configuración de Docker Compose. La primera propuesta no creaba automáticamente los topics de Kafka, por lo que fue necesario incorporar el servicio `kafka-init`. Además, descubrí que utilizar únicamente `docker compose ps` no permitía visualizar este contenedor después de finalizar su ejecución; para comprobarlo correctamente debía utilizar `docker compose ps -a`.

Otro caso ocurrió al configurar el tiempo de espera del productor Kafka. La IA sugirió un comportamiento del constructor de `KafkaProducer` que no coincidía con el observado durante las pruebas: al no encontrar un broker, la inicialización podía tardar aproximadamente 30 segundos. Fue necesario revisar la configuración y ajustar el parámetro `bootstrap_timeout_ms` para cumplir con el límite de espera de 5 segundos establecido en el contrato.

Asimismo, las pruebas generadas inicialmente utilizaban un broker simulado. Aunque fueron útiles para comprobar la lógica de los componentes, no demostraban por sí solas el funcionamiento de la aplicación con una instancia real de Kafka. Por ello, fue necesario realizar pruebas adicionales con Kafka ejecutándose mediante Docker Compose.

Finalmente, algunas recomendaciones sobre el uso de Zookeeper y la configuración de `ADVERTISED_LISTENERS` no correspondían a nuestra implementación, ya que utilizamos KRaft y los servicios Python se ejecutaban fuera de Docker.

En conclusión, la IA fue una herramienta de apoyo para acelerar el desarrollo y facilitar la resolución de problemas, pero sus respuestas no siempre fueron compatibles con las decisiones técnicas del equipo. Por ello, fue necesario verificar cada propuesta con los contratos, la arquitectura, la documentación y las pruebas reales antes de incorporarla al proyecto.
