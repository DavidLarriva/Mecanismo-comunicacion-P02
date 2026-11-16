# ADR-002: Uso de Apache Kafka para la comunicación entre servicios

- **Estado:** Propuesto
- **Fecha:** 2026-11-15
- **Responsable:** Integrante 2 (Carolina Fortmann)

## Contexto

El sistema CampusBite requiere que los pedidos registrados por los estudiantes sean comunicados a diferentes servicios internos, como Cocina, Caja, Facturación y Estadísticas. Estos servicios deben recibir y procesar los eventos correspondientes sin que la API de Pedidos tenga que esperar a que todos estén disponibles.

Una comunicación basada exclusivamente en solicitudes REST síncronas generaría una dependencia directa entre la API y los demás servicios. Por ejemplo, si Cocina estuviera detenida, la API podría verse afectada al intentar comunicarse con ella, aunque el pedido ya pudiera registrarse.

Esta necesidad se relaciona principalmente con los escenarios de calidad priorizados por el equipo:

- **Disponibilidad:** cuando Kafka no está disponible, la API debe evitar registrar pedidos sin confirmar su publicación y responder con un error `503 SERVICIO_NO_DISPONIBLE`.
- **Rendimiento:** la API debe responder después de recibir la confirmación de Kafka, sin esperar a que Cocina, Caja o Facturación terminen de procesar el pedido.
- **Confiabilidad:** los reintentos del cliente deben evitar la creación de pedidos duplicados mediante mecanismos de idempotencia.
- **Interoperabilidad:** los servicios deben intercambiar eventos que respeten un contrato de comunicación compartido.

Por estas necesidades, se requiere un mecanismo de mensajería que permita separar la publicación de eventos de su procesamiento por los distintos servicios.

## Decisión

Se decidió utilizar Apache Kafka como mecanismo de mensajería para implementar la comunicación asíncrona entre los servicios de CampusBite.

La solución utiliza un broker Kafka ejecutado mediante Docker Compose en modo KRaft y una capa común de productores y consumidores para facilitar la integración de los diferentes componentes.

La configuración implementada contempla los siguientes elementos:

| Aspecto | Decisión implementada |
|---|---|
| Broker | Apache Kafka mediante Docker Compose y KRaft |
| `pedido.creado` | Topic con 3 particiones para nuevos pedidos |
| `pedido.estado` | Topic con 3 particiones para cambios de estado |
| `pedido.dlq` | Topic con 1 partición para mensajes fallidos |
| Productor común | `EventProducer` |
| Consumidor común | `ConsumerBase` |
| Consumer groups | Grupos independientes para los diferentes servicios |
| Manejo de errores | Reintentos y envío de mensajes fallidos a la DLQ |
| Deduplicación | Control de eventos repetidos en los consumidores |

Los productores publican eventos en los topics correspondientes y los consumidores los procesan según las responsabilidades de cada servicio. De esta manera, la API no necesita realizar llamadas directas a todos los componentes internos para comunicar la creación de un pedido.

**Archivos de implementación:**
- `backend/kafka/config.py`
- `backend/kafka/eventos.py`
- `backend/kafka/producer.py`
- `backend/kafka/consumer_base.py`
- `backend/kafka/idempotencia.py`
- `backend/kafka/crear_topics.py`
- `infra/docker-compose.yml`

## Alternativas consideradas

Se identificaron otros mecanismos de comunicación que podrían utilizarse entre los servicios:

| Alternativa | Motivo por el que no se seleccionó como mecanismo principal |
|---|---|
| REST síncrono | Requiere que el servicio destinatario esté disponible para responder a la solicitud, generando dependencia temporal entre los componentes. |
| gRPC | Permite una comunicación eficiente entre servicios, pero mantiene una interacción directa entre el cliente y el servidor. |
| Sockets TCP | Requiere gestionar directamente aspectos de conexión, intercambio de datos y recuperación de la comunicación. |
| RabbitMQ | También permite implementar mensajería asíncrona, pero el equipo seleccionó Kafka para trabajar con topics, particiones y grupos de consumidores. |

La selección de Kafka responde a la necesidad de distribuir eventos entre varios servicios, mantenerlos desacoplados y permitir que cada consumidor procese los mensajes de acuerdo con su disponibilidad.

## Justificación

Kafka se seleccionó porque permite implementar una arquitectura basada en eventos, donde los productores y consumidores no necesitan estar disponibles simultáneamente para intercambiar información.

Esta decisión se relaciona con los siguientes conceptos:

- **Persistencia de mensajes:** Kafka almacena los eventos publicados en sus topics durante el periodo de retención configurado. Esto permite que los consumidores puedan recuperar mensajes que todavía no han procesado.
- **Desacoplamiento temporal:** la API puede publicar un pedido sin esperar a que Cocina, Caja o Facturación terminen de procesarlo. Cada servicio puede consumir el evento cuando esté disponible.
- **Desacoplamiento lógico:** los servicios intercambian información mediante eventos y topics, sin necesitar conocer directamente la implementación interna de los demás.
- **Consumer groups:** permiten organizar el consumo de mensajes por servicio y distribuir el procesamiento entre las instancias pertenecientes a un mismo grupo.
- **Particiones:** permiten distribuir los mensajes de un topic y facilitar el procesamiento paralelo. El orden se conserva dentro de cada partición.
- **Reintentos y DLQ:** permiten volver a procesar mensajes que presentan errores y separar aquellos que no pueden procesarse después de agotar los intentos configurados.
- **Idempotencia:** permite evitar que la repetición de un evento produzca efectos duplicados en los consumidores.

La prueba documentada en `docs/pruebas/prueba-kafka.md` permitió comprobar el comportamiento de recuperación del consumidor de Cocina. Se detuvo temporalmente el consumidor, se publicó un pedido mediante la API y posteriormente se reinició Cocina. Se observó que el mismo pedido pasó por los estados `EN_PREPARACION` y `LISTO`, demostrando que el consumidor pudo continuar con el procesamiento después de su interrupción.

## Consecuencias

### Consecuencias positivas

- Los servicios pueden procesar los eventos de manera independiente.
- La API no necesita esperar a que los consumidores completen sus operaciones.
- Los mensajes pueden permanecer almacenados mientras un consumidor está temporalmente detenido.
- Los grupos de consumidores permiten organizar la distribución del procesamiento.
- Los mecanismos de reintentos y DLQ permiten manejar errores durante el consumo.
- La comunicación mediante eventos facilita la integración de los servicios desarrollados por los diferentes integrantes.

### Consecuencias negativas y limitaciones

- La solución incorpora un broker Kafka que debe mantenerse disponible para publicar y consumir eventos.
- La implementación actual utiliza un único broker con factor de replicación 1, por lo que no proporciona alta disponibilidad del broker.
- La deduplicación en memoria puede perder su información cuando un consumidor se reinicia.
- Los reintentos pueden retrasar el procesamiento de otros mensajes de una misma partición.
- La implementación de Cocina procesa los pedidos de manera secuencial.
- Un timeout del productor puede generar incertidumbre sobre si el mensaje llegó a publicarse, por lo que es necesario considerar la idempotencia en los reintentos.
- La persistencia de los eventos depende de la configuración y del almacenamiento del broker; en la configuración actual, eliminar el contenedor puede provocar la pérdida de los mensajes.

### Referencias

- Código de la capa Kafka: `backend/kafka/`
- Servicio de Cocina: `backend/cocina/`
- Servicio de Estadísticas: `backend/estadisticas/`
- Contrato de comunicación: `docs/contratos.md`
- Escenarios de atributos de calidad: `docs/arquitectura/escenarios-calidad.md`
- Prueba de recuperación del consumidor: `docs/pruebas/prueba-kafka.md`