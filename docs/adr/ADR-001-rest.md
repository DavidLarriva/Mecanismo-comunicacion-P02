# ADR-001: REST como API pública de pedidos

- **Estado:** Aceptado
- **Fecha:** 2026-11-15
- **Responsable:** Integrante 1 (David)
- **Escenarios de calidad relacionados:** 2 (confiabilidad e idempotencia) y 1 (disponibilidad)

## Contexto

Los estudiantes crean y consultan sus pedidos desde un navegador o un celular. La consigna pide que esta parte use REST, pero cómo usarlo lo decide el grupo, y la solución tiene que cubrir cinco necesidades del sistema.

1. **Respuesta inmediata.** Cuando el cliente crea un pedido necesita su `idPedido` para seguirlo por WebSocket y consultarlo después, así que la operación tiene que devolver algo en ese momento.
2. **Reintentos sin duplicados.** En una red móvil la respuesta a un `POST` se puede perder aunque el servidor ya lo haya procesado. El cliente va a reintentar y eso no puede crear dos pedidos.
3. **No mentir sobre el registro.** Para el resto del sistema un pedido existe recién cuando el evento `pedido.creado` está en Kafka. La API no puede decir que lo registró si esa publicación no ocurrió.
4. **Consultar después de una desconexión.** Si el cliente pierde el WebSocket necesita una forma sencilla de ver el estado actual de su pedido.
5. **Precio confiable.** El total no puede depender de lo que mande el cliente.

## Decisión

La API de Pedidos (`backend/api_rest`, puerto 8000) es un servicio REST sobre HTTP con JSON hecho con FastAPI. Su contrato está en la sección 2 de `docs/contratos.md` y en `contracts/pedido.schema.json`.

| Método y ruta | Uso |
|---|---|
| `GET /menu` | Productos y precios en centavos |
| `POST /pedidos` | Crear un pedido. Exige la cabecera `Idempotency-Key` |
| `GET /pedidos/{idPedido}` | Estado actual del pedido |
| `PATCH /pedidos/{idPedido}/entregar` | El cajero lo marca como entregado, solo si está `LISTO` y `PAGADO` |

Dentro de REST tomamos estas decisiones de diseño.

- **Idempotencia por clave.** El cliente genera una `Idempotency-Key` por pedido y la API la reserva con un lock como `EN_PROCESO` **antes** de publicar.
  - Si todo sale bien pasa a `COMPLETADA` y un reintento recibe `200` con la misma respuesta.
  - Si llega otra solicitud mientras sigue en proceso, se responde `409 SOLICITUD_EN_CURSO`.
  - Si la clave se usa con otro cuerpo se responde `409 CLAVE_REUTILIZADA`. Para saberlo se compara un hash SHA-256 del cuerpo.
- **`201` solo después de que Kafka confirma.** La API publica `pedido.creado` con `acks='all'` y espera hasta 5 s. Si no llega la confirmación responde `503 SERVICIO_NO_DISPONIBLE`, no deja el pedido registrado y libera la clave.
- **El reintento conserva el `idPedido`.** Si la clave se liberó por un `503`, el reintento con el mismo cuerpo usa el mismo `idPedido`. Así, si Kafka sí alcanzó a escribir el primer mensaje, los consumidores pueden descartar el duplicado.
- **Los precios los calcula el servidor.** El cliente solo manda `idProducto` y `cantidad`. Los precios salen del menú y el dinero se maneja en centavos enteros.
- **La API se entera de los cambios de estado por Kafka.** Consume `pedido.estado` con el grupo `api` usando el consumidor base común del equipo. De esta forma `GET /pedidos/{id}` muestra lo que hicieron cocina, caja y facturación sin tener que consultarles.
- **Errores con el mismo formato.** Todos los errores tienen la forma `{"error": "CODIGO", "mensaje": "..."}` y la validación de FastAPI, que normalmente da `422`, se convierte a `400`.

## Alternativas consideradas

| Alternativa | Por qué no se eligió |
|---|---|
| **gRPC para el cliente** | Los navegadores no hablan gRPC directamente. Haría falta gRPC-Web y un proxy como Envoy, que es más infraestructura sin resolver mejor ninguna de las cinco necesidades. Usamos gRPC donde sí aporta, en la llamada interna de Facturación a Caja (ADR-003). |
| **GraphQL** | Sirve cuando el cliente necesita juntar muchos recursos en una sola consulta. Aquí hay un solo recurso, el pedido, con cuatro operaciones fijas, y GraphQL tampoco resuelve mejor la idempotencia ni el `503`. |
| **Crear el pedido por WebSocket** | El WebSocket es un canal con estado pensado para que el servidor notifique al cliente (ADR-004). Para crear pedidos tendríamos que inventar un protocolo de petición y respuesta, con sus códigos de error y reintentos, cuando HTTP ya los trae. |
| **Que el cliente publique directo en Kafka** | Dejaría el broker expuesto a Internet. Además el cliente no recibiría su `idPedido` ni los errores de validación, y se perdería el único punto donde se validan los datos y se aplica la idempotencia. |
| **REST sin clave de idempotencia** | Un `POST` no es idempotente, así que cada reintento crearía un pedido nuevo y no se cumpliría el escenario 2. |
| **Aceptar el pedido con `202` y publicarlo después (patrón outbox)** | Daría disponibilidad aunque Kafka esté caído, pero necesita guardar la cola de salida en algún almacenamiento persistente. Como trabajamos con datos en memoria, que la consigna permite, un pedido aceptado y no publicado se perdería al reiniciar la API. Preferimos responder `503`, que es honesto. |

## Justificación

- **Respuesta inmediata.** Con petición y respuesta síncrona el `idPedido` llega en el mismo intercambio. La espera es corta porque la API no espera a cocina, caja ni facturación, solo a que Kafka confirme el evento (escenario 4, rendimiento).
- **Reintentos.** El cliente reintenta, que es la semántica "al menos una vez", y el servidor filtra los duplicados, que es "a lo sumo una vez". Juntas dan como resultado un solo pedido por clave. En `docs/pruebas/prueba-rest.md` lo comprobamos contando los eventos en `pedido.creado`, donde 7 solicitudes con 2 claves dejaron 2 eventos.
- **No mentir.** Como la API espera la confirmación de Kafka antes de responder `201`, lo que dice coincide con lo que el resto del sistema puede ver.
- **Consultar después de desconectarse.** `GET` es seguro e idempotente, así que se puede repetir sin efectos.
- **Precio.** Si el total se calcula en el servidor, el cliente no puede manipular el precio.
- **Interoperabilidad.** HTTP y JSON funcionan desde cualquier navegador, celular o `curl` sin generar código. FastAPI además publica la documentación interactiva en `/docs`, que nos sirve para la demostración.

## Consecuencias

**Positivas**
- Un pedido nunca se duplica por reintentos ni por envíos simultáneos con la misma clave. Lo verificamos con Kafka real.
- Si Kafka falla la API no se cae. Responde `503` y sigue atendiendo las demás rutas. Con el broker detenido `GET /menu` respondió en 1 ms, y cuando Kafka vuelve la API se reconecta sola.
- El contrato es fácil de probar y de usar, tanto para el cliente web como para los demás integrantes.

**Negativas y riesgos aceptados**
- **Dependencia temporal de Kafka.** Con el broker caído, crear un pedido tarda 5 s y falla. Es el costo de no aceptar pedidos que el sistema no puede procesar.
- **Ambigüedad del timeout.** Un `503` no asegura que el evento no se haya escrito. Lo mitigamos reutilizando el `idPedido` en el reintento y con la deduplicación de los consumidores.
- **Estado en memoria.** Los pedidos y las claves de idempotencia se pierden si la API se reinicia, y un reintento después de un reinicio crearía otro pedido.
- **Una sola instancia.** El lock y el registro de claves viven dentro del proceso. Para tener varias réplicas habría que moverlos a un almacén compartido como Redis.
- **El `GET` muestra un estado eventual.** Lo actualiza el consumidor de `pedido.estado`, así que puede ir unos milisegundos detrás de lo que publicó cocina o caja.
