# ADR-001: REST como API pública de pedidos

- **Estado:** Aceptado
- **Fecha:** 2026-11-15
- **Responsable:** Integrante 1 (David)
- **Escenarios de calidad relacionados:** 2 (confiabilidad / idempotencia) y 1 (disponibilidad)

## Contexto

Los estudiantes crean y consultan pedidos desde un navegador o un celular. La consigna exige que esta comunicación use REST, pero la forma de usarlo es una decisión del grupo. Esa forma tiene que resolver cinco necesidades concretas del sistema:

1. **Respuesta inmediata.** Al crear un pedido, el cliente necesita su `idPedido` para seguirlo (por WebSocket) y consultarlo después. Por eso la operación debe devolver algo en el momento.
2. **Reintentos sin duplicados.** En una red móvil, la respuesta a un `POST` puede perderse aunque el servidor lo haya procesado. El cliente reintentará, y eso no puede crear dos pedidos.
3. **No mentir sobre el registro.** El pedido solo existe para el resto del sistema cuando el evento `pedido.creado` está en Kafka. La API no puede responder "registrado" si esa publicación no ocurrió.
4. **Consultar después de una desconexión.** Tras perder el WebSocket, el cliente necesita una forma simple de obtener el estado vigente del pedido.
5. **Precio confiable.** El total no puede depender de lo que envíe el cliente.

## Decisión

La API de Pedidos (`backend/api_rest`, puerto 8000) es un servicio REST sobre HTTP con JSON, implementado con FastAPI. Su contrato está en `docs/contratos.md` (sección 2) y en `contracts/pedido.schema.json`.

| Método y ruta | Uso |
|---|---|
| `GET /menu` | Productos y precios (en centavos) |
| `POST /pedidos` | Crear un pedido. Exige la cabecera `Idempotency-Key` |
| `GET /pedidos/{idPedido}` | Estado vigente del pedido |
| `PATCH /pedidos/{idPedido}/entregar` | El cajero lo marca entregado (solo si está `LISTO` y `PAGADO`) |

Decisiones de diseño dentro de REST:

- **Idempotencia por clave.** El cliente genera una `Idempotency-Key` por pedido. La API la reserva bajo un lock como `EN_PROCESO` **antes** de publicar.
  - Al terminar con éxito pasa a `COMPLETADA`: un reintento recibe `200` con la misma respuesta.
  - Si llega mientras está en proceso, se responde `409 SOLICITUD_EN_CURSO`.
  - Si se reutiliza con otro cuerpo (se compara un hash SHA-256), se responde `409 CLAVE_REUTILIZADA`.
- **`201` solo después de la confirmación de Kafka.** La API publica `pedido.creado` con `acks='all'` y espera hasta 5 s. Si no hay confirmación, responde `503 SERVICIO_NO_DISPONIBLE`, no deja el pedido registrado y libera la clave.
- **El reintento conserva el `idPedido`.** Si la clave se liberó por un `503`, el reintento con el mismo cuerpo reutiliza el `idPedido` asignado. Así, si Kafka sí había escrito el primer mensaje, los consumidores lo deduplican.
- **El servidor calcula los precios.** El cliente solo envía `idProducto` y `cantidad`. Los precios salen del menú y el dinero va en centavos enteros.
- **La API se entera de los cambios de estado por Kafka.** Consume `pedido.estado` (grupo `api`) con el consumidor base común del equipo. Así `GET /pedidos/{id}` refleja lo que hicieron cocina, caja y facturación, sin consultarlos directamente.
- **Errores uniformes.** Todos los errores tienen la forma `{"error": "CODIGO", "mensaje": "..."}`, y la validación de FastAPI (`422`) se convierte a `400`.

## Alternativas consideradas

| Alternativa | Por qué no se eligió |
|---|---|
| **gRPC para el cliente** | Los navegadores no hablan gRPC directamente: se necesita gRPC-Web y un proxy (por ejemplo Envoy). Agrega infraestructura sin resolver ninguna de las cinco necesidades mejor que REST. El grupo usa gRPC donde sí aporta: la llamada interna Facturación → Caja (ADR-003). |
| **GraphQL** | Aporta cuando el cliente necesita combinar muchos recursos en una sola consulta. Aquí hay un solo recurso (el pedido) con cuatro operaciones fijas, y GraphQL no resuelve la idempotencia ni el `503` mejor que REST. |
| **Crear el pedido por WebSocket** | El WebSocket es un canal con estado, pensado para notificaciones del servidor al cliente (ADR-004). Usarlo para crear pedidos obligaría a inventar un protocolo de petición y respuesta, códigos de error y reintentos que HTTP ya trae. |
| **Que el cliente publique directamente en Kafka** | Expondría el broker a Internet, el cliente no podría recibir su `idPedido` ni un error de validación, y se perdería el punto único donde se validan los datos y se aplica la idempotencia. |
| **REST sin clave de idempotencia** | El `POST` no es idempotente por definición: cada reintento crearía un pedido nuevo, lo que incumple el escenario 2. |
| **Aceptar el pedido (`202`) y publicarlo después (patrón outbox)** | Daría disponibilidad aun con Kafka caído, pero necesita almacenamiento persistente para la cola de salida. Con datos en memoria (permitido por la consigna), un pedido aceptado y no publicado se perdería al reiniciar la API. Se prefirió responder `503`, que es honesto. |

## Justificación

- **Necesidad 1 (respuesta inmediata):** la petición y respuesta síncrona de HTTP entrega el `idPedido` en el mismo intercambio. El costo de la espera es bajo, porque la API no espera a cocina, caja ni facturación: solo a que Kafka confirme el evento (escenario 4, rendimiento).
- **Necesidad 2 (reintentos):** la combinación de reintento del cliente (al menos una vez) y filtro de duplicados en el servidor (a lo sumo una vez) da como efecto un solo pedido por clave. La prueba `docs/pruebas/prueba-rest.md` lo verificó contando los eventos en `pedido.creado`: 7 solicitudes con 2 claves produjeron 2 eventos.
- **Necesidad 3 (no mentir):** esperar la confirmación de Kafka antes del `201` hace que la respuesta de la API coincida con lo que el resto del sistema puede ver.
- **Necesidad 4 (consultar tras desconectarse):** `GET` es seguro e idempotente, así que se puede repetir sin efectos.
- **Necesidad 5 (precio):** calcular el total en el servidor elimina la manipulación del precio desde el cliente.
- **Interoperabilidad:** HTTP y JSON funcionan desde cualquier navegador, celular o `curl`, sin generar código. Además, FastAPI publica la documentación interactiva en `/docs`, que sirve para la demostración.

## Consecuencias

**Positivas**
- Un pedido nunca se duplica por reintentos ni por envíos simultáneos con la misma clave (verificado con Kafka real).
- La API no se cae si Kafka falla: responde `503` y sigue atendiendo, por ejemplo `GET /menu` en 1 ms con el broker detenido, y se reconecta sola.
- El contrato es simple de probar y de consumir para el cliente web y para los demás integrantes.

**Negativas y riesgos aceptados**
- **Acoplamiento temporal con Kafka.** Con el broker caído, crear un pedido tarda 5 s y falla. Es el precio de no aceptar pedidos que el sistema no puede procesar.
- **Ambigüedad del timeout.** Un `503` no garantiza que el evento no se escribió. Se mitiga reutilizando el `idPedido` en el reintento y con la deduplicación de los consumidores.
- **Estado en memoria.** Los pedidos y las claves de idempotencia se pierden al reiniciar la API, y un reintento después de un reinicio crearía otro pedido.
- **Una sola instancia.** El lock y el registro de claves son locales al proceso. Para escalar a varias réplicas habría que moverlos a un almacén compartido, por ejemplo Redis.
- **El estado del `GET` es eventual.** Lo actualiza el consumidor de `pedido.estado`, así que puede ir unos milisegundos detrás de lo que publicó cocina o caja.
