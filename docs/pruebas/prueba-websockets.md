# Prueba WebSockets: cliente desconectado y reconectado

- **Escenario de calidad relacionado:** Escenario 5, recuperación del cliente tras una desconexión (`docs/arquitectura/escenarios-calidad.md`). Medida: reintento cada 3 s, estado correcto tras reconectar (el mismo que devuelve `GET`) y 0 cambios de estado perdidos después de la reconexión.
- **Responsable:** Cinthy (Integrante 4)
- **Mecanismo:** WebSocket (con REST para recuperar el estado)

## Hipótesis

Si el estudiante pierde la conexión antes de que cocina publique `LISTO`:

1. El servidor detecta el cierre, quita la conexión y **sigue funcionando**.
2. El aviso `LISTO` **se descarta**, porque no hay nadie suscrito (el WebSocket es transitorio).
3. Al volver, el cliente se suscribe y luego hace `GET /pedidos/{id}`: **ve `LISTO` igual**, porque la API guardó el estado al consumir `pedido.estado`.
4. Un evento posterior (`ENTREGADO`) **sí llega por WebSocket**.

## Ejecución

Servicios corriendo: Kafka, API (8000), Cocina con `--demora 10`, Notificaciones (8765). Caja (8001) es opcional.

**Opción A, automática** (deja un log con hora; las evidencias van en `docs/pruebas/evidencias/websockets/`, según la convención del equipo):
```bash
python -m backend.websockets.prueba_reconexion --espera 15 | tee docs/pruebas/evidencias/websockets/reconexion.txt
```
Guardar también el log de la terminal del servidor en `docs/pruebas/evidencias/websockets/log-servidor.txt`.

**Opción B, con la página** (para el video):
1. Abrir `backend/websockets/cliente/index.html`, elegir productos y pulsar **Crear pedido**.
2. Ver `RECIBIDO` y luego `EN_PREPARACION` por push.
3. Antes de que pasen los 10 s de cocina, pulsar **Simular caída de conexión** (o cerrar la pestaña o apagar el Wi-Fi).
4. Esperar unos 15 s. En el log del servidor debe aparecer `sin clientes para ...: PREPARACION/LISTO descartado`.
5. Pulsar **Volver a conectar**. El registro de la página debe mostrar `suscribir` → `suscrito` → `GET ... preparación=LISTO`.
6. Pagar en caja y entregar (`PATCH /pedidos/{id}/entregar`) y ver llegar `ENTREGA/ENTREGADO` por push.

## Resultado obtenido (real)

> Pegar aquí la salida de `prueba_reconexion.py`, el log del servidor y capturas de la página. Anotar lo que realmente pasó, aunque sea inesperado.

```
(pegar log)
```

## Explicación (conceptos del Cap. 2)

- **Conexión persistente y full-duplex:** mientras la conexión está abierta, el servidor hace **server push** de cada cambio sin que el cliente pregunte. Cuando se cierra, el servidor lo detecta (cierre del canal o falta de respuesta al ping) y elimina la conexión de `suscripciones` sin afectar a los demás clientes.
- **Comunicación transitoria vs persistente:** el WebSocket es transitorio: si el receptor no está activo, el mensaje se descarta. Por eso `LISTO` no llega al cliente desconectado. Lo que **no** se pierde es el estado: Kafka es persistente (el evento quedó en el log y la API lo consumió) y la API lo expone por REST.
- **Por qué primero WebSocket y después `GET`:** si fuera al revés, un evento publicado entre el `GET` y la suscripción no llegaría por ningún lado. Suscribiéndose primero, todo evento posterior llega por push y todo lo anterior viene en el `GET`.
- **Sincronía:** el aviso es asíncrono para el cliente (no espera, recibe un evento), mientras que la recuperación por REST es síncrona y fiable. Combinar ambos da inmediatez y consistencia.
