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

**Opción A, automática.** Deja un log con la hora de cada paso en `docs/pruebas/evidencias/websockets/`.
```bash
python -m backend.websockets.prueba_reconexion --espera 15 | tee docs/pruebas/evidencias/websockets/reconexion.txt
```

**Opción B, manual con la página del estudiante.**
1. Abrir `backend/websockets/cliente/index.html`, elegir productos y pulsar **Crear pedido**.
2. Ver `RECIBIDO` y luego `EN_PREPARACION` por push.
3. Antes de que pasen los 10 s de cocina, pulsar **Simular caída de conexión** (o cerrar la pestaña o apagar el Wi-Fi).
4. Esperar unos 15 s. En el log del servidor debe aparecer `sin clientes para ...: PREPARACION/LISTO descartado`.
5. Pulsar **Volver a conectar**. El registro de la página debe mostrar `suscribir` → `suscrito` → `GET ... preparación=LISTO`.
6. Pagar en caja y entregar (`PATCH /pedidos/{id}/entregar`) y ver llegar `ENTREGA/ENTREGADO` por push.

## Resultado obtenido (real)

Ejecución de `prueba_reconexion.py` con el sistema completo (Kafka, API REST, Cocina y WebSocket):

```text
[23:24:43 +  0.3s] 1. POST /pedidos → 201 idPedido=e589b83b-3ddc-4751-8a98-673fb341cae5
[23:24:43 +  0.3s] 2. WebSocket abierto (101 Switching Protocols)
[23:24:43 +  0.3s] → WS suscribir: {'tipo': 'suscrito', 'idPedido': 'e589b83b-3ddc-4751-8a98-673fb341cae5'}
[23:24:44 +  0.5s] → GET /pedidos → 200  preparación=EN_PREPARACION pago=PENDIENTE facturación=None entregado=False
[23:24:44 +  0.5s] ← WS push PREPARACION/EN_PREPARACION (origen cocina)
[23:24:46 +  2.5s] 3. El cliente se DESCONECTA antes de que cocina termine
[23:24:46 +  2.5s]    ...esperando 15 s sin conexión (cocina publica LISTO mientras tanto)...
[23:25:01 + 17.5s] 4. El cliente VUELVE: nuevo WebSocket
[23:25:01 + 17.5s] → WS suscribir: {'tipo': 'suscrito', 'idPedido': 'e589b83b-3ddc-4751-8a98-673fb341cae5'}
[23:25:01 + 17.7s] → GET /pedidos → 200  preparación=LISTO pago=PENDIENTE facturación=None entregado=False
[23:25:01 + 17.7s]    Estado recuperado por GET: LISTO -> OK
[23:25:01 + 17.7s] 5. Se provoca un evento posterior
[23:25:03 + 20.0s] Caja no está corriendo: se usa el stub de Caro para publicar ENTREGA/ENTREGADO
[23:25:04 + 20.5s] → stub publicó ENTREGA/ENTREGADO en pedido.estado
[23:25:04 + 20.5s] ← WS push ENTREGA/ENTREGADO (origen api)
[23:25:10 + 26.5s] RESULTADO: estado recuperado por REST=sí; ENTREGADO recibido por WebSocket=sí
```

### Incidencia y resultado inesperado resuelto
En las primeras pruebas sobre Windows, las peticiones HTTP a la API fallaban con `WinError 10061` ("No se pudo cargar el menú" y "GET falló") mientras que el WebSocket sí conectaba.
- **Causa:** En Windows, `localhost` se resuelve por defecto a la dirección IPv6 `::1`. La API REST (Uvicorn) escuchaba en `0.0.0.0` (solo IPv4), rechazando la conexión en IPv6. El servidor WebSocket funcionaba porque `websockets` enlaza por defecto en dual-stack (IPv4 e IPv6).
- **Corrección:** Se configuró explícitamente `127.0.0.1` en el cliente web y en el script de prueba para forzar IPv4, logrando la ejecución exitosa del flujo completo.

## Explicación (conceptos del Cap. 2)

- **Conexión persistente y full-duplex:** mientras la conexión está abierta, el servidor hace **server push** de cada cambio sin que el cliente pregunte. Cuando se cierra, el servidor lo detecta (cierre del canal o falta de respuesta al ping) y elimina la conexión de `suscripciones` sin afectar a los demás clientes.
- **Comunicación transitoria vs persistente:** el WebSocket es transitorio: si el receptor no está activo, el mensaje se descarta. Por eso `LISTO` no llega al cliente desconectado. Lo que **no** se pierde es el estado: Kafka es persistente (el evento quedó en el log y la API lo consumió) y la API lo expone por REST.
- **Por qué primero WebSocket y después `GET`:** si fuera al revés, un evento publicado entre el `GET` y la suscripción no llegaría por ningún lado. Suscribiéndose primero, todo evento posterior llega por push y todo lo anterior viene en el `GET`.
- **Sincronía:** el aviso es asíncrono para el cliente (no espera, recibe un evento), mientras que la recuperación por REST es síncrona y fiable. Combinar ambos da inmediatez y consistencia.
