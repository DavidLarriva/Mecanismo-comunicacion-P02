# Bitácora de uso de IA

Marcar en el código los fragmentos con asistencia de IA (p. ej. comentario `# [IA]`).

| Integrante y fecha | Herramienta | Consulta | Respuesta (resumen) | Acción del grupo | Validación |
|---|---|---|---|---|---|
| #1 · Cinthy · 15/11/2026 | Claude (Anthropic) | Cómo implementar el servidor WebSocket de Notificaciones según el contrato (sección 7), reutilizando `ConsumerBase` de Caro, y cómo pasar eventos de un consumidor bloqueante (`kafka-python`) al loop de `asyncio` | Propuso `servidor.py` (clase `Notificador`), `consumidor.py` con `asyncio.run_coroutine_threadsafe` en un hilo aparte y `simulador.py` para probar sin Kafka | Se incorporaron los tres archivos, marcados en el código. Se cambió la primera versión del simulador (reemplazaba un método en tiempo de ejecución) por un parámetro `al_suscribir`, más simple de explicar | 6 pruebas con `pytest` (incluida una de integración con el Kafka falso de Caro) y ejecución real en modo simulado con desconexión a mitad de la secuencia |
| #2 · Cinthy · 15/11/2026 | Claude (Anthropic) | Página del cliente: crear pedido con `Idempotency-Key`, abrir WebSocket y luego `GET`, reconexión cada 3 s | Propuso `cliente/index.html` y señaló una carrera: un `GET` desactualizado puede hacer retroceder un estado ya recibido por push | Se incorporó y se agregó la regla "cada estado solo avanza" | Probada en Chromium: creación, push, caída simulada, vuelta y reconexión automática tras reiniciar el servidor |
| #3 · Cinthy · 15/11/2026 | Claude (Anthropic) | Script de la prueba de desconexión/reconexión y borrador del ADR-004 | Propuso `prueba_reconexion.py`, `tests/test_servidor.py`, ADR-004 y la guía de la prueba | Se revisaron y adaptaron al contrato v1.0 (eventos en `{"tipo":"estado","evento":...}`, centavos) | Resultado real de la prueba con el sistema completo registrado en `docs/pruebas/prueba-websockets.md` |

## Reflexión final
_Casos en que la IA se equivocó o no aplicaba a nuestra arquitectura._
