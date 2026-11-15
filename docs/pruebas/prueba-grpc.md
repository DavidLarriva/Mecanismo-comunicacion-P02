# Prueba gRPC: servicio no disponible

- **Escenario:** resiliencia ante la caída del servicio Caja.
- **Responsable:** Integrante 3

## Ejecución

1. Iniciar Facturación con el cliente gRPC configurado con timeout de 2 s.
2. Detener el servidor gRPC de Caja.
3. Intentar llamar `ConfirmarPago` para un pedido de prueba.
4. Registrar el error y el tiempo observado.

## Resultado esperado

La llamada termina sin esperar indefinidamente. El cliente recibe
`UNAVAILABLE` o `DEADLINE_EXCEEDED`, y Facturación maneja el error
según los reintentos acordados.

## Resultado obtenido (real)

Completar después de ejecutar la prueba; no inventar el resultado.

## Explicación

gRPC permite fijar un límite de espera. La llamada puede fallar si el servicio
no responde, y el cliente debe manejar ese error de forma controlada.