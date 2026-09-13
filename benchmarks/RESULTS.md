# Comparación de transporte Scryfall — 2026-09-12

## Conclusión

La consulta real no demuestra una mejora de fiabilidad ni velocidad: antes y
después se resolvieron 4/4 cartas sin 429. La simulación sí muestra una reducción
de solicitudes y una recuperación mejor ante concurrencia y límites temporales.
Esto valida el comportamiento del transporte bajo ese modelo, no demuestra que
el incidente de producción esté resuelto.

## Método y alcance

- Cartas: Lightning Bolt, Sol Ring, Counterspell, Swords to Plowshares.
- Operación: cuatro POST a `/cards/collection`, una carta por petición, verificando
  que el nombre aparece en `data`.
- No se escribe en PostgreSQL ni se descargan imágenes, traducciones o rulings.
  No se mide el tiempo hasta que una carta queda importada en la aplicación.
- Antes: copia exacta del transporte previo en `transport_before.py`.
- Después: `src/services/scryfall_transport.py` con coordinación por host para
  Tor/directo, límites por tipo de endpoint y pausa común ante 429. Se conserva
  el máximo de diez reintentos para no atribuir la mejora a cambiar ese máximo.
- El código anterior también tenía un `continue` ausente tras un error de
  transporte directo; se corrige y cubre con una prueba de regresión.

### Consulta real

Misma máquina, salida directa en ambos casos, Tor desactivado solo en el proceso
de diagnóstico. Consultas secuenciales con un segundo de espera después de cada
consulta. El tiempo total incluye esas cuatro pausas. Se aborta al primer 429.
Una ejecución por versión, primero antes y luego después: no hay significación
estadística, control de caché remoto ni aleatorización del orden. Los servicios
Docker seguían activos; no se aisló su posible tráfico concurrente. No representa
el comportamiento de la IP de salida Tor de producción.

La primera tentativa en sandbox no tuvo acceso DNS; se descartó y se repitió con
permiso de red. Los JSON `before-live.json` y `after-live.json` son exclusivamente
las ejecuciones con conectividad, con nombres devueltos y tiempos por consulta.

### Simulación controlada

Se ejecuta el transporte real con un servidor HTTP sustituido por un doble de
prueba; no se contacta con Scryfall. Los nombres son reales, pero las respuestas
de este servidor son sintéticas y solo sirven para comprobar el transporte.
Cuatro tareas empiezan simultáneamente usando la rama Tor en ambos casos.

El servidor exige 500 ms entre llamadas y rechaza las llamadas durante 30 s tras
una infracción. Por defecto, los rechazos durante esa pausa **no la prolongan**:
no asumimos que Scryfall la reinicie. En `retry-after`, la primera respuesta
impone una pausa de 45 s y envía `Retry-After: 45`.

El reloj del transporte está acelerado 50 veces, sin modificar el reloj del
bucle asyncio; la espera aleatoria del transporte nuevo se fija en 0,5 s.
Los tiempos de esta tabla son segundos del reloj simulado, con pequeñas
variaciones por planificación del sistema. No son latencias reales de Scryfall.

## Resultados guardados

| Caso | Cartas antes → después | Peticiones antes → después | HTTP 429 antes → después | Tiempo antes → después |
|---|---:|---:|---:|---:|
| Real, secuencial | 4/4 → 4/4 | 4 → 4 | 0 → 0 | 5,794 → 5,351 s reales |
| Simulado, ráfaga | 4/4 → 4/4 | 27 → 4 | 23 → 0 | 110,642 → 2,149 s simulados |
| Simulado, Retry-After 45 s | 2/4 → 4/4 | 40 → 5 | 38 → 1 | 112,321 → 47,676 s simulados |

No interpretamos la diferencia de 443 ms de la prueba real como una mejora.
El resultado relevante de la simulación es respetar las pausas y reducir los
intentos rechazados; la velocidad máxima se reduce deliberadamente.

## Repetición

Desde el directorio `worker`, ejecutar cada comando en un proceso separado:

```sh
.venv/bin/python scripts/benchmark_scryfall.py --mode simulated --transport benchmarks/transport_before.py --label before
.venv/bin/python scripts/benchmark_scryfall.py --mode simulated --label after
.venv/bin/python scripts/benchmark_scryfall.py --mode simulated --scenario retry-after --transport benchmarks/transport_before.py --label before
.venv/bin/python scripts/benchmark_scryfall.py --mode simulated --scenario retry-after --label after
.venv/bin/python scripts/benchmark_scryfall.py --mode live --transport benchmarks/transport_before.py --label before
.venv/bin/python scripts/benchmark_scryfall.py --mode live --label after
```

Si la prueba real devuelve 429, esperar a que termine la restricción antes de
ejecutar otra. No usar el benchmark para generar carga sostenida contra Scryfall.
`--extend-cooldown` permite explorar otro modelo de restricción en simulación;
no se ha usado para los resultados de la tabla.

## Limitaciones y siguiente comprobación

- El limitador solo coordina este proceso. El backend todavía puede llamar a
  Scryfall por fuera; tampoco hay cola persistente ni deduplicación entre trabajos.
- No se ha cambiado la configuración Tor ni reiniciado el contenedor de producción.
  El antes/después se ejecutó en procesos aislados con el código correspondiente.
- Para validar el incidente completo: tras activar el cambio, medir trabajos
  prioritarios únicos completados, 429 por petición HTTP, latencia hasta guardar
  la carta y trabajos pendientes bajo carga de uso comparable. Registrar también
  ruta de salida, tráfico de fondo y número de cartas; comparar tasas, no solo
  totales de logs. Si persisten los fallos, el arreglo de transporte es insuficiente.
- Pruebas automatizadas: suite completa del worker, incluyendo concurrencia en
  Tor/directo, pausa compartida incluso al agotar reintentos, `Retry-After` en
  segundos/fecha/valores inválidos y recuperación tras un error de conexión.

Referencia del modelo: https://scryfall.com/docs/api/rate-limits
