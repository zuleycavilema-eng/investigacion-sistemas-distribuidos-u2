# Bitácora de trazabilidad y uso ético de IA

**Proyecto:** Implementación y Análisis de Resiliencia del Algoritmo de Elección de Líder (Bully Algorithm) ante Fallas de Caída de Nodos (Crash-Stop) en Clústeres Distribuidos
**Autora:** Zuleyca Anahi Vilema Alvarez
**Fechas de trabajo:** 12 a 14 de noviembre de 2026

## Herramientas de IA utilizadas

- **OpenCode** (agente de código de terminal), versión 2.2.7. Modelos que aparecieron en la sesión: DeepSeek V4.1 Flash Free, Auto Router y Flint (free).No registrado por versión.
- **Claude** (asistente de IA de Anthropic), usado fuera de OpenCode para: auditar el código generado, redactar los prompts de corrección, diagnosticar el entorno (Python, Git, Quarto, TinyTeX/Typst) y preparar un borrador de `articulo.qmd`, `README.md` y de esta bitácora.

## Prompts principales enviados a OpenCode

Se resumen fielmente; el texto completo de cada prompt puede agregarse si se requiere.

1. **Rol y contexto.** Actuar como redactor experto en artículos IEEE Conference con Quarto; estructura del repositorio y del artículo; 5 a 8 referencias reales sin inventar datos; todo dato del artículo debe salir de `results.csv`; dejar marcadores `[REDACTAR]` para el análisis. Pidió confirmar sin generar archivos.
2. **Corrección del título.** Usar el título oficial del tema textualmente, sin abreviarlo.
3. **Generación de `benchmark.py`.** Simulación asyncio del Bully con ELECTION/OK/COORDINATOR, latencia 1 a 5 ms, 4 escenarios (A, B, C, D), N en {5, 10, 20, 40, 80}, 30 repeticiones, semilla, métricas, columnas de `results.csv`, tabla resumen y argparse.
4. **Corrección tras auditoría #1.** Iniciador elegido entre los vivos, ELECTION solo a IDs mayores, `es_lider` separado de `leader_id`, un único consumidor de la cola, envíos concurrentes, timeouts de 50/100 ms, escenario C, semillas independientes por corrida, `--quick`.
5. **Corrección tras auditoría #2.** `tarea_nodo` no debe bloquearse, basta un OK, sin oráculo de nodos vivos, conteo de mensajes al enviar, rondas del clúster, reintentos, convergencia contra el mayor ID vivo, cascada una sola vez, cancelación de tareas.
6. **Corrección tras auditoría #3.** Activar la cascada, función `proclamar_lider`, un solo iniciador y una sola espera de convergencia, guardia contra doble líder, limitaciones, mensajes de progreso.
7. **Corrección tras auditoría #4.** Re-anuncio de COORDINATOR por el líder, evitar rondas duplicadas, un nodo caído no envía mensajes, manejo de corridas sin convergencia.

## Correcciones aplicadas tras la supervisión humana

Revisé el código línea por línea tras cada ronda. En cada versión el agente declaró el trabajo como completo y la revisión encontró errores:

- **Ronda 1:** el candidato ganador estaba decidido de antemano (no era un Bully real); dos consumidores leían la misma cola; envíos en serie; la misma semilla en todas las repeticiones; timeouts de 2 s frente a latencias de milisegundos.
- **Ronda 2:** el bucle principal de cada nodo se bloqueaba al lanzar una ronda; se exigían todos los OK en lugar de uno; el escenario C no producía una cascada real; las rondas y la convergencia se medían mal.
- **Ronda 3:** el bucle de `ejecutar` esperaba 10 s por corrida; la cascada nunca se activaba ni mataba al nodo; condición de carrera con dos líderes.
- **Ronda 4:** rondas duplicadas; un líder ya proclamado ignoraba los ELECTION tardíos; un nodo caído podía seguir enviando mensajes.
- **Validación:** prueba `--quick` (24 corridas, todas correctas) y corrida completa de 600 corridas. Se detectaron 48 corridas sin converger (A, B y C, casi todas con N = 80) y se hizo una prueba exploratoria con timeouts de 200/400 ms, que se reporta aparte de `results.csv`.

## Declaración de integridad académica

Declaro que usé OpenCode y Claude como herramientas de apoyo, según lo descrito arriba. Ejecuté y validé el código y los experimentos, revisé el trabajo del agente y corregí sus errores. Los datos del artículo provienen de `results.csv`, generado por mi ejecución de `benchmark.py`. El análisis de resultados, la discusión y las conclusiones fueron redactados por mí con mi propio criterio.