# Implementación y Análisis de Resiliencia del Algoritmo de Elección de Líder (Bully Algorithm) ante Fallas de Caída de Nodos (Crash-Stop) en Clústeres Distribuidos

**Asignatura:** Sistemas Distribuidos y Paradigmas de la Computación (ETL1TP50), Unidad 2
**Carrera:** Ingeniería en Telemática, ESPOCH (FIE)
**Autora:** Zuleyca Anahi Vilema Alvarez
**Actividad:** Investigación Formativa Individual (ACD, 1.5 pts)

## Abstract

Este trabajo evalúa experimentalmente la resiliencia del algoritmo de elección de líder Bully ante fallas crash-stop en clústeres de 5 a 80 nodos. Se implementó una simulación en Python con asyncio y se ejecutaron 600 corridas (4 escenarios de falla, 5 tamaños de clúster y 30 repeticiones), midiendo tiempo de convergencia, mensajes, rondas y corrección. En los escenarios A, B y D el tiempo medio de convergencia se mantuvo entre 93 y 110 ms; en la falla en cascada (C) fue de unos 234 a 249 ms y, con N = 80 y timeouts de 50/100 ms, ninguna corrida convergió. En total, 33 de 600 corridas (5,5 %) no convergieron. Una prueba exploratoria con timeouts mayores sí convergió, pero no se determinó si la causa es el algoritmo o el simulador.

## Contenido del repositorio

| Archivo | Descripción |
|---|---|
| `articulo.qmd` | Fuente reproducible del artículo (Quarto) |
| `articulo.pdf` | Artículo compilado |
| `_quarto.yml` | Configuración de compilación |
| `references.bib` | Referencias BibTeX |
| `benchmark.py` | Simulación del Bully con fallas crash-stop |
| `results.csv` | Datos de las 600 corridas |
| `PROMPTS_OPENCODE.md` | Bitácora de uso de IA y declaración de integridad |

## Diseño del experimento

- **Escenarios:** A (cae el líder), B (cae el líder y 2 nodos aleatorios), C (falla en cascada: cae el líder y el primer nodo que se proclama nuevo líder, antes de enviar COORDINATOR) y D (cae el 50 % de los nodos, incluido el líder).
- **Parámetros:** N en {5, 10, 20, 40, 80}; 30 repeticiones por combinación; semilla base 42; latencia de red uniforme de 1 a 5 ms; timeouts de 50 ms (OK) y 100 ms (COORDINATOR).
- **Métricas:** tiempo de convergencia (ms), mensajes ELECTION/OK/COORDINATOR, rondas de elección y corrección.

## Reproducción

Requisitos: Python 3.14, Quarto 1.8, y las librerías `pandas`, `matplotlib`, `jupyter` y `pyyaml`.

Para una prueba rápida (N = 5 y 10, 3 repeticiones): `python benchmark.py --quick --output results_quick.csv`.

Prueba exploratoria (escenario C, N = 80, timeouts de 200/400 ms, fuera de `results.csv`):

## Notas y limitaciones

- La simulación corre en tiempo real con asyncio en un solo hilo: los resultados varían entre ejecuciones, aun con la misma semilla.
- La resolución del temporizador depende del sistema operativo (se ejecutó en Windows).
- El tiempo de detección de la falla se asume cero y los mensajes a nodos caídos se cuentan como enviados.
- **Formato del PDF:** la instalación local de TinyTeX/LaTeX falló, por lo que el artículo se compiló con Typst (incluido en Quarto), con dos columnas y citas numeradas estilo IEEE. No usa la clase oficial `IEEEtran`.