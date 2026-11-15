#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
=============================================================================
  Benchmark de Resiliencia del Algoritmo de Elección de Líder (Bully)
                           ante Fallas Crash-Stop
              en Clústeres Distribuidos (Python 3 · stdlib asyncio)
=============================================================================

Simulación en tiempo real de un clúster que aplica el Algoritmo Bully para
la elección de un líder, inyectando fallas de tipo Crash-Stop (caída de nodo
sin recuperación).

Escenarios de falla evaluados:
    A: El líder cae.
    B: El líder y K nodos aleatorios caen.
    C: El líder cae y, a mitad del proceso de elección, el candidato que iba
       ganando cae (falla en cascada).
    D: El 50% de los nodos cae, incluido el líder.

LIMITACIONES:
    - Simulación en tiempo real con asyncio (los resultados varían entre
      ejecuciones por la planificación del event loop).
    - Latencia simulada con asyncio.sleep (la resolución real depende del
      sistema operativo).
    - (a) El tiempo de detección de la falla se asume cero (t0 es el instante
      de la caída y el detector inicia la elección de inmediato).
    - (b) El detector es aleatorio en A, B y D y el de menor ID vivo en C.
    - (c) Los mensajes a nodos caídos se cuentan como enviados.

Métricas por ejecución (registro en results.csv):
    - tiempo_convergencia_ms : desde la detección de la falla hasta que todos
                               los nodos vivos reconocen al nuevo líder.
    - msgs_election / msgs_ok / msgs_coordinator / msgs_total : conteo de mensajes.
    - rondas : total de rondas de elección iniciadas en el clúster.
    - correcto : True si el líder final es el mayor ID vivo y todos coinciden.

Uso:
    python benchmark.py --seed 42 --reps 30 --output results.csv
"""

import argparse
import asyncio
import csv
import math
import random
import statistics
import time
from typing import Dict, List, Optional

# Mensajes del protocolo Bully
ELECTION = "ELECTION"
OK = "OK"
COORDINATOR = "COORDINATOR"

# Parámetros de red y timeouts
LAT_MIN_MS = 1.0
LAT_MAX_MS = 5.0
TIMEOUT_OK = 50.0          # ms: timeout de espera de OK en una elección
TIMEOUT_COORD = 100.0      # ms: timeout de espera de COORDINADOR

# Parámetros experimentales
ESCENARIOS = ("A", "B", "C", "D")
K_FALLOS_B = 2
N_VALORES = (5, 10, 20, 40, 80)
REPETICIONES = 30
DEFAULT_SEED = 42
QUICK_N = (5, 10)
QUICK_REPS = 3
TIMEOUT_CONVERGENCIA_S = 10.0

RESULT_COLUMNS = [
    "escenario", "n_nodos", "repeticion", "seed", "tiempo_convergencia_ms",
    "msgs_election", "msgs_ok", "msgs_coordinator", "msgs_total", "rondas", "correcto",
]


class Nodo:
    """Nodo del clúster: cola de mensajes, liderazgo y estado de la elección."""

    def __init__(self, node_id: int, red: "Red"):
        self.id = node_id
        self.red = red
        self.alive = True
        self.inbox = asyncio.Queue()
        self.es_lider = False
        self.leader_id: Optional[int] = None
        self.in_eleccion = False
        self.ok_recibidos = 0
        self.evento_ok = asyncio.Event()
        self.evento_coordinador = asyncio.Event()


class Red:
    """Red simulada: latencia por mensaje (uniforme) y descarte a fallos (Crash-Stop)."""

    def __init__(self, ids: List[int], rng: random.Random):
        self.n_ids = list(ids)
        self.nodos = {i: Nodo(i, self) for i in ids}
        self.contadores = {ELECTION: 0, OK: 0, COORDINATOR: 0}
        self.rng = rng
        self.rondas = 0
        self.cascada = False
        self.cascada_ejecutada = False
        self.tareas: set = set()
        self.tareas_eleccion: set = set()

    async def enviar(self, emisor_id: int, destino_id: int, tipo: str, payload: Optional[int] = None) -> None:
        emisor = self.nodos.get(emisor_id)
        if emisor is None or not emisor.alive:
            # Un nodo crash-stop no envia mensajes
            return
        destino = self.nodos.get(destino_id)
        self.contadores[tipo] = self.contadores.get(tipo, 0) + 1
        if destino is None or not destino.alive:
            # Crash-Stop: el mensaje se cuenta como enviado y se pierde
            return

        async def _entrega() -> None:
            latencia_ms = self.rng.uniform(LAT_MIN_MS, LAT_MAX_MS)
            await asyncio.sleep(latencia_ms / 1000.0)
            await destino.inbox.put((emisor_id, tipo, payload))

        tarea = asyncio.create_task(_entrega())
        self.tareas.add(tarea)
        tarea.add_done_callback(self.tareas.discard)


async def realizar_ronda(red: Red, nodo: Nodo, tareas_nodo: Dict[int, asyncio.Task], escenario: str) -> bool:
    """Ejecuta una ronda de elección (Bully) a cargo del nodo `nodo` (iniciador).

    Flujo:
      1. El iniciador envía ELECTION a TODOS los IDs mayores (alive o no).
      2. Si no hay mayores -> se proclama de inmediato.
      3. Espera el primer OK con TIMEOUT_OK.
      4. Si no hay OK -> se proclama líder y envía COORDINATOR a los ID menores.
      5. Si hay OK -> espera COORDINADOR con TIMEOUT_COORD; si expira, reinicia
         la elección (nueva ronda) con el mismo nodo.

    Retorna True si la ronda finalizo con liderazgo resuelto, False si debe reiniciar.
    """
    while True:
        if not nodo.alive or nodo.es_lider:
            return True
        nodo.in_eleccion = True
        red.rondas += 1
        nodo.evento_ok.clear()
        nodo.evento_coordinador.clear()
        nodo.ok_recibidos = 0
        higher = [pid for pid in sorted(red.nodos) if pid > nodo.id]
        if not higher:
            # No hay IDs mayores -> se proclama de inmediato
            await proclamar_lider(red, nodo, tareas_nodo)
            return True
        for pid in higher:
            await red.enviar(nodo.id, pid, ELECTION)
        ok_conseguido = False
        try:
            await asyncio.wait_for(nodo.evento_ok.wait(), timeout=TIMEOUT_OK / 1000.0)
            ok_conseguido = True
        except asyncio.TimeoutError:
            ok_conseguido = False
        if not ok_conseguido:
            if nodo.evento_coordinador.is_set() or not nodo.alive:
                # Evita dos lideres por carrera
                return True
            await proclamar_lider(red, nodo, tareas_nodo)
            return True
        # OK recibido -> espera COORDINADOR
        try:
            await asyncio.wait_for(nodo.evento_coordinador.wait(), timeout=TIMEOUT_COORD / 1000.0)
            return True
        except asyncio.TimeoutError:
            nodo.in_eleccion = False
            continue


async def tarea_nodo(red: Red, nodo: Nodo, tareas_nodo: Dict[int, asyncio.Task], escenario: str) -> None:
    """Unico consumidor de la cola de mensajes del nodo."""
    while True:
        if not nodo.alive:
            break
        emisor, tipo, payload = await nodo.inbox.get()
        if tipo == ELECTION:
            if emisor < nodo.id:
                # Responde OK siempre; si no esta en una eleccion, lanza la ronda
                await red.enviar(nodo.id, emisor, OK)
                if nodo.es_lider:
                    # Re-anuncio del lider: envia COORDINATOR al emisor
                    await red.enviar(nodo.id, emisor, COORDINATOR, payload=nodo.id)
                elif not nodo.in_eleccion:
                    nodo.in_eleccion = True
                    tarea_elec = asyncio.create_task(realizar_ronda(red, nodo, tareas_nodo, escenario))
                    red.tareas_eleccion.add(tarea_elec)
                    tarea_elec.add_done_callback(red.tareas_eleccion.discard)
        elif tipo == OK:
            nodo.ok_recibidos += 1
            nodo.evento_ok.set()
        elif tipo == COORDINATOR:
            nodo.evento_coordinador.set()
            if payload == nodo.id:
                nodo.es_lider = True
                nodo.leader_id = nodo.id
            else:
                nodo.es_lider = False
                nodo.in_eleccion = False
                nodo.leader_id = payload


async def matar(red: Red, node_id: int, tareas_nodo: Dict[int, asyncio.Task]) -> None:
    """Simula una falla Crash-Stop: el nodo muere y sus mensajes se pierden."""
    nodo = red.nodos[node_id]
    nodo.alive = False
    tarea = tareas_nodo.get(node_id)
    if tarea is not None and not tarea.done():
        tarea.cancel()


async def proclamar_lider(red: Red, nodo: Nodo, tareas_nodo: Dict[int, asyncio.Task]) -> None:
    """Proclama a `nodo` como líder y envía COORDINATOR a los IDs menores."""
    nodo.es_lider = True
    nodo.leader_id = nodo.id
    nodo.in_eleccion = False
    if red.cascada and not red.cascada_ejecutada:
        red.cascada_ejecutada = True
        await matar(red, nodo.id, tareas_nodo)
        return
    for pid in [p for p in sorted(red.nodos) if p < nodo.id]:
        await red.enviar(nodo.id, pid, COORDINATOR, payload=nodo.id)


async def esperar_convergencia(red: Red, timeout_s: float = TIMEOUT_CONVERGENCIA_S) -> bool:
    """Espera a que todos los nodos vivos tengan el mismo leader (max ID vivo)."""
    fin = time.perf_counter() + timeout_s
    while time.perf_counter() < fin:
        vivos = [pid for pid in sorted(red.nodos) if red.nodos[pid].alive]
        if vivos and all(red.nodos[pid].leader_id == max(vivos) for pid in vivos):
            return True
        await asyncio.sleep(0.001)
    return False


async def ejecutar(escenario: str, n: int, seed: int, rep: int) -> Dict[str, object]:
    """Ejecuta una repeticion completa: inicializacion, fallo, reeleccion y metricas."""
    rng = random.Random(f"{seed}-{escenario}-{n}-{rep}")
    red = Red(list(range(n)), rng)
    red.cascada = (escenario == "C")
    tareas_nodo: Dict[int, asyncio.Task] = {}
    for i in range(n):
        tareas_nodo[i] = asyncio.create_task(tarea_nodo(red, red.nodos[i], tareas_nodo, escenario))

    # Estado inicial: el nodo con mayor ID (n-1) es el coordinador inicial
    red.nodos[n - 1].es_lider = True
    red.nodos[n - 1].leader_id = n - 1
    for pid in range(n):
        if pid != n - 1:
            await red.enviar(n - 1, pid, COORDINATOR, payload=n - 1)
    if not await esperar_convergencia(red):
        raise RuntimeError("La inicializacion del cluster no convergio")

    red.contadores = {ELECTION: 0, OK: 0, COORDINATOR: 0}
    red.rondas = 0

    t0 = time.perf_counter_ns()

    # Inyeccion de fallas segun el escenario
    if escenario == "A":
        await matar(red, n - 1, tareas_nodo)
    elif escenario == "B":
        await matar(red, n - 1, tareas_nodo)
        posibles = [i for i in range(n) if i != n - 1 and red.nodos[i].alive]
        for pid in red.rng.sample(posibles, min(K_FALLOS_B, len(posibles))):
            await matar(red, pid, tareas_nodo)
    elif escenario == "C":
        await matar(red, n - 1, tareas_nodo)
    elif escenario == "D":
        total_muertos = max(1, n // 2)
        await matar(red, n - 1, tareas_nodo)
        posibles = [i for i in range(n) if i != n - 1 and red.nodos[i].alive]
        for pid in red.rng.sample(posibles, min(total_muertos - 1, len(posibles))):
            await matar(red, pid, tareas_nodo)

    vivos = [pid for pid in sorted(red.nodos) if red.nodos[pid].alive]
    if vivos:
        if escenario == "C":
            iniciador_id = min(vivos)
        else:
            iniciador_id = red.rng.choice(vivos)
        iniciador = red.nodos[iniciador_id]
        if not iniciador.es_lider and not iniciador.in_eleccion:
            tarea_elec = asyncio.create_task(realizar_ronda(red, iniciador, tareas_nodo, escenario))
            red.tareas_eleccion.add(tarea_elec)
            tarea_elec.add_done_callback(red.tareas_eleccion.discard)
    convergido = await esperar_convergencia(red, TIMEOUT_CONVERGENCIA_S)

    t1 = time.perf_counter_ns()
    if convergido:
        convergencia_ms = (t1 - t0) / 1e6
    else:
        convergencia_ms = ""

    vivos = [pid for pid in sorted(red.nodos) if red.nodos[pid].alive]
    max_vivo = max(vivos) if vivos else None
    correcto = False
    if convergido and vivos and max_vivo is not None:
        con_lider = [pid for pid in vivos if red.nodos[pid].es_lider]
        correcto = len(con_lider) == 1 and con_lider[0] == max_vivo and red.nodos[max_vivo].leader_id == max_vivo

    tareas = list(tareas_nodo.values()) + list(red.tareas) + list(red.tareas_eleccion)
    for t in tareas:
        t.cancel()
    if tareas:
        await asyncio.gather(*tareas, return_exceptions=True)

    return {
        "escenario": escenario,
        "n_nodos": n,
        "repeticion": rep,
        "seed": seed,
        "tiempo_convergencia_ms": convergencia_ms,
        "msgs_election": red.contadores[ELECTION],
        "msgs_ok": red.contadores[OK],
        "msgs_coordinator": red.contadores[COORDINATOR],
        "msgs_total": red.contadores[ELECTION] + red.contadores[OK] + red.contadores[COORDINATOR],
        "rondas": red.rondas,
        "correcto": correcto,
    }


def percentil(datos: List[float], p: float) -> float:
    """Calcula el percentil p (0-100) de una lista de datos."""
    if not datos:
        return 0.0
    ordenados = sorted(datos)
    k = (len(ordenados) - 1) * (p / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return ordenados[int(k)]
    return ordenados[f] + (ordenados[c] - ordenados[f]) * (k - f)


def escribir_resultados(path: str, filas: List[Dict[str, object]]) -> None:
    """Escribe las filas de resultados en results.csv."""
    with open(path, "w", newline="", encoding="utf-8") as f:
        escritor = csv.DictWriter(f, fieldnames=RESULT_COLUMNS)
        escritor.writeheader()
        for fila in filas:
            escritor.writerow({
                "escenario": fila["escenario"],
                "n_nodos": fila["n_nodos"],
                "repeticion": fila["repeticion"],
                "seed": fila["seed"],
                "tiempo_convergencia_ms": fila["tiempo_convergencia_ms"],
                "msgs_election": fila["msgs_election"],
                "msgs_ok": fila["msgs_ok"],
                "msgs_coordinator": fila["msgs_coordinator"],
                "msgs_total": fila["msgs_total"],
                "rondas": fila["rondas"],
                "correcto": str(fila["correcto"]),
            })


def imprimir_resumen(filas: List[Dict[str, object]]) -> None:
    """Imprime tabla resumen por (escenario, N): media, std, p95 y tasa de exito."""
    grupos: Dict[tuple, List[Dict[str, object]]] = {}
    for fila in filas:
        clave = (str(fila["escenario"]), int(fila["n_nodos"]))
        grupos.setdefault(clave, []).append(fila)

    print("Resumen por (escenario, N):")
    print(f"{'Escen.':<10}{'N':<6}{'Reps':<8}{'Tiempo_ms_media':<16}{'Tiempo_ms_std':<16}{'Tiempo_ms_p95':<16}{'Msgs_tot_media':<18}{'Msgs_tot_std':<18}{'Msgs_tot_p95':<18}{'Tasa_exito':<14}")
    print("-" * 150)
    for (escenario, n), items in sorted(grupos.items()):
        tiempos = [float(f["tiempo_convergencia_ms"]) for f in items if f["tiempo_convergencia_ms"] != ""]
        msgss = [float(f["msgs_total"]) for f in items]
        tasa_exito = sum(1 for f in items if f["correcto"]) / len(items)
        if tiempos:
            tiempo_media = statistics.mean(tiempos)
            tiempo_std = statistics.stdev(tiempos) if len(tiempos) > 1 else 0.0
            tiempo_p95 = percentil(tiempos, 95.0)
            tiempo_media_s = f"{tiempo_media:<16.3f}"
            tiempo_std_s = f"{tiempo_std:<16.3f}"
            tiempo_p95_s = f"{tiempo_p95:<16.3f}"
        else:
            tiempo_media_s = "n/a".ljust(16)
            tiempo_std_s = "n/a".ljust(16)
            tiempo_p95_s = "n/a".ljust(16)
        print(
            f"{escenario:<10}{n:<6}{len(items):<8}"
            f"{tiempo_media_s}"
            f"{tiempo_std_s}"
            f"{tiempo_p95_s}"
            f"{statistics.mean(msgss):<18.2f}"
            f"{statistics.stdev(msgss) if len(msgss) > 1 else 0.0:<18.2f}"
            f"{percentil(msgss, 95.0):<18.2f}"
            f"{tasa_exito:<14.2f}"
        )


async def corrida(seed: int, reps: int, quick: bool) -> List[Dict[str, object]]:
    """Corrida completa: todos los escenarios, N y repeticiones dentro de un solo asyncio.run."""
    filas = []
    n_para_correr = QUICK_N if quick else N_VALORES
    reps_para_correr = QUICK_REPS if quick else reps
    total_correcto_false = 0
    total_no_convergen = 0
    for escenario in ESCENARIOS:
        for n in n_para_correr:
            for rep in range(1, reps_para_correr + 1):
                fila = await ejecutar(escenario, n, seed, rep)
                filas.append(fila)
                if not fila["tiempo_convergencia_ms"]:
                    total_no_convergen += 1
                if not fila["correcto"]:
                    total_correcto_false += 1
            print(f"Escenario {escenario} N={n} listo ({reps_para_correr} reps)")
    print(f"Corrida finalizada: {len(filas)} ejecuciones, {total_correcto_false} con correcto=False, {total_no_convergen} sin converger.")
    return filas


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark de resiliencia del Algoritmo Bully ante fallas Crash-Stop")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED, help="Semilla base de la simulacion (default: 42)")
    parser.add_argument("--reps", type=int, default=REPETICIONES, help="Numero de repeticiones por (escenario, N) (default: 30)")
    parser.add_argument("--output", type=str, default="results.csv", help="Archivo de salida de resultados (default: results.csv)")
    parser.add_argument("--quick", action="store_true", help="Pruebas rapidas: N en (5, 10) con 3 repeticiones")
    args = parser.parse_args()
    filas = asyncio.run(corrida(args.seed, args.reps, args.quick))
    escribir_resultados(args.output, filas)
    imprimir_resumen(filas)


if __name__ == "__main__":
    main()
