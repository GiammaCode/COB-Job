import time
import sys
import os
import statistics
import json

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from drivers.swarm_driver import SwarmDriver


def run_test():
    driver = SwarmDriver()
    driver.clean_jobs()

    # Abbiamo 3 nodi x 4 CPU = 12 CPU Totali.
    NUM_JOBS = 12
    CPU_REQ = "1.0"

    print(f"--- TEST: PARALLELISM & FAIRNESS ({NUM_JOBS} Jobs on Cluster) ---")

    print("[TEST] Submitting jobs...")
    for i in range(NUM_JOBS):
        driver.submit_job(
            job_id=f"fair_{i}",
            job_type="cpu",
            duration=20,
            cpu_reservation=CPU_REQ
        )

    print("[TEST] Waiting 5s for scheduler to settle...")
    time.sleep(5)

    # 2. Analisi Distribuzione
    distribution = driver.get_node_distribution()
    print(f"\n[ANALYSIS] Node Distribution: {distribution}")

    counts = list(distribution.values())

    if not counts:
        print("Error: No running jobs found.")
        driver.clean_jobs()
        return

    #Stats
    total_jobs = sum(counts)
    avg_jobs = statistics.mean(counts)
    try:
        stdev = statistics.stdev(counts)
    except statistics.StatisticsError:
        stdev = 0.0  # Se c'è un solo dato (1 nodo)

    print("\n--- RESULTS ---")
    print(f"Total Running Jobs: {total_jobs}/{NUM_JOBS}")
    print(f"Average Jobs/Node:  {avg_jobs:.2f}")
    print(f"Standard Deviation: {stdev:.2f}")

    # Interpretazione
    # Deviazione Standard bassa (es. < 1) significa ottimo bilanciamento.
    # Esempio perfetto su 3 nodi con 12 job: [4, 4, 4] -> Stdev 0.0

    if stdev < 1.5:
        print("STATUS: BALANCED (Scheduler is distributing load fairly)")
    else:
        print("STATUS: UNBALANCED (Load is concentrated on few nodes)")

    results = {
        "test_name": "parallelism_fairness",
        "orchestrator": "swarm",
        "distribution": distribution,
        "stdev": stdev,
        "balanced": stdev < 1.5
    }

    os.makedirs("results/swarm", exist_ok=True)
    with open("results/swarm/fairness.json", "w") as f:
        json.dump(results, f, indent=2)
        print(f"[RESULT] Report saved to results/swarm/fairness.json")

    driver.clean_jobs()


if __name__ == "__main__":
    run_test()