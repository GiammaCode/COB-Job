import sys
import os
import time
import glob

# Aggiungiamo la cartella superiore al path per importare i driver
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from drivers.swarm_driver import SwarmDriver

# CONFIGURAZIONE
NUM_JOBS = 10  # Inizia con 10, poi alzeremo a 50/100
JOB_DURATION = 5  # Secondi di lavoro simulato
RESULTS_DIR = "/srv/nfs/cob_results"


def run_test():
    print(f"--- TEST 1: BURST THROUGHPUT ({NUM_JOBS} Jobs) ---")

    # 1. Inizializza Driver
    driver = SwarmDriver()

    # 2. Pulizia Preliminare
    driver.clean_jobs()
    # Pulisce anche i file json vecchi
    os.system(f"rm -f {RESULTS_DIR}/*.json")

    print("[TEST] Launching jobs...")
    start_time = time.time()

    # 3. Burst Launch
    for i in range(NUM_JOBS):
        job_id = f"burst-{i}"
        driver.submit_job(job_id=job_id, job_type="cpu", duration=JOB_DURATION)
        # Piccolo sleep per non intasare il socket locale di docker se il PC è lento
        # time.sleep(0.05)

    launch_time = time.time() - start_time
    print(f"[TEST] All jobs submitted in {launch_time:.2f}s")

    # 4. Polling per il completamento
    print("[TEST] Waiting for completion...")
    completed = 0
    while completed < NUM_JOBS:
        # Conta i file JSON generati nella cartella condivisa
        files = glob.glob(f"{RESULTS_DIR}/burst-*.json")
        completed = len(files)
        print(f"\rStatus: {completed}/{NUM_JOBS} finished...", end="")

        if completed >= NUM_JOBS:
            break

        time.sleep(1)

    total_time = time.time() - start_time
    print(f"\n[TEST] DONE! Total Makespan: {total_time:.2f}s")
    print(f"[TEST] Throughput: {NUM_JOBS / total_time:.2f} jobs/sec (End-to-End)")

    # 5. Cleanup Finale
    driver.clean_jobs()


if __name__ == "__main__":
    # Assicuriamoci che la directory esista
    if not os.path.exists(RESULTS_DIR):
        print(f"ERROR: Directory {RESULTS_DIR} not found. Please create it.")
        exit(1)

    run_test()