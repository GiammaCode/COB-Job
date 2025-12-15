import time
import sys
import os

# Aggiunge la cartella superiore al path per importare i moduli
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from drivers.swarm_driver import SwarmDriver


def run_test():
    driver = SwarmDriver()
    driver.clean_jobs()

    print("--- TEST 4: BATCH FAULT RECOVERY (At-Least-Once) ---")

    job_id = "recovery_test"

    # 1. Lanciamo un job che fallisce intenzionalmente dopo 5 secondi
    # Usiamo "sh -c" per simulare un crash del processo
    print(f"[TEST] Launching 'Suicide Job' (sleep 5; exit 1)...")

    success = driver.submit_job(
        job_id=job_id,
        cpu_reservation="0.1",
        restart_policy="on-failure",  # FONDAMENTALE: Diciamo a Swarm di riavviare se fallisce
        command="sh -c \"sleep 5; echo 'CRASHING NOW'; exit 1\""
    )

    if not success:
        print("Failed to submit job.")
        return

    print("[TEST] Job submitted. Monitoring recovery...")

    # Monitoraggio
    start_time = time.time()
    recovered = False
    failure_detected = False

    for i in range(20):  # Monitora per 20 secondi
        history = driver.get_task_history(job_id)

        # Analizziamo la storia del servizio
        # Cerchiamo una riga che indica "Failed" e una successiva "Running"
        failed_count = sum(1 for line in history if "Failed" in line)
        running_count = sum(1 for line in history if "Running" in line)

        if failed_count > 0 and not failure_detected:
            print(f"   [{i}s] Detection: Job has failed (as expected). Waiting for restart...")
            failure_detected = True

        if failure_detected and running_count > 0:
            print(f"   [{i}s] SUCCESS: New container spawned and is Running!")
            recovered = True
            break

        time.sleep(1)

    print("-" * 30)
    if recovered:
        print("RESULT: PASSED. Swarm successfully rescheduled the crashed job.")
    else:
        print("RESULT: FAILED. Job crashed but was not rescheduled.")

    driver.clean_jobs()


if __name__ == "__main__":
    run_test()