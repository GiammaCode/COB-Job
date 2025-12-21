import time
import sys
import os
import json

# Setup path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from drivers.k8s_driver import K8sDriver

JSON_OUTPUT_FILE = os.path.join(parent_dir, "results/k8s/recovery.json")


def run_test():
    print("--- TEST: BATCH FAULT RECOVERY (K8s) ---")
    driver = K8sDriver()
    driver.clean_jobs()

    job_id = "recovery-test"

    # 1. Lanciamo un "Suicide Job"
    # Usa 'Never' per costringere K8s a creare un NUOVO Pod quando il primo muore
    print(f"[TEST] Launching 'Suicide Job' (sleep 5; exit 1)...")

    success = driver.submit_job(
        job_id=job_id,
        cpu_reservation="0.1",
        restart_policy="Never",  # <--- FONDAMENTALE (era 'on-failure')
        command='sh -c "sleep 5; echo CRASHING NOW; exit 1"'
    )

    if not success:
        print("[ERROR] Failed to submit job.")
        return

    print("[TEST] Job submitted. Monitoring recovery (30s)...")  # <--- 30s (era 20s)

    # Monitoraggio
    start_time = time.time()
    recovered = False
    failure_detected = False

    # Loop di monitoraggio esteso a 30s per dare tempo a K8s di reagire
    for i in range(30):
        history = driver.get_task_history(job_id)

        # Cerca "Error" (K8s) invece di "Failed" (Swarm)
        error_count = sum(1 for line in history if "Error" in line or "Failed" in line)
        running_count = sum(1 for line in history if "Running" in line)
        completed_count = sum(1 for line in history if "Completed" in line)

        # Detection logic
        if error_count > 0 and not failure_detected:
            print(f"   [{i}s] Detection: Pod has failed (Error/Crash). Waiting for restart...")
            failure_detected = True

        # Recovery Logic: Abbiamo visto un errore PRIMA, e ORA c'è un pod Running
        if failure_detected and (running_count > 0 or completed_count > 0):
            print(f"   [{i}s] SUCCESS: New Pod spawned and is Active!")
            recovered = True
            break

        time.sleep(1)

    print("-" * 30)
    if recovered:
        print("RESULT: PASSED (Job recovered automatically)")
        status = "PASSED"
    else:
        print("RESULT: FAILED (No recovery detected or timeout)")
        status = "FAILED"

    # Save Results
    output_data = {
        "test_name": "fault_recovery",
        "orchestrator": "k8s",
        "results": {
            "status": status,
            "failure_detected": failure_detected,
            "recovered": recovered,
            "mechanism": "Pod Replacement (restartPolicy: Never)"
        }
    }

    os.makedirs(os.path.dirname(JSON_OUTPUT_FILE), exist_ok=True)
    with open(JSON_OUTPUT_FILE, "w") as f:
        json.dump(output_data, f, indent=2)
    print(f"[RESULT] Report saved to: {JSON_OUTPUT_FILE}")

    driver.clean_jobs()


if __name__ == "__main__":
    run_test()