import sys
import os
import time
import glob
import json
import numpy as np

# Setup path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

#from drivers.swarm_driver import SwarmDriver
#from drivers.k8s_driver import K8sDriver
from drivers.nomad_driver import NomadDriver

NUM_JOBS = 30
JOB_DURATION = 15
CPU_REQ = "1.0"
RESULTS_DIR = "/srv/nfs/cob_results"
JSON_OUTPUT_FILE = os.path.join(parent_dir, "results/k8s/saturation.json")


def run_test():
    print(f"--- TEST: SATURATION & QUEUEING ({NUM_JOBS} Jobs, {CPU_REQ} CPU req) ---")

    #driver = SwarmDriver()
    #driver = K8sDriver()
    driver = NomadDriver()

    driver.clean_jobs()
    os.system(f"rm -f {RESULTS_DIR}/*.json")

    submission_times = {}
    print("[TEST] Burst Launching jobs...")

    for i in range(NUM_JOBS):
        job_id = f"sat-{i}"
        submission_times[job_id] = time.time()

        success = driver.submit_job(
            job_id=job_id,
            job_type="sleep",  # Usiamo sleep per non stressare davvero la CPU, ma occupare lo slot logico
            duration=JOB_DURATION,
            cpu_reservation=CPU_REQ
        )
        if not success:
            print(f"[WARNING] Job {job_id} rejected by orchestrator immediately!")

    print(f"[TEST] All {NUM_JOBS} jobs submitted. Monitoring queue processing...")

    while True:
        files = glob.glob(f"{RESULTS_DIR}/sat-*.json")
        completed = len(files)
        print(f"\rStatus: {completed}/{NUM_JOBS} finished...", end="")

        if completed >= NUM_JOBS:
            break
        time.sleep(1)

    print("\n[TEST] All jobs finished. Analyzing Queue Times...")
    queue_times = []

    for i in range(NUM_JOBS):
        job_id = f"sat-{i}"
        fpath = os.path.join(RESULTS_DIR, f"{job_id}.json")

        if os.path.exists(fpath):
            with open(fpath, 'r') as f:
                data = json.load(f)

            # Start TS (dal container) - Submission TS (dal driver)
            start_ts = data["start_ts"]
            submit_ts = submission_times.get(job_id, start_ts)

            wait_time = start_ts - submit_ts
            # Correggi eventuali negativi dovuti a clock drift millimetrico
            if wait_time < 0: wait_time = 0

            queue_times.append(wait_time)

    # Stats
    avg_wait = np.mean(queue_times)
    max_wait = np.max(queue_times)
    min_wait = np.min(queue_times)

    print(f"\n--- RESULTS ---")
    print(f"Average Queue Time: {avg_wait:.2f}s")
    print(f"Max Queue Time:     {max_wait:.2f}s")

    if max_wait > 2.0:
        print("SUCCESS: Queueing behavior detected (Saturation reached).")
    else:
        print("WARNING: Queue times are very low. Cluster was not saturated. Increase CPU_REQ or NUM_JOBS.")

    output_data = {
        "test_name": "saturation_queueing",
        "orchestrator": "k8s",
        "parameters": {
            "num_jobs": NUM_JOBS,
            "cpu_reservation": CPU_REQ,
            "job_duration": JOB_DURATION
        },
        "results": {
            "avg_queue_time_seconds": round(avg_wait, 4),
            "max_queue_time_seconds": round(max_wait, 4),
            "min_queue_time_seconds": round(min_wait, 4),
            "queue_times_series": [round(x, 2) for x in queue_times]
        }
    }

    os.makedirs(os.path.dirname(JSON_OUTPUT_FILE), exist_ok=True)
    with open(JSON_OUTPUT_FILE, "w") as f:
        json.dump(output_data, f, indent=2)

    print(f"[RESULT] Report saved to: {JSON_OUTPUT_FILE}")

    driver.clean_jobs()


if __name__ == "__main__":
    run_test()