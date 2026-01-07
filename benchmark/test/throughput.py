import sys
import os
import time
import glob
import json

# Setup path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

#from drivers.swarm_driver import SwarmDriver
#from drivers.k8s_driver import K8sDriver
from drivers.nomad_driver import NomadDriver

NUM_JOBS = 10
JOB_DURATION = 5
RESULTS_DIR = "/srv/nfs/cob_results"
JSON_OUTPUT_FILE = os.path.join(parent_dir, "results/k8s/throughput.json")


def run_test():
    print(f"--- TEST: BURST THROUGHPUT ({NUM_JOBS} Jobs) ---")

    #driver = SwarmDriver()
    #driver = K8sDriver()
    driver = NomadDriver()


    driver.clean_jobs()
    os.system(f"rm -f {RESULTS_DIR}/*.json")

    print("[TEST] Launching jobs...")
    start_time = time.time()

    for i in range(NUM_JOBS):
        job_id = f"burst-{i}"
        driver.submit_job(job_id=job_id, job_type="cpu", duration=JOB_DURATION)

    launch_time = time.time() - start_time
    print(f"[TEST] All jobs submitted in {launch_time:.2f}s")

    # Polling
    print("[TEST] Waiting for completion...")
    while True:
        files = glob.glob(f"{RESULTS_DIR}/burst-*.json")
        completed = len(files)
        print(f"\rStatus: {completed}/{NUM_JOBS} finished...", end="")

        if completed >= NUM_JOBS:
            break
        time.sleep(1)

    total_time = time.time() - start_time
    throughput = NUM_JOBS / total_time

    print(f"\n[TEST] DONE! Total Makespan: {total_time:.2f}s")
    print(f"[TEST] Throughput: {throughput:.2f} jobs/sec")

    output_data = {
        "test_name": "burst_throughput",
        "orchestrator": "k8s",
        "parameters": {
            "num_jobs": NUM_JOBS,
            "job_duration": JOB_DURATION
        },
        "results": {
            "launch_overhead_seconds": round(launch_time, 4),
            "total_makespan_seconds": round(total_time, 4),
            "throughput_jobs_per_sec": round(throughput, 4)
        }
    }

    os.makedirs(os.path.dirname(JSON_OUTPUT_FILE), exist_ok=True)

    with open(JSON_OUTPUT_FILE, "w") as f:
        json.dump(output_data, f, indent=2)

    print(f"[RESULT] Report saved to: {JSON_OUTPUT_FILE}")

    driver.clean_jobs()


if __name__ == "__main__":
    if not os.path.exists(RESULTS_DIR):
        print(f"ERROR: Directory {RESULTS_DIR} not found.")
        exit(1)
    run_test()