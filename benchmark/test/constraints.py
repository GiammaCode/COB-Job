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

RESULTS_DIR = "/srv/nfs/cob_results"
NUM_GPU_JOBS = 3
NUM_CPU_JOBS = 3
JSON_OUTPUT_FILE = os.path.join(parent_dir, "results/nomad/placement_constraints.json")


def check_placement(file_path):
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
        return data.get("node", "unknown"), data.get("job_id", "unknown")
    except Exception as e:
        return "error", "error"


def run_test():
    print(f"--- TEST: PLACEMENT CONSTRAINTS COMPLIANCE ---")
    #driver = SwarmDriver()
    #driver = K8sDriver()
    driver = NomadDriver()


    driver.clean_jobs()
    os.system(f"rm -f {RESULTS_DIR}/*.json")

    print("[TEST] Launching Mixed Workload...")

    # Launch GPU Jobs
    for i in range(NUM_GPU_JOBS):
        driver.submit_job(job_id=f"job-gpu-{i}",
                          job_type="sleep",
                          duration=5,
                          constraints={"type": "gpu"}
                          #constraints={"hardware": "gpu"}
                          )

    # Launch CPU Jobs
    for i in range(NUM_CPU_JOBS):
        driver.submit_job(job_id=f"job-cpu-{i}",
                          job_type="sleep",
                          duration=5,
                          constraints={"type": "cpu"}
                          #constraints={"hardware": "cpu"}
                          )

    # Wait
    expected_files = NUM_GPU_JOBS + NUM_CPU_JOBS
    print(f"[TEST] Waiting for {expected_files} results...")

    while True:
        files = glob.glob(f"{RESULTS_DIR}/*.json")
        if len(files) >= expected_files:
            break
        print(f"\rStatus: {len(files)}/{expected_files} finished...", end="")
        time.sleep(2)

    print("\n[TEST] All jobs finished. Analyzing placement...")

    # Analysis
    gpu_nodes_used = set()
    cpu_nodes_used = set()
    errors = 0

    #Check nodes
    for i in range(NUM_GPU_JOBS):
        fpath = os.path.join(RESULTS_DIR, f"job-gpu-{i}.json")
        if os.path.exists(fpath):
            node, _ = check_placement(fpath)
            gpu_nodes_used.add(node)
        else:
            errors += 1

    for i in range(NUM_CPU_JOBS):
        fpath = os.path.join(RESULTS_DIR, f"job-cpu-{i}.json")
        if os.path.exists(fpath):
            node, _ = check_placement(fpath)
            cpu_nodes_used.add(node)
        else:
            errors += 1

    intersection = gpu_nodes_used.intersection(cpu_nodes_used)

    if len(intersection) == 0 and errors == 0:
        result_status = "PASSED"
        print("\nPASSED: Perfect isolation.")
    else:
        result_status = "FAILED"
        print(f"\nFAILED: Overlap or Errors.")

    output_data = {
        "test_name": "placement_constraints",
        "orchestrator": "nomad",
        "parameters": {
            "gpu_jobs": NUM_GPU_JOBS,
            "cpu_jobs": NUM_CPU_JOBS
        },
        "results": {
            "status": result_status,
            "gpu_nodes_used": list(gpu_nodes_used),
            "cpu_nodes_used": list(cpu_nodes_used),
            "errors": errors,
            "overlap_detected": len(intersection) > 0
        }
    }

    os.makedirs(os.path.dirname(JSON_OUTPUT_FILE), exist_ok=True)
    with open(JSON_OUTPUT_FILE, "w") as f:
        json.dump(output_data, f, indent=2)

    print(f"[RESULT] Report saved to: {JSON_OUTPUT_FILE}")

    driver.clean_jobs()


if __name__ == "__main__":
    run_test()