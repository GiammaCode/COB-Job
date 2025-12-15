import sys
import os
import time
import glob
import json

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from drivers.swarm_driver import SwarmDriver

RESULTS_DIR = "/srv/nfs/cob/results"
NUM_GPU_JOBS = 3
NUM_CPU_JOBS = 3

def check_placement(file_path):
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
        return data.get("node", "unknown"), data.get("job_id", "unknown")
    except Exception as e:
        print(f"[ERROR]: Reading {file_path}: {e}")
        return "error", "error"

def run_test():
    print(f"--- TEST 2: PLACEMENTS CONSSTRAINTS COMPPLIANCE ---")
    driver = SwarmDriver()

    driver.clean_jobs()
    os.system(f"rm -f {RESULTS_DIR}/*.json")

    # 2. Lancia Job GPU (Vincolo: hardware=gpu)
    # Usiamo 'sleep' come job_type perché ci interessa dove finisce, non il calcolo
    for i in range(NUM_GPU_JOBS):
        driver.submit_job(
            job_id=f"job-gpu-{i}",
            job_type="sleep",
            duration=5,
            constraints={"hardware": "gpu"}
        )
        print(f"   -> Submitted GPU job: job-gpu-{i}")

        # 3. Lancia Job CPU (Vincolo: hardware=cpu)
        for i in range(NUM_CPU_JOBS):
            driver.submit_job(
                job_id=f"job-cpu-{i}",
                job_type="sleep",
                duration=5,
                constraints={"hardware": "cpu"}
            )
            print(f"   -> Submitted CPU job: job-cpu-{i}")

        #wait to complete
        expected_files = NUM_GPU_JOBS + NUM_CPU_JOBS
        print(f"[TEST] Waiting for {expected_files} results in NFS...")

        while True:
            files = glob.glob(f"{RESULTS_DIR}/*.json")
            if len(files) >= expected_files:
                break
            print(f"\rStatus: {len(files)}/{expected_files} finished...", end="")
            time.sleep(2)

        print("\n[TEST] All jobs finished. Analyzing placement...")

        gpu_nodes_used = set()
        cpu_nodes_used = set()
        errors = 0

        for i in range(NUM_GPU_JOBS):
            fpath = os.path.join(RESULTS_DIR, f"gpu-{i}.json")
            if os.path.exists(fpath):
                node, jid = check_placement(fpath)
                gpu_nodes_used.add(node)
                print(f"   [GPU Job] {jid} executed on: {node}")
            else:
                print(f"   [ERROR] Missing report for job-gpu-{i}")
                errors += 1

        # Analisi Job CPU
        for i in range(NUM_CPU_JOBS):
            fpath = os.path.join(RESULTS_DIR, f"job-cpu-{i}.json")
            if os.path.exists(fpath):
                node, jid = check_placement(fpath)
                cpu_nodes_used.add(node)
                print(f"   [CPU Job] {jid} executed on: {node}")
            else:
                print(f"   [ERROR] Missing report for job-cpu-{i}")
                errors += 1

        print("\n--- RESULT SUMMARY ---")
        print(f"Nodes handling GPU workload: {gpu_nodes_used}")
        print(f"Nodes handling CPU workload: {cpu_nodes_used}")

        # Verifica Intersezione (Se vuota, isolamento perfetto)
        intersection = gpu_nodes_used.intersection(cpu_nodes_used)

        if len(intersection) == 0 and errors == 0:
            print("\nPASSED: Perfect isolation between GPU and CPU workloads.")
        elif len(intersection) > 0:
            print(f"\nFAILED: Overlap detected! Nodes {intersection} executed both types.")
        else:
            print("\nWARNING: Test completed with execution errors.")

        driver.clean_jobs()

    if __name__ == "__main__":
        run_test()

