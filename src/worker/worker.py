import os
import time
import json
import socket
import numpy as np
from datetime import datetime

# --- CONFIGURAZIONE DA ENV VARS ---
JOB_ID = os.environ.get("JOB_ID", "unknown")
JOB_TYPE = os.environ.get("JOB_TYPE", "cpu")  # 'cpu', 'io', 'sleep'
DURATION = float(os.environ.get("DURATION", "10"))
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "/mnt/results")
# Simulazione vincolo hardware (solo descrittivo per il log)
REQUIRES_GPU = os.environ.get("REQUIRES_GPU", "false").lower() == "true"

def do_cpu_work(duration_sec):
    start = time.time()
    #matrice 500x500
    matrix_size = 500
    while time.time() < start + duration_sec:
        a = np.random.rand(matrix_size, matrix_size)
        b = np.random.rand(matrix_size, matrix_size)
        _ = np.dot(a, b)
        # Breve sleep per evitare di bloccare completamente il GIL o il container se necessario
        time.sleep(0.01)

def do_io_work(duration_sec):
    start = time.time()
    while time.time() < start + duration_sec:
        with open("/tmp/io_test.dat", "w") as f:
            f.write("data" * 1000)
        time.sleep(0.1)

def run_job():
    print(f"[WORKER] Starting job {JOB_ID} on {socket.gethostname()} (Type: {JOB_TYPE}, Duration: {DURATION}s)")
    start_ts = time.time()
    start_dt = datetime.now().isoformat()

    try:
        if JOB_TYPE == "cpu":
            do_cpu_work(DURATION)
        elif JOB_TYPE == "io":
            do_io_work(DURATION)
        else:
            # Default sleep (utile per test di scheduling puro)
            time.sleep(DURATION)

        status = "completed"
        error_msg = None

    except Exception as e:
        print(f"[WORKER] Exception: {e}")
        status = "failed"
        error_msg = str(e)

    end_ts = time.time()
    end_dt = datetime.now().isoformat()
    real_duration = end_ts - start_ts

    #to write in the shared volume
    result_data = {
        "job_id": JOB_ID,
        "node": socket.gethostname(),
        "status": status,
        "job_type": JOB_TYPE,
        "start_ts": start_ts,
        "end_ts": end_ts,
        "start_dt": start_dt,
        "end_dt": end_dt,
        "duration_target": DURATION,
        "duration_real": real_duration,
        "error": error_msg
    }

    # Assicuriamoci che la directory esista (se il volume è montato correttamente)
    if not os.path.exists(OUTPUT_DIR):
        print(f"[WORKER] Warning: Output dir {OUTPUT_DIR} does not exist. Using /tmp")
        final_output_dir = "/tmp"
    else:
        final_output_dir = OUTPUT_DIR

    output_file = os.path.join(final_output_dir, f"{JOB_ID}.json")

    try:
        with open(output_file, "w") as f:
            json.dump(result_data, f)
        print(f"[WORKER] Result written to {output_file}")
    except Exception as e:
        print(f"[WORKER] CRITICAL: Could not write result file! {e}")
        # Exit code != 0 notifica all'orchestratore il fallimento
        exit(1)


if __name__ == "__main__":
    run_job()