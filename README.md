![Python](https://img.shields.io/badge/Python-3.10%2B-blue)
![License](https://img.shields.io/badge/license-MIT-green)
![Status](https://img.shields.io/badge/Status-Thesis_Project-orange)
![Docker](https://img.shields.io/badge/Orchestrator-Docker%20Swarm-2496ED)
![K8s](https://img.shields.io/badge/Orchestrator-Kubernetes-326CE5)
![Nomad](https://img.shields.io/badge/Orchestrator-Nomad-00CA8E)

# COB-Job: Container Orchestrator Benchmark (Short-Lived)

**COB-Job** is the second component of a Master's Thesis project designed to analyze and compare container orchestration technologies. While the first component (*COB-Service*) focused on long-running services, **COB-Job focuses on batch processing, scheduling performance, and short-lived compute tasks.**

The system evaluates three major orchestrators under identical workload conditions:
- **Docker Swarm**
- **Kubernetes (K3s)**
- **HashiCorp Nomad**

## Goal
The primary objective is to measure the overhead and efficiency of the orchestrator's control plane when handling:
1.  **Burst Submissions:** High-frequency job creation.
2.  **Resource Saturation:** Queue management when cluster resources are exhausted.
3.  **Scheduling Fairness:** Load distribution across the cluster.
4.  **Constraints Handling:** Placement accuracy (e.g., GPU vs CPU nodes).
5.  **Fault Recovery:** Ability to reschedule failed batch jobs.

## Supported Orchestrators & Drivers
The benchmark suite includes specific drivers to interface with each technology.

| Orchestrator | Driver Source | Status |
|:---|:---|:---|
| **Docker Swarm** | `drivers/swarm_driver.py` | Ready |
| **Kubernetes** | `drivers/k8s_driver.py` | Ready |
| **Nomad** | `drivers/nomad_driver.py` | Ready |

## Project Structure

```text
COB-Job/
├── benchmark/
│   ├── drivers/          # Orchestrator abstraction layer (Swarm, K8s, Nomad drivers)
│   ├── results/          # JSON outputs generated during tests
│   ├── test/             # Python test scripts (The actual benchmark logic)
│   │   ├── throughput.py
│   │   ├── saturation.py
│   │   ├── fairness.py
│   │   ├── constraints.py
│   │   └── recovery.py
│   └── requirements.txt  # Python dependencies for the test suite
├── src/
│   └── worker/           # The job container logic
│       ├── worker.py     # Script performing CPU/IO operations
│       ├── Dockerfile    # Image definition
│       └── requirements.txt
├── README.md
└── LICENSE

```

## Architecture & Prerequisites
Unlike the Service benchmark (which relied on HTTP/Locust), COB-Job relies on a shared file system (NFS) to collect start/end timestamps directly from the containers.

1. Requirements
- Python 3.10+ 
- NFS Server: A shared directory must be mounted on all worker nodes.
  - Host Path: /srv/nfs/cob_results 
  - Container Path: /mnt/results 
- Docker Registry: To host the cob-job-worker image accessible by the cluster.

2. Build & Push Worker Image
Before running tests, build the worker image and push it to your local 
registry (e.g., 192.168.15.9:5000 as seen in config):

```
cd src/worker
docker build -t 192.168.15.9:5000/cob-job-worker:latest .
docker push 192.168.15.9:5000/cob-job-worker:latest
```

3. Setup Environment
```
cd benchmark
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## How to Run Benchmarks
To execute a benchmark, run the corresponding Python script from the benchmark/test/ directory. 
Note: Ensure the correct driver is uncommented/instantiated in the python script before running.

1. Burst Throughput (Scalability)
Measures the pure launch overhead and throughput (Jobs/sec) by submitting a burst of short jobs.
```
python test/throughput.py
```

2. Saturation & Queueing
Submits more jobs than the cluster CPUs can handle to verify FIFO queue behavior and Wait Time.

```
python test/saturation.py
```

3. Parallelism & Fairness
Checks if the scheduler distributes jobs evenly across available nodes (Standard Deviation analysis).
```
python test/fairness.py
```

4. Placement Constraints
Verifies that jobs tagged with type=gpu or type=cpu land on the correct nodes.
```
python test/constraints.py
```

5. Fault Recovery
Launches a "suicide job" (exits with error) to measure the time taken by the orchestrator to detect failure and spawn a replacement.
```
python test/recovery.py
```

## Benchmark Metrics & Results
Results are saved automatically in benchmark/results/<orchestrator>/ 
as JSON files.

## License
This project is part of a Computer Engineering Thesis at the University
of Bologna. Distributed under the MIT License. 
See LICENSE for more information.
