import subprocess
import time
import os

class SwarmDriver:
    def __init__(self, stack_name="cob-job", image="192.168.15.9:5000/cob-job-worker:latest"):
        self.stack_name = stack_name
        self.image = image
        # Assicurati che questo path esista sul nodo dove lanci lo script e sui worker
        self.nfs_mount = "type=bind,source=/srv/nfs/cob_results,target=/mnt/results"

    def _run(self, cmd):
        return subprocess.run(cmd, shell=True, capture_output=True, text=True)

    def submit_job(self, job_id, job_type="cpu", duration=10, constraints=None):
        service_name = f"{self.stack_name}_{job_id}"

        # Gestione Constraints (es. GPU)
        constraint_flag = ""
        if constraints:
            for key, val in constraints.items():
                constraint_flag += f" --constraint node.labels.{key}=={val}"

        # Comando Docker Service Create (One-Shot)
        cmd = (
            f"docker service create "
            f"--name {service_name} "
            f"--replicas 1 "
            f"--restart-condition none "
            f"--env JOB_ID={job_id} "
            f"--env JOB_TYPE={job_type} "
            f"--env DURATION={duration} "
            f"--mount {self.nfs_mount} "
            f"{constraint_flag} "
            f"{self.image}"
        )

        res = self._run(cmd)
        if res.returncode != 0:
            print(f"[SWARM] Error launching {job_id}: {res.stderr}")
            return False
        return True

    def cleanup_job(self):
        print(f"[SWARM] Cleaning services ({self.stack_name})...")
        # Trova tutti i servizi del benchmark e li rimuove
        cmd = f"docker service ls --filter name={self.stack_name} -q | xargs -r docker service rm"
        self._run(cmd)
        time.sleep(5)  # Attesa tecnica per il cleanup

