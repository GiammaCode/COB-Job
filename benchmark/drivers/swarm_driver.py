import subprocess
import time
import json
import collections


class SwarmDriver:
    def __init__(self, stack_name="cob-job", image="192.168.15.9:5000/cob-job-worker:latest"):
        self.stack_name = stack_name
        self.image = image
        self.nfs_mount = "type=bind,source=/srv/nfs/cob_results,target=/mnt/results"

    def _run(self, cmd):
        return subprocess.run(cmd, shell=True, capture_output=True, text=True)

    def submit_job(self, job_id, job_type="cpu", duration=10, constraints=None, cpu_reservation=None,
                   restart_policy="none", command=None):
        service_name = f"{self.stack_name}_{job_id}"

        # Argomenti base
        args = ""
        if constraints:
            for key, val in constraints.items():
                args += f" --constraint node.labels.{key}=={val}"

        if cpu_reservation:
            args += f" --reserve-cpu {cpu_reservation}"

        # Gestione comando custom (utile per far crashare il job nel Test 4)
        cmd_overwrite = ""
        if command:
            cmd_overwrite = f"--command '{command}'"

        # Comando Docker Service Create
        cmd = (
            f"docker service create "
            f"--detach "
            f"--name {service_name} "
            f"--replicas 1 "
            f"--restart-condition {restart_policy} "  # Qui gestiamo il riavvio
            f"--env JOB_ID={job_id} "
            f"--env JOB_TYPE={job_type} "
            f"--env DURATION={duration} "
            f"--mount {self.nfs_mount} "
            f"{args} "
            f"{cmd_overwrite} "
            f"{self.image}"
        )

        res = self._run(cmd)
        if res.returncode != 0:
            print(f"[SWARM] Error launching {job_id}: {res.stderr}")
            return False
        return True

    def get_node_distribution(self):
        """Restituisce un dizionario {nome_nodo: numero_job_running}"""
        # Formattiamo l'output per avere solo il nodo dei task attivi
        cmd = f"docker service ps $(docker service ls -q --filter name={self.stack_name}) --format '{{{{.Node}}}}' --filter desired-state=running"
        res = self._run(cmd)
        nodes = res.stdout.strip().split('\n')
        # Filtra righe vuote
        nodes = [n for n in nodes if n]
        return dict(collections.Counter(nodes))

    def get_task_history(self, job_id):
        """Restituisce la storia dei task per un servizio (utile per vedere i crash)"""
        service_name = f"{self.stack_name}_{job_id}"
        # Prende ID, Stato Corrente, Stato Desiderato, Errore
        cmd = f"docker service ps {service_name} --format '{{{{.CurrentState}}}}|{{{{.DesiredState}}}}|{{{{.Error}}}}'"
        res = self._run(cmd)
        return res.stdout.strip().split('\n')

    def clean_jobs(self):
        print(f"[SWARM] Cleaning services ({self.stack_name})...")
        cmd = f"docker service ls --filter name={self.stack_name} -q | xargs -r docker service rm"
        self._run(cmd)
        time.sleep(5)