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

        args = ""
        if constraints:
            for key, val in constraints.items():
                args += f" --constraint node.labels.{key}=={val}"

        if cpu_reservation:
            args += f" --reserve-cpu {cpu_reservation}"

        final_cmd = ""
        if command:
            final_cmd = f" {command}"

        cmd = (
            f"docker service create "
            f"--detach "
            f"--name {service_name} "
            f"--replicas 1 "
            f"--restart-condition {restart_policy} "
            f"--env JOB_ID={job_id} "
            f"--env JOB_TYPE={job_type} "
            f"--env DURATION={duration} "
            f"--mount {self.nfs_mount} "
            f"{args} "
            f"{self.image}"  
            f"{final_cmd}"
        )

        res = self._run(cmd)
        if res.returncode != 0:
            print(f"[SWARM] Error launching {job_id}: {res.stderr}")
            return False
        return True

    def get_node_distribution(self):
        """Return {name_node: number_job_running}"""
        # Retrieve only task actived
        cmd = (f"docker service ps $(docker service ls -q "
               f"--filter name={self.stack_name}) "
               f"--format '{{{{.Node}}}}' "
               f"--filter desired-state=running")

        res = self._run(cmd)
        nodes = res.stdout.strip().split('\n')
        #Filter empty raw
        nodes = [n for n in nodes if n]
        return dict(collections.Counter(nodes))

    def get_task_history(self, job_id):
        """Return {task_name: task_history}"""
        service_name = f"{self.stack_name}_{job_id}"
        # Prende ID, Stato Corrente, Stato Desiderato, Errore
        cmd = (f"docker service ps {service_name} "
               f"--format '{{{{.CurrentState}}}}|{{{{.DesiredState}}}}|{{{{.Error}}}}'")
        res = self._run(cmd)
        return res.stdout.strip().split('\n')

    def clean_jobs(self):
        print(f"[SWARM] Cleaning services ({self.stack_name})...")
        cmd = f"docker service ls --filter name={self.stack_name} -q | xargs -r docker service rm"
        self._run(cmd)
        time.sleep(5)