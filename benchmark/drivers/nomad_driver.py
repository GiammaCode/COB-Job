import subprocess
import time
import json
import collections
import os


class NomadDriver:
    def __init__(self, job_prefix="cob-job", image="192.168.15.9:5000/cob-job-worker:latest"):
        self.job_prefix = job_prefix
        self.image = image
        # Nomad Docker driver: mount type bind
        self.host_path = "/srv/nfs/cob_results"
        self.container_mount = "/mnt/results"
        self.datacenters = ["dc1"]

    def _run(self, cmd, input_str=None):
        return subprocess.run(cmd, input=input_str, shell=True, capture_output=True, text=True)

    def submit_job(self, job_id, job_type="cpu", duration=10, constraints=None, cpu_reservation=None,
                   restart_policy="none", command=None):
        # Nomad ID non accetta underscore, meglio usare trattini
        safe_job_id = f"{self.job_prefix}-{job_id}".replace("_", "-")

        # 1. Configurazione Restart Policy (HCL friendly)
        attempts = 0
        if restart_policy.lower() not in ["none", "never"]:
            attempts = 2

        restart_stanza = {
            "interval": "5m",
            "attempts": attempts,
            "delay": "15s",
            "mode": "fail"
        }

        # 2. Configurazione Risorse CPU
        cpu_mhz = 100
        if cpu_reservation:
            try:
                cpu_mhz = int(float(cpu_reservation) * 2000)
            except:
                pass

        # 3. Configurazione Constraints (HCL Syntax)
        # HCL: constraint { attribute = ... value = ... }
        nomad_constraints = []
        if constraints:
            for k, v in constraints.items():
                nomad_constraints.append({
                    "attribute": "${meta." + k + "}",
                    "value": v,
                    "operator": "="
                })

        # 4. Configurazione Comando Custom (Docker Driver Config)
        docker_config = {
            "image": self.image,
            "mounts": [{
                "type": "bind",
                "source": self.host_path,
                "target": self.container_mount,
                "readonly": False
            }]
        }

        if command:
            if "sh -c" in command:
                clean_cmd = command.replace("sh -c ", "").strip('"').strip("'")
                docker_config["command"] = "/bin/sh"
                docker_config["args"] = ["-c", clean_cmd]
            else:
                parts = command.split()
                docker_config["command"] = parts[0]
                docker_config["args"] = parts[1:]

        # 5. Costruzione Job Spec in formato HCL-JSON
        # Struttura: { "job": { "ID_JOB": { ... } } }
        job_spec = {
            "job": {
                safe_job_id: {
                    "id": safe_job_id,
                    "type": "batch",
                    "datacenters": self.datacenters,
                    "group": {
                        "worker-group": {
                            "count": 1,
                            "restart": restart_stanza,
                            "constraint": nomad_constraints,
                            "task": {
                                "worker": {
                                    "driver": "docker",
                                    "config": docker_config,
                                    "env": {
                                        "JOB_ID": str(job_id),
                                        "JOB_TYPE": str(job_type),
                                        "DURATION": str(duration),
                                        "OUTPUT_DIR": self.container_mount
                                    },
                                    "resources": {
                                        "cpu": cpu_mhz,
                                        "memory": 256
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        # Invio a Nomad via CLI
        json_str = json.dumps(job_spec)
        cmd = "nomad job run -detach -"
        res = self._run(cmd, input_str=json_str)

        if res.returncode != 0:
            print(f"[NOMAD] Error launching {job_id}: {res.stderr}")
            return False
        return True

    def get_node_distribution(self):
        """Ritorna {nome_nodo: numero_allocazioni_running}"""
        cmd = f"nomad job status -short | grep {self.job_prefix} | awk '{{print $1}}'"
        res = self._run(cmd)
        job_ids = res.stdout.strip().split('\n')
        job_ids = [j for j in job_ids if j]

        node_counts = collections.defaultdict(int)

        for jid in job_ids:
            alloc_cmd = (f"nomad job allocs -json {jid}")
            alloc_res = self._run(alloc_cmd)
            try:
                data = json.loads(alloc_res.stdout)
                for alloc in data:
                    if alloc['ClientStatus'] == 'running':
                        node_name = alloc.get('NodeName', 'unknown')
                        node_counts[node_name] += 1
            except json.JSONDecodeError:
                pass

        return dict(node_counts)

    def clean_jobs(self):
        print(f"[NOMAD] Cleaning jobs starting with {self.job_prefix}...")
        cmd = f"nomad job status -short | grep {self.job_prefix} | awk '{{print $1}}' | xargs -r nomad job stop -purge"
        self._run(cmd)
        time.sleep(2)

    def get_task_history(self, job_id):
        """Ritorna lo stato delle allocazioni per un dato job"""
        safe_job_id = f"{self.job_prefix}-{job_id}".replace("_", "-")
        cmd = f"nomad job allocs {safe_job_id}"
        res = self._run(cmd)
        return res.stdout.strip().split('\n')