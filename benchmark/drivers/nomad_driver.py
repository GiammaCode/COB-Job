import subprocess
import time
import json
import collections
import os

class NomadDriver:
    def __init__(self, job_prefix="cob-job", image="192.168.15.9:5000/cob-job-worker:latest"):
        self.job_prefix = job_prefix
        self.image = image

        self.host_path = "/srv/nfs/cob_results"
        self.container_mount = "/mnt/results"
        self.datacenters = ["dc1"]

    def _run(self, cmd, input_str=None):
        return subprocess.run(cmd, input=input_str, shell=True, capture_output=True, text=True)

    def submit_job(self, job_id, job_type="cpu", duration=10, constraints=None, cpu_reservation=None,
                   restart_policy="none", command=None):
        # Nomad ID non accetta underscore in certi contesti, meglio usare trattini
        safe_job_id = f"{self.job_prefix}-{job_id}".replace("_", "-")

        # 1. Configurazione Restart Policy
        # In Nomad "batch" job, se restart è 'fail', il job fallisce e basta.
        attempts = 0
        if restart_policy.lower() not in ["none", "never"]:
            attempts = 2  # Consenti qualche riavvio se richiesto (es. recovery test)

        restart_stanza = {
            "Interval": 300000000000,  # 5 minuti in ns
            "Attempts": attempts,
            "Mode": "fail"
        }

        # 2. Configurazione Risorse CPU
        # Convertiamo la reservation (es "1.0") in MHz.
        # Assumiamo arbitrariamente che 1.0 CPU ~= 2000 MHz per questo ambiente virtuale
        cpu_mhz = 100
        if cpu_reservation:
            try:
                cpu_mhz = int(float(cpu_reservation) * 2000)
            except:
                pass

        # 3. Configurazione Constraints
        # Mappiamo constraints={"type": "gpu"} in Nomad constraints
        # Assumiamo che i nodi abbiano meta-tag o attributi.
        # Esempio: "meta.type" = "gpu"
        nomad_constraints = []
        if constraints:
            for k, v in constraints.items():
                # Cerchiamo nei meta tags del nodo (es. meta.type) o attributi diretti
                # Se usi node.labels di docker come meta in nomad client config
                nomad_constraints.append({
                    "LTarget": "${meta." + k + "}",
                    "RTarget": v,
                    "Operand": "="
                })

        # 4. Configurazione Comando Custom (es. per crash test)
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
            # Se il comando è una stringa complessa tipo 'sh -c "..."',
            # Nomad/Docker driver preferisce command + args
            if "sh -c" in command:
                clean_cmd = command.replace("sh -c ", "").strip('"').strip("'")
                docker_config["command"] = "/bin/sh"
                docker_config["args"] = ["-c", clean_cmd]
            else:
                parts = command.split()
                docker_config["command"] = parts[0]
                docker_config["args"] = parts[1:]

        # 5. Costruzione Job Spec JSON
        job_spec = {
            "Job": {
                "ID": safe_job_id,
                "Name": safe_job_id,
                "Type": "batch",
                "Datacenters": self.datacenters,
                "TaskGroups": [{
                    "Name": "worker-group",
                    "Count": 1,
                    "RestartPolicy": restart_stanza,
                    "Constraints": nomad_constraints,
                    "Tasks": [{
                        "Name": "worker",
                        "Driver": "docker",
                        "Config": docker_config,
                        "Env": {
                            "JOB_ID": str(job_id),
                            "JOB_TYPE": str(job_type),
                            "DURATION": str(duration),
                            "OUTPUT_DIR": self.container_mount
                        },
                        "Resources": {
                            "CPU": cpu_mhz,
                            "MemoryMB": 256
                        }
                    }]
                }]
            }
        }

        # Invio a Nomad via CLI
        json_str = json.dumps(job_spec)
        # Usa 'nomad job run -' per passare il json da stdin
        cmd = "nomad job run -detach -"
        res = self._run(cmd, input_str=json_str)

        if res.returncode != 0:
            print(f"[NOMAD] Error launching {job_id}: {res.stderr}")
            return False
        return True

    def get_node_distribution(self):
        """Ritorna {nome_nodo: numero_allocazioni_running}"""
        # Recupera tutte le allocazioni per i job che iniziano con il prefisso
        cmd = f"nomad job status -short | grep {self.job_prefix} | awk '{{print $1}}'"
        res = self._run(cmd)
        job_ids = res.stdout.strip().split('\n')
        job_ids = [j for j in job_ids if j]

        node_counts = collections.defaultdict(int)

        for jid in job_ids:
            # Per ogni job, prendi l'ID del nodo dell'allocazione running
            # Nota: questo è lento se hai 100 job, ma per il benchmark va bene
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
        # Ferma ed elimina (purge) i job
        cmd = f"nomad job status -short | grep {self.job_prefix} | awk '{{print $1}}' | xargs -r nomad job stop -purge"
        self._run(cmd)
        time.sleep(2)

    def get_task_history(self, job_id):
        """Ritorna lo stato delle allocazioni per un dato job"""
        safe_job_id = f"{self.job_prefix}-{job_id}".replace("_", "-")
        # Ritorna righe grezze per parsing compatibile con i test esistenti
        cmd = f"nomad job allocs {safe_job_id}"
        res = self._run(cmd)
        # L'output contiene righe tipo: "ID ... Node Name ... Status"
        # Mappiamo 'failed' -> 'Failed' o 'Error' per compatibilità con recovery.py
        output = res.stdout
        # Nomad usa 'failed' (lowercase), K8s usa 'Error', Swarm 'Failed'.
        # Adattiamo l'output al volo se necessario o lasciamo che il test legga le stringhe.
        return output.split('\n')
