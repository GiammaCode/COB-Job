import subprocess
import time
import json
import collections
import os


class K8sDriver:
    def __init__(self, namespace="cob-job", image="192.168.15.9:5000/cob-job-worker:latest"):
        self.namespace = namespace
        self.image = image
        # Percorso sul nodo HOST dove risiedono i risultati (NFS mount point)
        self.host_path = "/srv/nfs/cob_results"
        # Percorso dentro il CONTAINER dove scrive il worker
        self.container_mount = "/mnt/results"

    def _run(self, cmd):
        return subprocess.run(cmd, shell=True, capture_output=True, text=True)

    def submit_job(self, job_id, job_type="cpu", duration=10, constraints=None, cpu_reservation=None,
                   restart_policy="Never", command=None):
        # I nomi in K8s devono essere minuscoli e senza caratteri strani
        safe_job_id = str(job_id).lower().replace("_", "-")
        job_name = f"{self.namespace}-{safe_job_id}"

        # --- 1. Gestione Comando Personalizzato (per Crash Test) ---
        container_cmd = None
        if command:
            # Se arriva come stringa tipo 'sh -c "..."', lo convertiamo in lista per K8s
            if isinstance(command, str) and "sh -c" in command:
                # Pulizia base per comandi sh -c
                clean_cmd = command.replace("sh -c ", "").strip('"').strip("'")
                container_cmd = ["/bin/sh", "-c", clean_cmd]
            elif isinstance(command, list):
                container_cmd = command
            else:
                container_cmd = command.split()

        # --- 2. Logica Recovery (BackoffLimit) ---
        # Se restart_policy NON è "none" o "never" (quindi è "on-failure" o simile),
        # alziamo il backoffLimit per permettere a K8s di riprovare.
        limit = 0
        k8s_restart_policy = "Never"  # Default: crea un NUOVO pod se muore

        if restart_policy.lower() not in ["none", "never"]:
            limit = 4  # Permetti fino a 4 tentativi (Pod Replacement)

        # Se l'utente specifica esplicitamente OnFailure, lo passiamo (riavvio container locale)
        # Ma per i test di visualizzazione è meglio Never + BackoffLimit > 0
        if restart_policy.lower() == "on-failure":
            k8s_restart_policy = "OnFailure"

        # --- 3. Costruzione Manifest JSON ---
        job_manifest = {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": job_name,
                "namespace": self.namespace,
                "labels": {"app": "cob-job", "job_id": str(job_id)}
            },
            "spec": {
                "backoffLimit": limit,  # 0 = Fail Fast, 4 = Recovery Enabled
                "ttlSecondsAfterFinished": 600,  # Pulizia automatica dopo 10 min
                "template": {
                    "metadata": {
                        "labels": {"app": "cob-job", "job_id": str(job_id)}
                    },
                    "spec": {
                        "restartPolicy": k8s_restart_policy,
                        "containers": [{
                            "name": "worker",
                            "image": self.image,
                            "command": container_cmd,  # Inietta il comando qui
                            "imagePullPolicy": "Always",
                            "env": [
                                {"name": "JOB_ID", "value": str(job_id)},
                                {"name": "JOB_TYPE", "value": str(job_type)},
                                {"name": "DURATION", "value": str(duration)},
                                {"name": "OUTPUT_DIR", "value": self.container_mount}
                            ],
                            "volumeMounts": [{
                                "name": "results-vol",
                                "mountPath": self.container_mount
                            }]
                        }],
                        "volumes": [{
                            "name": "results-vol",
                            "hostPath": {
                                "path": self.host_path,
                                "type": "Directory"
                            }
                        }]
                    }
                }
            }
        }

        # Risorse
        if cpu_reservation:
            job_manifest["spec"]["template"]["spec"]["containers"][0]["resources"] = {
                "requests": {"cpu": str(cpu_reservation)}
            }

        # Node Selector (Constraints)
        if constraints:
            job_manifest["spec"]["template"]["spec"]["nodeSelector"] = constraints

        # Apply via stdin
        manifest_str = json.dumps(job_manifest)
        cmd = f"kubectl apply -f - -n {self.namespace}"

        res = subprocess.run(cmd, input=manifest_str, shell=True, text=True, capture_output=True)

        if res.returncode != 0:
            print(f"[K8S] Error launching {job_id}: {res.stderr}")
            return False
        return True

    def get_node_distribution(self):
        """Ritorna {nome_nodo: numero_pod_running}"""
        cmd = (f"kubectl get pods -n {self.namespace} "
               f"-l app=cob-job "
               f"--field-selector=status.phase=Running "
               f"-o jsonpath='{{.items[*].spec.nodeName}}'")
        res = self._run(cmd)
        nodes = res.stdout.strip().split()
        return dict(collections.Counter(nodes))

    def get_pod_status_counts(self):
        """Conta gli stati dei pod (Running, Pending, Error, etc)"""
        cmd = (f"kubectl get pods -n {self.namespace} "
               f"-l app=cob-job "
               f"--no-headers "
               f"-o custom-columns=STATUS:.status.phase")
        res = self._run(cmd)
        if res.returncode != 0: return {}
        # Filtra linee vuote
        statuses = [s for s in res.stdout.strip().split('\n') if s]
        return dict(collections.Counter(statuses))

    def clean_jobs(self):
        print(f"[K8S] Cleaning jobs in namespace {self.namespace}...")
        cmd =