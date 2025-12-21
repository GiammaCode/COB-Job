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

        # Risorse (CPU Request)
        resources_section = {}
        if cpu_reservation:
            resources_section = {
                "requests": {"cpu": str(cpu_reservation)}
            }

        # Constraints (Node Selector)
        # Swarm usa: node.labels.type == gpu
        # K8s usa: nodeSelector: {type: gpu}
        node_selector = {}
        if constraints:
            node_selector = constraints

        # Costruzione Manifest JSON
        job_manifest = {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": job_name,
                "namespace": self.namespace,
                "labels": {"app": "cob-job", "job_id": str(job_id)}
            },
            "spec": {
                "backoffLimit": 0,  # Non riprovare se fallisce (comportamento 'none')
                "ttlSecondsAfterFinished": 600,  # Pulizia automatica dopo 10 min
                "template": {
                    "metadata": {
                        "labels": {"app": "cob-job", "job_id": str(job_id)}
                    },
                    "spec": {
                        "restartPolicy": "Never",
                        "containers": [{
                            "name": "worker",
                            "image": self.image,
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
                            }],
                            "resources": resources_section
                        }],
                        "volumes": [{
                            "name": "results-vol",
                            "hostPath": {
                                "path": self.host_path,
                                "type": "Directory"  # Fallisce se la dir non esiste sul nodo
                            }
                        }]
                    }
                }
            }
        }

        # Aggiunta Node Selector se presente
        if node_selector:
            job_manifest["spec"]["template"]["spec"]["nodeSelector"] = node_selector

        # Apply via stdin
        manifest_str = json.dumps(job_manifest)
        cmd = f"kubectl apply -f - -n {self.namespace}"

        # Eseguiamo passando il JSON allo standard input di kubectl
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

    def clean_jobs(self):
        print(f"[K8S] Cleaning jobs in namespace {self.namespace}...")
        # Cancella tutti i job creati da questo benchmark
        cmd = f"kubectl delete jobs -l app=cob-job -n {self.namespace} --wait=false"
        self._run(cmd)
        # Per sicurezza puliamo anche i pod orfani (anche se delete job dovrebbe farlo)
        time.sleep(1)

    def get_task_history(self, job_id):
        # Filtriamo per label job_id
        cmd = f"kubectl get pods -n {self.namespace} -l job_id={job_id} --no-headers"
        res = self._run(cmd)
        # Ritorna le righe grezze (es. 'pod-xyz  0/1  Error ...')
        return res.stdout.strip().split('\n')

