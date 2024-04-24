import argparse
import yaml

import os
import sys
from uuid import uuid4


def create_parser():
    p = argparse.ArgumentParser()
    p.add_argument("-j", "--job-name", default="job", help="Update the job name")
    p.add_argument(
        "-n",
        "--namespace",
        default="rarg-test-compute",
        help="The kubernetes namespace in which the job will be run",
    )
    p.add_argument(
        "-i",
        "--image",
        default="ghcr.io/dask/dask",
        help="Container image run by job, scheduler and worker pods",
    )
    p.add_argument(
        "-s",
        "--service-account",
        default="compute-runner",
        help="The kubernetes service account which will run the job",
    )
    p.add_argument(
        "--pull-policy",
        default="Always",
        help="imagePullPolicy setting for pods",
    )
    p.add_argument(
        "-f",
        "--mount-file",
        nargs="*",
        help=(
            "Configuration files that will be mounted into the pod. "
            "Can be of the form /path/to/file.yaml or "
            "/host/file.yaml:/container/file.yaml. "
            "If the first form is selected, file.yaml will be "
            "mounted at /mnt/file.yaml"
        ),
    )
    p.add_argument(
        "-v",
        "--volume",
        nargs="*",
        help=(
            "PersistentVolumeClaims that will be mounted into the pod."
            "Should be of the form pvc-name:mount_point"
        ),
    )
    p.add_argument(
        "-w", "--nworkers", default=1, type=int, help="Number of dask workers to launch"
    )
    p.add_argument(
        "-cr",
        "--cpu-request",
        default=1,
        type=float,
        help="Number of cpu's to request for the worker pods",
    )
    p.add_argument(
        "-mr",
        "--mem-request",
        default="1Gi",
        help="Amount of memory to request for the worker pods",
    )
    p.add_argument(
        "-cl",
        "--cpu-limit",
        default=1,
        type=float,
        help="cpu limit for the worker pods",
    )
    p.add_argument(
        "-ml", "--mem-limit", default="1Gi", help="memory limit for the worker pods"
    )

    return p


def split_args(args):
    try:
        i = args.index("--")
    except ValueError:
        return args, []
    else:
        return args[:i], args[i + 1 :]


def daskjob_template(args):
    labels = dict(args.labels)

    env_var = [{"name": k, "value": v} for k, v in args.environment_variables.items()]

    return {
        "apiVersion": "kubernetes.dask.org/v1",
        "kind": "DaskJob",
        "metadata": {"name": args.job_name, "namespace": args.namespace, "labels": labels },
        "spec": {
            "cluster": {
                "spec": {
                    "scheduler": {
                        "service": {
                            "ports": [
                                {
                                    "name": "tcp-comm",
                                    "port": 8786,
                                    "protocol": "TCP",
                                    "targetPort": "tcp-comm",
                                },
                                {
                                    "name": "http-dashboard",
                                    "port": 8787,
                                    "protocol": "TCP",
                                    "targetPort": "http-dashboard",
                                },
                            ],
                            "selector": {
                                "dask.org/cluster-name": args.job_name,
                                "dask.org/component": "scheduler",
                            },
                            "type": "ClusterIP",
                        },
                        "metadata": { "labels": labels },
                        "spec": {
                            "containers": [
                                {
                                    "args": ["dask-scheduler"],
                                    "env": [
                                        {
                                            "name": "SCHEDULER_ENV",
                                            "value": "hello-world",
                                        },
                                        *env_var
                                    ],
                                    "image": args.image,
                                    "imagePullPolicy": args.pull_policy,
                                    "livenessProbe": {
                                        "httpGet": {
                                            "path": "/health",
                                            "port": "http-dashboard",
                                        },
                                        "initialDelaySeconds": 15,
                                        "periodSeconds": 20,
                                    },
                                    "name": "scheduler",
                                    "ports": [
                                        {
                                            "containerPort": 8786,
                                            "name": "tcp-comm",
                                            "protocol": "TCP",
                                        },
                                        {
                                            "containerPort": 8787,
                                            "name": "http-dashboard",
                                            "protocol": "TCP",
                                        },
                                    ],
                                    "readinessProbe": {
                                        "httpGet": {
                                            "path": "/health",
                                            "port": "http-dashboard",
                                        },
                                        "initialDelaySeconds": 5,
                                        "periodSeconds": 10,
                                    },
                                }
                            ],
                        },
                    },
                    "worker": {
                        "replicas": args.nworkers,
                        "metadata": { "labels": labels },
                        "spec": {
                            "containers": [
                                {
                                    "args": [
                                        "dask-worker",
                                        "--name",
                                        "$(DASK_WORKER_NAME)",
                                        "--nworkers", '1',
                                        "--nthreads", str(args.threads_per_worker),
                                        "--memory-limit", str(args.memory_limit),
                                        "$(DASK_SCHEDULER_ADDRESS)",
                                    ],
                                    "env": [
                                        {
                                            "name": "WORKER_ENV",
                                            "value": "hello-world"
                                        },
                                        *env_var
                                    ],
                                    "image": args.image,
                                    "imagePullPolicy": args.pull_policy,
                                    "name": "worker",
                                }
                            ],
                        },
                    },
                }
            },
            "job": {
                "metadata": { "labels": labels},
                "spec": {
                    "containers": [
                        {
                            "image": args.image,
                            "imagePullPolicy": args.pull_policy,
                            "name": "job",
                        }
                    ],
                }
            },
        },
    }


def parse_mount_file(mount_file):
    bits = mount_file.split(":")

    if len(bits) == 2:
        host, container = bits
        _, key = os.path.split(host)
    elif len(bits) == 1:
        host = bits[0]
        _, key = os.path.split(host)
        container = f"/mnt/{key}"
    else:
        raise ValueError(
            f"mount_file should be of the form "
            f"'/mnt/filename' or "
            f"'/host/path/to/filename:/container/path/to/filename' "
            f"Got {mount_file}"
        )

    return host, container, key


def render(args):
    d = daskjob_template(args)
    segments = [d]

    job_spec = d["spec"]["job"]["spec"]
    worker_spec = d["spec"]["cluster"]["spec"]["worker"]["spec"]
    scheduler_spec = d["spec"]["cluster"]["spec"]["scheduler"]["spec"]

    if args.service_account:
        for entry in [job_spec, worker_spec, scheduler_spec]:
            entry["serviceAccountName"] = args.service_account
            entry["automountServiceAccountToken"] = True

    if args.mount_file:
        for i, mount in enumerate(args.mount_file):
            host_path, container_path, mount_key = parse_mount_file(mount)

            with open(host_path, "r") as f:
                payload = f.read()

            volume_name = f"config-map-volume-{i}"
            mount_name = f"file-mount-{uuid4().hex[:6]}"

            segments.append(
                {
                    "apiVersion": "v1",
                    "data": {mount_key: payload},
                    "kind": "ConfigMap",
                    "metadata": {
                        "name": mount_name,
                        "namespace": args.namespace,
                    },
                }
            )

            for entry in [job_spec, worker_spec, scheduler_spec]:
                volumes = entry.setdefault("volumes", [])

                for container in entry["containers"]:
                    volume_mounts = container.setdefault("volumeMounts", [])

                    volume_mounts.append(
                        {
                            "name": volume_name,
                            "mountPath": container_path,
                            "subPath": mount_key,
                        }
                    )
                    volumes.append(
                        {
                            "name": volume_name,
                            "configMap": {"name": mount_name},
                        }
                    )

    return segments


if __name__ == "__main__":
    args, cmdline = split_args(sys.argv[1:])
    args = create_parser().parse_args(args)
    args.cmdline = cmdline
    yaml.dump_all(render(args), sys.stdout, default_flow_style=False)
