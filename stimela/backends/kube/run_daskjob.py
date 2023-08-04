from contextlib import contextmanager
from kubernetes import client, config, watch
from kubernetes.client.rest import ApiException
from pprint import pprint
import yaml
import time

from stimela.backends.kube.daskjob import render, create_parser, split_args


    # Define custom resource details
DASKJOB_GROUP = 'kubernetes.dask.org'  # the CRD's group name
DASKJOB_VERSION = 'v1'  # the CRD's version
DASKJOB_PLURAL = 'daskjobs'  # the plural name of the CRD


def find_daskjob_components(daskjob, namespace):
    # pprint(daskjob)
    time.sleep(2)
    v1 = client.CoreV1Api()
    job_name = daskjob["metadata"]["name"]

    runner_pod = v1.list_namespaced_pod(namespace=namespace,
                                        label_selector=f"dask.org/cluster-name={job_name},dask.org/component=job-runner")
    runner_pod_name = runner_pod.items[0].metadata.name

    scheduler_pod = v1.list_namespaced_pod(namespace=namespace, label_selector=f"dask.org/cluster-name={job_name},dask.org/component=scheduler")
    scheduler_pod_name = scheduler_pod.items[0].metadata.name

    worker_pods = v1.list_namespaced_pod(namespace=namespace, label_selector=f"dask.org/cluster-name={job_name},dask.org/component=worker")
    worker_pod_names = [wp.metadata.name for wp in worker_pods.items]


    w = watch.Watch()
    for e in w.stream(v1.read_namespaced_pod_log, name=runner_pod_name, namespace=namespace):
        print(f"Event {e}")
    # response = v1.read_namespaced_pod_log(name=runner_pod_name, namespace=namespace)

    # pprint(response)

    # print(runner_pod_name)
    # print(scheduler_pod_name)
    # print(worker_pod_names)



def daskjob_factory(job_name="sjp", image="ghcr.io/dask/dask", namespace="rarg-test-compute"):
    # CustomObjectsApi is used to handle custom resources
    api_instance = client.CustomObjectsApi()


    arg_list = [
        "--nworkers", "2",
        "--job-name", job_name,
        "--namespace", namespace,
        "--image", image,
        "--cpu-request", "1",
        "--mem-request", "8Gi",
        "--cpu-limit", "1",
        "--mem-limit", "8Gi",
        "--",
        # Job arguments (not scheduler or worker) go here
        "python -c 'from distributed import Client; client = Client(); while True: continue'"
    ]
    args, cmdline = split_args(arg_list)
    args = create_parser().parse_args(args)
    args.cmdline = cmdline
    args.labels = ()
    args.threads_per_worker = 1
    body = render(args)[0] # DaskJob CRD is first in list, Volume stuff + Configmaps follow

    # Create an instance of the CRD
    api_instance.create_namespaced_custom_object(DASKJOB_GROUP, DASKJOB_VERSION, namespace, DASKJOB_PLURAL, body)

    # List all instances of the CRD
    try:
        api_response = api_instance.list_namespaced_custom_object(DASKJOB_GROUP, DASKJOB_VERSION, namespace, DASKJOB_PLURAL)
    except ApiException as e:
        print("Exception when calling CustomObjectsApi->list_namespaced_custom_object: %s\n" % e)
    else:
        #api_response = [api_response] if isinstance(api_response, dict) else api_response

        for instance in api_response["items"]:
            if instance["metadata"]["name"] == job_name:
                find_daskjob_components(instance, namespace)

if __name__ == '__main__':
    # Load configuration
    config.load_kube_config()

    JOBNAME="sjp-test"
    NAMESPACE = "rarg-test-compute"

    try:
        daskjob_factory(job_name=JOBNAME, namespace=NAMESPACE)
    finally:
        api_instance = client.CustomObjectsApi()

        # Delete the instance of the CRD
        api_instance.delete_namespaced_custom_object(DASKJOB_GROUP, DASKJOB_VERSION, NAMESPACE, DASKJOB_PLURAL, JOBNAME)
