from kubernetes import client, config
from kubernetes.client.rest import ApiException
import yaml

from stimela.backends.kube.daskjob import render, create_parser, split_args


def daskjob_factory(image="ghcr.io/dask/dask", namespace="rarg-test-compute"):
    # Load configuration
    config.load_kube_config()

    # CustomObjectsApi is used to handle custom resources
    api_instance = client.CustomObjectsApi()

    # Define custom resource details
    group = 'kubernetes.dask.org'  # the CRD's group name
    version = 'v1'  # the CRD's version
    plural = 'daskjobs'  # the plural name of the CRD

    arg_list = [
        "--namespace", namespace,
        "--image", image,
        "--cpu-request", "8",
        "--mem-request", "8Gi",
        "--cpu-limit", "8",
        "--mem-limit", "8Gi",
        "--",
        # Job arguments (not scheduler or worker) go here
        "goquartical",
        # whatever the commandline option pointing to scheduler address is
        "--address", "$(DASK_SCHEDULER_ADDRESS)",
    ]
    args, cmdline = split_args(arg_list)
    args = create_parser().parse_args(args)
    args.cmdline = cmdline
    body = render(args)[0] # DaskJob CRD is first in list, Volume stuff + Configmaps follow

    # Body of the custom resource
    # body = {
    #     "apiVersion": "kubernetes.dask.org/v1",
    #     "kind": "DaskJob",
    #     "metadata": {
    #         "name": "my-example-crd",
    #         "namespace": namespace,
    #     },
    #     "spec": yaml.safe_load(body)
    # }

    # Create an instance of the CRD
    api_instance.create_namespaced_custom_object(group, version, namespace, plural, body)

    # List all instances of the CRD
    try:
        api_response = api_instance.list_namespaced_custom_object(group, version, namespace, plural)
        print(api_response)
    except ApiException as e:
        print("Exception when calling CustomObjectsApi->list_namespaced_custom_object: %s\n" % e)

    # Delete the instance of the CRD
    api_instance.delete_namespaced_custom_object(group, version, namespace, plural, args.job_name)
                                                 #, client.V1DeleteOptions())

if __name__ == '__main__':
    daskjob_factory()
