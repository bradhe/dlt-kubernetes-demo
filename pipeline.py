import dlt
from dlt.sources.helpers import requests
from kubernetes import client, config

@dlt.source
def kubernetes_source():
    return kubernetes_resource()

@dlt.resource(primary_key="name", write_disposition="merge")
def kubernetes_resource():
    config.load_kube_config()

    v1 = client.CoreV1Api()
    ret = v1.list_pod_for_all_namespaces(watch=False)

    for i in ret.items:
        name = i.metadata.name
        namespace = i.metadata.namespace

        yield {
            'id': "{namespace}:{name}".format(namespace=namespace, name=name),
            'pod_ip': i.status.pod_ip,
            'status': i.status.phase,
            'namespace': i.metadata.namespace,
            'name': i.metadata.name,
        }

if __name__ == "__main__":
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(
        pipeline_name='kubernetes',
        destination='duckdb',
        dataset_name='kubernetes_data',
    )

    # run the pipeline with your parameters
    load_info = pipeline.run(kubernetes_source())

    # pretty print the information on data that was loaded
    print(load_info)
