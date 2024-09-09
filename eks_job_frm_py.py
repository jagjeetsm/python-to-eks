import boto3
import base64
import json
from kubernetes import client, config, watch
import time
from eks_token import get_token

REGION = "us-east-1"
CLUSTER_NAME = "XXXX"
NAMESPACE = "XXXXX"


def get_eks_credentials(cluster_name):
    """Get EKS token, endpoint, and certificate authority data."""
    session = boto3.Session()
    eks_client = session.client("eks", region_name=REGION)

    # Get cluster info (API server endpoint and certificate authority data)
    cluster_info = eks_client.describe_cluster(name=cluster_name)["cluster"]
    endpoint = cluster_info["endpoint"]
    cert_authority_data = cluster_info["certificateAuthority"]["data"]

    # Get authentication token using the eks_token
    token = get_token(cluster_name=CLUSTER_NAME)['status']['token']

    return token, endpoint, cert_authority_data


def main():
    try:
        # Get the token, API server endpoint, and CA data
        token, endpoint, cert_authority_data = get_eks_credentials(CLUSTER_NAME)

        # Save the CA certificate in a temporary file
        with open('/tmp/ca.crt', 'w') as f:
            f.write(base64.b64decode(cert_authority_data).decode('utf-8'))

        # Create Kubernetes client configuration using the token and cluster details
        configuration = client.Configuration()
        configuration.host = endpoint
        configuration.verify_ssl = True
        configuration.ssl_ca_cert = '/tmp/ca.crt'
        configuration.api_key = {"authorization": f"Bearer {token}"}

        # Set the configuration for the Kubernetes client
        client.Configuration.set_default(configuration)

        # Create API instances for BatchV1Api (for managing Jobs) and CoreV1Api (for reading logs)
        batch_v1 = client.BatchV1Api()
        core_v1 = client.CoreV1Api()
        ts = time.time()
        job = "calculator-job-{}".format(ts)
        # Job definition with one container that calculates 5 + 12
        job = client.V1Job(
            metadata=client.V1ObjectMeta(name=job),
            spec=client.V1JobSpec(
                ttl_seconds_after_finished=30,  # Auto-delete 30 seconds after completion
                template=client.V1PodTemplateSpec(
                    metadata=client.V1ObjectMeta(name="calculator-pod"),
                    spec=client.V1PodSpec(
                        containers=[
                            client.V1Container(
                                name="calculator-container",
                                image="python:3.9",
                                command=["/bin/sh", "-c", 'python -c "print(5 + 12)" && sleep 10']
                            )
                        ],
                        restart_policy="Never"
                    )
                )
            )
        )

        # Create the job in the specified namespace
        response = batch_v1.create_namespaced_job(namespace=NAMESPACE, body=job)
        print(f"Job created. Name: {response.metadata.name}")

        # Wait for the job to complete
        job_name = response.metadata.name
        w = watch.Watch()

        print("Waiting for the job to complete...")
        job_completed = False
        while not job_completed:
            for event in w.stream(batch_v1.list_namespaced_job, namespace=NAMESPACE):
                job_status = event['object'].status
                if job_status.succeeded is not None and job_status.succeeded > 0:
                    job_completed = True
                    print("Job completed successfully.")
                    w.stop()
                time.sleep(2)

        # Get the pod created by the job
        job_pod_list = core_v1.list_namespaced_pod(namespace=NAMESPACE,
                                                   label_selector=f"job-name={response.metadata.name}")

        # Check if any pods are returned
        if not job_pod_list.items:
            print("No pods found for the job. It might have completed too quickly, or failed to create a pod.")
        else:
            pod_name = job_pod_list.items[0].metadata.name
            print(f"Fetching logs from pod: {pod_name}")

            # Fetch logs from the container
            log = core_v1.read_namespaced_pod_log(name=pod_name, container="calculator-container", namespace=NAMESPACE)

            # Print the log output
            print(f"Container output:\n{log}")


    except client.ApiException as e:
        print(f"API Exception: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()

