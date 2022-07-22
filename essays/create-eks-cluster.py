from contextlib import contextmanager, ExitStack
import json
import logging
from uuid import uuid4
import time

from typing import List

import botocore
import boto3

logging.basicConfig(format='%(levelname)s - %(message)s', level=logging.INFO)

EKS_VPC_TEMPLATE ="https://amazon-eks.s3-us-west-2.amazonaws.com/cloudformation/2019-02-11/amazon-eks-vpc-sample.yaml"

EKS_ROLE_NAME = "EKSServiceManager"
EKS_CLUSTER_POLICY_ARN = "arn:aws:iam::aws:policy/AmazonEKSClusterPolicy"
EKS_SERVICE_POLICY_ARN = "arn:aws:iam::aws:policy/AmazonEKSServicePolicy"
EC2_CONTAINER_REGISTRY_READONLY_ARN = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"

EKS_VPC_STACK_NAME = "EKSVPCStack"

EKS_CLUSTER_NAME = "EKSRARGCluster"

@contextmanager
def eks_service_manager_role(iam):
    try:
        role = iam.get_role(RoleName=EKS_ROLE_NAME)
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] != "NoSuchEntity":
            raise e

        logging.info("Creating %s", EKS_ROLE_NAME)

        # This is an AWS role policy document.  Allows access for EKS.
        policy_doc = json.dumps({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Action": "sts:AssumeRole",
                    "Effect": "Allow",
                    "Principal": {
                        "Service": "eks.amazonaws.com"
                    }
                }
            ]
        })

        # Create role.
        role = iam.create_role(
            RoleName=EKS_ROLE_NAME,
            AssumeRolePolicyDocument=policy_doc,
            Description="Role providing access to EKS resources from EKS"
        )

        logging.info("Created %s role", EKS_ROLE_NAME)

        try:
            # Add policies allowing access to EKS API.
            iam.attach_role_policy(
                RoleName=EKS_ROLE_NAME,
                PolicyArn=EKS_CLUSTER_POLICY_ARN,
            )

            logging.info("Attached %s policy to %s", EKS_CLUSTER_POLICY_ARN, EKS_ROLE_NAME)

            iam.attach_role_policy(
                RoleName=EKS_ROLE_NAME,
                PolicyArn=EKS_SERVICE_POLICY_ARN
            )

            logging.info("Attached %s policy to %s", EKS_SERVICE_POLICY_ARN, EKS_ROLE_NAME)

            iam.attach_role_policy(
                RoleName=EKS_ROLE_NAME,
                PolicyArn=EC2_CONTAINER_REGISTRY_READONLY_ARN
            )

            logging.info("Attached %s policy to %s", EC2_CONTAINER_REGISTRY_READONLY_ARN, EKS_ROLE_NAME)
        except botocore.exceptions.ClientError as e:
            iam.delete_role(RoleName=EKS_ROLE_NAME)
            raise e
    else:
        logging.info("Found previously created Role %s", EKS_ROLE_NAME)

    yield role["Role"]["Arn"]

@contextmanager
def vpc_factory(cf):
    try:
        response = cf.describe_stacks(StackName=EKS_VPC_STACK_NAME)
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] != "ValidationError":
            raise e

        cf.create_stack(StackName=EKS_VPC_STACK_NAME, TemplateURL=EKS_VPC_TEMPLATE)
        waiter = cf.get_waiter("stack_create_complete")
        waiter.wait(StackName=EKS_VPC_STACK_NAME)
        logging.info("Created CloudFormation Stack %s", EKS_VPC_STACK_NAME)
        response = cf.describe_stacks(StackName=EKS_VPC_STACK_NAME)
    else:
        logging.info("Found previously created CloudFormation Stack %s", EKS_VPC_STACK_NAME)

    stacks = response["Stacks"]
    assert len(stacks) == 1
    yield {v["OutputKey"]: v["OutputValue"] for v in stacks[0]["Outputs"]}


@contextmanager
def eks_cluster_factory(eks, role_arn: str, vpc_subnet_ids: List[str], vpc_security_groups: str):
    try:
        cluster = eks.describe_cluster(name=EKS_CLUSTER_NAME)
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise e

        response = eks.create_cluster(
            name=EKS_CLUSTER_NAME,
            roleArn=role_arn,
            resourcesVpcConfig={
                "subnetIds": vpc_subnet_ids,
                "securityGroupIds": [vpc_security_groups],
            })

        waiter = eks.get_waiter("cluster_active")
        waiter.wait(name=EKS_CLUSTER_NAME)
        logging.info("Created EKS Cluster %s", EKS_CLUSTER_NAME)

        cluster = eks.describe_cluster(name=EKS_CLUSTER_NAME)
    else:
        logging.info("Found previously created EKS Cluster %s", EKS_CLUSTER_NAME)

    yield cluster




def run():
    cf = boto3.client("cloudformation")
    eks = boto3.client("eks")
    iam = boto3.client("iam")

    with ExitStack() as stack:
        role_arn = stack.enter_context(eks_service_manager_role(iam))
        vpc_outputs = stack.enter_context(vpc_factory(cf))
        cluster = stack.enter_context(
            eks_cluster_factory(
                eks,
                role_arn,
                vpc_outputs["SubnetIds"].split(","),
                vpc_outputs["SecurityGroups"]
            )
        )

        from pprint import pprint
        pprint(cluster)


if __name__ == "__main__":
    run()
