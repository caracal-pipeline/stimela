import json
import logging
import os
import os.path

from typing import List

import botocore
import boto3
import yaml

logging.basicConfig(format="%(levelname)s - %(message)s", level=logging.INFO)


class EKSClusterBuilder:
    EKS_VPC_TEMPLATE = "https://amazon-eks.s3-us-west-2.amazonaws.com/cloudformation/2019-02-11/amazon-eks-vpc-sample.yaml"
    # EKS_NODE_GROUP_TEMPLATE = "https://amazon-eks.s3-us-west-2.amazonaws.com/cloudformation/2019-02-11/amazon-eks-nodegroup.yaml"
    EKS_NODE_GROUP_TEMPLATE = "https://s3.us-west-2.amazonaws.com/amazon-eks/cloudformation/2020-10-29/amazon-eks-nodegroup.yaml"
    EKS_CLUSTER_POLICY_ARN = "arn:aws:iam::aws:policy/AmazonEKSClusterPolicy"
    EKS_SERVICE_POLICY_ARN = "arn:aws:iam::aws:policy/AmazonEKSServicePolicy"
    EC2_CONTAINER_REGISTRY_READONLY_ARN = (
        "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
    )

    INSTANCE_TYPE = "t2.medium"
    NODE_IMAGE_IDS = {"af-south-1": "ami-0cfa14b35e42b97fe"}

    def __init__(self):
        self._session = boto3.Session()
        self._keypairs = {}
        self._cluster = {}
        self._aws_region = "af-south-1"
        self._eks_role_name = "EKSServiceManager"
        self._eks_vpc_stack_name = "EKSVPCStack"
        self._eks_cluster_name = "EKSRARGCluster"
        self._eks_worker_stack_name = f"{self._eks_cluster_name}Workers"
        self._node_group_min = 3
        self._node_group_max = 5
        self._config_file = "kubeconfig.yaml"
        self._keypair_name = f"{self._eks_cluster_name}Key"
        self._secret_file = "secret.pem"

    def build(self):
        role = self.build_service_manager_role()
        vpc = self.build_vpc_manager()
        self._keypairs = self.build_keypairs()
        self._cluster = cluster = self.build_eks_cluster(
            role["Arn"], vpc["SubnetIds"].split(","), vpc["SecurityGroups"]
        )
        self._worker_stack = self.build_worker_stack(
            vpc["VpcId"], vpc["SubnetIds"].split(","), vpc["SecurityGroups"]
        )

    def build_service_manager_role(self):
        iam = self._session.client("iam")

        try:
            role = iam.get_role(RoleName=self._eks_role_name)
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] != "NoSuchEntity":
                raise e

            logging.info("Creating %s", self._eks_role_name)

            # This is an AWS role policy document.  Allows access for EKS.
            policy_doc = json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Action": "sts:AssumeRole",
                            "Effect": "Allow",
                            "Principal": {"Service": "eks.amazonaws.com"},
                        }
                    ],
                }
            )

            # Create role.
            role = iam.create_role(
                RoleName=self._eks_role_name,
                AssumeRolePolicyDocument=policy_doc,
                Description="Role providing access to EKS resources from EKS",
            )

            logging.info("Created %s role", self._eks_role_name)

            try:
                # Add policies allowing access to EKS API.
                iam.attach_role_policy(
                    RoleName=self._eks_role_name,
                    PolicyArn=self.EKS_CLUSTER_POLICY_ARN,
                )

                logging.info(
                    "Attached %s policy to %s",
                    self.EKS_CLUSTER_POLICY_ARN,
                    self._eks_role_name,
                )

                iam.attach_role_policy(
                    RoleName=self._eks_role_name, PolicyArn=self.EKS_SERVICE_POLICY_ARN
                )

                logging.info(
                    "Attached %s policy to %s",
                    self.EKS_SERVICE_POLICY_ARN,
                    self._eks_role_name,
                )

                iam.attach_role_policy(
                    RoleName=self._eks_role_name,
                    PolicyArn=self.EC2_CONTAINER_REGISTRY_READONLY_ARN,
                )

                logging.info(
                    "Attached %s policy to %s",
                    self.EC2_CONTAINER_REGISTRY_READONLY_ARN,
                    self._eks_role_name,
                )
            except botocore.exceptions.ClientError as e:
                iam.delete_role(RoleName=self._eks_role_name)
                raise e
        else:
            logging.info("Found previously created Role %s", self._eks_role_name)

        return role["Role"]

    def build_vpc_manager(self):
        cf = self._session.client("cloudformation")

        try:
            response = cf.describe_stacks(StackName=self._eks_vpc_stack_name)
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] != "ValidationError":
                raise e

            cf.create_stack(
                StackName=self._eks_vpc_stack_name, TemplateURL=self.EKS_VPC_TEMPLATE
            )
            waiter = cf.get_waiter("stack_create_complete")
            waiter.wait(StackName=self._eks_vpc_stack_name)
            logging.info("Created CloudFormation Stack %s", self._eks_vpc_stack_name)
            response = cf.describe_stacks(StackName=self._eks_vpc_stack_name)
        else:
            logging.info(
                "Found previously created CloudFormation Stack %s",
                self._eks_vpc_stack_name,
            )

        stacks = response["Stacks"]
        assert len(stacks) == 1
        return {v["OutputKey"]: v["OutputValue"] for v in stacks[0]["Outputs"]}

    def build_eks_cluster(
        self, role_arn: str, vpc_subnet_ids: List[str], vpc_security_groups: str
    ):
        eks = self._session.client("eks")

        try:
            cluster = eks.describe_cluster(name=self._eks_cluster_name)
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] != "ResourceNotFoundException":
                raise e

            response = eks.create_cluster(
                name=self._eks_cluster_name,
                roleArn=role_arn,
                resourcesVpcConfig={
                    "subnetIds": vpc_subnet_ids,
                    "securityGroupIds": [vpc_security_groups],
                },
            )

            waiter = eks.get_waiter("cluster_active")
            waiter.wait(name=self._eks_cluster_name)
            logging.info("Created EKS Cluster %s", self._eks_cluster_name)

            cluster = eks.describe_cluster(name=self._eks_cluster_name)
        else:
            logging.info(
                "Found previously created EKS Cluster %s", self._eks_cluster_name
            )

        return cluster["cluster"]

    def build_worker_stack(
        self, vpc_id: str, vpc_subnet_ids: List[str], vpc_security_groups: str
    ):
        cf = self._session.client("cloudformation")

        try:
            response = cf.describe_stacks(StackName=self._eks_worker_stack_name)
            logging.info(
                "Found existing EKS Worker Stack %s", self._eks_worker_stack_name
            )
            return response
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] != "ValidationError":
                raise e

            try:
                node_image_id = self.NODE_IMAGE_IDS[self._aws_region]
            except KeyError:
                raise ValueError(
                    f"No Node Image ID was found for " f"the {self._aws_region} region"
                )

            parameters = [
                ("ClusterName", self._eks_cluster_name),
                ("ClusterControlPlaneSecurityGroup", vpc_security_groups),
                ("NodeGroupName", f"{self._eks_cluster_name}WorkerGroup"),
                ("NodeAutoScalingGroupMinSize", str(self._node_group_min)),
                ("NodeAutoScalingGroupMaxSize", str(self._node_group_max)),
                ("NodeAutoScalingGroupDesiredCapacity", str(self._node_group_min)),
                ("NodeInstanceType", self.INSTANCE_TYPE),
                ("NodeImageId", node_image_id),
                ("KeyName", self._keypair_name),
                ("VpcId", vpc_id),
                ("Subnets", ",".join(vpc_subnet_ids))
            ]

            response = cf.create_stack(
                StackName=self._eks_worker_stack_name,
                TemplateURL=self.EKS_NODE_GROUP_TEMPLATE,
                Capabilities=["CAPABILITY_IAM"],
                Parameters=[{"ParameterKey": k, "ParameterValue": v} for k, v in parameters],
                TimeoutInMinutes=15,
                # OnFailure="DELETE",
            )

            if response is None or "StackId" not in response:
                raise ValueError(
                    f"Unable to create a Worker Stack {self._eks_worker_stack_name}"
                )

            # This is a water which waits for the stack deployment to complete.
            waiter = cf.get_waiter("stack_create_complete")
            response = waiter.wait(StackName=self._eks_worker_stack_name)
            logging.info("Created EKS Worker Stack %s", self._eks_worker_stack_name)
            return cf.describe_stacks(StackName=self._eks_worker_stack_name)

    def build_keypairs(self):
        ec2 = self._session.client("ec2")

        try:
            key_pairs = ec2.describe_key_pairs(KeyNames=[self._keypair_name])
            logging.info("Found existing keypair %s", self._keypair_name)
            return key_pairs["KeyPairs"][0]
        except:
            try:
                return ec2.create_key_pair(KeyName=self._keypair_name, KeyType="ed25519")
            finally:
                logging.info("Created keypair %s", self._keypair_name)

    def dump_secret(self):
        try:
            private_key = self._keypairs["KeyMaterial"]
        except KeyError:
            logging.warning(
                "Existing keypair %s found. "
                "Not dumping %s as the private key "
                "is only available on keypair creation.",
                self._keypair_name,
                self._secret_file,
            )
            return

        os.unlink(self._secret_file)

        with open(self._secret_file, "w") as f:
            f.write(private_key)

        os.chmod(self._secret_file, 0o400)
        logging.info(
            "Wrote %s keypair private key to %s",
            self._keypair_name,
            self._secret_file,
        )

    def dump_kubeconfig(self):
        try:
            certificate = self._cluster["certificateAuthority"]["data"]
            endpoint = self._cluster["endpoint"]
        except KeyError:
            raise RuntimeError("EKSClusterBuilder.build() must first be called")

        cluster_config = {
            "apiVersion": "v1",
            "kind": "Config",
            "clusters": [
                {
                    "cluster": {
                        "server": str(endpoint),
                        "certificate-authority-data": str(certificate),
                    },
                    "name": "kubernetes",
                }
            ],
            "contexts": [
                {"context": {"cluster": "kubernetes", "user": "aws"}, "name": "aws"}
            ],
            "current-context": "aws",
            "preferences": {},
            "users": [
                {
                    "name": "aws",
                    "user": {
                        "exec": {
                            "apiVersion": "client.authentication.k8s.io/v1beta1",
                            "command": "aws",
                            "args": [
                                "--region",
                                self._aws_region,
                                "eks",
                                "get-token",
                                "--cluster-name",
                                self._eks_cluster_name,
                            ],
                        }
                    },
                }
            ],
        }

        with open(self._config_file, "w") as f:
            yaml.safe_dump(cluster_config, f)


def run():
    builder = EKSClusterBuilder()
    builder.build()
    builder.dump_kubeconfig()
    builder.dump_secret()


if __name__ == "__main__":
    run()
