import boto3
import configparser
import json
import logging


def create_dwh_role(role_name: str, region: str, key: str, secret: str) -> str:
    """
    Creates an IAM role for Redshift to be able to access AWS services and to have S3 Read permissions

    :param role_name: iam role name
    :param region: region name
    :param key: access key id
    :param secret: secret access key
    :return: role Arn
    """
    iam = boto3.client(
        "iam",
        region_name=region,
        aws_access_key_id=key,
        aws_secret_access_key=secret,
    )

    try:
        iam.create_role(
            Path='/',
            RoleName=role_name,
            Description="Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps({
                'Statement': [{
                    'Action': 'sts:AssumeRole',
                    'Effect': 'Allow',
                    'Principal': {'Service': 'redshift.amazonaws.com'}
                }],
                'Version': '2012-10-17'
            }),
        )
    except Exception as e:
        logging.error("role creation failed", exc_info=e)

    iam.attach_role_policy(
        RoleName=role_name,
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
    )

    return iam.get_role(RoleName=role_name)['Role']['Arn']


def create_redshift_cluster(
        cluster_id: str,
        cluster_type: str,
        node_type: str,
        num_nodes: int,
        db_name: str,
        db_user: str,
        db_pass: str,
        role_arn: str,
        region: str,
        key: str,
        secret: str,
):
    """
    Creates a Redshift Cluster with the specified configuration and attaches an IAM role to it

    :param cluster_id: cluster identifier
    :param cluster_type: cluster type
    :param node_type: node type
    :param num_nodes: number of nodes
    :param db_name: database name
    :param db_user: database user name
    :param db_pass: database password
    :param role_arn: IAM role Arn
    :param region: region name
    :param key: access key id
    :param secret: secret access key
    """
    redshift = boto3.client(
        "redshift",
        region_name=region,
        aws_access_key_id=key,
        aws_secret_access_key=secret,
    )

    try:
        redshift.create_cluster(
            ClusterType=cluster_type,
            NodeType=node_type,
            NumberOfNodes=num_nodes,
            DBName=db_name,
            ClusterIdentifier=cluster_id,
            MasterUsername=db_user,
            MasterUserPassword=db_pass,
            IamRoles=[role_arn]
        )
    except Exception as e:
        logging.error("cluster creation failed", exc_info=e)


def allow_dwh_inbound_traffic(vpc_id: str, port: int, region: str, key: str, secret: str):
    """
    Adds a Security Group Rule to allow inbound traffic for the specified port to access the cluster

    :param vpc_id: VPC Identifier
    :param port: port
    :param region: region name
    :param key: access key id
    :param secret: secret access key
    """
    ec2 = boto3.resource(
        "ec2",
        region_name=region,
        aws_access_key_id=key,
        aws_secret_access_key=secret,
    )

    try:
        vpc = ec2.Vpc(id=vpc_id)
        default_sg = list(vpc.security_groups.all())[0]
        default_sg.authorize_ingress(
            GroupName=default_sg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=port,
            ToPort=port,
        )
    except Exception as e:
        logging.error("setting ingress rule failed", exc_info=e)


def main():
    config = configparser.ConfigParser()
    config_file_name = "dwh.cfg"
    config.read(config_file_name)

    role_arn = create_dwh_role(
        role_name=config["IAM_ROLE"]["ROLE_NAME"],
        region=config["CLUSTER"]["REGION"],
        key=config["CREDENTIALS"]["KEY"],
        secret=config["CREDENTIALS"]["SECRET"],
    )

    config.set("IAM_ROLE", "ARN", role_arn)
    with open(config_file_name, 'w') as configfile:
        config.write(configfile)

    create_redshift_cluster(
        cluster_id=config["CLUSTER"]["CLUSTER_IDENTIFIER"],
        cluster_type=config["CLUSTER"]["CLUSTER_TYPE"],
        node_type=config["CLUSTER"]["NODE_TYPE"],
        num_nodes=int(config["CLUSTER"]["NUM_NODES"]),
        db_name=config["CLUSTER"]["DB_NAME"],
        db_user=config["CLUSTER"]["DB_USER"],
        db_pass=config["CLUSTER"]["DB_PASSWORD"],
        role_arn=config["IAM_ROLE"]["ARN"],
        region=config["CLUSTER"]["REGION"],
        key=config["CREDENTIALS"]["KEY"],
        secret=config["CREDENTIALS"]["SECRET"],
    )

    allow_dwh_inbound_traffic(
        vpc_id=config["CLUSTER"]["VPC_ID"],
        port=int(config["CLUSTER"]["DB_PORT"]),
        region=config["CLUSTER"]["REGION"],
        key=config["CREDENTIALS"]["KEY"],
        secret=config["CREDENTIALS"]["SECRET"],
    )


if __name__ == "__main__":
    main()
