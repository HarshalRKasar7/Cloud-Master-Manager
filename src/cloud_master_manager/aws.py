from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Dict, Iterable, List, Optional, Tuple

import boto3
from botocore.exceptions import BotoCoreError, ClientError


@dataclass
class AwsContext:
    session: boto3.session.Session
    region_name: Optional[str]

    @staticmethod
    def from_profile(profile: Optional[str], region: Optional[str]) -> "AwsContext":
        if profile:
            session = boto3.Session(profile_name=profile, region_name=region)
        else:
            session = boto3.Session(region_name=region)
        return AwsContext(session=session, region_name=region or session.region_name)


def list_ec2_instances(ctx: AwsContext) -> List[Dict]:
    ec2 = ctx.session.client("ec2")
    reservations = ec2.describe_instances().get("Reservations", [])
    instances: List[Dict] = []
    for res in reservations:
        for inst in res.get("Instances", []):
            name_tag = next((t["Value"] for t in inst.get("Tags", []) if t.get("Key") == "Name"), "")
            instances.append(
                {
                    "InstanceId": inst.get("InstanceId"),
                    "InstanceType": inst.get("InstanceType"),
                    "State": inst.get("State", {}).get("Name"),
                    "Name": name_tag,
                    "AZ": inst.get("Placement", {}).get("AvailabilityZone"),
                    "LaunchTime": inst.get("LaunchTime"),
                }
            )
    return instances


def list_s3_buckets(ctx: AwsContext) -> List[Dict]:
    s3 = ctx.session.client("s3")
    buckets = s3.list_buckets().get("Buckets", [])
    results: List[Dict] = []
    for b in buckets:
        name = b.get("Name")
        try:
            loc = s3.get_bucket_location(Bucket=name).get("LocationConstraint") or "us-east-1"
        except (ClientError, BotoCoreError):
            loc = "unknown"
        results.append({"Name": name, "CreationDate": b.get("CreationDate"), "Region": loc})
    return results


def list_rds_instances(ctx: AwsContext) -> List[Dict]:
    rds = ctx.session.client("rds")
    resp = rds.describe_db_instances()
    results: List[Dict] = []
    for db in resp.get("DBInstances", []):
        results.append(
            {
                "DBInstanceIdentifier": db.get("DBInstanceIdentifier"),
                "DBInstanceClass": db.get("DBInstanceClass"),
                "Engine": db.get("Engine"),
                "Status": db.get("DBInstanceStatus"),
                "MultiAZ": db.get("MultiAZ"),
            }
        )
    return results


def list_lambda_functions(ctx: AwsContext) -> List[Dict]:
    lam = ctx.session.client("lambda")
    paginator = lam.get_paginator("list_functions")
    results: List[Dict] = []
    for page in paginator.paginate():
        for fn in page.get("Functions", []):
            results.append(
                {
                    "FunctionName": fn.get("FunctionName"),
                    "Runtime": fn.get("Runtime"),
                    "LastModified": fn.get("LastModified"),
                }
            )
    return results


def get_month_cost(ctx: AwsContext) -> Tuple[str, str]:
    # returns (amount, currency)
    ce = ctx.session.client("ce")
    start = date.today().replace(day=1).isoformat()
    end = date.today().isoformat()
    resp = ce.get_cost_and_usage(
        TimePeriod={"Start": start, "End": end},
        Granularity="MONTHLY",
        Metrics=["UnblendedCost"],
    )
    results = resp.get("ResultsByTime", [])
    if not results:
        return ("0.0", "USD")
    total = results[0].get("Total", {}).get("UnblendedCost", {})
    return (total.get("Amount", "0.0"), total.get("Unit", "USD"))


def get_top_usage(ctx: AwsContext, top_n: int = 10) -> List[Tuple[str, float, str]]:
    # returns list of (service, amount, unit)
    ce = ctx.session.client("ce")
    start = date.today().replace(day=1).isoformat()
    end = date.today().isoformat()
    resp = ce.get_cost_and_usage(
        TimePeriod={"Start": start, "End": end},
        Granularity="MONTHLY",
        Metrics=["UsageQuantity"],
        GroupBy=[{"Type": "DIMENSION", "Key": "SERVICE"}],
    )
    groups = resp.get("ResultsByTime", [{}])[0].get("Groups", [])
    rows: List[Tuple[str, float, str]] = []
    for g in groups:
        service = g.get("Keys", [""])[0]
        amount = float(g.get("Metrics", {}).get("UsageQuantity", {}).get("Amount", 0.0))
        unit = g.get("Metrics", {}).get("UsageQuantity", {}).get("Unit", "")
        rows.append((service, amount, unit))
    rows.sort(key=lambda x: x[1], reverse=True)
    return rows[:top_n]


def allocate_ec2_instance(
    ctx: AwsContext,
    ami_id: str,
    instance_type: str,
    key_name: Optional[str],
    security_group_ids: Optional[List[str]],
    subnet_id: Optional[str],
    count: int,
    name_tag: Optional[str],
) -> List[str]:
    ec2 = ctx.session.client("ec2")
    params: Dict = {
        "ImageId": ami_id,
        "InstanceType": instance_type,
        "MinCount": count,
        "MaxCount": count,
    }
    if key_name:
        params["KeyName"] = key_name
    if security_group_ids:
        params["SecurityGroupIds"] = security_group_ids
    if subnet_id:
        params["SubnetId"] = subnet_id
    if name_tag:
        params["TagSpecifications"] = [
            {
                "ResourceType": "instance",
                "Tags": [{"Key": "Name", "Value": name_tag}],
            }
        ]
    resp = ec2.run_instances(**params)
    ids = [i["InstanceId"] for i in resp.get("Instances", [])]
    return ids


def deallocate_ec2_instances(
    ctx: AwsContext, instance_ids: List[str], terminate: bool = True
) -> List[str]:
    ec2 = ctx.session.client("ec2")
    if terminate:
        resp = ec2.terminate_instances(InstanceIds=instance_ids)
        return [i.get("InstanceId") for i in resp.get("TerminatingInstances", [])]
    else:
        resp = ec2.stop_instances(InstanceIds=instance_ids)
        return [i.get("InstanceId") for i in resp.get("StoppingInstances", [])]


def allocate_s3_bucket(ctx: AwsContext, bucket_name: str) -> None:
    s3 = ctx.session.client("s3")
    region = ctx.region_name
    if region and region != "us-east-1":
        s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": region},
        )
    else:
        s3.create_bucket(Bucket=bucket_name)


def deallocate_s3_bucket(ctx: AwsContext, bucket_name: str, force: bool) -> None:
    s3r = ctx.session.resource("s3")
    bucket = s3r.Bucket(bucket_name)
    if force:
        bucket.objects.all().delete()
        # delete versions if versioned
        bucket.object_versions.all().delete()
    bucket.delete()


def ensure_stack(ctx: AwsContext, stack_name: str, template_body: str, parameters: Dict[str, str], capabilities: List[str]) -> str:
    cf = ctx.session.client("cloudformation")
    param_list = [{"ParameterKey": k, "ParameterValue": v} for k, v in parameters.items()]
    # Detect if stack exists
    try:
        cf.describe_stacks(StackName=stack_name)
        action = "update"
    except ClientError as e:
        if "does not exist" in str(e):
            action = "create"
        else:
            raise

    if action == "create":
        resp = cf.create_stack(
            StackName=stack_name,
            TemplateBody=template_body,
            Parameters=param_list,
            Capabilities=capabilities or [],
        )
        cf.get_waiter("stack_create_complete").wait(StackName=stack_name)
        return resp["StackId"]
    else:
        try:
            resp = cf.update_stack(
                StackName=stack_name,
                TemplateBody=template_body,
                Parameters=param_list,
                Capabilities=capabilities or [],
            )
            cf.get_waiter("stack_update_complete").wait(StackName=stack_name)
            return resp["StackId"]
        except ClientError as e:
            if "No updates are to be performed" in str(e):
                # Return the existing stack id
                desc = cf.describe_stacks(StackName=stack_name)
                return desc["Stacks"][0]["StackId"]
            raise

