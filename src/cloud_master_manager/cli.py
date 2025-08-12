from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Optional

import click
from botocore.exceptions import BotoCoreError, ClientError

from .aws import (
    AwsContext,
    allocate_ec2_instance,
    allocate_s3_bucket,
    deallocate_ec2_instances,
    deallocate_s3_bucket,
    ensure_stack,
    get_month_cost,
    get_top_usage,
    list_ec2_instances,
    list_lambda_functions,
    list_rds_instances,
    list_s3_buckets,
)
from .render import print_error, print_header, print_info, print_table, print_warn


@click.group()
@click.option("--profile", envvar="AWS_PROFILE", help="AWS named profile to use")
@click.option("--region", envvar="AWS_REGION", help="AWS region (overrides profile default)")
@click.option(
    "--highlight-color",
    default="cyan",
    show_default=True,
    help="CLI highlight color (e.g., cyan, magenta, green)",
)
@click.pass_context
def cli(ctx: click.Context, profile: Optional[str], region: Optional[str], highlight_color: str):
    """Cloud Master Manager (cmm)

    Manage AWS resources, costs, usage, and CloudFormation deployments via CLI.
    """
    ctx.ensure_object(dict)
    ctx.obj["aws"] = AwsContext.from_profile(profile, region)
    ctx.obj["color"] = highlight_color


# -----------------------------------------------------------------------------
# Resources
# -----------------------------------------------------------------------------

@cli.group()
@click.pass_context
def resources(ctx: click.Context):
    """Inspect active resources across services."""


@resources.command("list")
@click.pass_context
def list_resources(ctx: click.Context):
    color = ctx.obj["color"]
    aws = ctx.obj["aws"]
    print_header("Active Resources", highlight_color=color)

    any_resources = False

    # EC2 Instances
    try:
        ec2_instances = list_ec2_instances(aws)
        rows = [
            [
                i.get("InstanceId"),
                i.get("Name"),
                i.get("InstanceType"),
                i.get("State"),
                i.get("AZ"),
                i.get("LaunchTime"),
            ]
            for i in ec2_instances
            if i.get("State") in {"pending", "running", "stopping", "stopped"}
        ]
        if rows:
            any_resources = True
            print_table(
                "EC2 Instances",
                columns=[
                    ("InstanceId", None),
                    ("Name", None),
                    ("Type", None),
                    ("State", None),
                    ("AZ", None),
                    ("LaunchTime", None),
                ],
                rows=rows,
                highlight_color=color,
            )
    except Exception as e:  # broad to avoid CLI crash in missing perms
        print_warn(f"EC2: {e}")

    # S3 Buckets
    try:
        buckets = list_s3_buckets(aws)
        rows = [[b.get("Name"), b.get("Region"), b.get("CreationDate")] for b in buckets]
        if rows:
            any_resources = True
            print_table(
                "S3 Buckets",
                columns=[("Name", None), ("Region", None), ("Created", None)],
                rows=rows,
                highlight_color=color,
            )
    except Exception as e:
        print_warn(f"S3: {e}")

    # RDS
    try:
        rds = list_rds_instances(aws)
        rows = [
            [d.get("DBInstanceIdentifier"), d.get("Engine"), d.get("DBInstanceClass"), d.get("Status"), d.get("MultiAZ")]
            for d in rds
        ]
        if rows:
            any_resources = True
            print_table(
                "RDS Instances",
                columns=[("Identifier", None), ("Engine", None), ("Class", None), ("Status", None), ("MultiAZ", None)],
                rows=rows,
                highlight_color=color,
            )
    except Exception as e:
        print_warn(f"RDS: {e}")

    # Lambda
    try:
        lams = list_lambda_functions(aws)
        rows = [[l.get("FunctionName"), l.get("Runtime"), l.get("LastModified")] for l in lams]
        if rows:
            any_resources = True
            print_table(
                "Lambda Functions",
                columns=[("Name", None), ("Runtime", None), ("LastModified", None)],
                rows=rows,
                highlight_color=color,
            )
    except Exception as e:
        print_warn(f"Lambda: {e}")

    if not any_resources:
        print_info("No resource is allocated")


# -----------------------------------------------------------------------------
# Billing and Usage
# -----------------------------------------------------------------------------

@cli.group()
@click.pass_context
def billing(ctx: click.Context):
    """Cost and usage insights."""


@billing.command("show")
@click.pass_context
def billing_show(ctx: click.Context):
    color = ctx.obj["color"]
    aws = ctx.obj["aws"]
    try:
        amount, unit = get_month_cost(aws)
        print_header("Current Month Cost", highlight_color=color)
        print_info(f"Estimated spend: {amount} {unit}")
    except (ClientError, BotoCoreError) as e:
        print_error(str(e))


@billing.command("usage")
@click.option("--top", "top_n", default=10, show_default=True, help="Show top N services by usage")
@click.pass_context
def billing_usage(ctx: click.Context, top_n: int):
    color = ctx.obj["color"]
    aws = ctx.obj["aws"]
    try:
        rows = get_top_usage(aws, top_n=top_n)
        print_table(
            "Top Usage by Service (Current Month)",
            columns=[("Service", None), ("Amount", "right"), ("Unit", None)],
            rows=rows,
            highlight_color=color,
        )
    except (ClientError, BotoCoreError) as e:
        print_error(str(e))


# -----------------------------------------------------------------------------
# Allocate / Deallocate
# -----------------------------------------------------------------------------

@cli.group()
@click.pass_context
def allocate(ctx: click.Context):
    """Allocate resources (create/start)."""


@cli.group()
@click.pass_context
def deallocate(ctx: click.Context):
    """Deallocate resources (stop/terminate/delete)."""


@allocate.command("ec2")
@click.option("--ami-id", required=True, help="AMI ID")
@click.option("--instance-type", required=True, help="EC2 instance type, e.g., t3.micro")
@click.option("--key-name", help="EC2 key pair name")
@click.option("--sg", "security_groups", help="Comma-separated security group IDs")
@click.option("--subnet-id", help="Subnet ID")
@click.option("--count", default=1, show_default=True, help="Number of instances")
@click.option("--name", "name_tag", help="Value for Name tag")
@click.pass_context
def allocate_ec2(ctx: click.Context, ami_id: str, instance_type: str, key_name: Optional[str], security_groups: Optional[str], subnet_id: Optional[str], count: int, name_tag: Optional[str]):
    aws = ctx.obj["aws"]
    try:
        sg_ids = [s.strip() for s in security_groups.split(",")] if security_groups else None
        ids = allocate_ec2_instance(
            aws,
            ami_id=ami_id,
            instance_type=instance_type,
            key_name=key_name,
            security_group_ids=sg_ids,
            subnet_id=subnet_id,
            count=count,
            name_tag=name_tag,
        )
        print_info(f"Launched instances: {', '.join(ids)}")
    except (ClientError, BotoCoreError) as e:
        print_error(str(e))


@deallocate.command("ec2")
@click.option("--instance-ids", required=True, help="Comma-separated EC2 instance IDs")
@click.option("--stop/--terminate", default=False, help="Stop instead of terminate")
@click.pass_context
def deallocate_ec2(ctx: click.Context, instance_ids: str, stop: bool):
    aws = ctx.obj["aws"]
    try:
        ids = [s.strip() for s in instance_ids.split(",") if s.strip()]
        acted = deallocate_ec2_instances(aws, ids, terminate=not stop)
        action = "stopped" if stop else "terminated"
        print_info(f"Instances {action}: {', '.join(acted)}")
    except (ClientError, BotoCoreError) as e:
        print_error(str(e))


@allocate.command("s3")
@click.option("--name", "bucket_name", required=True, help="S3 bucket name")
@click.pass_context
def allocate_s3(ctx: click.Context, bucket_name: str):
    aws = ctx.obj["aws"]
    try:
        allocate_s3_bucket(aws, bucket_name)
        print_info(f"Created bucket: {bucket_name}")
    except (ClientError, BotoCoreError) as e:
        print_error(str(e))


@deallocate.command("s3")
@click.option("--name", "bucket_name", required=True, help="S3 bucket name")
@click.option("--force", is_flag=True, help="Delete all objects before deleting bucket")
@click.pass_context
def deallocate_s3(ctx: click.Context, bucket_name: str, force: bool):
    aws = ctx.obj["aws"]
    try:
        deallocate_s3_bucket(aws, bucket_name, force)
        print_info(f"Deleted bucket: {bucket_name}")
    except (ClientError, BotoCoreError) as e:
        print_error(str(e))


# -----------------------------------------------------------------------------
# CloudFormation Deploy
# -----------------------------------------------------------------------------

@cli.group()
@click.pass_context
def deploy(ctx: click.Context):
    """Deploy infrastructure templates."""


@deploy.command("template")
@click.option("--stack-name", required=True, help="CloudFormation stack name")
@click.option("--template-file", type=click.Path(exists=True, path_type=Path), required=True, help="Path to .yaml or .json template")
@click.option("--param", "params", multiple=True, help="Template parameter as KEY=VALUE. Repeat for multiple.")
@click.option("--capability", "capabilities", multiple=True, help="Capability, e.g., CAPABILITY_IAM, CAPABILITY_NAMED_IAM")
@click.pass_context
def deploy_template(ctx: click.Context, stack_name: str, template_file: Path, params: tuple[str, ...], capabilities: tuple[str, ...]):
    aws = ctx.obj["aws"]
    try:
        body = template_file.read_text(encoding="utf-8")
        parameters: Dict[str, str] = {}
        for p in params:
            if "=" not in p:
                raise click.ClickException(f"Invalid --param '{p}', expected KEY=VALUE")
            k, v = p.split("=", 1)
            parameters[k] = v
        stack_id = ensure_stack(
            aws,
            stack_name=stack_name,
            template_body=body,
            parameters=parameters,
            capabilities=list(capabilities),
        )
        print_info(f"Stack ready: {stack_id}")
    except (ClientError, BotoCoreError) as e:
        print_error(str(e))
