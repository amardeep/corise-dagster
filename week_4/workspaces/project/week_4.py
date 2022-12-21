from typing import List

from dagster import Nothing, String, asset, with_resources
from workspaces.resources import redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@asset(config_schema={"s3_key": str}, required_resource_keys={"s3"}, group_name="corise")
def get_s3_data(context) -> List[Stock]:
    s3_key = context.op_config["s3_key"]
    context.log.info(f"Getting {s3_key} from S3")
    records = context.resources.s3.get_data(s3_key)
    stocks = [Stock.from_list(r) for r in records]
    return stocks


@asset(group_name="corise")
def process_data(get_s3_data: List[Stock]) -> Aggregation:
    max_stock = max(get_s3_data, key=lambda stock: stock.high)
    return Aggregation(date=max_stock.date, high=max_stock.high)


@asset(required_resource_keys={"redis"}, group_name="corise")
def put_redis_data(context, process_data: Aggregation):
    context.log.info(f"Putting {process_data} into Redis")
    context.resources.redis.put_data(process_data.date.isoformat(), f"{process_data.high}")


@asset(required_resource_keys={"s3"}, group_name="corise")
def put_s3_data(context, process_data: Aggregation):
    s3_key = process_data.date.isoformat()
    context.log.info(f"Putting {process_data} into S3 at {s3_key}")
    context.resources.s3.put_data(s3_key, process_data)


get_s3_data_docker, process_data_docker, put_redis_data_docker, put_s3_data_docker = with_resources(
    definitions=[get_s3_data, process_data, put_redis_data, put_s3_data],
    resource_defs={"s3": s3_resource, "redis": redis_resource},
    resource_config_by_key={
        "s3": {
            "config": {
                "bucket": "dagster",
                "access_key": "test",
                "secret_key": "test",
                "endpoint_url": "http://localstack:4566",
            }
        },
        "redis": {
            "config": {
                "host": "redis",
                "port": 6379,
            }
        },
    },
)
