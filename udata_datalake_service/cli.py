import logging
import click
from dotenv import load_dotenv

from udata_datalake_service.consumer import consume_kafka
from udata_datalake_service.background_tasks import celery


@click.group()
@click.version_option()
def cli():
    """
    udata-datalake-service
    """


@cli.command()
def consume():
    load_dotenv()
    logging.basicConfig(level=logging.INFO)
    consume_kafka()


@cli.command()
def work():
    '''Starts a worker'''
    worker = celery.Worker()
    worker.start()
    return worker.exitcode
