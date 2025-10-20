import click
from src import etl


@click.group()
def cli():
    """Campaign finance CLI"""
    pass


@cli.command()
def migrate():
    """Run DB migrations"""
    from migrations.upgrade import run_migrations
    run_migrations()


@cli.command()
def run():
    """Run full ETL"""
    etl.run_etl()


if __name__ == '__main__':
    cli()
