from pyfiglet import Figlet
from rich import print

# Print CLI title
fig = Figlet()
text = fig.renderText("Resolve Proxy Encoder")
print(f"[green]{text}[/]\n")

import subprocess
import webbrowser

import typer
from typing import Optional
from rich.console import Console
from rich.prompt import Confirm

from resolve_proxy_encoder import checks
from resolve_proxy_encoder.helpers import get_rich_logger
from resolve_proxy_encoder.settings.app_settings import Settings

# Init classes
console = Console()
cli_app = typer.Typer()
settings = Settings()

config = settings.user_settings
logger = get_rich_logger(config["app"]["loglevel"])


@cli_app.command()
def queue():
    """
    Queue proxies from the currently open
    DaVinci Resolve timeline
    """
    checks.check_worker_compatability()

    print("\n")

    ver_colour = "green" if VERSION_INFO["is_latest"] else "yellow"
    print(
        f"[cyan]Routing to queue:[/] [{ver_colour}]'{VERSION_INFO['current_version']}'[/]"
    )

    print("\n\n[green]Queuing proxies from Resolve's active timeline[/] :outbox_tray:")
    from resolve_proxy_encoder import resolve_queue_proxies

    resolve_queue_proxies.main()


@cli_app.command()
def link():
    """
    Manually link proxies from directory to
    source media in open DaVinci Resolve project
    """

    from resolve_proxy_encoder import link_proxies

    link_proxies.main()


@cli_app.command()

# TODO: Figure out how to pass optional celery args to Typer
def work(
    workers_to_launch: Optional[int] = typer.Argument(
        0, help="How many workers to start"
    )
):
    """Prompt to start Celery workers on local machine"""

    print("\n")

    # Print worker queue
    ver_colour = "green" if VERSION_INFO["is_latest"] else "yellow"
    print(
        f"[cyan]Consuming from queue: [/][{ver_colour}]'{VERSION_INFO['current_version']}'[/]"
    )

    if workers_to_launch > 0:
        print(f"[green]Starting workers! :construction_worker:[/]")
    else:
        print(f"[cyan]Starting worker launcher prompt :construction_worker:[/]")

    from resolve_proxy_encoder import start_workers

    start_workers.main(workers_to_launch)


@cli_app.command()
def purge():
    """Purge all tasks from Celery.

    All tasks will be removed from all queues,
    including results and any history in Flower.

    Args:
        None
    Returns:
        None
    Raises:
        None
    """

    if Confirm.ask(
        "[yellow]Are you sure you want to purge all tasks?\n"
        "All active tasks and task history will be lost![/]"
    ):
        print("[green]Purging all worker queues[/] :fire:")
        subprocess.run(["celery", "-A", "resolve_proxy_encoder.worker", "purge", "-f"])


@cli_app.command()
def mon():
    """
    Launch Flower Celery monitor in default browser new window
    """

    print("[green]Launching Flower celery monitor[/] :sunflower:")
    webbrowser.open_new(config["celery_settings"]["flower_url"])


def init():
    """Run before CLI App load."""

    global VERSION_INFO

    VERSION_INFO = checks.check_for_updates(
        github_url=config["app"]["update_check_url"],
        package_name="resolve_proxy_encoder",
    )
    # TODO: Add update method to settings class
    # There are a few dynamic variables that would be nice to have globally
    # E.g. `settings.add_setting(current_version)`
    # labels: enhancement


def main():
    init()
    cli_app()


if __name__ == "__main__":
    main()
