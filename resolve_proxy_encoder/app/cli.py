from pydoc import resolve
from pyfiglet import Figlet
from rich import print

# Print CLI title
fig = Figlet()
text = fig.renderText("Resolve Proxy Encoder")
print(f"[green]{text}\n")
import logging
import subprocess
import webbrowser
from typing import Optional

import typer
from rich.console import Console
from rich.prompt import Confirm

from ..app import checks
from ..settings.manager import SettingsManager
from .utils.core import setup_rich_logging


def print_routing_key():

    ver_colour = "red"
    queue = "Unknown"

    if settings["app"]["disable_version_constrain"]:

        ver_colour = "yellow"
        queue = "celery"

    else:
        ver_colour = "green" if settings["version_info"]["is_latest"] else "yellow"
        queue = settings["version_info"]["commit_short_sha"]

    print(f"[cyan]Routing to queue:[/] [{ver_colour}]'{queue}'[/]")


# Init classes
console = Console()
settings = SettingsManager()

cli_app = typer.Typer()

setup_rich_logging()
logger = logging.getLogger(__name__)
logger.setLevel(settings["app"]["loglevel"])


@cli_app.command()
def queue():
    """
    Queue proxies from the currently open
    DaVinci Resolve timeline
    """
    checks.check_worker_compatability()

    print_routing_key()

    print("\n\n[green]Queuing proxies from Resolve's active timeline[/] :outbox_tray:")
    from ..queuer import queue

    queue.main()


@cli_app.command()
def link():
    """
    Manually link proxies from directory to
    source media in open DaVinci Resolve project
    """

    from ..queuer import link

    link.main()


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

    ver_colour = "green" if settings["version_info"]["is_latest"] else "yellow"
    print(
        f"[cyan]Consuming from queue: [/][{ver_colour}]'{settings['version_info']['commit_short_sha']}'[/]"
    )

    if not workers_to_launch:
        workers_to_launch = 0

    if workers_to_launch > 0:
        print(f"[green]Starting workers! :construction_worker:[/]")
    else:
        print(f"[cyan]Starting worker launcher prompt :construction_worker:[/]")

    from ..worker import launch_workers

    launch_workers.main(workers_to_launch)


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
    webbrowser.open_new(settings["celery"]["flower_url"])


# TODO: Test and flesh out new config command
# labels: feature
@cli_app.command()
def config():
    """Open user settings configuration file for editing"""

    print("[green]Opening user settings file for modification")
    webbrowser.open_new(settings.user_file)


def init():
    """Run before CLI App load."""

    version_info = checks.check_for_updates(
        github_url=settings["app"]["update_check_url"],
        package_name="resolve_proxy_encoder",
    )

    settings.update({"version_info": version_info})


def main():
    init()
    cli_app()


if __name__ == "__main__":
    main()
