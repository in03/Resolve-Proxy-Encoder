#!/usr/bin/env python3.6

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

    checks.check_worker_compatability(settings["online_workers"])

    print("\n")
    console.rule(
        f"[green bold]Queuing proxies from Resolve's active timeline[/] :outbox_tray:",
        align="left",
    )
    print("\n")

    from ..queuer import queue

    queue.main()


@cli_app.command()
def link():
    """
    Manually link proxies from directory to
    source media in open DaVinci Resolve project
    """

    from ..queuer import link

    print("\n")
    console.rule(f"[green bold]Link proxies[/] :link:", align="left")
    print("\n")

    link.main()


@cli_app.command()

# TODO: Figure out how to pass optional celery args to Typer
def work(
    workers_to_launch: Optional[int] = typer.Argument(
        0, help="How many workers to start"
    )
):
    """Prompt to start Celery workers on local machine"""

    if not workers_to_launch:
        workers_to_launch = 0

    print("\n")

    if workers_to_launch > 0:
        console.rule(
            f"[green bold]Starting workers![/] :construction_worker:", align="left"
        )
    else:
        console.rule(
            f"[green bold]Starting worker launcher prompt[/] :construction_worker:",
            align="left",
        )

    print("\n")

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

    print("\n")
    console.rule(f"[red bold]Purge all tasks! :fire:", align="left")
    print("\n")

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

    print("\n")
    console.rule(
        f"[green bold]Start Flower Celery monitor[/] :sunflower:", align="left"
    )
    print("\n")

    webbrowser.open_new(settings["celery"]["flower_url"])


# TODO: Test and flesh out new config command
# labels: feature
@cli_app.command()
def config():
    """Open user settings configuration file for editing"""

    print("\n")
    console.rule(
        f"[green bold]Open 'user_settings.yaml' config[/] :gear:", align="left"
    )
    print("\n")

    webbrowser.open_new(settings.user_file)


def init():
    """Run before CLI App load."""

    # Check for any updates and inject version info into user settings.
    version_info = checks.check_for_updates(
        github_url=settings["app"]["update_check_url"],
        package_name="resolve_proxy_encoder",
    )

    settings.update({"version_info": version_info})
    print(f"[bold]VERSION: {settings['version_info']['commit_short_sha']}")

    # Check for online workers to pass to other checks
    online_workers = checks.check_worker_presence()
    settings.update({"online_workers": online_workers})


def main():
    init()
    cli_app()


if __name__ == "__main__":
    main()
