#!/usr/bin/env python3.6

import logging
import subprocess
import webbrowser
from pathlib import Path
from typing import Optional, List

import typer
from pyfiglet import Figlet
from rich import print
from rich.console import Console
from rich.rule import Rule

from .utils import pkg_info

# Init classes
cli_app = typer.Typer()
console = Console()

# Get VC Key
VC_KEY_FILE = Path(__file__).parent.parent.parent.joinpath("version_constraint_key")
with open(VC_KEY_FILE) as file:
    VC_KEY = file.read()

# TODO: Add global option to hide banner
# labels: enhancement
hide_banner = typer.Option(
    default=False, help="Hide the title and build info on startup"
)

# Special functions


@cli_app.callback(invoke_without_command=True)
def draw_banner():

    # Print CLI title
    fig = Figlet()
    text = fig.renderText("Resolve Proxy Encoder")
    print(f"[green]{text}\n")

    # Get build info
    build_info = pkg_info.get_build_info("Resolve-Proxy-Encoder")

    # Print banner data
    if build_info["build"] == "release":

        print(
            f"[bold]{str(build_info['build']).capitalize()} build[/] "
            f"{build_info['version']} | "
            f"[bold]VC key:[/] '{VC_KEY}'"
        )

    else:

        print(
            f"[bold]{str(build_info['build']).capitalize()} build[/] "
            f"{'([green]installed[/] :package:)' if build_info['installed'] else '([yellow]cloned[/] :hammer_and_wrench:)'} "
            f"'{build_info['version'][:7:]}' | "
            f"[bold]Version constraint key[/] '{VC_KEY}'"
        )

    Rule()


def run_checks():
    """Run before CLI App load."""

    from ..app import checks
    from ..settings.manager import SettingsManager

    settings = SettingsManager()

    # Check for any updates and inject version info into user settings.
    version_info = checks.check_for_updates(
        github_url=settings["app"]["update_check_url"],
        package_name="resolve_proxy_encoder",
    )

    settings.update({"version_info": version_info})


def cli_init():

    draw_banner()
    run_checks()


# Commands


@cli_app.command()
def queue():
    """
    Queue proxies from the currently open
    DaVinci Resolve timeline
    """

    # Init
    from ..app import checks
    from ..settings.manager import SettingsManager
    from .utils.core import setup_rich_logging

    settings = SettingsManager()

    setup_rich_logging()
    logger = logging.getLogger(__name__)
    logger.setLevel(settings["app"]["loglevel"])
    # End init

    checks.check_worker_compatibility()

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

    # Init
    from ..settings.manager import SettingsManager
    from .utils.core import setup_rich_logging

    settings = SettingsManager()

    setup_rich_logging()
    logger = logging.getLogger(__name__)
    logger.setLevel(settings["app"]["loglevel"])
    # End init

    from ..queuer import link

    print("\n")
    console.rule(f"[green bold]Link proxies[/] :link:", align="left")
    print("\n")

    link.main()


@cli_app.command()
def work(
    hide_banner: bool = hide_banner,
    workers_to_launch: Optional[int] = typer.Argument(
        0, help="How many workers to start"
    ),
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
    """Purge all tasks from Celery according to VC key

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

    subprocess.run(
        ["celery", "-A", "resolve_proxy_encoder.worker", "purge", "-Q", VC_KEY]
    )


# TODO: Would be great if we can pass options unparsed by Typer
# This command could serve as a gateway to all Celery commands,
# but typer parses 'f' in 'broker purge -f' as an undefined option
@cli_app.command()
def celery(command: List[str]):
    """Pass celery commands to Celery buried within venv"""

    print("\n")
    console.rule(f"[cyan bold]Celery command :memo:", align="left")
    print("\n")

    subprocess.run(["celery", "-A", "resolve_proxy_encoder.worker", command])


@cli_app.command()
def config():
    """Open user settings configuration file for editing"""

    from ..settings.manager import SettingsManager

    settings = SettingsManager()

    print("\n")
    console.rule(
        f"[green bold]Open 'user_settings.yaml' config[/] :gear:", align="left"
    )
    print("\n")

    webbrowser.open_new(settings.user_file)


def main():
    cli_app()


if __name__ == "__main__":
    main()
