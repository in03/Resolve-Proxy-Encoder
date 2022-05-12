# Launch multiple workers

import logging
import multiprocessing
import os
import platform
import subprocess
from shutil import which

from rich import print

from ..app.utils import core, pkg_info
from ..settings.manager import SettingsManager
from ..worker.utils import get_queue

core.install_rich_tracebacks()

config = SettingsManager()
logger = logging.getLogger(__name__)


def prompt_worker_amount(cpu_cores: int):
    """Prompt the user for the amount of Celery workers they want to run.
    Start 2 less workers than amount of CPU cores by default."""

    answer = 0
    safe_cores_suggestion = cpu_cores - 2

    def invalid_answer():
        """Restart prompt if answer invalid"""
        print(f"[red]Invalid number! Please enter a whole number.[/]")
        prompt_worker_amount(cpu_cores)

    try:

        # Input doesn't like parsing colours
        print(
            f"[yellow]How many workers would you like to start?[/]\n"
            + f"Press ENTER for default: {safe_cores_suggestion}\n"
        )

        answer = int(input() or safe_cores_suggestion)

    except ValueError:
        invalid_answer()

    if answer == 0:
        print(f"[yellow]Using suggested amount: {safe_cores_suggestion}[/]")
        answer = safe_cores_suggestion

    return answer


def new_worker(id=None):
    """Start a new celery worker in a new process

    Used to start workers even when the script binaries are buried
    in a virtual env like in pipx.

    Args:
        - id: Used to differentiate multiple workers on the same host

    Returns:
        - none

    Raises:
        - none
    """

    def get_worker_name(id):

        # @h for 'hostname'
        return f"-n worker{id}@{platform.node()}"

    def get_worker_queue():

        return " -Q " + get_queue()

    def get_celery_binary_path():

        # Check if in virtual env. Find.
        celery_bin = pkg_info.get_script_from_package("Celery")
        if celery_bin:
            return celery_bin

        logger.warning("[yellow]Can't get Celery from package.[/]")

        # Assume global
        celery_bin = which("celery")
        if celery_bin:
            return celery_bin
        logger.warning(
            "[yellow]Using Celery on path." + "Please ensure version compatability![/]"
        )

        logger.error("[red]Couldn't find celery binary! Is it installed?[/]")
        core.app_exit(1, -1)

    def get_module_path():
        """Get absolute module path to pass to Celery worker"""

        # Change dir to package root
        module_path = os.path.dirname(os.path.abspath(__file__))

        logger.debug(f"[magenta]Path to worker module: '{module_path}'")
        assert os.path.exists(module_path)
        return os.path.abspath(module_path)

    def get_new_console():
        """Get os command to spawn process in a new console window"""

        # Get new terminal
        worker_terminal_args = config["worker"]["terminal_args"]

        # Check if any args are on path. Probably terminal executable.
        executable_args = [which(x) for x in worker_terminal_args]
        if executable_args:
            return ""

        # TODO: Ensure partial matches work here too
        # Make sure no starting args exist
        start_commands = ["open", "start"]
        if any(x in start_commands for x in worker_terminal_args):
            return ""

        # TODO: Need to test starting workers on other platforms
        # Get new terminal cmd
        os_ = platform.system()

        if os_ is "Windows":
            return 'start "RPROX Worker"'  # First double quotes as title

        elif os_ is "Mac":
            return "open"

        elif os_ is "Linux":
            logger.error(
                "Cannot guess installed Linux terminal. Too many distros."
                + "Please provide a terminal executable in 'worker_terminal_args' settings."
            )
            core.app_exit(1, -1)
        else:
            logger.error(
                f"Could not determine terminal executable for OS: {os_}."
                + "Please provide a terminal executable in 'worker_terminal_args' settings."
            )
            core.app_exit(1, -1)

    launch_cmd = [
        get_new_console(),
        *config["worker"]["terminal_args"],
        f'"{get_celery_binary_path()}"',
        "-A resolve_proxy_encoder.worker",
        "worker",
        get_worker_name(id),
        "-Q ",
        get_queue(),
        *config["worker"]["celery_args"],
    ]

    logger.info(f"[cyan]NEW WORKER - {id}[/]")
    logger.debug(f"[magenta]{' '.join(launch_cmd)}[/]\n")

    subprocess.Popen(
        cwd=get_module_path(),
        args=" ".join(launch_cmd),
        shell=True,
    )


def launch_workers(workers_to_launch: int):

    # Start launching

    for i in range(0, workers_to_launch):
        new_worker(id=i + 1)
    return


def main(workers: int = 0):
    """Main function"""

    os_ = platform.system()
    cpu_cores = multiprocessing.cpu_count()

    # Don't bother with tips if not prompting
    if workers:

        launch_workers(workers)

    else:

        print(f"[green]Running on {os_} with {cpu_cores} cores.[/]\n")
        print("For maximum performance, start as many workers as CPU cores.")
        print("Default recommendation is 2 cores spare for Resolve and other tasks.\n")
        launch_workers(prompt_worker_amount(cpu_cores))

    print(f"[green]Done![/]")
    core.app_exit(0, 5)


if __name__ == "__main__":

    # Change to the expected directory of START_WIN_WORKER
    module_path = os.path.dirname(os.path.abspath(__file__))
    package_path = os.path.dirname(module_path)

    os.chdir(package_path)
    main(0)
