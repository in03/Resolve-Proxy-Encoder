import time
from typing import Union

from rich import print
from rich.prompt import Confirm
from yaspin import yaspin

from resolve_proxy_encoder.helpers import (
    app_exit,
    get_package_current_commit,
    get_remote_latest_commit,
    get_rich_logger,
    install_rich_tracebacks,
)
from resolve_proxy_encoder.settings.app_settings import Settings
from resolve_proxy_encoder.worker.celery import app as celery_app

install_rich_tracebacks()

settings = Settings()
config = settings.user_settings

logger = get_rich_logger(config["app"]["loglevel"])


def check_for_updates(github_url: str, package_name: str) -> Union[dict, None]:

    """Compare git origin to local git or package dist for updates

    Args:
        - github_url(str): origin repo url
        - package_name(str): offical package name

    Returns:
        - dict:
            - 'is_latest': bool,
            - 'current_version': git_short_sha

    Raises:
        - none
    """

    if not config["app"]["check_for_updates"]:
        return None

    latest = False

    spinner = yaspin(
        text="Checking for updates...",
        color="cyan",
    )

    spinner.start()

    package_latest_commit = get_package_current_commit(package_name)
    remote_latest_commit = get_remote_latest_commit(github_url)

    if not remote_latest_commit or not package_latest_commit:

        spinner.fail("❌ ")
        logger.warning("[red]Failed to check for updates[/]")
        return None

    elif remote_latest_commit != package_latest_commit:

        spinner.ok("🔼 ")
        logger.warning(
            "[yellow]Update available.\n"
            + "Fully uninstall and reinstall when possible:[/]\n"
            + '"pip uninstall resolve-proxy-encoder"\n'
            + f'"pip install git+{github_url}"\n'
        )

        logger.info(f"Remote: {remote_latest_commit}")
        logger.info(f"Current: {package_latest_commit}")

    else:

        latest = True
        spinner.ok("✨ ")

    return {
        "is_latest": latest,
        "current_version": package_latest_commit[::8],
    }


def check_worker_compatability():

    if config["app"]["disable_version_constrain"]:
        logger.warning(
            "[yellow]Worker compatability check disabled in user settings![/]\n"
        )
        time.sleep(2)
        return

    spinner = yaspin(
        text="Checking worker compatability...",
        color="cyan",
    )

    # Get online workers and package current commit
    spinner.start()
    online_workers = celery_app.control.inspect().active_queues()
    git_full_sha = get_package_current_commit("resolve_proxy_encoder")

    if git_full_sha is None:
        spinner.fail("❌ ")
        logger.warning(
            "[yellow]Couldn't get local package git commit SHA\n"
            + "Any incompatible workers will not be reported.\n"
            + "[red]CONTINUE AT OWN RISK![/]"
        )
        return

    git_short_sha = git_full_sha[::8]

    if online_workers is None:

        spinner.fail("❌ ")
        logger.warning(
            "[yellow]No workers found. Can't check compatability.\n"
            + "Jobs may not be received if no compatible workers are available!\n[/]"
            + "[red]CONTINUE AT OWN RISK![/]\n"
        )

        if not Confirm.ask("[cyan]Do you wish to continue?[/]"):
            app_exit(1, -1)

        return

    spinner.stop()
    logger.debug(f"Online workers: {online_workers}")
    spinner.start()

    # Get incompatible workers
    incompatible_workers = []
    compatible_workers = []
    for worker, attributes in online_workers.items():

        routing_key = attributes[0]["routing_key"]

        # Strip whitespace
        routing_key = "".join(routing_key.split())
        git_short_sha = "".join(git_short_sha.split())

        worker_dict = {
            "name": worker,
            "host": str(worker).split("@")[1],
            "routing_key": routing_key,
        }

        # Compare git sha
        if not routing_key == git_short_sha:
            incompatible_workers.append(worker_dict)
        else:
            compatible_workers.append(worker_dict)

    incompatible_hosts = set()
    for x in incompatible_workers:
        incompatible_hosts.add(x["host"])

    compatible_hosts = set()
    for x in compatible_workers:
        compatible_hosts.add(x["host"])

    # Prompt incompatible workers
    if incompatible_workers:
        spinner.fail("❌ ")

        # Get singular or plural based on list lengths
        worker_plural = "workers" if len(online_workers) > 1 else "worker"
        incompatible_hosts_count = len(incompatible_hosts)
        multi_incompatible_hosts_warning = (
            f"across {len(incompatible_hosts)} hosts "
            if incompatible_hosts_count > 1
            else ""
        )

        logger.warning(
            f"[yellow]Incompatible {worker_plural} detected!\n"
            + f"{len(incompatible_workers)}/{len(online_workers)} workers {multi_incompatible_hosts_warning}are incompatible.\n"
            + f"Only compatible workers can consume jobs from this queuer.\n"
            + f"\n[green]To fix, update any incompatible hosts to this git commit:[/]\n'{git_full_sha}':\n"
            + f"\n[magenta]Incompatible hosts:\n{incompatible_hosts}[/]"
            + "\n"
        )

        # If no compatible workers are available
        if len(online_workers) == len(incompatible_workers):

            spinner.fail("❌ ")
            logger.error(
                "[red]All online workers are incompatible!\n" + "Cannot continue[/]"
            )
            app_exit(1, -1)

        else:

            if not Confirm.ask("[cyan]Do you wish to continue?[/]"):
                app_exit(1, -1)

    spinner.ok("👍 ")
    return
