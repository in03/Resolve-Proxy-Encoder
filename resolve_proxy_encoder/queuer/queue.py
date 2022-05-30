#!/usr/bin/env python3.6

import logging

from celery import group
from rich import print as print

from ..app.utils import core
from ..settings.manager import SettingsManager
from ..worker.tasks.encode.tasks import encode_proxy
from . import handlers, link, resolve

settings = SettingsManager()

core.install_rich_tracebacks()
logger = logging.getLogger(__name__)
logger.setLevel(settings["app"]["loglevel"])

# Set global flags
SOME_ACTION_TAKEN = False


def add_queuer_data(jobs, **kwargs):
    """
    Extend jobs with relevant queuer data

    Args:
        **kwargs - any keyword arguments to pass with the tasks to the worker\n
         queuer-side configuration extra Resolve variables, etc can be passed

    Returns:
        jobs - the original jobs with added data

    Raises:
        nothing
    """

    jobs = [dict(item, **kwargs) for item in jobs]
    return jobs


def queue_jobs(jobs):
    """Send jobs as a Celery 'group'"""

    # Wrap job objects in Celery task function
    callable_tasks = [encode_proxy.s(x) for x in jobs]
    logger.debug(f"[magenta]callable_tasks:[/] {callable_tasks}")

    # Create job group to retrieve job results as batch
    task_group = group(callable_tasks)

    # Queue job
    queued_group = task_group.apply_async()
    logger.info(f"[cyan]Queued tasks {queued_group}[/]")

    return queued_group


def wait_jobs(jobs):
    """Block until all queued jobs finish, notify results."""

    result = jobs.join()

    # Notify failed
    if jobs.failed():
        fail_message = (
            "Some videos failed to encode!"
            + f"Check flower dashboard at address: {settings['celery']['flower_url']}."
        )
        print("[red]fail_message[/]")
        core.notify(fail_message)

    # Notify complete
    complete_message = f"Completed encoding {jobs.completed_count()} proxies."
    print(f"[green]{complete_message}[/]")
    print("\n")

    core.notify(complete_message)

    return result


def main():
    """Main function"""

    r_ = resolve.ResolveObjects()
    project_name = r_.project.GetName()
    timeline_name = r_.timeline.GetName()

    print("\n")
    print(f"[cyan]Working on: '{r_.project.GetName()}[/]'")
    print("\n")

    # Lets make it happen!
    track_items = resolve.get_video_track_items(r_.timeline)
    media_pool_items = resolve.get_media_pool_items(track_items)
    jobs = resolve.get_resolve_proxy_jobs(media_pool_items)

    # Prompt user for intervention if necessary
    print()
    jobs = handlers.handle_already_linked(jobs, unlinked_types=["Offline", "None"])

    print()
    jobs = handlers.handle_existing_unlinked(jobs, unlinked_types=["Offline", "None"])

    print()
    jobs = handlers.handle_offline_proxies(jobs)

    # Remove unhashable PyRemoteObj
    for job in jobs:
        del job["media_pool_item"]

    print("\n")

    # Alert user final queuable. Confirm.
    handlers.handle_final_queuable(jobs)

    tasks = add_queuer_data(
        jobs,
        project=project_name,
        timeline=timeline_name,
        proxy_settings=settings["proxy"],
        paths_settings=settings["paths"],
    )

    job_group = queue_jobs(tasks)

    core.notify(f"Started encoding job '{project_name} - {timeline_name}'")
    print(f"[yellow]Waiting for job to finish. Feel free to minimize.[/]")
    job_results = wait_jobs(job_group)

    # Post-encode link
    logger.info("[cyan]Linking proxies")

    try:
        proxies = [x["unlinked_proxy"] for x in jobs]
        link.find_and_link_proxies(r_.project, proxies)
        core.app_exit(0)

    except Exception as e:

        print("[red]Couldn't link jobs. Link manually:[/]")
        print(e)
        core.app_exit(1, -1)


if __name__ == "__main__":
    main()
