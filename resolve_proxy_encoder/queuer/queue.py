#!/usr/bin/env python3.6

import logging
import os

from celery import group
from rich import print


from ..app import broker
from ..app.utils import core
from ..settings.manager import SettingsManager
from ..worker.tasks.encode.tasks import encode_proxy
from ..worker.celery import app as celery_app
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


def queue_tasks(tasks):
    """Block until all queued tasks finish, notify results."""

    # Wrap task objects in Celery task function
    callable_tasks = [encode_proxy.s(x) for x in tasks]
    logger.debug(f"[magenta]callable_tasks:[/] {callable_tasks}")

    # Create task group to retrieve job results as batch
    task_group = group(callable_tasks)

    progress = broker.ProgressTracker(settings, callable_tasks)

    # Queue job
    results = task_group.apply_async(expires=settings["broker"]["job_expires"])
    logger.debug(f"[cyan]Queued tasks {results}[/]")

    # report progress is blocking!
    final_results = progress.report_progress(results)
    return final_results


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
    logger.debug(f"[magenta]Remaining queuable:[/]\n{[x['file_name'] for x in jobs]}")

    print()
    jobs = handlers.handle_existing_unlinked(jobs, unlinked_types=["Offline", "None"])
    logger.debug(f"[magenta]Remaining queuable:[/]\n{[x['file_name'] for x in jobs]}")

    print()
    jobs = handlers.handle_offline_proxies(jobs)
    logger.debug(f"[magenta]Remaining queuable:[/]\n{[x['file_name'] for x in jobs]}")

    print("\n")

    # Alert user final queuable. Confirm.
    handlers.handle_final_queuable(jobs)

    # Get output paths for queueable jobs
    for x in jobs:

        proxy_output_path = os.path.join(
            x["proxy_dir"],
            os.path.splitext(x["file_name"])[0]
            + settings["proxy"]["ext"],  # Output ext, differs from source
        )

        x.update({"proxy_media_path": proxy_output_path})

        logger.debug(
            "[magenta]Set proxy_media_path to output path:[/]\n"
            f"'{x['proxy_media_path']}'\n"
        )

    # Celery can't accept MPI (pyremoteobj)
    # Convert to string for later reference
    jobs_with_mpi = []
    for x in jobs:
        jobs_with_mpi.append({str(x["media_pool_item"]): x["media_pool_item"]})
        x.update({"media_pool_item": str(x["media_pool_item"])})

    tasks = add_queuer_data(
        jobs,
        project=project_name,
        timeline=timeline_name,
        proxy_settings=settings["proxy"],
        paths_settings=settings["paths"],
    )

    print("\n")

    core.notify(f"Started encoding job '{project_name} - {timeline_name}'")
    # print(f"[yellow]Waiting for job to finish. Feel free to minimize.[/]")

    # Queue tasks to workers and track task progress
    results = queue_tasks(tasks)

    if results.failed():
        fail_message = "Some videos failed to encode!"
        print("[red]fail_message[/]")
        core.notify(fail_message)

    # Notify complete
    complete_message = f"Completed encoding {results.completed_count()} proxies."
    print(f"[green]{complete_message}[/]")
    print("\n")

    core.notify(complete_message)

    _ = results.join()  # Must always call join, or results don't expire

    # Get media pool items back
    logger.debug(f"[magenta]Restoring media-pool-items[/]")

    for x, y in zip(jobs, jobs_with_mpi):
        for k, v in y.items():
            assert hasattr(v, "GetClipProperty()")
            if x["media_pool_item"] == k:
                x.update({"media_pool_item": v})

    try:

        unlinkable = link.link_proxies_with_mpi(
            jobs,
            linkable_types=["None"],
            prompt_rerender=False,
        )
        assert len(unlinkable) == 0

    except Exception as e:

        logger.error(f"[red]Couldn't link jobs. Link manually.[/]\nError: {e}")
        core.app_exit(1, -1)

    finally:
        print("[bold][green]All linked up![/bold] Nothing to queue[/] :link:")
        core.app_exit(0)


if __name__ == "__main__":
    main()
