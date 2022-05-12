#!/usr/bin/env python3.6

import logging
import os
import tkinter
import tkinter.messagebox

from celery import group
from rich import print as print
from yaspin import yaspin

from ..app.utils import core
from ..settings.manager import SettingsManager
from ..worker.tasks.encode.tasks import encode_proxy
from . import handlers, link, resolve

settings = SettingsManager()

core.install_rich_tracebacks()
logger = logging.getLogger(__name__)

# Set global flags
SOME_ACTION_TAKEN = False
tk_root = tkinter.Tk()
tk_root.withdraw()


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
    logger.debug(f"callable_tasks: {callable_tasks}")

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


# TODO: Need to get new search and link working
# Legacy link is the original code and it's soooo bad.
# I haven't looked at it in ages. But then I bit off more than I could
# chew working on this. Find a middle ground!
# labels: bug


def legacy_link(project, media_list):
    """This is sooooo dank. But it's the only way that works atm."""

    print(f"[cyan]Linking {len(media_list)} proxies.[/]")
    existing_proxies = []

    for media in media_list:
        proxy = media.get("Unlinked Proxy", None)
        if proxy == None:
            continue

        existing_proxies.append(proxy)

        if not os.path.exists(proxy):
            tkinter.messagebox.showerror(
                title="Error linking proxy",
                message=f"Proxy media not found at '{proxy}'",
            )
            print(f"[red]Error linking proxy: Proxy media not found at '{proxy}'[/]")
            continue

        else:
            media.update({"Unlinked Proxy": None})  # Set existing to none once linked

        media.update({"Proxy": "1280x720"})

    link.find_and_link_proxies(project, existing_proxies)

    print("\n")

    pre_len = len(media_list)
    media_list = [x for x in media_list if "Unlinked Proxy" not in x]
    post_len = len(media_list)
    print(f"{pre_len - post_len} proxy(s) linked, will not be queued.")
    print(f"[magenta]Queueing {post_len}[/]")
    print("\n")

    return media_list


def main():
    """Main function"""

    r_ = resolve.ResolveObjects()
    project_name = r_.project.GetName()
    timeline_name = r_.timeline.GetName()

    print(f"[cyan]Working on: {r_.project.GetName()}[/]")
    handlers.handle_workers()
    print("\n")

    # Lets make it happen!
    track_items = resolve.get_video_track_items(r_.timeline)
    media_pool_items = resolve.get_media_pool_items(track_items)
    jobs = resolve.get_resolve_proxy_jobs(media_pool_items)

    print("\n")

    # Prompt user for intervention if necessary
    jobs = handlers.handle_already_linked(jobs, offline_types=["Offline", "None"])
    jobs = handlers.handle_offline_proxies(jobs)
    jobs = handlers.handle_existing_unlinked(jobs)

    # TODO: Find out if there's any way to pass media_pool_item through celery
    # This would allow us to do post-encode linking without searching timelines
    # labels: enhancement

    # Remove unhashable PyRemoteObj
    for job in jobs:
        del job["media_pool_item"]

    # Alert user final queuable. Confirm.
    handlers.handle_final_queuable(jobs)

    print("\n")

    tasks = add_queuer_data(
        jobs,
        project=project_name,
        timeline=timeline_name,
        proxy_settings=settings["proxy"],
        paths_settings=settings["paths"],
    )

    job = queue_jobs(tasks)

    core.notify(f"Started encoding job '{project_name} - {timeline_name}'")
    print(f"[yellow]Waiting for job to finish. Feel free to minimize.[/]")
    wait_jobs(job)

    # Post-encode link
    try:

        jobs = link.link_proxies_with_mpi(jobs)
        core.app_exit(0)

    except:

        print("[red]Couldn't link jobs. Link manually...[/]")
        core.app_exit(1, -1)


if __name__ == "__main__":
    main()
