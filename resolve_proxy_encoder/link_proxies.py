# Link proxies

import os
from resolve_proxy_encoder import helpers
from resolve_proxy_encoder.settings.app_settings import Settings

settings = Settings()
config = settings.user_settings


# Get global variables
resolve_obj = helpers.get_resolve_objects()
resolve = resolve_obj["resolve"]
project = resolve_obj["project"]
timeline = resolve_obj["timeline"]
media_pool = resolve_obj["media_pool"]


def get_proxy_path():

    proxy_path_root = config["paths"]["proxy_path_root"]

    f = input("Enter path to search for proxies: ")
    if f is None:
        print("User cancelled. Exiting.")
        exit(0)
    return f


def recurse_dir(root):
    """Recursively search given directory for files
    and return full filepaths
    """

    all_files = [
        os.path.join(root, f) for root, dirs, files in os.walk(root) for f in files
    ]
    print(f"Found {len(all_files)} files in folder {root}")
    return all_files


def filter_files(dir_, extension_whitelist):
    """Filter files by allowed filetype"""

    # print(f"{timeline.GetName()} - Video track count: {track_len}")
    allowed = [x for x in dir_ if os.path.splitext(x) in extension_whitelist]
    return allowed


def get_track_items(timeline, track_type="video"):
    """Retrieve all video track items from a given timeline object"""

    track_len = timeline.GetTrackCount("video")

    if config["app"]["loglevel"] == "DEBUG":
        print(f"{timeline.GetName()} - Video track count: {track_len}")

    items = []

    for i in range(track_len):
        i += 1  # Track index starts at one...
        items.extend(timeline.GetItemListInTrack(track_type, i))

    return items


def get_resolve_timelines(active_timeline_first=True):
    """Return a list of all Resolve timeline objects in current project."""

    timelines = []

    timeline_len = project.GetTimelineCount()
    if timeline_len > 0:

        for i in range(1, timeline_len + 1):
            timeline = project.GetTimelineByIndex(i)
            timelines.append(timeline)

        if active_timeline_first:
            active = project.GetCurrentTimeline().GetName()  # Get active timeline
            timeline_names = [x.GetName() for x in timelines]
            active_i = timeline_names.index(
                active
            )  # It's already in the list, find it's index
            timelines.insert(
                0, timelines.pop(active_i)
            )  # Move it to the front, indexing should be the same as name list
    else:
        return False

    return timelines


def get_timeline_data(timeline):
    """Return a dictionary containing timeline names,
    their tracks, clips media paths, etc.
    """

    clips = get_track_items(timeline, track_type="video")
    data = {
        "timeline": timeline,
        "name": timeline.GetName(),
        "track count": timeline.GetTrackCount(),
        "clips": clips,
    }

    return data


def __link_proxies(proxy_files, clips):
    """Actual linking function.
    Matches filenames between lists of paths.
    'clips' actually needs to be a Resolve timeline item object.
    """

    linked_proxies = []
    failed_proxies = []
    linked_clips = []

    for proxy in proxy_files:
        for clip in clips:

            proxy_name = os.path.splitext(os.path.basename(proxy))[0]
            if proxy in failed_proxies:
                if config["app"]["loglevel"] == "DEBUG":
                    print(f"Skipping {proxy_name}, already failed.")
                break

            try:
                media_pool_item = clip.GetMediaPoolItem()
                path = media_pool_item.GetClipProperty("File Path")
                filename = os.path.splitext(os.path.basename(path))[0]

                if proxy_name.lower() in filename.lower():

                    print(f"Found match:")
                    print(f"{proxy} &\n{path}")

                    if media_pool_item.LinkProxyMedia(proxy):

                        print(f"Linked\n")
                        linked_proxies.append(proxy)
                        linked_clips.append(clip)

                    else:
                        print(f"Failed link.\n")
                        failed_proxies.append(proxy)

            except AttributeError:

                if config["app"]["loglevel"] == "DEBUG":
                    print(
                        f"{clip.GetName()} has no 'file path' attribute,"
                        + " probably Resolve internal media."
                    )

    return linked_proxies, failed_proxies


def link_proxies(proxy_files):
    """Attempts to match source media in active Resolve project
    with a list of filepaths to proxy files."""

    linked = []
    failed = []

    timelines = get_resolve_timelines()
    if not timelines:
        raise Exception("No timelines exist in current project.")

    # Get clips from all timelines.
    for timeline in timelines:

        timeline_data = get_timeline_data(timeline)
        clips = timeline_data["clips"]
        unlinked_source = [x for x in clips if x not in linked]

        if len(unlinked_source) == 0:
            if config["app"]["loglevel"] == "DEBUG":
                print(f"No more clips to link in {timeline_data['name']}")
            continue
        else:
            print(f"Searching timeline {timeline_data['name']}")

        unlinked_proxies = [x for x in proxy_files if x not in linked]
        print(f"Unlinked source count: {len(unlinked_source)}")
        print(f"Unlinked proxies count: {len(unlinked_proxies)}")

        if len(unlinked_proxies) == 0:
            print(f"No more proxies to link in {timeline_data['name']}")
            return

        linked_, failed_ = __link_proxies(proxy_files, clips)

        linked.extend(linked_)
        failed.extend(failed_)

        if config["app"]["loglevel"] == "DEBUG":
            print(f"Linked: {linked}, Failed: {failed}")

    if len(failed) > 0:
        print(
            f"The following files matched, but couldn't be linked. Suggest rerendering them:"
        )
        [print(os.path.basename(x)) for x in failed]
        print()

    return linked


def main():
    try:
        proxy_dir = get_proxy_path()

        print(f"Passed directory: '{proxy_dir}'\n")

        all_files = recurse_dir(proxy_dir)
        proxy_files = filter_files(all_files, config["filters"]["extension_whitelist"])
        linked = link_proxies(proxy_files)

    except Exception as e:
        print("ERROR - " + str(e))


if __name__ == "__main__":
    main()
