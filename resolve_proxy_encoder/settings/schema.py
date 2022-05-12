import os
import re

from commonregex import link

from schema import And, Optional, Schema

settings_schema = Schema(
    {
        "app": {
            "loglevel": lambda s: s
            in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
            "check_for_updates": bool,
            "update_check_url": lambda s: re.match(link, s),
            "disable_version_constrain": bool,
        },
        "paths": {
            "proxies_with_source_media": bool,
            "proxies_in_one_folder": lambda p: os.path.exists(p),
        },
        "proxy": {
            "ffmpeg_loglevel": lambda l: l
            in [
                "quiet",
                "panic",
                "fatal",
                "error",
                "warning",
                "info",
                "verbose",
                "debug",
            ],
            "codec": str,
            "vertical_res": str,
            "profile": str,
            "pix_fmt": str,
            "audio_codec": str,
            "audio_samplerate": str,
            Optional("misc_args"): list,
            "ext": And(str, lambda s: s.startswith(".")),
        },
        "filters": {
            "extension_whitelist": And(
                list, lambda l: all(map(lambda s: s.startswith("."), l))
            ),
            "framerate_whitelist": And(list, lambda l: all(map(lambda s: int(s), l))),
        },
        "celery": {
            "host_address": str,
            "broker_url": str,
            "flower_url": str,
            "result_backend": str,
            "result_expires": int,
        },
        "worker": {
            "loglevel": lambda s: s
            in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
            "concurrency": int,
            "prefetch_multiplier": int,
            "max_tasks_per_child": int,
            "terminal_args": list,
            "celery_args": list,
        },
    },
    ignore_extra_keys=True,
)
