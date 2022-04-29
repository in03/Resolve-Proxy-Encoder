import re
from commonregex import link
import os
from ..helpers import resolve_network_path
from schema import Schema, And, Optional


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
            "proxy_in_project": bool,
            "proxy_path_root": lambda p: resolve_network_path(
                p, must_exist=True, return_local_path_on_fail=False
            ),
            "ffmpeg_logfile_path": lambda p: resolve_network_path(
                p, must_exist=True, return_local_path_on_fail=False
            ),
        },
        "proxy_settings": {
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
            "vid_codec": str,
            "h_res": str,
            "v_res": str,
            "vid_profile": str,
            "pix_fmt": str,
            "audio_codec": str,
            "audio_samplerate": str,
            Optional("misc_args"): list,
            "ext": And(str, lambda s: s.startswith(".")),
        },
        "filters": {
            "use_extension_whitelist": bool,
            "extension_whitelist": And(
                list, lambda l: all(map(lambda s: s.startswith("."), l))
            ),
            "use_framerate_whitelist": bool,
            "framerate_whitelist": And(list, lambda l: all(map(lambda s: int(s), l))),
        },
        "celery_settings": {
            "host_address": str,
            "broker_url": str,
            "flower_url": str,
            "result_backend": str,
            "result_expires": int,
            "worker_loglevel": lambda s: s
            in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
            "worker_concurrency": int,
            "worker_prefetch_multiplier": int,
            "worker_max_tasks_per_child": int,
            "worker_terminal_args": list,
            "worker_celery_args": list,
        },
    },
    ignore_extra_keys=True,
)
