import re
from commonregex import link
import os
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
            "proxy_path_root": lambda p: os.path.exists(p),
            "ffmpeg_logfile_path": lambda p: os.path.exists(os.path.dirname(p)),
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
        "broker": {
            "url": str,
            "job_expires": int,
            "result_expires": int,
        },
        "worker": {
            "loglevel": lambda s: s
            in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
            "terminal_args": list,
            "celery_args": list,
        },
    },
    ignore_extra_keys=True,
)
