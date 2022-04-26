"""
This file serves to return a DaVinci Resolve object
"""
import imp
import sys

from rich import print as pprint

# from resolve_proxy_encoder import helpers


def GetResolve():
    ext = ".so"
    if sys.platform.startswith("darwin"):
        path = "/Applications/DaVinci Resolve/DaVinci Resolve.app/Contents/Libraries/Fusion/"
    elif sys.platform.startswith("win") or sys.platform.startswith("cygwin"):
        path = "C:\\Program Files\\Blackmagic Design\\DaVinci Resolve\\"
        ext = ".dll"
    elif sys.platform.startswith("linux"):
        path = "/opt/resolve/libs/Fusion/"
    else:
        raise Exception("Unsupported system! " + sys.platform)

    bmd = imp.load_dynamic("fusionscript", path + "fusionscript" + ext)
    resolve = bmd.scriptapp("Resolve")

    if not resolve:
        _fail_import()

    try:
        sys.modules[__name__] = resolve
    except ImportError:
        _fail_import()

    return resolve


def _fail_import():
    """Internally fail if can't import Python remote mmodule"""

    pprint(
        "[red] :warning: Couldn't access the Resolve Python API. Is DaVinci Resolve running?[/]"
    )
    # helpers.app_exit(1, -1)


if __name__ == "__main__":
    GetResolve()
