# Celery settings should be configured using celery setting or environment variables.
# See the .env-sample if using Docker, or celery_settings-sample.py if running in virtual env.

from __future__ import absolute_import

import os
import sys

from resolve_proxy_encoder.helpers import get_rich_logger, install_rich_tracebacks
from resolve_proxy_encoder.settings.app_settings import Settings

from celery import Celery

install_rich_tracebacks()

settings = Settings()
config = settings.user_settings

logger = get_rich_logger(config["app"]["loglevel"])

# Windows can't fork processes. It'll choke if you make it try.
if sys.platform == "win32":
    os.environ.setdefault("FORKED_BY_MULTIPROCESSING", "1")

app = Celery("worker")

app.autodiscover_tasks(["resolve_proxy_encoder.worker.tasks.standard"])

try:
    app.config_from_object(config["celery_settings"])
except Exception as e:
    logger.error("Couldn't load settings from YAML!")

# Fragile! Moved from user settings to here.
app.conf.update(
    task_serializer="json",  # Pickle allows us to post-encode link using remote objects
    result_serializer="json",  # Keep same as above
    result_extended=True,  # Allows us to get task args after task completion
    acks_late=True,
    accept_content=["json", "pickle", "application/x-python-serialize"],
    result_accept_content=["json", "pickle", "application/x-python-serialize"],
    worker_pool_restarts=True,
    worker_send_task_events=True,
    worker_cancel_long_running_tasks_on_connection_loss=True,
    worker_hijack_root_logger=False,
    worker_redirect_stdouts=False,
)
