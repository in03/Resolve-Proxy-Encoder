# Resolve Proxy Encoder
[![pre-commit.ci status](https://results.pre-commit.ci/badge/github/in03/Resolve-Proxy-Encoder/main.svg)](https://results.pre-commit.ci/latest/github/in03/Resolve-Proxy-Encoder/main) [![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

## Work in progress! ⚠️

 Please excuse anything that doesn't make sense in this repository. I code when I can find the time (which is scarce!) and often I'll leave quick-fixes and band-aid-  solutions sitting pretty. I'm doing my best to separate configuration from code and keep things cross platform, but at times I bend the rules to keep this working   everyday in our specific office environment. If you have the time to contribute to this repo, please do! Forks, PRs, issues and suggestions welcome.

 ---
 
## What's it for? ##
DaVinci Resolve Studio has a fantastic remote-rendering system built-in that allows queuing renders on other networked Resolve computers.
Unfortunately Resolve doesn't have a remote-rendering, or even background-rendering solution for proxies. 
*Resolve Proxy Encoder* is a Python application that can be used in a similar way to Resolve's remote-rendering system, but for proxies.
 
## How does it work? ##
**Resolve Proxy Encoder works using three major parts:**
- the Celery app that manages and runs the workers
- the `resolve_queue_proxies` module responsible for interfacing with DaVinci Resolve's python API 
- the broker (Redis or RabbitMQ server) that's responsible for sending data between Celery instances

## What does it need?
**This app has a few non-negotiable prerequisites:**
- Python 3.6 **ONLY** on Resolve 17 OR Python 3.3 to Python 3.10 on Resolve 18
- DaVinci Resolve Studio, with scripting set up (read Resolve's scripting README)
- Redis or RabbitMQ broker (preferably running on a NAS or server)
- Worker computers (decent resources, connected to LAN, can access source media and proxies output folder)
- *Resolve Proxy Encoder* installed on every 'queuer' and 'worker'. Not necessary to run broker.

**It also makes a few assumptions about the way you have your environment set up:**
- Running Windows (Mac OS, Linux and BSD untested - though should be easy to make work)
- All source media is accessible from a single networked volume / mapped-drive
- All proxies are to be encoded to and accessible from a single networked volume / mapped-drive


## How do I install it?

### A Warning about Python on Resolve 17 and Below

DaVinci Resolve 17 and below require Python 3.6 to communicate with the Resolve API, no versions over Python 3.6 will work.
Unfortunately this means that *Resolve Proxy Encoder* may get stuck using older versions of certain packages as they begin to drop support for 3.6.
It also means that security patches for some dependencies won't make it into *Resolve Proxy Encoder* 
This kind of setup almost guarantees dependency conflicts if you have multiple Python CLI tools you keep installed.
To mitigate this you can:

- Install Resolve 18, use newer Python versions.

- Use Python 3.6 for *Resolve Proxy Encoder* **ONLY** and install a newer Python alongside for your other needs.

- Install a tool like *pipx* that isolates Python CLI tools with their own virtual environments but keeps them on path.
(Recommend doing this anyway to keep dependencies isolated!)


### CLI / Worker
The CLI app is bundled with everything necessary to queue from Resolve and start workers that run the encoding.
```
pipx install git+https://github.com/in03/Resolve-Proxy-Encoder
```
Or with *pip*
``` 
py -3.6 -m pip install git+https://github.com/in03/Resolve-Proxy-Encoder
```
### Broker
The broker needs to be accessible by each computer over LAN.
Install Redis on an always-on computer or server (nice and easy to run in a docker container):
```
docker run -d --name some-redis -p 6379:6379 redis-server --append-only yes
```
OR RabbitMQ:
```
docker run -d --hostname my-rabbit --name some-rabbit -p 5672:5672 -p 8080:15672 rabbitmq:3-management
```

### Monitor / Notifications
If you want to monitor your jobs, it's a good idea to install Flower.
```
docker run --name flower -e $POSTGRES_PASSWORD -e $POSTGRES_USER -d flower
```
Make sure you set the environment variables for `CELERY_BROKER_URL` or Flower won't be able to connect to the broker. Also consider setting `FLOWER_PURGE_OFFLINE_WORKERS` if you don't have a well-defined set of computers to run the workers, or there's potential to change hostnames. `300` for example, would clear the list of offline workers every 5 minutes. This just keeps things tidy.

If you opt not to run Flower, keep in mind some non-essential CLI commands may not work.

## Configuration
On first run, you'll be prompted to alter your settings. The app will copy the default settings to the OS user configuration folder. 
- **Linux/Mac:** `$XDG_HOME_CONFIG/resolve_proxy_encoder/user_settings.yml`
- **Windows:** `%homepath%/resolve_proxy_encoder/user_settings.yml`



### Some Key Settings

The default global log-level:
```
loglevel: WARNING 
```

All proxies will be encoded to this directory. They'll retain the source media's directory structure:
```
paths:
  proxy_path_root: R:/ProxyMedia
``` 
These are celery specific settings. Make sure you set all of the below addresses as per your environment!
```
celery_settings:

  host_address: 192.168.1.19
  broker_url:  redis://192.168.1.19:6379/0
  flower_url: http://192.168.1.19:5555
  result_backend: redis://192.168.1.19:6379/0
```

Set this to a reasonable value in minutes so that the broker doesn't fill with results and run out of memory!
If you need persistent results, consider configuring your broker for persistent storage. Both Redis and RabbitMQ have options for persistence, though they come with some trade-offs. Consider your needs carefully.
```
celery_settings:
  result_expires: 60
```
Windows doesn't support preforking for concurrency. Actually, Celery doesn't officially support Windows anymore at all. Running workers on Mac, Linux or containerised gets around this limitation. By default the configuration encourages starting multiple workers processes as 'solo' to work with Windows. Change this as necessary to reduce overhead:
```
celery_settings:
  worker_concurrency: 1
```
  
Encoding is a long-running task. Tasks won't get divided nicely between workers if they fetch more than one task at a time:
```
celery_settings:
  worker_prefetch_multiplier: 1 
```

## How can I contribute?
Clone the repo, install dependencies, call from poetry shell:
```
git clone https://github.com/in03/Resolve-Proxy-Encoder
cd Resolve-Proxy-Encoder
py -3.6 -m pip install poetry
py -3.6 -m poetry shell
poetry install
rprox
```
If you're unfamiliar with using Poetry for dependency management and packaging, [give it a look](https://python-poetry.org/docs/basic-usage).

## How do I use it?

```
Usage: rprox [OPTIONS] COMMAND [ARGS]...

Options:
  --install-completion  Install completion for the current shell.
  --show-completion     Show completion for the current shell, to copy it or
                        customize the installation.

  --help                Show this message and exit.

Commands:
  link   Manually link proxies from directory to source media in open...
  mon    Launch Flower Celery monitor in default browser new window
  purge  Purge all proxy jobs from all queues
  queue  Queue proxies from the currently open DaVinci Resolve timeline
  work   Prompt to start Celery workers on local machine
  ```
