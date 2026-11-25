# lsst-ctrl-bps-canfar

CANFAR Skaha WMS plugin for [LSST BPS](https://pipelines.lsst.io/modules/lsst.ctrl.bps/quickstart.html)
(`ctrl_bps`).

Skaha has no native DAG. This plugin runs a **warm pool** of headless workers
and feeds them through a shared filesystem queue on `/arc`, the same idea as
the NewHorizons `run_single_qgraph.sh` / `launch_qrunners.sh` pattern. BPS owns
dependencies: a job is only moved into `queue/` when its parents have
succeeded. That keeps CANFAR from seeing thousands of session creates for a
wide quantum graph.

```text
bps submit
   -> prepare: payloads/ + dag.json under submitPath on /arc
   -> submit:  start N headless workers + orchestrator
               orchestrator enqueues ready jobs
               workers claim queue/ -> processing/ -> processed|failed
   -> report/cancel/restart use the queue dirs (+ worker session ids)
```

## Install

Into the LSST Science Pipelines / sciplat environment (where `bps` already
works):

```bash
pip install "https://github.com/ijiraq/ctrl_bps_canfar/releases/download/v0.1.0/lsst_ctrl_bps_canfar-0.1.0-py3-none-any.whl"
```

From a checkout (EUPS):

```bash
setup -k -r .
# or: export PYTHONPATH="$PWD/python:$PYTHONPATH"
```

Or with pip:

```bash
pip install -e ".[test]"
```

Do not add `__init__.py` under `python/lsst/`, `python/lsst/ctrl/`, or
`python/lsst/ctrl/bps/` — those shadow the real `ctrl_bps` package (same
layout as `ctrl_bps_htcondor`).

Requires the [`canfar`](https://github.com/opencadc/canfar) client (`>=1.5`) and
a headless-capable LSST image (for example
`images.canfar.net/lsst/sciplat-lab:latest`). Confirm with:

```bash
canfar login cadc
canfar image ls --kind headless
```

## BPS configuration

```yaml
wmsServiceClass: lsst.ctrl.bps.canfar.CanfarService
computeSite: canfar
site:
  canfar:
    image: images.canfar.net/lsst/sciplat-lab:latest_daily
    nWorkers: 8
    cores: 1
    ram: 4
    idleTimeout: 86900
    pollInterval: 30
    replaceWorkers: true
    reclaimGraceSeconds: 120
    stallCpuFraction: 0.01
    stallPolls: 5
    httpTimeout: 120
    httpRetries: 3
    envScript: /arc/projects/MyProject/env.sh
```

`submitPath` must be on storage visible to both the submit host and the
workers (typically under `/arc/...`).

`envScript` is the script each worker sources before it takes jobs: the
project environment (butler location, extra EUPS products, this plugin if it
is not installed in the image). The path is the one the worker sees. When it
is unset, workers load the image's stack (`loadLSST.bash`, `setup
lsst_distrib`) and nothing else, so a run whose commands use variables such as
`${BUTLER_DIR}` needs it.

`httpTimeout` is passed to `canfar.sessions.Session` (library max 300s; their
default is 30s). Session create on a busy Skaha often needs more; create and
destroy also retry transient timeouts up to `httpRetries` times.

## Usage

```bash
canfar login cadc
bps submit my_run.yaml
bps report --id /arc/path/to/submitPath
bps cancel --id /arc/path/to/submitPath
```

`bps submit` blocks in the orchestrator until the DAG finishes or is
cancelled (MVP). Workers that sit idle longer than `idleTimeout` exit; while
DAG work remains, `replaceWorkers: true` starts replacements up to
`nWorkers`.

Stall detection uses Skaha `Session.info`: after one poll grace following a
claim, if `cpuCoresInUse / requestedCPUCores` stays below `stallCpuFraction`
(default 1%) for `stallPolls` consecutive polls (default 5), the orchestrator
destroys that session and moves the job back to `queue/`. Workers publish the
active job name in `workers/<id>.alive` (and a `processing/<job>.owner`
sidecar) so claims can be mapped to sessions. Jobs left in `processing/` with
no owning heartbeat are reclaimed after `reclaimGraceSeconds`.

## Command lines and job files

BPS hands the plugin command lines that still contain placeholders
(`useLazyCommands: true` is the BPS default). They are resolved at
`bps prepare`, so every payload under `payloads/` holds a command that can be
passed to `bash` as it is:

| Placeholder | Becomes |
| --- | --- |
| `{key}` | the value BPS stored for the job |
| `<FILE:name>` | a path, chosen by the access mode below |
| `<ENV:VAR>` | `${VAR}`, evaluated on the worker (also in the job's `environment`) |
| `<WMS:attemptNum>` | `${BPS_CANFAR_ATTEMPT}`: earlier starts of the job, 0 on the first run |

A placeholder that cannot be resolved fails `bps prepare` with the job and
placeholder named.

Each job runs in its own scratch directory, removed when the job ends. It is
created under `$BPS_CANFAR_SCRATCH` if the worker's environment sets that
(for example in `envScript`), otherwise under the system temporary directory.

### Access modes

`bpsUseShared` (a standard BPS key, default `true`) says whether workers see
the submit host's filesystem.

**Shared** (`bpsUseShared: true`). `<FILE:runQgraphFile>` becomes the
absolute path BPS wrote, for example
`/arc/projects/MyProject/bps/.../run.qg`. Nothing is copied.

**Non-shared** (`bpsUseShared: false`). Workers reach submit-side files only
through `fileDistributionEndPoint`, a URI that must end in `/`:

```yaml
bpsUseShared: false
fileDistributionEndPoint: "https://example.org/files/bps/{uniqProcName}/"
```

- `bps submit` copies the files the jobs need (the QuantumGraph, and the
  final-job script when the final job is part of the run, see below) to the
  endpoint before any worker starts.
- `<FILE:runQgraphFile>` becomes the bare file name. The BPS command templates
  already write `{fileDistributionEndPoint}{qgraphFile}`, so `pipetask` opens
  `<endpoint>run.qg` itself. A custom `runQuantumCommand` has to keep that
  prefix.
- Files a job cannot open remotely, and executables BPS asks to have
  transferred (the final-job script), are copied into the job's scratch
  directory first.
- `butlerConfig` is passed through untouched, so it can be a remote butler
  URL or label.

Give each run its own endpoint directory (`{uniqProcName}` above): files are
published under their base names and an existing file is overwritten.

Copies use `lsst.resources.ResourcePath`, so any scheme it supports for both
the submit host and the workers will do. Only `file://` is covered by the
tests in this package.

### The final job

BPS adds a final job (`final_job.bash`, which runs `butler aggregate-graph`
to bring the outputs of the quantum-backed jobs into the butler). This plugin
runs it only when it is an ordinary job at the end of the DAG:

```yaml
finalJob:
  whenRun: SUCCESS
```

With the BPS default, `whenRun: ALWAYS`, BPS keeps the final job outside the
DAG and this plugin does not run it. The job outputs are then on disk but not
registered in the butler until you run
`<submitPath>/final_job.bash <qgraph file> <butlerConfig>` yourself.

### Still tied to a shared filesystem

- The queue: `submitPath` (payloads, queue directories, heartbeats, logs,
  `run_worker.sh`) must be one POSIX directory seen by the submit host and
  every worker, whichever access mode is used. Claims rely on atomic renames.
  All access to it goes through `QueueLayout` in `queue.py`.
- The plugin must be importable on the workers: installed in the image, or
  set up by `envScript`.
- Output files that a job writes to its working directory and expects the
  WMS to copy back (`bps submitcmd` with `outputs`) are not supported;
  `bps prepare` raises `NotImplementedError`.
- Directories cannot be published to the endpoint.

## Queue layout

Under each `submitPath`:

| Path | Role |
| --- | --- |
| `payloads/` | Jobs waiting for parents |
| `queue/` | Ready jobs |
| `processing/` | Claimed by a worker |
| `processed/` | Succeeded |
| `failed/` | Failed |
| `logs/` | Per-job logs |
| `workers/` | Worker heartbeats (`*.alive` JSON: `currentJob`, `sessionId`) |
| `processing/<job>.owner` | Claim ownership sidecar (`workerId`, `sessionId`, `claimedAt`) |
| `dag.json` / `run.yaml` / `state.json` | DAG, config snapshot (site settings, access mode, files to publish), report state |
| `run_worker.sh` | Copied entrypoint for headless sessions |

## Development

```bash
pip install -e ".[test]"
pytest -q
```

Live CANFAR smoke (one headless worker, one-job DAG):

```bash
canfar login cadc
export BPS_CANFAR_SUBMIT_PATH=/arc/projects/YOUR_PROJECT/bps-smoke/$USER
./examples/smoke_canfar.sh
```

## Optional: install into the sciplat image

After a tagged release, bake the wheel into the LSST science platform
image (for example
`science-containers/Dockerfiles/sciplat-lab/Dockerfile`) so
notebook / terminal users get the plugin without a manual `pip install`:

```dockerfile
RUN source /opt/lsst/software/stack/loadLSST.bash \
 && pip install --no-cache-dir \
      "https://github.com/ijiraq/ctrl_bps_canfar/releases/download/v0.1.0/lsst_ctrl_bps_canfar-0.1.0-py3-none-any.whl"
```

Prefer documenting the existing headless `lsst/sciplat-lab` image for
workers; the interactive sciplat-lab image can remain notebook-only. Auth
for workers is the same as qrunners: `canfar login cadc` writes
`~/.ssl/cadcproxy.pem`, which headless sessions inherit from the mounted
home. A Rubin data-facility token is only needed if workers must call
`data.lsst.cloud`.

## License

BSD-3-Clause.
