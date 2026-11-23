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

From a checkout:

```bash
pip install -e ".[test]"
```

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
```

`submitPath` must be on storage visible to both the submit host and the
workers (typically under `/arc/...`).

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
| `dag.json` / `run.yaml` / `state.json` | DAG, config snapshot, report state |
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
