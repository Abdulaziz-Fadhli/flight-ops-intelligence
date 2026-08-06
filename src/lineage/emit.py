"""OpenLineage START/COMPLETE/FAIL event emission for each pipeline stage.

Uses OpenLineage's console transport (prints structured lineage events to
stdout as JSON) rather than requiring a running Marquez/lineage backend -
the events themselves, and the fact that they're emitted around every real
stage, are the deliverable here, not a specific downstream consumer of them.
"""

import contextlib
import datetime
import logging
import traceback
import uuid

from openlineage.client import OpenLineageClient
from openlineage.client.transport.console import ConsoleConfig, ConsoleTransport
from openlineage.client.run import Job, Run, RunEvent, RunState

NAMESPACE = "flight-ops-intelligence"
PRODUCER = "https://github.com/Abdulaziz-Fadhli/flight-ops-intelligence"

# ConsoleTransport emits each event via the `logging` module at INFO level
# rather than print(), so it's silent unless a handler is configured here.
logging.basicConfig(level=logging.INFO, format="%(message)s")
logging.getLogger("openlineage.client.transport.console").setLevel(logging.INFO)

_client = OpenLineageClient(transport=ConsoleTransport(ConsoleConfig()))


def _emit(job_name: str, run_id: str, state: RunState) -> None:
    event = RunEvent(
        eventType=state,
        eventTime=datetime.datetime.now(datetime.timezone.utc).isoformat(),
        run=Run(runId=run_id),
        job=Job(namespace=NAMESPACE, name=job_name),
        producer=PRODUCER,
    )
    _client.emit(event)


@contextlib.contextmanager
def lineage_run(job_name: str):
    """Wrap a pipeline stage: emits START on entry, COMPLETE on clean exit,
    FAIL (with the error printed) if the stage raises - then re-raises so
    the caller (an Airflow task) still fails and halts downstream tasks.
    """
    run_id = str(uuid.uuid4())
    print(f"[lineage] START job={job_name} run_id={run_id}")
    _emit(job_name, run_id, RunState.START)
    try:
        yield run_id
    except Exception:
        print(f"[lineage] FAIL job={job_name} run_id={run_id}\n{traceback.format_exc()}")
        _emit(job_name, run_id, RunState.FAIL)
        raise
    else:
        print(f"[lineage] COMPLETE job={job_name} run_id={run_id}")
        _emit(job_name, run_id, RunState.COMPLETE)
