"""Tests for src/lineage/emit.py's lineage_run() context manager.

The third test here is a regression test for the exact edge case documented
in docs/quality_lineage_run.txt: pointing a quality gate at a nonexistent
LAKEHOUSE_ROOT caused the deltalake (delta-rs) Rust binding to raise
pyo3_runtime.PanicException, which subclasses BaseException directly rather
than Exception - so it slipped past the original `except Exception` clause
and no FAIL event was emitted. `_FakePanic` stands in for that exception
type so the test doesn't require actually triggering a Rust-level panic.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src", "lineage"))
import emit  # noqa: E402


class _FakePanic(BaseException):
    """Stands in for pyo3_runtime.PanicException."""


def test_lineage_run_emits_start_then_complete_on_success(monkeypatch):
    states = []
    monkeypatch.setattr(emit, "_emit", lambda job_name, run_id, state: states.append(state))

    with emit.lineage_run("test-job"):
        pass

    assert states == [emit.RunState.START, emit.RunState.COMPLETE]


def test_lineage_run_emits_fail_on_normal_exception(monkeypatch):
    states = []
    monkeypatch.setattr(emit, "_emit", lambda job_name, run_id, state: states.append(state))

    with pytest.raises(ValueError):
        with emit.lineage_run("test-job"):
            raise ValueError("business rule failed")

    assert states == [emit.RunState.START, emit.RunState.FAIL]


def test_lineage_run_emits_fail_on_baseexception_regression(monkeypatch):
    """Regression test for the documented pyo3 panic edge case. Fails on the
    original `except Exception` and passes after changing it to
    `except BaseException` in emit.py.
    """
    states = []
    monkeypatch.setattr(emit, "_emit", lambda job_name, run_id, state: states.append(state))

    with pytest.raises(_FakePanic):
        with emit.lineage_run("test-job"):
            raise _FakePanic("simulated pyo3 panic")

    assert states == [emit.RunState.START, emit.RunState.FAIL]
