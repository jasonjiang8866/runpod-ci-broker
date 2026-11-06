import pytest

from runpod_ci_broker.fake import FakeProvider
from runpod_ci_broker.results import save_bundle, verify_saved


def test_bundle_verifies_identity_size_hash_and_metric_gate(tmp_path, task, candidates):
    manifest, files = FakeProvider(candidates).collect("fake-1", task)
    assert save_bundle(task, "fake-1", manifest, files, tmp_path / "good")
    assert verify_saved(tmp_path / "good", task)
    manifest["source_sha"] = "f" * 40
    with pytest.raises(ValueError, match="provenance"):
        save_bundle(task, "fake-1", manifest, files, tmp_path / "bad")
    manifest["source_sha"] = task.source_sha
    files["worker.log"] += b"corrupt"
    with pytest.raises(ValueError, match="checksum"):
        save_bundle(task, "fake-1", manifest, files, tmp_path / "bad")


def test_metric_failure_keeps_export_but_rejects_experiment(tmp_path, task, candidates):
    task.results.metric_min = {"macro_f1": 0.99}
    manifest, files = FakeProvider(candidates).collect("fake-1", task)
    assert save_bundle(task, "fake-1", manifest, files, tmp_path) is False
    assert verify_saved(tmp_path, task)


def test_symlink_and_oversized_artifacts_refused(tmp_path, task, candidates):
    manifest, files = FakeProvider(candidates).collect("fake-1", task)
    (tmp_path / "worker.log").symlink_to(tmp_path / "outside")
    with pytest.raises(ValueError, match="symlink"):
        save_bundle(task, "fake-1", manifest, files, tmp_path)
    task.results.max_bytes = 1
    manifest["config_fingerprint"] = task.fingerprint()
    with pytest.raises(ValueError, match="size"):
        save_bundle(task, "fake-1", manifest, files, tmp_path / "oversized")
