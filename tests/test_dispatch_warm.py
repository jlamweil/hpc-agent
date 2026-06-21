"""Tests for warm-job-aware dispatch (Slice 03) and keepwarm takeover."""
import pytest
from argparse import Namespace
from unittest.mock import patch


class TestDispatchWarm:
    def test_dispatch_skips_sbatch_when_warm(self, monkeypatch, db_path):
        from hpc_batch import cmd_dispatch, submit_task
        import hpc_batch as hb
        submit_task("test", db_path=db_path)
        monkeypatch.setattr(hb, 'check_hpc_jobs',
            lambda cfg=None: [{"id": "999", "state": "RUNNING", "name": "dsv4-batch"}])
        sbatch_called = False
        def fake_sbatch(cfg=None, server_mode=False, exclude_nodes=None):
            nonlocal sbatch_called
            sbatch_called = True
            return {"success": True, "job_id": "999"}
        monkeypatch.setattr(hb, 'sbatch_on_hpc', fake_sbatch)
        monkeypatch.setattr(hb, 'rsync_to_hpc', lambda db, cfg: True)
        monkeypatch.setattr(hb, '_recover_needs_rsync', lambda db: None)
        args = Namespace(db=db_path, force=True, batch=False)
        cmd_dispatch(args)
        assert not sbatch_called, "Should not submit new job when warm"

    def test_dispatch_submits_sbatch_when_cold(self, monkeypatch, db_path):
        from hpc_batch import cmd_dispatch, submit_task
        import hpc_batch as hb
        submit_task("test", db_path=db_path)
        monkeypatch.setattr(hb, 'check_hpc_jobs', lambda cfg=None: [])
        sbatch_called = False
        def fake_sbatch(cfg=None, server_mode=False, exclude_nodes=None):
            nonlocal sbatch_called
            sbatch_called = True
            return {"success": True, "job_id": "1000"}
        monkeypatch.setattr(hb, 'sbatch_on_hpc', fake_sbatch)
        monkeypatch.setattr(hb, 'rsync_to_hpc', lambda db, cfg: True)
        monkeypatch.setattr(hb, '_recover_needs_rsync', lambda db: None)
        args = Namespace(db=db_path, force=True, batch=False)
        cmd_dispatch(args)
        assert sbatch_called, "Should submit new job when cold"

    def test_dispatch_server_mode_defaults(self, monkeypatch, db_path):
        """Server mode env vars are set when sbatch is called."""
        from hpc_batch import cmd_dispatch, submit_task
        import hpc_batch as hb
        submit_task("test", db_path=db_path)
        monkeypatch.setattr(hb, 'check_hpc_jobs', lambda cfg=None: [])
        sbatch_called_with_server = False
        def fake_sbatch(cfg=None, server_mode=False, exclude_nodes=None):
            nonlocal sbatch_called_with_server
            sbatch_called_with_server = server_mode
            return {"success": True, "job_id": "1001"}
        monkeypatch.setattr(hb, 'sbatch_on_hpc', fake_sbatch)
        monkeypatch.setattr(hb, 'rsync_to_hpc', lambda db, cfg: True)
        monkeypatch.setattr(hb, '_recover_needs_rsync', lambda db: None)
        args = Namespace(db=db_path, force=True, batch=False)
        cmd_dispatch(args)
        assert sbatch_called_with_server, "Default should be server mode"

    def test_dispatch_needs_rsync_recovery(self, monkeypatch, db_path):
        """Pending task with needs_rsync triggers rsync retry."""
        import json, sqlite3
        from hpc_batch import cmd_dispatch, submit_task
        from argparse import Namespace
        submit_task("test", db_path=db_path)
        conn = sqlite3.connect(db_path)
        row = conn.execute("SELECT id, metadata FROM tasks LIMIT 1").fetchone()
        if row:
            meta = json.loads(row[1])
            meta["needs_rsync"] = True
            conn.execute("UPDATE tasks SET metadata=? WHERE id=?", (json.dumps(meta), row[0]))
            conn.commit()
        conn.close()
        import hpc_batch as hb
        monkeypatch.setattr(hb, 'check_hpc_jobs', lambda cfg=None: [])
        monkeypatch.setattr(hb, 'rsync_to_hpc', lambda db, cfg: True)
        monkeypatch.setattr(hb, 'sbatch_on_hpc', lambda cfg=None, server_mode=False, exclude_nodes=None: {"success": True, "job_id": "1002"})
        monkeypatch.setattr(hb, '_recover_needs_rsync', lambda db: None)
        args = Namespace(db=db_path, force=True, batch=False)
        # Should not crash
        cmd_dispatch(args)


class TestCheckHpcJobsQos:
    """check_hpc_jobs parses QoS column from squeue."""

    def test_returns_qos_when_present(self):
        """When squeue includes %q, job dict has qos field."""
        from hpc_batch import check_hpc_jobs
        import subprocess
        from unittest.mock import MagicMock
        
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = (
            "12345 RUNNING node-001 10:00 2025-06-01T10:00 dsv4-fast preemptqos\n"
        )
        mock_result.stderr = ""
        
        with patch.object(subprocess, "run", return_value=mock_result):
            jobs = check_hpc_jobs({"host": "hpc-login", "user": "testuser"})
        
        assert len(jobs) == 1
        assert jobs[0]["id"] == "12345"
        assert jobs[0]["qos"] == "preemptqos"
        assert jobs[0]["name"] == "dsv4-fast"
        assert jobs[0]["state"] == "RUNNING"
        assert jobs[0]["node"] == "node-001"

    def test_returns_qos_defaultqos(self):
        """defaultqos jobs are correctly parsed."""
        from hpc_batch import check_hpc_jobs
        import subprocess
        from unittest.mock import MagicMock
        
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "12346 RUNNING node-002 5:00 2025-06-01T10:05 dsv4-fast defaultqos\n"
        mock_result.stderr = ""
        
        with patch.object(subprocess, "run", return_value=mock_result):
            jobs = check_hpc_jobs({"host": "hpc-login", "user": "testuser"})
        
        assert jobs[0]["qos"] == "defaultqos"

    def test_empty_output_returns_empty_list(self):
        """No squeue output → empty list."""
        from hpc_batch import check_hpc_jobs
        import subprocess
        from unittest.mock import MagicMock
        
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = ""
        mock_result.stderr = ""
        
        with patch.object(subprocess, "run", return_value=mock_result):
            jobs = check_hpc_jobs({"host": "hpc-login", "user": "testuser"})
        
        assert jobs == []

    def test_ssh_failure_propagates(self):
        """SSH failure propagates (caller handles it)."""
        from hpc_batch import check_hpc_jobs
        import subprocess
        
        with patch.object(subprocess, "run", side_effect=subprocess.TimeoutExpired(
            cmd=["ssh", "..."], timeout=15,
        )):
            with pytest.raises(subprocess.TimeoutExpired):
                check_hpc_jobs({"host": "hpc-login", "user": "testuser"})


class TestKeepwarmTakeover:
    """Preemptive takeover: replacement job when only keepwarm exists."""

    def test_submits_replacement_when_only_keepwarm(self, monkeypatch, db_path):
        """Real task pending + preemptqos job running → submit replacement on diff node."""
        from hpc_batch import cmd_dispatch, submit_task
        import hpc_batch as hb
        submit_task("test", db_path=db_path)
        monkeypatch.setattr(hb, 'check_hpc_jobs',
            lambda cfg=None: [{"id": "999", "state": "RUNNING", "node": "node-004",
                              "name": "dsv4-fast", "qos": "preemptqos"}])
        sbatch_exclude = None
        def fake_sbatch(cfg=None, server_mode=False, exclude_nodes=None):
            nonlocal sbatch_exclude
            sbatch_exclude = exclude_nodes
            return {"success": True, "job_id": "1000"}
        monkeypatch.setattr(hb, 'sbatch_on_hpc', fake_sbatch)
        monkeypatch.setattr(hb, 'rsync_to_hpc', lambda db, cfg: True)
        monkeypatch.setattr(hb, '_recover_needs_rsync', lambda db: None)
        args = Namespace(db=db_path, force=True, batch=False)
        cmd_dispatch(args)
        assert sbatch_exclude == ["node-004"], "Should exclude keepwarm node"




    def test_normal_job_skips_replacement(self, monkeypatch, db_path):
        """Normal QoS job running → warm_reused, no sbatch."""
        from hpc_batch import cmd_dispatch, submit_task
        import hpc_batch as hb
        submit_task("test", db_path=db_path)
        monkeypatch.setattr(hb, 'check_hpc_jobs',
            lambda cfg=None: [{"id": "999", "state": "RUNNING", "node": "node-001",
                              "name": "dsv4-fast", "qos": "defaultqos"}])
        sbatch_called = False
        def fake_sbatch(cfg=None, server_mode=False, exclude_nodes=None):
            nonlocal sbatch_called
            sbatch_called = True
            return {"success": True, "job_id": "1000"}
        monkeypatch.setattr(hb, 'sbatch_on_hpc', fake_sbatch)
        monkeypatch.setattr(hb, 'rsync_to_hpc', lambda db, cfg: True)
        monkeypatch.setattr(hb, '_recover_needs_rsync', lambda db: None)
        args = Namespace(db=db_path, force=True, batch=False)
        cmd_dispatch(args)
        assert not sbatch_called, "Should not submit new job when normal QoS job exists"

    def test_normal_job_with_keepwarm_skips_replacement(self, monkeypatch, db_path):
        """Both normal and keepwarm jobs running → normal wins, no sbatch."""
        from hpc_batch import cmd_dispatch, submit_task
        import hpc_batch as hb
        submit_task("test", db_path=db_path)
        monkeypatch.setattr(hb, 'check_hpc_jobs',
            lambda cfg=None: [
                {"id": "998", "state": "RUNNING", "node": "node-004",
                 "name": "dsv4-fast", "qos": "preemptqos"},
                {"id": "999", "state": "RUNNING", "node": "node-001",
                 "name": "dsv4-fast", "qos": "defaultqos"},
            ])
        sbatch_called = False
        def fake_sbatch(cfg=None, server_mode=False, exclude_nodes=None):
            nonlocal sbatch_called
            sbatch_called = True
            return {"success": True, "job_id": "1000"}
        monkeypatch.setattr(hb, 'sbatch_on_hpc', fake_sbatch)
        monkeypatch.setattr(hb, 'rsync_to_hpc', lambda db, cfg: True)
        monkeypatch.setattr(hb, '_recover_needs_rsync', lambda db: None)
        args = Namespace(db=db_path, force=True, batch=False)
        cmd_dispatch(args)
        assert not sbatch_called, "Normal job exists — no replacement needed"

    def test_fill_only_keepwarm_does_not_submit_replacement(self, monkeypatch, db_path):
        """Fill-only mode with keepwarm → keepwarm is fine for fills, no replacement."""
        from hpc_batch import ensure_hpc_worker, init_db
        import hpc_batch as hb
        init_db(db_path)  # Create tasks table so mark_dispatched doesn't crash
        monkeypatch.setattr(hb, 'check_hpc_jobs',
            lambda cfg=None: [{"id": "999", "state": "RUNNING", "node": "node-004",
                              "name": "dsv4-fast", "qos": "preemptqos"}])
        sbatch_called = False
        def fake_sbatch(cfg=None, server_mode=False, exclude_nodes=None):
            nonlocal sbatch_called
            sbatch_called = True
            return {"success": True, "job_id": "1000"}
        monkeypatch.setattr(hb, 'sbatch_on_hpc', fake_sbatch)
        monkeypatch.setattr(hb, 'rsync_to_hpc', lambda db, cfg: True)
        monkeypatch.setattr(hb, '_recover_needs_rsync', lambda db: None)
        monkeypatch.setattr(hb, '_get_hpc_config',
            lambda: {"host": "hpc-login", "user": "testuser",
                     "dir": "~/hpc-agent", "partition": "gpu",
                     "account": "acc", "qos": "defaultqos"})
        monkeypatch.setattr(hb, 'mark_dispatched', lambda db, job_id: None)
        result = ensure_hpc_worker(db_path, qos="preemptqos")
        assert not sbatch_called, "Should not submit replacement in fill-only mode"

    def test_fill_only_submits_keepwarm_when_normal_job_only(self, monkeypatch, db_path):
        """Fill-only mode with only defaultqos job → submit preemptqos keepwarm."""
        from hpc_batch import ensure_hpc_worker, init_db
        import hpc_batch as hb
        init_db(db_path)
        monkeypatch.setattr(hb, 'check_hpc_jobs',
            lambda cfg=None: [{"id": "999", "state": "RUNNING", "node": "node-003",
                              "name": "dsv4-fast", "qos": "defaultqos"}])
        sbatch_exclude = None
        def fake_sbatch(cfg=None, server_mode=False, exclude_nodes=None):
            nonlocal sbatch_exclude
            sbatch_exclude = exclude_nodes
            return {"success": True, "job_id": "1000"}
        monkeypatch.setattr(hb, 'sbatch_on_hpc', fake_sbatch)
        monkeypatch.setattr(hb, 'rsync_to_hpc', lambda db, cfg: True)
        monkeypatch.setattr(hb, '_recover_needs_rsync', lambda db: None)
        monkeypatch.setattr(hb, '_get_hpc_config',
            lambda: {"host": "hpc-login", "user": "testuser",
                     "dir": "~/hpc-agent", "partition": "gpu",
                     "account": "acc", "qos": "defaultqos"})
        monkeypatch.setattr(hb, 'mark_dispatched', lambda db, job_id: None)
        result = ensure_hpc_worker(db_path, qos="preemptqos")
        assert result["status"] == "submitted", "Should submit keepwarm for fill-only mode"
        assert sbatch_exclude == ["node-003"], "Should exclude normal node for keepwarm"


class TestExcludeMerging:
    """Banned nodes from config + script + runtime exclude_nodes."""

    def test_config_banned_nodes_included(self, monkeypatch):
        """sbatch_on_hpc includes config banned_nodes in --exclude."""
        from hpc_batch import sbatch_on_hpc
        import hpc_batch as hb
        import subprocess
        
        cmd_args = []
        def fake_run(*args, **kwargs):
            nonlocal cmd_args
            cmd_args.append(args[0])
            mock = type('Mock', (), {'returncode': 0, 'stdout': 'Submitted batch job 1\n', 'stderr': ''})()
            return mock
        monkeypatch.setattr(hb, '_ssh_cmd', lambda host, user: ['ssh', 'fake'])
        monkeypatch.setattr(hb, '_get_hpc_config', lambda: {
            "host": "hpc-login", "user": "u", "dir": "~/hpc-agent",
            "partition": "gpu", "account": "acc", "qos": "defaultqos",
            "banned_nodes": ["node-bad-1", "node-bad-2"]})
        monkeypatch.setattr(hb, 'rsync_to_hpc', lambda db, cfg: True)
        monkeypatch.setattr(hb, '_recover_needs_rsync', lambda db: None)
        monkeypatch.setattr(subprocess, 'run', fake_run)
        
        sbatch_on_hpc()
        
        cmd = ' '.join(cmd_args[0]) if cmd_args else ''
        assert "node-bad-1" in cmd, "Config banned_nodes must be included"
        assert "node-bad-2" in cmd, "Config banned_nodes must be included"

    def test_merges_config_and_runtime_excludes(self, monkeypatch):
        """sbatch_on_hpc merges config banned_nodes with runtime exclude_nodes."""
        from hpc_batch import sbatch_on_hpc
        import hpc_batch as hb
        import subprocess
        
        cmd_args = []
        def fake_run(*args, **kwargs):
            nonlocal cmd_args
            cmd_args.append(args[0])
            mock = type('Mock', (), {'returncode': 0, 'stdout': 'Submitted batch job 1\n', 'stderr': ''})()
            return mock
        monkeypatch.setattr(hb, '_ssh_cmd', lambda host, user: ['ssh', 'fake'])
        monkeypatch.setattr(hb, '_get_hpc_config', lambda: {
            "host": "hpc-login", "user": "u", "dir": "~/hpc-agent",
            "partition": "gpu", "account": "acc", "qos": "defaultqos",
            "banned_nodes": ["node-bad-1"]})
        monkeypatch.setattr(hb, 'rsync_to_hpc', lambda db, cfg: True)
        monkeypatch.setattr(hb, '_recover_needs_rsync', lambda db: None)
        monkeypatch.setattr(subprocess, 'run', fake_run)
        
        sbatch_on_hpc(exclude_nodes=["node-keepwarm"])
        
        cmd = ' '.join(cmd_args[0]) if cmd_args else ''
        assert "node-bad-1" in cmd, "Config banned_nodes must be included"
        assert "node-keepwarm" in cmd, "Runtime exclude_nodes must be included"
        assert cmd.count("exclude=") == 1, "Single --exclude with merged list"

    def test_returns_empty_for_missing_script(self):
        """Non-existent script → empty list."""
        from hpc_batch import _parse_slurm_excludes
        excludes = _parse_slurm_excludes("slurm/nonexistent.slurm")
        assert excludes == []
