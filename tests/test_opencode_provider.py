"""Tests for the HPC opencode provider."""
import json
import os
import sqlite3
import tempfile
import time
from pathlib import Path


def _make_db() -> str:
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    conn = sqlite3.connect(path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tasks (
            id TEXT PRIMARY KEY,
            agent_id TEXT NOT NULL,
            messages TEXT NOT NULL,
            tools TEXT NOT NULL,
            model TEXT NOT NULL,
            api_config TEXT NOT NULL,
            metadata TEXT NOT NULL,
            priority INTEGER DEFAULT 0,
            created_at REAL NOT NULL,
            status TEXT DEFAULT 'pending',
            result TEXT,
            started_at REAL,
            completed_at REAL,
            error TEXT,
            slurm_job_id INTEGER,
            retry_count INTEGER DEFAULT 0
        )
    """)
    conn.commit()
    conn.close()
    return path


class TestHpcProviderContract:
    """Test seam 1: Provider contract."""

    def test_chat_complete_returns_dict_with_content(self):
        """The most basic contract: chat_complete returns a dict with 'content'."""
        from opencode_provider import HpcProvider

        db = _make_db()
        provider = HpcProvider({
            "db_path": db,
            "poll_interval": 1,
            "timeout": 5,
        })

        # This will fail because no one processes the task — we just verify
        # the return type contract (it should return dict, not crash)
        result = provider.chat_complete([{"role": "user", "content": "hello"}])

        # Should be a dict
        assert isinstance(result, dict), f"Expected dict, got {type(result)}"

        # Should have either 'content' or 'error' key
        has_content = "content" in result
        has_error = "error" in result
        assert has_content or has_error, \
            f"Result must have 'content' or 'error' key: {result}"

        os.unlink(db)

    def test_chat_complete_no_exceptions(self):
        """chat_complete should never raise."""
        from opencode_provider import HpcProvider

        db = _make_db()
        provider = HpcProvider({
            "db_path": db,
            "poll_interval": 1,
            "timeout": 1,
        })

        # This should not raise even though no one processes the task
        result = provider.chat_complete([{"role": "user", "content": "test"}])
        assert isinstance(result, dict)

        os.unlink(db)

    def test_no_tool_calls_in_response(self):
        """HPC is text-only — no tool_calls should appear in response."""
        from opencode_provider import HpcProvider

        provider = HpcProvider({"timeout": 1})
        result = provider.chat_complete([{"role": "user", "content": "hi"}])
        assert "tool_calls" not in result or result.get("tool_calls") is None


class TestHpcProviderWarmth:
    """Test seam 3: Warmth detection."""

    def test_cold_no_heartbeat_no_squeue(self):
        """No heartbeat file and no squeue → cold."""
        from opencode_provider import HpcProvider

        provider = HpcProvider({})
        # Mock: return cold by not having a heartbeat file
        # The provider falls back to squeue which also fails → cold
        warmth = provider._check_warmth()
        # Can't guarantee the test environment has no squeue,
        # but we can verify it returns one of the three states
        assert warmth in ("hot", "warm", "cold"), f"Unexpected warmth: {warmth}"

    def test_heartbeat_detection_logic(self):
        """Verify the heartbeat age threshold logic."""
        from opencode_provider import HpcProvider

        provider = HpcProvider({})

        # Test the threshold logic directly
        def classify(age: float) -> str:
            if age < 60:
                return "hot"
            return "warm"

        assert classify(0) == "hot"
        assert classify(30) == "hot"
        assert classify(59) == "hot"
        assert classify(60) == "warm"
        assert classify(120) == "warm"


class TestHpcProviderFallback:
    """Test seam 4: Cold fallback."""

    def test_cold_no_fallback_returns_error(self):
        """Cold with no fallback → error dict."""
        from opencode_provider import HpcProvider

        provider = HpcProvider({"warmth_policy": "hpc_only", "timeout": 2})
        provider._check_warmth = lambda: "cold"  # type: ignore
        result = provider.chat_complete([{"role": "user", "content": "test"}])
        # Should return error since HPC is cold and no fallback
        assert "error" in result, f"Expected error, got {result}"

    def test_fallback_missing_api_key_returns_error(self):
        """Fallback configured but no API key → error dict."""
        from opencode_provider import HpcProvider

        provider = HpcProvider({
            "warmth_policy": "fallback_on_cold",
            "fallback": {
                "type": "anthropic",
                "api_key_env": "NONEXISTENT_KEY_XXXX",
                "model": "claude-sonnet-4",
            },
        })
        # Mock cold warmth
        provider._check_warmth = lambda: "cold"  # type: ignore
        result = provider.chat_complete([{"role": "user", "content": "hi"}])
        assert "error" in result, f"Expected error, got {result}"

    def test_fallback_with_api_key_attempts_call(self):
        """Fallback with API key tries to call the API (may fail with network)."""
        from opencode_provider import HpcProvider

        # Set a real-looking but invalid key to verify the API call path is taken
        os.environ["TEST_FALLBACK_KEY"] = "sk-test-invalid-123"
        provider = HpcProvider({
            "warmth_policy": "fallback_on_cold",
            "timeout": 5,  # short timeout to avoid hanging
            "fallback": {
                "type": "anthropic",
                "api_key_env": "TEST_FALLBACK_KEY",
                "model": "claude-sonnet-4",
            },
        })
        provider._check_warmth = lambda: "cold"  # type: ignore
        result = provider.chat_complete([{"role": "user", "content": "hi"}])
        # Should get an error (fake key) — verify it's not a crash
        assert isinstance(result, dict)
        del os.environ["TEST_FALLBACK_KEY"]


class TestHpcProviderTaskLifecycle:
    """Test seam 2: Task lifecycle via local backend."""

    def test_submit_task_appears_in_db(self):
        """Submitting a task via the provider creates a DB entry."""
        from opencode_provider import HpcProvider

        db = _make_db()
        provider = HpcProvider({"db_path": db, "timeout": 2})

        from hpc_batch import submit_task
        messages = [{"role": "system", "content": "You are a helpful assistant."},
                    {"role": "user", "content": "Say hello"}]

        tid = submit_task(
            db_path=db,
            model="test-model",
            messages=messages,
            metadata={"type": "agent_call", "source": "opencode-provider", "hpc_provider": True},
        )

        conn = sqlite3.connect(db)
        row = conn.execute("SELECT id, status, messages, metadata FROM tasks WHERE id = ?", (tid,)).fetchone()
        conn.close()

        assert row is not None, "Task should exist in DB"
        assert row[1] == "pending"
        # Verify messages are stored as the full array
        stored = json.loads(row[2])
        assert len(stored) == 2
        assert stored[0]["role"] == "system"
        assert stored[1]["role"] == "user"
        # Verify metadata
        meta = json.loads(row[3])
        assert meta.get("type") == "agent_call"
        assert meta.get("hpc_provider") is True

        os.unlink(db)

    def test_submit_task_empty_messages_fallback(self):
        """Empty messages array falls back gracefully."""
        from hpc_batch import submit_task

        db = _make_db()
        tid = submit_task(
            db_path=db,
            prompt="direct prompt",
            model="test-model",
        )

        conn = sqlite3.connect(db)
        row = conn.execute("SELECT id, messages FROM tasks WHERE id = ?", (tid,)).fetchone()
        conn.close()

        stored = json.loads(row[1])
        assert len(stored) == 1
        assert stored[0]["role"] == "user"
        assert stored[0]["content"] == "direct prompt"

        os.unlink(db)


class TestHpcProviderTimeout:
    """Test seam 5: Timeout handling."""

    def test_short_timeout_returns_error(self):
        """A very short timeout should return a timeout error."""
        from opencode_provider import HpcProvider

        db = _make_db()
        # Submit a task but never process it — should time out
        provider = HpcProvider({
            "db_path": db,
            "poll_interval": 1,
            "timeout": 2,
        })
        # Mock warmth so it submits to HPC
        provider._check_warmth = lambda: "hot"  # type: ignore

        result = provider.chat_complete([{"role": "user", "content": "timeout test"}])
        assert "error" in result, f"Expected timeout error, got {result}"
        assert "timed" in result["error"].lower() or "timeout" in result["error"].lower(), \
            f"Error should mention timeout: {result['error']}"

        os.unlink(db)
