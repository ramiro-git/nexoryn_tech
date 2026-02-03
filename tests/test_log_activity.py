import unittest

from desktop_app.database import Database


class _FailingConnectionContext:
    def __enter__(self):
        raise RuntimeError("simulated connection failure")

    def __exit__(self, exc_type, exc, tb):
        return False


class _FailingPool:
    def connection(self):
        return _FailingConnectionContext()


class LogActivityTests(unittest.TestCase):
    def test_log_activity_logs_error_without_raising(self):
        db = Database.__new__(Database)
        db.is_closing = False
        db.pool = _FailingPool()
        db.current_user_id = 123
        db.current_ip = "127.0.0.1"

        with self.assertLogs("desktop_app.database", level="ERROR") as captured:
            db.log_activity("ENTIDAD", "ACCION", id_entidad=1, resultado="OK", detalle={"k": "v"})

        self.assertTrue(
            any("Error logging activity" in msg for msg in captured.output),
            "Expected an error log entry when log_activity fails.",
        )


if __name__ == "__main__":
    unittest.main()
