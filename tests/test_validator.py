"""Unit tests for SQL validation logic."""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.pipeline import SQLValidator


class TestSQLValidator(unittest.TestCase):
    def test_valid_select(self):
        result = SQLValidator.validate("SELECT * FROM gaming_mental_health")
        self.assertTrue(result.is_valid)
        self.assertEqual(result.validated_sql, "SELECT * FROM gaming_mental_health")
        self.assertIsNone(result.error)

    def test_valid_select_with_where(self):
        sql = "SELECT gender, AVG(addiction_level) FROM gaming_mental_health GROUP BY gender"
        result = SQLValidator.validate(sql)
        self.assertTrue(result.is_valid)
        self.assertEqual(result.validated_sql, sql)

    def test_valid_select_with_db_path(self):
        # Test with db_path for dynamic column validation
        from pathlib import Path
        db_path = Path(__file__).resolve().parents[1] / "data" / "gaming_mental_health.sqlite"
        sql = "SELECT gender, age FROM gaming_mental_health"
        result = SQLValidator.validate(sql, db_path)
        self.assertTrue(result.is_valid)

    def test_rejects_none(self):
        result = SQLValidator.validate(None)
        self.assertFalse(result.is_valid)
        self.assertIn("No SQL", result.error)

    def test_rejects_empty(self):
        result = SQLValidator.validate("")
        self.assertFalse(result.is_valid)
        self.assertIn("Empty", result.error)

    def test_rejects_delete(self):
        result = SQLValidator.validate("DELETE FROM gaming_mental_health")
        self.assertFalse(result.is_valid)
        self.assertIn("SELECT", result.error)

    def test_rejects_drop(self):
        result = SQLValidator.validate("DROP TABLE gaming_mental_health")
        self.assertFalse(result.is_valid)
        self.assertIn("SELECT", result.error)

    def test_rejects_insert(self):
        result = SQLValidator.validate("INSERT INTO gaming_mental_health VALUES (1)")
        self.assertFalse(result.is_valid)
        self.assertIn("SELECT", result.error)

    def test_rejects_update(self):
        result = SQLValidator.validate("UPDATE gaming_mental_health SET gender='X'")
        self.assertFalse(result.is_valid)
        self.assertIn("SELECT", result.error)

    def test_rejects_select_with_delete_keyword(self):
        # SELECT that contains DELETE as a keyword (injection attempt)
        result = SQLValidator.validate("SELECT * FROM t; DELETE FROM t")
        self.assertFalse(result.is_valid)
        # May fail on forbidden keyword or multiple statements - either is correct
        self.assertTrue("delete" in result.error.lower() or "Multiple" in result.error)

    def test_rejects_multiple_statements(self):
        result = SQLValidator.validate("SELECT 1; SELECT 2")
        self.assertFalse(result.is_valid)
        self.assertIn("Multiple", result.error)

    def test_strips_trailing_semicolon(self):
        result = SQLValidator.validate("SELECT * FROM gaming_mental_health;")
        self.assertTrue(result.is_valid)
        self.assertEqual(result.validated_sql, "SELECT * FROM gaming_mental_health")

    def test_timing_is_recorded(self):
        result = SQLValidator.validate("SELECT 1")
        self.assertGreaterEqual(result.timing_ms, 0.0)


if __name__ == "__main__":
    unittest.main()
