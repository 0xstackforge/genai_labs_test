from __future__ import annotations

import logging
import re
import sqlite3
import time
from pathlib import Path

from src.llm_client import OpenRouterLLMClient, build_default_llm_client, load_schema_from_db
from src.types import (
    SQLGenerationOutput,
    SQLValidationOutput,
    SQLExecutionOutput,
    AnswerGenerationOutput,
    PipelineOutput,
)

logger = logging.getLogger(__name__)


BASE_DIR = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = BASE_DIR / "data" / "gaming_mental_health.sqlite"


FORBIDDEN_KEYWORDS = frozenset([
    "drop", "delete", "truncate", "insert", "update", "alter", "create",
    "replace", "grant", "revoke", "exec", "execute", "attach", "detach",
])

def get_valid_columns(db_path: Path) -> frozenset[str]:
    """Load valid column names from database dynamically."""
    try:
        with sqlite3.connect(db_path) as conn:
            cur = conn.cursor()
            cur.execute("PRAGMA table_info(gaming_mental_health)")
            cols = cur.fetchall()
        return frozenset(col[1].lower() for col in cols)
    except Exception:
        return frozenset()


class SQLValidator:
    @classmethod
    def validate(cls, sql: str | None, db_path: Path | None = None, valid_columns: frozenset[str] | None = None) -> SQLValidationOutput:
        start = time.perf_counter()
        if valid_columns is None:
            valid_columns = get_valid_columns(db_path) if db_path else frozenset()

        if sql is None:
            return SQLValidationOutput(
                is_valid=False,
                validated_sql=None,
                error="No SQL provided",
                timing_ms=(time.perf_counter() - start) * 1000,
            )

        normalized = sql.strip()
        if not normalized:
            return SQLValidationOutput(
                is_valid=False,
                validated_sql=None,
                error="Empty SQL",
                timing_ms=(time.perf_counter() - start) * 1000,
            )

        # Must start with SELECT
        if not normalized.upper().lstrip().startswith("SELECT"):
            logger.warning(f"SQL rejected: does not start with SELECT")
            return SQLValidationOutput(
                is_valid=False,
                validated_sql=None,
                error="Only SELECT queries are allowed",
                timing_ms=(time.perf_counter() - start) * 1000,
            )

        # Check for forbidden keywords
        sql_lower = normalized.lower()
        tokens = set(re.findall(r'\b[a-z_]+\b', sql_lower))
        forbidden_found = tokens & FORBIDDEN_KEYWORDS
        if forbidden_found:
            logger.warning(f"SQL rejected: forbidden keywords {forbidden_found}")
            return SQLValidationOutput(
                is_valid=False,
                validated_sql=None,
                error=f"Forbidden SQL operations: {', '.join(sorted(forbidden_found))}",
                timing_ms=(time.perf_counter() - start) * 1000,
            )

        # Check for multiple statements (semicolon injection)
        statements = [s.strip() for s in normalized.split(";") if s.strip()]
        if len(statements) > 1:
            logger.warning("SQL rejected: multiple statements detected")
            return SQLValidationOutput(
                is_valid=False,
                validated_sql=None,
                error="Multiple SQL statements not allowed",
                timing_ms=(time.perf_counter() - start) * 1000,
            )

        # Remove trailing semicolon for clean execution
        validated = statements[0] if statements else normalized

        # Check for non-existent columns in FROM clause context
        # Extract identifiers that appear before FROM or after WHERE/ON (likely column refs)
        # Skip aliases (identifiers after AS)
        alias_pattern = re.compile(r'\bAS\s+([a-z_][a-z0-9_]*)', re.IGNORECASE)
        aliases = set(m.lower() for m in alias_pattern.findall(normalized))
        
        # Find identifiers that look like column references (before FROM, after WHERE/GROUP BY/ORDER BY)
        # This is a simplified check - we look for identifiers not in schema that aren't aliases
        from_idx = sql_lower.find(" from ")
        if from_idx > 0:
            select_clause = sql_lower[:from_idx]
            # Get identifiers in SELECT clause that aren't functions or aliases
            select_ids = set(re.findall(r'\b([a-z_][a-z0-9_]*)\b', select_clause))
            sql_keywords = {"select", "distinct", "as", "case", "when", "then", "else", "end",
                           "avg", "sum", "count", "min", "max", "round", "cast", "coalesce",
                           "iif", "ifnull", "nullif", "typeof", "abs", "length", "lower",
                           "upper", "trim", "substr", "total", "group_concat", "null",
                           "not", "and", "or", "between", "like", "in", "is", "asc", "desc",
                           "limit", "offset", "having", "order", "by", "group", "where",
                           "from", "join", "inner", "left", "outer", "on", "union", "all",
                           "integer", "real", "text", "float", "int", "varchar", "numeric"}
            potential_cols = select_ids - sql_keywords - aliases
            invalid_cols = potential_cols - valid_columns if valid_columns else set()
            # Filter short identifiers (likely aliases) and numeric literals
            invalid_cols = {c for c in invalid_cols if len(c) > 2 and not c.isdigit()}
            if invalid_cols:
                logger.warning(f"SQL rejected: unknown columns {invalid_cols}")
                return SQLValidationOutput(
                    is_valid=False,
                    validated_sql=None,
                    error=f"Unknown columns: {', '.join(sorted(invalid_cols))}",
                    timing_ms=(time.perf_counter() - start) * 1000,
                )

        logger.debug(f"SQL validated: {validated[:100]}...")
        return SQLValidationOutput(
            is_valid=True,
            validated_sql=validated,
            error=None,
            timing_ms=(time.perf_counter() - start) * 1000,
        )


class SQLiteExecutor:
    def __init__(self, db_path: str | Path = DEFAULT_DB_PATH) -> None:
        self.db_path = Path(db_path)

    def run(self, sql: str | None) -> SQLExecutionOutput:
        start = time.perf_counter()
        error = None
        rows = []
        row_count = 0

        if sql is None:
            return SQLExecutionOutput(
                rows=[],
                row_count=0,
                timing_ms=(time.perf_counter() - start) * 1000,
                error=None,
            )

        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cur = conn.cursor()
                cur.execute(sql)
                rows = [dict(r) for r in cur.fetchmany(100)]
                row_count = len(rows)
        except Exception as exc:
            error = str(exc)
            rows = []
            row_count = 0

        return SQLExecutionOutput(
            rows=rows,
            row_count=row_count,
            timing_ms=(time.perf_counter() - start) * 1000,
            error=error,
        )


class AnalyticsPipeline:
    def __init__(self, db_path: str | Path = DEFAULT_DB_PATH, llm_client: OpenRouterLLMClient | None = None) -> None:
        self.db_path = Path(db_path)
        self.llm = llm_client or build_default_llm_client()
        self.executor = SQLiteExecutor(self.db_path)
        self._schema = load_schema_from_db(str(self.db_path))
        self._valid_columns = get_valid_columns(self.db_path)

    def _empty_result(self, question: str, request_id: str | None, start: float, status: str, error_msg: str) -> PipelineOutput:
        """Build a PipelineOutput for early-exit cases (empty question, etc.)."""
        empty_llm = {"llm_calls": 0, "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0, "model": self.llm.model}
        elapsed = (time.perf_counter() - start) * 1000
        return PipelineOutput(
            status=status,
            question=question,
            request_id=request_id,
            sql_generation=SQLGenerationOutput(sql=None, timing_ms=0.0, llm_stats=dict(empty_llm), error=error_msg),
            sql_validation=SQLValidationOutput(is_valid=False, validated_sql=None, error=error_msg),
            sql_execution=SQLExecutionOutput(rows=[], row_count=0, timing_ms=0.0),
            answer_generation=AnswerGenerationOutput(answer=f"Error: {error_msg}", timing_ms=0.0, llm_stats=dict(empty_llm)),
            sql=None,
            rows=[],
            answer=f"Error: {error_msg}",
            timings={"sql_generation_ms": 0.0, "sql_validation_ms": 0.0, "sql_execution_ms": 0.0, "answer_generation_ms": 0.0, "total_ms": elapsed},
            total_llm_stats=dict(empty_llm),
        )

    def run(self, question: str, request_id: str | None = None) -> PipelineOutput:
        start = time.perf_counter()
        question = (question or "").strip()
        logger.info(f"Pipeline started: {question[:80]}{'...' if len(question) > 80 else ''}")

        # Early exit for empty questions
        if not question:
            return self._empty_result(question, request_id, start, "error", "Empty question provided")

        schema = self._schema

        # Stage 1: SQL Generation
        sql_gen_output = self.llm.generate_sql(question, {"schema": schema})
        sql = sql_gen_output.sql
        logger.debug(f"SQL generated: {sql[:100] if sql else 'None'}...")

        # Stage 2: SQL Validation
        validation_output = SQLValidator.validate(sql, valid_columns=self._valid_columns)
        if not validation_output.is_valid:
            sql = None
        else:
            sql = validation_output.validated_sql

        # Stage 3: SQL Execution (with one retry on syntax error)
        execution_output = self.executor.run(sql)
        if execution_output.error and sql is not None:
            logger.warning(f"SQL execution failed: {execution_output.error[:100]}. Regenerating...")
            sql_gen_output2 = self.llm.generate_sql(question, {"schema": schema})
            if sql_gen_output2.sql:
                validation_output2 = SQLValidator.validate(sql_gen_output2.sql, valid_columns=self._valid_columns)
                if validation_output2.is_valid:
                    execution_output2 = self.executor.run(validation_output2.validated_sql)
                    if not execution_output2.error:
                        sql = validation_output2.validated_sql
                        validation_output = validation_output2
                        execution_output = execution_output2
                        # Merge LLM stats from retry
                        for key in ("llm_calls", "prompt_tokens", "completion_tokens", "total_tokens"):
                            sql_gen_output.llm_stats[key] = sql_gen_output.llm_stats.get(key, 0) + sql_gen_output2.llm_stats.get(key, 0)
                        sql_gen_output.timing_ms += sql_gen_output2.timing_ms

        rows = execution_output.rows

        # Stage 4: Answer Generation
        answer_output = self.llm.generate_answer(question, sql, rows)

        # Determine status
        status = "success"
        if sql_gen_output.error == "unanswerable":
            status = "unanswerable"
        elif sql_gen_output.sql is None and sql_gen_output.error:
            status = "error"
        elif not validation_output.is_valid:
            status = "invalid_sql"
        elif execution_output.error:
            status = "error"
        elif sql is None:
            status = "unanswerable"

        # Build timings aggregate
        timings = {
            "sql_generation_ms": sql_gen_output.timing_ms,
            "sql_validation_ms": validation_output.timing_ms,
            "sql_execution_ms": execution_output.timing_ms,
            "answer_generation_ms": answer_output.timing_ms,
            "total_ms": (time.perf_counter() - start) * 1000,
        }

        # Build total LLM stats
        total_llm_stats = {
            "llm_calls": sql_gen_output.llm_stats.get("llm_calls", 0) + answer_output.llm_stats.get("llm_calls", 0),
            "prompt_tokens": sql_gen_output.llm_stats.get("prompt_tokens", 0) + answer_output.llm_stats.get("prompt_tokens", 0),
            "completion_tokens": sql_gen_output.llm_stats.get("completion_tokens", 0) + answer_output.llm_stats.get("completion_tokens", 0),
            "total_tokens": sql_gen_output.llm_stats.get("total_tokens", 0) + answer_output.llm_stats.get("total_tokens", 0),
            "model": sql_gen_output.llm_stats.get("model", "unknown"),
        }

        logger.info(f"Pipeline completed: status={status}, total_ms={timings['total_ms']:.1f}, tokens={total_llm_stats['total_tokens']}")

        return PipelineOutput(
            status=status,
            question=question,
            request_id=request_id,
            sql_generation=sql_gen_output,
            sql_validation=validation_output,
            sql_execution=execution_output,
            answer_generation=answer_output,
            sql=sql,
            rows=rows,
            answer=answer_output.answer,
            timings=timings,
            total_llm_stats=total_llm_stats,
        )