from __future__ import annotations

import json
import logging
import os
import re
import time
from typing import Any

from src.types import SQLGenerationOutput, AnswerGenerationOutput

logger = logging.getLogger(__name__)

DEFAULT_MODEL = "openai/gpt-5-nano"

TABLE_SCHEMA = "Table: gaming_mental_health (schema unavailable - pass db_path)"


def load_schema_from_db(db_path: str) -> str:
    """Load enriched table schema from SQLite database with value ranges."""
    import sqlite3
    try:
        with sqlite3.connect(db_path) as conn:
            cur = conn.cursor()
            cur.execute("PRAGMA table_info(gaming_mental_health)")
            cols = cur.fetchall()
            if not cols:
                return "Table: gaming_mental_health (schema unavailable)"

            row_count = cur.execute("SELECT COUNT(*) FROM gaming_mental_health").fetchone()[0]
            lines = [f"Table: gaming_mental_health ({row_count} rows)", "Columns:"]
            for col in cols:
                col_name, col_type = col[1], col[2]
                if col_type in ("REAL", "INTEGER"):
                    row = cur.execute(
                        f'SELECT MIN("{col_name}"), MAX("{col_name}") FROM gaming_mental_health'
                    ).fetchone()
                    lines.append(f"- {col_name} ({col_type}, range {row[0]}-{row[1]})")
                else:
                    vals = [r[0] for r in cur.execute(
                        f'SELECT DISTINCT "{col_name}" FROM gaming_mental_health LIMIT 10'
                    ).fetchall()]
                    lines.append(f"- {col_name} ({col_type}, values: {vals})")
        return "\n".join(lines)
    except Exception:
        return "Table: gaming_mental_health (schema unavailable)"


class OpenRouterLLMClient:
    """LLM client using the OpenRouter SDK for chat completions."""

    provider_name = "openrouter"

    def __init__(self, api_key: str, model: str | None = None) -> None:
        try:
            from openrouter import OpenRouter
        except ModuleNotFoundError as exc:
            raise RuntimeError("Missing dependency: install 'openrouter'.") from exc
        self.model = model or os.getenv("OPENROUTER_MODEL", DEFAULT_MODEL)
        self._client = OpenRouter(api_key=api_key)
        self._stats = {"llm_calls": 0, "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}

    def _chat(self, messages: list[dict[str, str]], temperature: float, max_tokens: int) -> str:
        res = self._client.chat.send(
            messages=messages,
            model=self.model,
            temperature=temperature,
            max_tokens=max_tokens,
            stream=False,
        )

        self._stats["llm_calls"] += 1
        usage = getattr(res, "usage", None)
        if usage:
            self._stats["prompt_tokens"] += int(getattr(usage, "prompt_tokens", 0) or 0)
            self._stats["completion_tokens"] += int(getattr(usage, "completion_tokens", 0) or 0)
            self._stats["total_tokens"] += int(getattr(usage, "total_tokens", 0) or 0)

        choices = getattr(res, "choices", None) or []
        if not choices:
            raise RuntimeError("OpenRouter response contained no choices.")
        content = getattr(getattr(choices[0], "message", None), "content", None)
        if not isinstance(content, str):
            raise RuntimeError("OpenRouter response content is not text.")
        return content.strip()

    @staticmethod
    def _extract_sql(text: str) -> tuple[str | None, bool]:
        """Extract SQL from LLM response. Returns (sql, is_unanswerable)."""
        text = text.strip()
        
        # Check for explicit unanswerable response
        if "UNANSWERABLE" in text.upper():
            return None, True
        
        # Remove markdown code fences
        if "```" in text:
            # Extract content between code fences
            match = re.search(r"```(?:sql)?\s*(.*?)```", text, re.DOTALL | re.IGNORECASE)
            if match:
                text = match.group(1).strip()
            else:
                # Remove leading/trailing fences
                text = re.sub(r"^```(?:sql)?", "", text, flags=re.IGNORECASE).strip()
                text = re.sub(r"```$", "", text).strip()
        
        # Try JSON format
        if text.startswith("{") and text.endswith("}"):
            try:
                parsed = json.loads(text)
                sql = parsed.get("sql")
                if isinstance(sql, str) and sql.strip():
                    return sql.strip(), False
            except json.JSONDecodeError:
                pass
        
        # Find SQL statement (SELECT, DELETE, INSERT, UPDATE, DROP, etc.)
        # We extract non-SELECT statements too so the validator can reject them properly
        lower = text.lower()
        sql_starts = ["select ", "delete ", "insert ", "update ", "drop ", "alter ", "create ", "truncate "]
        best_idx = -1
        for keyword in sql_starts:
            idx = lower.find(keyword)
            if idx >= 0 and (best_idx < 0 or idx < best_idx):
                best_idx = idx
        if best_idx >= 0:
            sql = text[best_idx:].strip()
            if ";" in sql:
                sql = sql[:sql.index(";")]
            return (sql.strip(), False) if sql.strip() else (None, False)
        
        return None, False

    def generate_sql(self, question: str, context: dict) -> SQLGenerationOutput:
        schema = context.get("schema", TABLE_SCHEMA)
        system_prompt = (
            "You are a SQLite SQL generator for a gaming and mental health survey dataset.\n"
            "RULES:\n"
            "1. Output ONLY the SQL query — no explanation, no markdown\n"
            "2. Use ONLY the columns listed in the schema\n"
            "3. Output UNANSWERABLE only if the question requires columns that do not exist in the schema\n"
            "4. For grouping/bucketing numeric columns, use CASE WHEN or ROUND expressions\n"
            "5. For 'top N' questions, always use ORDER BY ... DESC/ASC with LIMIT N\n"
            "6. For proportions/shares, use COUNT with conditions divided by total COUNT\n"
        )
        user_prompt = f"Schema:\n{schema}\n\nQuestion: {question}\n\nSQL:"

        start = time.perf_counter()
        error = None
        sql = None
        is_unanswerable = False
        intermediate_outputs: list[dict[str, Any]] = []

        try:
            text = self._chat(
                messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}],
                temperature=0.0,
                max_tokens=300,
            )
            intermediate_outputs.append({"attempt": 1, "raw_response": text[:200]})
            sql, is_unanswerable = self._extract_sql(text)

            # Retry once if LLM returned empty (not explicit UNANSWERABLE) — rephrase with hint
            if sql is None and not is_unanswerable and not error:
                logger.info("SQL generation retry: first attempt returned no SQL")
                retry_prompt = (
                    f"Schema:\n{schema}\n\n"
                    f"Question: {question}\n\n"
                    "The question IS answerable with the schema above. "
                    "Use SQL aggregate functions (AVG, COUNT, SUM, MIN, MAX), "
                    "GROUP BY, CASE WHEN for bucketing, ROUND for rounding, "
                    "and ORDER BY with LIMIT as needed.\n\nSQL:"
                )
                text2 = self._chat(
                    messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": retry_prompt}],
                    temperature=0.0,
                    max_tokens=300,
                )
                intermediate_outputs.append({"attempt": 2, "raw_response": text2[:200]})
                sql2, is_unanswerable2 = self._extract_sql(text2)
                if sql2:
                    sql = sql2
                    is_unanswerable = False
                else:
                    is_unanswerable = is_unanswerable or is_unanswerable2
        except Exception as exc:
            error = str(exc)

        timing_ms = (time.perf_counter() - start) * 1000
        llm_stats = self.pop_stats()
        llm_stats["model"] = self.model

        if is_unanswerable and not error:
            error = "unanswerable"

        return SQLGenerationOutput(
            sql=sql,
            timing_ms=timing_ms,
            llm_stats=llm_stats,
            intermediate_outputs=intermediate_outputs,
            error=error,
        )

    def generate_answer(self, question: str, sql: str | None, rows: list[dict[str, Any]]) -> AnswerGenerationOutput:
        empty_stats = {"llm_calls": 0, "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0, "model": self.model}
        if not sql:
            return AnswerGenerationOutput(
                answer="I cannot answer this with the available table and schema. Please rephrase using known survey fields.",
                timing_ms=0.0,
                llm_stats=dict(empty_stats),
                error=None,
            )
        if not rows:
            return AnswerGenerationOutput(
                answer="Query executed, but no rows were returned.",
                timing_ms=0.0,
                llm_stats=dict(empty_stats),
                error=None,
            )

        system_prompt = (
            "You are a concise data analytics assistant. "
            "Answer ONLY based on the provided query results. "
            "Include specific numbers from the data. Do not invent data."
        )
        user_prompt = (
            f"Question:\n{question}\n\nSQL:\n{sql}\n\n"
            f"Results ({len(rows)} rows):\n{json.dumps(rows[:50], ensure_ascii=True)}\n\n"
            "Provide a clear, concise answer with specific numbers."
        )

        start = time.perf_counter()
        error = None
        answer = ""

        try:
            answer = self._chat(
                messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}],
                temperature=0.2,
                max_tokens=220,
            )
        except Exception as exc:
            error = str(exc)
            answer = f"Error generating answer: {error}"

        timing_ms = (time.perf_counter() - start) * 1000
        llm_stats = self.pop_stats()
        llm_stats["model"] = self.model

        return AnswerGenerationOutput(
            answer=answer,
            timing_ms=timing_ms,
            llm_stats=llm_stats,
            error=error,
        )

    def pop_stats(self) -> dict[str, Any]:
        out = dict(self._stats or {})
        self._stats = {"llm_calls": 0, "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
        return out


def build_default_llm_client() -> OpenRouterLLMClient:
    api_key = os.getenv("OPENROUTER_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("OPENROUTER_API_KEY is required.")
    return OpenRouterLLMClient(api_key=api_key)
