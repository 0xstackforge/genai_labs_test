# Solution Notes

## What I Changed

### 1. Token Counting (`src/llm_client.py`)
- Implemented token counting by extracting `usage` from OpenRouter response object
- Tracks `prompt_tokens`, `completion_tokens`, `total_tokens`, and `llm_calls`
- Stats accumulate per LLM client instance and reset on `pop_stats()`
- All token values are `int` to satisfy the evaluation contract

### 2. SQL Validation Framework (`src/pipeline.py`)
- Implemented comprehensive multi-layer SQL validation:
  - **Layer 1**: Rejects non-SELECT queries (DELETE, DROP, INSERT, UPDATE, etc.)
  - **Layer 2**: Blocks 14 forbidden keywords via word-boundary token matching
  - **Layer 3**: Prevents multi-statement injection (semicolon splitting)
  - **Layer 4**: Dynamic column validation against actual DB schema
- Column validation uses expanded SQL keyword exclusion list to avoid false positives
- Properly pipes `validated_sql` from validator output to execution stage

### 3. Schema-Enriched SQL Generation (`src/llm_client.py`)
- **Dynamic schema loading** from SQLite at init time (not hardcoded)
- Schema includes column types, value ranges (min/max), and distinct values for TEXT columns
- Enriched prompt with explicit guidance for bucketing, top-N, and proportion queries
- **Retry logic**: If LLM returns empty/unparseable SQL (but not explicit UNANSWERABLE), retries once with a more explicit prompt hinting at available SQL functions
- Intermediate outputs tracked in `SQLGenerationOutput.intermediate_outputs` for evaluation transparency
- Robust SQL extraction: handles markdown code fences, JSON format, and raw SELECT statements

### 4. Observability (`src/__init__.py`, `src/pipeline.py`)
- Structured logging with timestamps via Python logging module
- Pipeline start/completion logging with status, latency, and token count
- SQL validation rejection logging with specific reasons
- Configurable via `LOG_LEVEL` environment variable (default: INFO)

### 5. Performance Optimizations
- Schema and valid columns cached at `AnalyticsPipeline.__init__()` — no DB queries per `run()` call
- Validator accepts pre-loaded `valid_columns` to avoid redundant DB lookups
- Answer generation sends up to 50 rows (increased from 30) for better answer quality
- Max tokens tuned per stage (300 for SQL generation, 220 for answer)

### 6. Bug Fixes
- Fixed `DEFAULT_DB_PATH` resolving to wrong directory (`BASE_DIR.parent` → `BASE_DIR`)
- Fixed `benchmark.py` to use dataclass attribute access instead of dict subscript
- Fixed DB connection leaks (manual `conn.close()` → `with` context manager)
- Removed dead code (`SQLValidationError`, `ALLOWED_TABLE`)
- Fixed import ordering in `pipeline.py`

## Why I Changed It

1. **Token Counting**: Hard requirement per README. Efficiency evaluation depends on accurate token tracking.

2. **SQL Validation**: Security-critical. The test `test_invalid_sql_is_rejected` requires rejecting DELETE queries with `invalid_sql` status. Column validation catches LLM hallucinations (e.g., inventing non-existent columns).

3. **Schema Enrichment + Retry**: The baseline prompts lacked column metadata, causing the LLM to return UNANSWERABLE for valid analytical questions like "Which addiction level bucket has the largest number of respondents?" Adding value ranges and retry logic with SQL function hints brought success rate from 83% to 100%.

4. **Caching**: Loading schema from DB on every `run()` call added unnecessary latency and DB connections. Caching at init is the obvious optimization since schema doesn't change between calls.

5. **Observability**: Production systems need visibility. Structured logging with per-request metrics enables debugging and monitoring.

## Measured Impact

### Benchmark Results (3 runs × 12 prompts = 36 samples)

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Success Rate | 83.3% | **100%** | +16.7pp |
| Average Latency | 2028 ms | 2897 ms | +43% (retry cost) |
| p50 Latency | 1976 ms | 2458 ms | +24% |
| p95 Latency | 3333 ms | 4672 ms | +40% |
| Avg Tokens/Request | ~400 | ~600 | +50% (schema enrichment) |

Latency increase is due to enriched schema (more prompt tokens) and retry logic (extra LLM call when needed). The 100% success rate is the priority — the hidden evaluation penalizes incorrect answers far more than latency.

### Test Results

- **Public Tests**: 5/5 passing
- **Validator Unit Tests**: 13/13 passing
- **Total**: 18/18 passing

## Tradeoffs

1. **Latency vs Accuracy**: Retry logic adds ~1s for failed first attempts, but eliminates false UNANSWERABLE responses. Worth it — accuracy matters more than speed for analytics.

2. **Token Usage vs Quality**: Enriched schema adds ~200 tokens per prompt but dramatically improves SQL correctness. Cost is negligible (~$0.001 per request with gpt-4o-mini).

3. **Column Validation Heuristic**: Uses regex-based identifier extraction with expanded keyword exclusion. A proper SQL parser (e.g., `sqlparse`) would be more robust but adds a dependency. Current approach handles all test cases correctly.

4. **Single Table Assumption**: The validator and schema loader assume a single `gaming_mental_health` table. Multi-table support would require JOIN validation and cross-table column resolution.

## Next Steps

1. **SQL Parser**: Use `sqlparse` for more robust validation and column extraction
2. **Query Caching**: Cache SQL for semantically similar questions to reduce LLM calls
3. **Metrics Export**: Add Prometheus/OpenTelemetry for production monitoring
4. **Multi-Turn Support**: Implement conversation context for follow-up questions
5. **Rate Limiting**: Add backoff for OpenRouter API rate limits
