from contextlib import contextmanager
from . import _constants as c
from ._query import WatermarkQuery
from ._admin import _esc
from ._config import WatermarkConfig


def _to_iso(value) -> str:
    """Returns an ISO-format string for any datetime-like value."""
    if hasattr(value, 'strftime'):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return str(value)


def _write_state(spark, source: str, table: str,
                 watermark, ok: bool, error: str,
                 clear_next_run_mode: bool = False):
    """
    Internal: MERGE watermark state back into the control table in ONE transaction.
    """
    wm_sql  = f"TIMESTAMP('{_to_iso(watermark)}')" if watermark else "NULL"
    err_sql = "NULL" if not error else f"'{_esc(str(error)[:500])}'"
    next_run_sql = "NULL" if clear_next_run_mode else f"t.{c.COL_NEXT_RUN_MODE}"

    spark.sql(f"""
        MERGE INTO {c.TABLE_NAME} AS t
        USING (
            SELECT
                '{_esc(source)}'    AS {c.COL_SOURCE},
                '{_esc(table)}'     AS {c.COL_TABLE_NAME},
                {wm_sql}            AS {c.COL_WATERMARK_LAST},
                CURRENT_TIMESTAMP() AS {c.COL_LAST_RUN_AT},
                {str(ok).upper()}   AS {c.COL_LAST_RUN_OK},
                {err_sql}           AS {c.COL_LAST_ERROR},
                CURRENT_TIMESTAMP() AS {c.COL_UPDATED_AT}
        ) AS s
        ON t.{c.COL_SOURCE} = s.{c.COL_SOURCE} AND t.{c.COL_TABLE_NAME} = s.{c.COL_TABLE_NAME}
        WHEN MATCHED THEN UPDATE SET
            t.{c.COL_WATERMARK_LAST} = s.{c.COL_WATERMARK_LAST},
            t.{c.COL_NEXT_RUN_MODE}  = {next_run_sql},
            t.{c.COL_LAST_RUN_AT}    = s.{c.COL_LAST_RUN_AT},
            t.{c.COL_LAST_RUN_OK}    = s.{c.COL_LAST_RUN_OK},
            t.{c.COL_LAST_ERROR}     = s.{c.COL_LAST_ERROR},
            t.{c.COL_UPDATED_AT}     = s.{c.COL_UPDATED_AT}
    """)


@contextmanager
def watermark_session(spark, source: str = None, table: str = None, config: WatermarkConfig = None):
    """
    Context manager that handles the full watermark lifecycle.
    """
    if config:
        cfg = config
        source = cfg.source
        table = cfg.table_name
    else:
        cfg = WatermarkQuery(spark).source(source).table(table).active_only().one()

    # ── SKIP mode: yield and exit without writing anything ───
    if not cfg.should_run:
        print(f"[SKIP] {source}/{table}: effective mode is SKIP. No watermark written.")
        yield cfg
        return

    # ── Normal / FULL_RELOAD mode ────────────────────────────
    try:
        yield cfg

        # ── Success: one single MERGE updates state + clears NEXT_RUN_MODE ──
        _write_state(
            spark, source, table,
            watermark=cfg._new_watermark,
            ok=True,
            error=None,
            clear_next_run_mode=bool(cfg.next_run_mode),
        )
        print(
            f"[OK] {source}/{table}: watermark → {cfg._new_watermark}. "
            f"Mode was: {cfg.effective_mode}."
        )

    except Exception as exc:
        # ── Error: keep old watermark, record error — still one MERGE ──────
        _write_state(
            spark, source, table,
            watermark=cfg.watermark_last,
            ok=False,
            error=str(exc),
            clear_next_run_mode=False,
        )
        print(f"[ERROR] {source}/{table}: {exc}. Watermark NOT advanced.")
        raise
