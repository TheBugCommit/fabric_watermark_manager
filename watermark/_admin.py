from . import _constants as c

def _esc(value: str) -> str:
    """Escapes a string value for safe embedding in a SQL literal."""
    return value.replace("'", "''")

class WatermarkAdmin:
    """
    Administrative operations on Watermark_Control.
    """

    def __init__(self, spark):
        self._spark = spark

    def _audit(self) -> str:
        """SQL snippet that stamps UPDATED_AT."""
        return f"{c.COL_UPDATED_AT} = CURRENT_TIMESTAMP()"

    def _where(self, conditions: dict) -> str:
        """
        Builds a safe WHERE clause dynamically from a dictionary of conditions.
        All user-supplied string values are escaped.
        """
        parts = []
        for col, val in conditions.items():
            if val is not None:
                parts.append(f"{col} = '{_esc(str(val))}'")
        
        return ("WHERE " + " AND ".join(parts)) if parts else ""

    def _sql(self, stmt: str):
        self._spark.sql(stmt)

    # ── Internal Update Helper ───────────────────────────────

    def _update_fields(self, updates: dict, source: str = None, area: str = None, table: str = None, extra_conditions: dict = None):
        if not any([source, area, table]):
            raise ValueError("Must provide at least one of: source, area, table")
        
        # Build SET clause (escaping values if they are strings, keeping NULLs)
        set_parts = []
        for col, val in updates.items():
            if val is None:
                set_parts.append(f"{col} = NULL")
            elif isinstance(val, bool):
                set_parts.append(f"{col} = {'true' if val else 'false'}")
            else:
                set_parts.append(f"{col} = '{_esc(str(val))}'")
        
        set_clause = ", ".join(set_parts)
        
        # Build WHERE clause dynamically
        where_conditions = {c.COL_SOURCE: source, c.COL_AREA: area, c.COL_TABLE_NAME: table}
        if extra_conditions:
            where_conditions.update(extra_conditions)

        self._sql(
            f"UPDATE {c.TABLE_NAME} "
            f"SET {set_clause}, {self._audit()} "
            f"{self._where(where_conditions)}"
        )

    # ── Activate / Deactivate ────────────────────────────────

    def activate(self, source: str = None, area: str = None, table: str = None):
        """Activates tables matching the provided criteria."""
        self._update_fields({c.COL_ACTIVE: True}, source, area, table)

    def deactivate(self, source: str = None, area: str = None, table: str = None):
        """Deactivates tables matching the provided criteria."""
        self._update_fields({c.COL_ACTIVE: False}, source, area, table)

    # ── Load mode (permanent) ────────────────────────────────

    def set_mode(self, mode: str, source: str = None, area: str = None, table: str = None):
        if mode not in c.VALID_MODES:
            raise ValueError(f"Invalid mode '{mode}'. Must be one of: {c.VALID_MODES}")
        self._update_fields({c.COL_LOAD_MODE: mode}, source, area, table)

    # ── One-time overrides ───────────────────────────────────

    def force_full_reload(self, source: str = None, area: str = None, table: str = None):
        self._update_fields({c.COL_NEXT_RUN_MODE: c.MODE_FULL_RELOAD}, source, area, table)

    def skip_next_run(self, source: str = None, area: str = None, table: str = None):
        self._update_fields({c.COL_NEXT_RUN_MODE: c.MODE_SKIP}, source, area, table)

    def resume_from_skip(self, source: str = None, area: str = None, table: str = None):
        # We only want to resume tables that are currently skipped.
        self._update_fields(
            updates={c.COL_LOAD_MODE: c.MODE_INCREMENTAL},
            source=source, area=area, table=table,
            extra_conditions={c.COL_LOAD_MODE: c.MODE_SKIP}
        )
        self._update_fields(
            updates={c.COL_NEXT_RUN_MODE: None},
            source=source, area=area, table=table,
            extra_conditions={c.COL_NEXT_RUN_MODE: c.MODE_SKIP}
        )

    def reset_watermark(self, source: str = None, area: str = None, table: str = None):
        self._update_fields({c.COL_WATERMARK_LAST: None}, source, area, table)

    # ── Diagnostics ──────────────────────────────────────────

    def status(self):
        return self._spark.sql(f"""
            SELECT
                {c.COL_SOURCE}, {c.COL_AREA}, {c.COL_TABLE_NAME}, {c.COL_ACTIVE}, {c.COL_IS_DIMENSION},
                {c.COL_LOAD_MODE}, {c.COL_NEXT_RUN_MODE}, {c.COL_RUN_ORDER},
                {c.COL_WATERMARK_LAST}, {c.COL_LAST_RUN_AT}, {c.COL_LAST_RUN_OK}, {c.COL_LAST_ERROR},
                {c.COL_CREATED_AT}, {c.COL_UPDATED_AT}
            FROM {c.TABLE_NAME}
            ORDER BY {c.COL_SOURCE}, {c.COL_RUN_ORDER}, {c.COL_TABLE_NAME}
        """)

    def errors(self):
        return self._spark.sql(f"""
            SELECT {c.COL_SOURCE}, {c.COL_TABLE_NAME}, {c.COL_LAST_RUN_AT}, {c.COL_LAST_ERROR}
            FROM {c.TABLE_NAME}
            WHERE {c.COL_LAST_RUN_OK} = false AND {c.COL_ACTIVE} = true
            ORDER BY {c.COL_LAST_RUN_AT} DESC
        """)

    def stale(self, hours: int = 25):
        return self._spark.sql(f"""
            SELECT {c.COL_SOURCE}, {c.COL_TABLE_NAME}, {c.COL_LOAD_MODE}, {c.COL_LAST_RUN_AT}, {c.COL_WATERMARK_LAST}
            FROM {c.TABLE_NAME}
            WHERE {c.COL_ACTIVE} = true
              AND (
                  {c.COL_LAST_RUN_AT} IS NULL
                  OR {c.COL_LAST_RUN_AT} < current_timestamp() - INTERVAL {int(hours)} HOURS
              )
            ORDER BY {c.COL_LAST_RUN_AT} ASC NULLS FIRST
        """)

    # ── Delta Time Travel ────────────────────────────────────

    def show_history(self, limit: int = 20):
        return self._spark.sql(f"DESCRIBE HISTORY {c.TABLE_NAME} LIMIT {limit}")

    def inspect_table(self, version: int = None, timestamp: str = None):
        if version is not None:
            ref = f"VERSION AS OF {int(version)}"
        elif timestamp:
            ref = f"TIMESTAMP AS OF '{_esc(timestamp)}'"
        else:
            raise ValueError("Provide either version= or timestamp=")

        return self._spark.sql(
            f"SELECT * FROM {c.TABLE_NAME} {ref} "
            f"ORDER BY {c.COL_SOURCE}, {c.COL_RUN_ORDER}, {c.COL_TABLE_NAME}"
        )

    def recover_table(self, version: int = None, timestamp: str = None):
        if version is not None:
            ref = f"VERSION AS OF {int(version)}"
        elif timestamp:
            ref = f"TIMESTAMP AS OF '{_esc(timestamp)}'"
        else:
            raise ValueError("Provide either version= or timestamp=")

        print(f"[RESTORE] Restoring {c.TABLE_NAME} to {ref} ...")
        self._spark.sql(f"RESTORE TABLE {c.TABLE_NAME} TO {ref}")
        print(f"[RESTORE] Done. Table restored to {ref}.")
