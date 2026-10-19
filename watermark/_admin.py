from datetime import datetime
import json
from . import _constants as c
from ._schema import (
    CONTROL_TABLE_SCHEMA,
    SYSTEM_AUDIT_COLUMNS,
    get_column_name,
    get_column_meta,
    validate_column_value,
    validate_record,
)

def _esc(value: str) -> str:
    """Escapes a string value for safe embedding in a SQL literal."""
    return str(value).replace("'", "''")

def _to_iso(value) -> str:
    """Returns an ISO-format string for any datetime-like value."""
    if hasattr(value, 'strftime'):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return str(value)

def _sql_val(val) -> str:
    """Formats a Python value as a SQL literal string or NULL."""
    if val is None:
        return "NULL"
    if isinstance(val, bool):
        return "true" if val else "false"
    if isinstance(val, (int, float)):
        return str(val)
    if hasattr(val, 'strftime'):
        return f"TIMESTAMP('{_esc(_to_iso(val))}')"
    if isinstance(val, (list, tuple, set)):
        return f"'{_esc(json.dumps([str(x).strip() for x in val]))}'"
    return f"'{_esc(str(val))}'"

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
        
        # Validate all updates against schema rules
        validated_updates = {}
        for col, val in updates.items():
            clean_col, clean_val = validate_column_value(col, val)
            validated_updates[clean_col] = clean_val

        # Build SET clause (escaping values if they are strings, keeping NULLs)
        set_parts = [f"{col} = {_sql_val(val)}" for col, val in validated_updates.items()]
        set_clause = ", ".join(set_parts)
        
        # Build WHERE clause dynamically
        where_conditions = {
            c.COL_SOURCE: source.strip() if isinstance(source, str) else source,
            c.COL_AREA: area.strip() if isinstance(area, str) else area,
            c.COL_TABLE_NAME: table.strip() if isinstance(table, str) else table,
        }
        if extra_conditions:
            where_conditions.update(extra_conditions)

        self._sql(
            f"UPDATE {c.TABLE_NAME} "
            f"SET {set_clause}, {self._audit()} "
            f"{self._where(where_conditions)}"
        )
        return validated_updates

    # ── Table Registration / Management ─────────────────────

    def insert_table(
        self,
        source: str,
        table: str = None,
        area: str = None,
        silver_table: str = None,
        bronze_path: str = None,
        source_query: str = None,
        source_datecol: str = None,
        source_datecol_format: str = "%Y-%m-%d %H:%M:%S",
        pk_columns: list[str] | tuple[str] | set[str] = None,
        window_days: int = 0,
        active: bool = True,
        run_order: int = 1,
        next_run_mode: str = None,
        watermark_last: datetime = None,
        is_dimension: bool = False,
        load_mode: str = c.MODE_INCREMENTAL,
        overwrite: bool = False,
        table_name: str = None,
        **extra_fields,
    ):
        """
        Inserts a new table configuration into Watermark_Control.

        Parameters:
            source: Source system name (e.g. 'ORACLE', 'SAP').
            table: Table name in the source system (or table_name).
            area: Business area (e.g. 'COMPRES', 'VENDES').
            silver_table: Destination Delta table name in Silver.
            bronze_path: Target directory or file path in Bronze lakehouse.
            source_query: Custom SQL extraction query.
            source_datecol: Timestamp/date column used for incremental filtering.
            source_datecol_format: Format string for date column in source_query (default: "%Y-%m-%d %H:%M:%S").
            pk_columns: List, tuple, or set of primary key columns.
            window_days: Number of lookback days to subtract from watermark (default: 0).
            active: Whether the table is active for execution (default: True).
            run_order: Integer execution priority / ordering (default: 1).
            next_run_mode: One-time override mode for the next execution (optional).
            watermark_last: Initial watermark timestamp (optional, default: None).
            is_dimension: Boolean flag indicating if it is a dimension table (default: False).
            load_mode: Default load mode ('INCREMENTAL', 'FULL_RELOAD', 'SKIP'). Default: 'INCREMENTAL'.
            overwrite: If True, updates existing row if (source, table) already exists. If False and exists, raises ValueError.
            table_name: Alias for table parameter.
            **extra_fields: Any additional custom columns to persist in the control table.
        """
        target_table = table_name or table
        cols_values = validate_record({
            "source": source,
            "table": target_table,
            "area": area,
            "silver_table": silver_table,
            "bronze_path": bronze_path,
            "source_query": source_query,
            "source_datecol": source_datecol,
            "source_datecol_format": source_datecol_format,
            "pk_columns": pk_columns,
            "window_days": window_days,
            "active": active,
            "run_order": run_order,
            "next_run_mode": next_run_mode,
            "watermark_last": watermark_last,
            "is_dimension": is_dimension,
            "load_mode": load_mode,
            **extra_fields,
        })

        source_clean = cols_values[c.COL_SOURCE]
        table_clean = cols_values[c.COL_TABLE_NAME]

        # Check if table already exists
        exists = False
        try:
            check_sql = (
                f"SELECT 1 FROM {c.TABLE_NAME} "
                f"WHERE {c.COL_SOURCE} = '{_esc(source_clean)}' AND {c.COL_TABLE_NAME} = '{_esc(table_clean)}' "
                f"LIMIT 1"
            )
            exists = bool(self._spark.sql(check_sql).take(1))
        except Exception:
            exists = False

        if exists and not overwrite:
            raise ValueError(
                f"Table '{source_clean}.{table_clean}' already exists in {c.TABLE_NAME}. "
                "Set overwrite=True to overwrite it."
            )

        # Build dynamic SELECT expressions for USING (...) AS s
        select_exprs = [f"{_sql_val(val)} AS {col}" for col, val in cols_values.items()]
        for col, meta in SYSTEM_AUDIT_COLUMNS.items():
            if col not in cols_values:
                select_exprs.append(f"{meta['default']} AS {col}")

        # Build dynamic WHEN MATCHED THEN UPDATE SET
        matched_sets = []
        for col in cols_values:
            if col in (c.COL_SOURCE, c.COL_TABLE_NAME):
                continue
            if col == c.COL_WATERMARK_LAST and cols_values[c.COL_WATERMARK_LAST] is None:
                continue
            matched_sets.append(f"t.{col} = s.{col}")
        matched_sets.append(f"t.{c.COL_UPDATED_AT} = s.{c.COL_UPDATED_AT}")

        # Build dynamic WHEN NOT MATCHED THEN INSERT
        all_insert_cols = list(cols_values.keys()) + [col for col in SYSTEM_AUDIT_COLUMNS if col not in cols_values]
        insert_cols_str = ", ".join(all_insert_cols)
        insert_vals_str = ", ".join(f"s.{col}" for col in all_insert_cols)

        merge_sql = f"""
            MERGE INTO {c.TABLE_NAME} AS t
            USING (
                SELECT
                    {",\n        ".join(select_exprs)}
            ) AS s
            ON t.{c.COL_SOURCE} = s.{c.COL_SOURCE} AND t.{c.COL_TABLE_NAME} = s.{c.COL_TABLE_NAME}
            WHEN MATCHED THEN UPDATE SET
                {",\n    ".join(matched_sets)}
            WHEN NOT MATCHED THEN INSERT (
                {insert_cols_str}
            ) VALUES (
                {insert_vals_str}
            )
        """
        self._sql(merge_sql)
        action = "overwritten" if exists else "inserted"
        print(f"[OK] Table '{source_clean}.{table_clean}' {action} in {c.TABLE_NAME}.")


    def delete_table(self, source: str, table: str, area: str = None):
        """Removes a table entry from the control table."""
        target = table
        if not source or not target or not str(source).strip() or not str(target).strip():
            raise ValueError("Both 'source' and table name are required.")

        source = str(source).strip()
        target = str(target).strip()
        where_area = f" AND {c.COL_AREA} = '{_esc(area)}'" if area else ""
        self._sql(
            f"DELETE FROM {c.TABLE_NAME} "
            f"WHERE {c.COL_SOURCE} = '{_esc(source)}' AND {c.COL_TABLE_NAME} = '{_esc(target)}' {where_area}"
        )
        print(f"[OK] Table '{source}.{target}' deleted from {c.TABLE_NAME}.")

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

    # ── Generic Column Update ────────────────────────────────

    def update_table(
        self,
        source: str = None,
        area: str = None,
        table: str = None,
        updates: dict = None,
        **kwargs,
    ):
        """
        Updates specific columns in Watermark_Control for records matching the filter criteria.

        Parameters:
            source: Source system filter (optional).
            area: Business area filter (optional).
            table: Table name filter (optional).
            updates: Dictionary of {column_name: new_value} to update.
            **kwargs: Additional column=value pairs to update.

        Examples:
            # Updating via updates dictionary
            admin.update_table(source="SAP", table="BKPF", updates={"RUN_ORDER": 2, "WINDOW_DAYS": 10})

            # Updating via keyword arguments
            admin.update_table(source="ORACLE", table="CUSTOMERS", active=False, window_days=5)

            # Updating multiple tables in an area
            admin.update_table(source="ORACLE", area="COMPRES", updates={"LOAD_MODE": "FULL_RELOAD"})
        """
        if not any([source, area, table]):
            raise ValueError("Must provide at least one filter of: source, area, table")

        # Collect updates from updates, and kwargs
        target_updates = {}
        if updates is not None:
            if not isinstance(updates, dict):
                raise ValueError(f"The 'updates' parameter must be a dict of {{column: value}}, got {type(updates).__name__}")
            target_updates.update(updates)

        if kwargs:
            target_updates.update(kwargs)

        if not target_updates:
            raise ValueError("No columns to update were provided. Pass a dict via 'updates' or keyword arguments.")

        # Validate and apply updates through _update_fields (single validation point)
        updated_cols = self._update_fields(target_updates, source=source, area=area, table=table)

        filter_parts = []
        if source:
            filter_parts.append(f"source='{source.strip()}'")
        if area:
            filter_parts.append(f"area='{area.strip()}'")
        if table:
            filter_parts.append(f"table='{table.strip()}'")
        cols_summary = ", ".join(updated_cols.keys())
        print(f"[OK] Updated [{cols_summary}] in {c.TABLE_NAME} for {', '.join(filter_parts)}.")

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

    def _time_travel_ref(self, version: int = None, timestamp: str = None) -> str:
        if version is not None:
            return f"VERSION AS OF {int(version)}"
        if timestamp:
            return f"TIMESTAMP AS OF '{_esc(timestamp)}'"
        raise ValueError("Provide either version= or timestamp=")

    def inspect_table(self, version: int = None, timestamp: str = None):
        ref = self._time_travel_ref(version, timestamp)
        return self._spark.sql(
            f"SELECT * FROM {c.TABLE_NAME} {ref} "
            f"ORDER BY {c.COL_SOURCE}, {c.COL_RUN_ORDER}, {c.COL_TABLE_NAME}"
        )

    def recover_table(self, version: int = None, timestamp: str = None):
        ref = self._time_travel_ref(version, timestamp)
        print(f"[RESTORE] Restoring {c.TABLE_NAME} to {ref} ...")
        self._spark.sql(f"RESTORE TABLE {c.TABLE_NAME} TO {ref}")
        print(f"[RESTORE] Done. Table restored to {ref}.")
