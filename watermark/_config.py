import json
from datetime import datetime, timedelta
from . import _constants as c


class WatermarkConfig:
    """
    Encapsulates one row from Watermark_Control.

    Do NOT instantiate directly. Use WatermarkQuery to get instances.

    Computed properties (use these in your notebook logic):
        effective_mode  str   — resolves NEXT_RUN_MODE > LOAD_MODE > NULL-watermark fallback
        from_date       ts    — cutoff timestamp (None = full load)
        should_run      bool  — False when effective_mode is 'SKIP'
        is_first_run    bool  — True when WATERMARK_LAST_VALUE is NULL (new table)

    Example:
        cfg = WatermarkQuery(spark).source("ORACLE").table("PORDER").one()
        query = cfg.build_source_query()   # None = full load (no SOURCE_QUERY set)
        df = load_from_oracle(since=cfg.from_date)   # None = full load
    """

    def __init__(self, row):
        self._row = row
        # Cache expensive/repeated derivations at construction time.
        self._effective_mode: str = self._resolve_effective_mode()
        self._watermark_last = row[c.COL_WATERMARK_LAST]
        
        pk = row.get(c.COL_PK_COLUMNS)
        self._pk_columns: list = json.loads(pk) if pk else []
        
        self._from_date: datetime = self._resolve_from_date()
        # Mutable slot for the caller to override the watermark written on success
        self._new_watermark = self._watermark_last or datetime.now()

    # ── Identity ─────────────────────────────────────────────
    @property
    def source(self) -> str:
        return self._row[c.COL_SOURCE]

    @property
    def table_name(self) -> str:
        return self._row[c.COL_TABLE_NAME]

    @property
    def area(self) -> str:
        return self._row.get(c.COL_AREA)

    @property
    def is_dimension(self) -> bool:
        return bool(self._row.get(c.COL_IS_DIMENSION))

    # ── Routing ──────────────────────────────────────────────
    @property
    def silver_table(self):
        return self._row.get(c.COL_SILVER_TABLE)

    @property
    def bronze_path(self):
        return self._row.get(c.COL_BRONZE_PATH)

    @property
    def source_datecol(self):
        return self._row.get(c.COL_SOURCE_DATECOL)

    @property
    def pk_columns(self) -> list:
        return self._pk_columns 

    # ── Load behaviour ───────────────────────────────────────
    @property
    def active(self) -> bool:
        return bool(self._row.get(c.COL_ACTIVE))

    @property
    def load_mode(self) -> str:
        return self._row.get(c.COL_LOAD_MODE)

    @property
    def next_run_mode(self):
        return self._row.get(c.COL_NEXT_RUN_MODE)

    @property
    def watermark_last(self) -> datetime:
        return self._watermark_last

    # ── Source & Windows ─────────────────────────────────────
    @property
    def source_query(self) -> str:
        return self._row.get(c.COL_SOURCE_QUERY)

    @property
    def source_datecol(self) -> str:
        return self._row.get(c.COL_SOURCE_DATECOL)

    @property
    def source_datecol_format(self) -> str:
        fmt = self._row.get(c.COL_SOURCE_DATECOL_FORMAT)
        return fmt if fmt else "%Y-%m-%d %H:%M:%S"

    @property
    def window_days(self) -> int:
        val = self._row.get(c.COL_WINDOW_DAYS)
        return int(val) if val is not None else 0

    # ── Computed (strategy)
    def _resolve_effective_mode(self) -> str:
        """
        Priority: NEXT_RUN_MODE > LOAD_MODE.
        WATERMARK_LAST_VALUE = NULL always forces FULL_RELOAD regardless of LOAD_MODE.
        Evaluated once at construction; result cached in self._effective_mode.
        """
        if self._row.get(c.COL_NEXT_RUN_MODE):
            return self._row[c.COL_NEXT_RUN_MODE]
        if self._row.get(c.COL_WATERMARK_LAST) is None:
            return c.MODE_FULL_RELOAD
        return self._row.get(c.COL_LOAD_MODE, c.MODE_FULL_RELOAD)

    def _resolve_from_date(self) -> datetime:
        """
        Cutoff timestamp for incremental queries, with window subtracted.
        Evaluated once at construction; result cached in self._from_date.
        """
        if self._effective_mode != c.MODE_INCREMENTAL or self._watermark_last is None:
            return None
        return self._watermark_last - timedelta(days=self.window_days)

    @property
    def effective_mode(self) -> str:
        """
        The load mode resolved for THIS run.
        """
        return self._effective_mode

    @property
    def from_date(self) -> datetime:
        """
        Cutoff timestamp to use as the lower bound for source queries.
        Already has window_days subtracted.
        Returns None when a full load is needed.
        """
        return self._from_date

    def build_source_query(self) -> str:
        """
        Returns the SQL query to run against the source system.

        - If SOURCE_QUERY is not set: returns None (caller handles the query).
        - If FULL_RELOAD or no watermark: returns SOURCE_QUERY unchanged.
        - If INCREMENTAL: wraps SOURCE_QUERY to add a WHERE filter on
          SOURCE_DATECOL using the computed from_date (watermark - window_days).
        """
        base_query = self.source_query
        if not base_query:
            return None

        # If not incremental, or if no datecol is configured, do a full extraction without filters
        if self.effective_mode != c.MODE_INCREMENTAL or not self.source_datecol or self._from_date is None:
            return base_query

        # INCREMENTAL mode: wrap in a subquery and filter by date
        cutoff_str = self._from_date.strftime(self.source_datecol_format)
        return f"SELECT * FROM ({base_query}) _wm WHERE _wm.{self.source_datecol} >= '{cutoff_str}'"

    @property
    def should_run(self) -> bool:
        """False if effective_mode is 'SKIP'. Use to short-circuit the notebook."""
        return self._effective_mode != c.MODE_SKIP

    @property
    def is_first_run(self) -> bool:
        """True if WATERMARK_LAST_VALUE is NULL (table has never been loaded)."""
        return self._watermark_last is None

    def set_new_watermark(self, new_watermark):
        """
        Overrides the watermark value to be saved on a successful run.
        If not called, defaults to the current execution time.
        """
        self._new_watermark = new_watermark

    def __repr__(self):
        return (
            f"WatermarkConfig("
            f"source={self.source!r}, table={self.table_name!r}, "
            f"mode={self._effective_mode!r}, from={self._from_date}, "
            f"first_run={self.is_first_run})"
        )
