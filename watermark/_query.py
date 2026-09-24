from . import _constants as c
from ._config import WatermarkConfig


class WatermarkQuery:
    """
    Fluent builder for querying Watermark_Control.

    Quick reference:
    ┌─────────────────────────────────────────────────────────────────┐
    │  FILTERS              │  TERMINAL                               │
    ├───────────────────────┼─────────────────────────────────────────│
    │  .source("ORACLE")    │  .build()  → list[WatermarkConfig]     │
    │  .area("PURCHASES")   │  .one()    → WatermarkConfig (1 only)  │
    │  .table("PORDER")     │                                         │
    │  .dimension()         │                                         │
    │  .active_only()       │                                         │
    │  .mode("INCREMENTAL") │                                         │
    │  .ordered()           │                                         │
    └───────────────────────┴─────────────────────────────────────────┘
    """

    def __init__(self, spark):
        self._spark   = spark
        self._filters = []
        self._sort    = False
        self._single  = False

    def source(self, *sources):
        """Filter by source system. Multiple values are OR-ed."""
        s = ", ".join(f"'{v}'" for v in sources)
        self._filters.append(f"{c.COL_SOURCE} IN ({s})")
        return self

    def area(self, *areas):
        """Filter by business area. Multiple values are OR-ed."""
        s = ", ".join(f"'{v}'" for v in areas)
        self._filters.append(f"{c.COL_AREA} IN ({s})")
        return self

    def table(self, *tables):
        """Filter by table name. Multiple values are OR-ed."""
        s = ", ".join(f"'{v}'" for v in tables)
        self._filters.append(f"{c.COL_TABLE_NAME} IN ({s})")
        return self

    def dimension(self, is_dimension: bool = True):
        """Filter by IS_DIMENSION flag."""
        val = 'true' if is_dimension else 'false'
        self._filters.append(f"{c.COL_IS_DIMENSION} = {val}")
        return self

    def active_only(self):
        """Exclude rows where ACTIVE = false."""
        self._filters.append(f"{c.COL_ACTIVE} = true")
        return self

    def runnable(self):
        """
        Securely filters out tables that should not run this execution
        (i.e. effective_mode is SKIP).
        Automatically applies active_only() as well.
        """
        self.active_only()
        # Evaluate effective mode in SQL to push filter to Spark engine
        expr = (
            f"COALESCE({c.COL_NEXT_RUN_MODE}, "
            f"CASE WHEN {c.COL_WATERMARK_LAST} IS NULL THEN '{c.MODE_FULL_RELOAD}' "
            f"ELSE {c.COL_LOAD_MODE} END) != '{c.MODE_SKIP}'"
        )
        self._filters.append(expr)
        return self

    def mode(self, *modes):
        """Filter by permanent LOAD_MODE."""
        s = ", ".join(f"'{v}'" for v in modes)
        self._filters.append(f"{c.COL_LOAD_MODE} IN ({s})")
        return self

    def ordered(self):
        """Sort results by SOURCE, RUN_ORDER, TABLE_NAME."""
        self._sort = True
        return self

    def one(self) -> WatermarkConfig:
        """Execute and return exactly one WatermarkConfig."""
        self._single = True
        return self._run()

    def build(self) -> list:
        """Execute and return a list of WatermarkConfig objects."""
        return self._run()

    # ── Internal ─────────────────────────────────────────────
    def _run(self):
        df = self._spark.read.table(c.TABLE_NAME)
        if self._filters:
            df = df.filter(" AND ".join(self._filters))
        if self._sort:
            df = df.orderBy(c.COL_SOURCE, c.COL_RUN_ORDER, c.COL_TABLE_NAME)

        rows = df.collect()
        configs = [WatermarkConfig(r) for r in rows]

        if self._single:
            if len(configs) == 0:
                raise ValueError(
                    f"No row found in {c.TABLE_NAME} with filters: {self._filters}"
                )
            if len(configs) > 1:
                raise ValueError(
                    f"Expected exactly 1 row but got {len(configs)}. "
                    f"Add .source() or .table() to narrow down. Filters: {self._filters}"
                )
            return configs[0]

        return configs
