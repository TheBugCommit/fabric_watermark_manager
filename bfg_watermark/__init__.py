import sys
from ._config   import WatermarkConfig
from ._query    import WatermarkQuery
from ._admin    import WatermarkAdmin
from ._session  import watermark_session

from . import _constants as constants
from . import _schema as schema

# Register submodules so `from bfg_watermark.constants import ...` and `from watermark.schema import ...` also work
sys.modules[f"{__name__}.constants"] = constants
sys.modules[f"{__name__}.schema"] = schema

__version__ = "1.0.6"
__all__ = [
    "WatermarkConfig",
    "WatermarkQuery",
    "WatermarkAdmin",
    "watermark_session",
    "constants",
    "schema",
]
