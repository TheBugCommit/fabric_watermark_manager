# Submòdul `constants`

El submòdul `bfg_watermark.constants` centralitza totes les cadenes i identificadors físics per evitar l'ús de literals hardcoded als Notebooks.

---

## Importació

```python
from bfg_watermark import constants as c

# O directament les constants individuals:
from bfg_watermark.constants import (
    TABLE_NAME,
    MODE_INCREMENTAL,
    MODE_FULL_RELOAD,
    MODE_SKIP,
    VALID_MODES
)
```

---

## Taula i Modes de Càrrega

| Constant | Valor | Descripció |
|:---|:---|:---|
| `TABLE_NAME` | `"Watermark_Control"` | Nom canònic de la taula Delta al Lakehouse. |
| `MODE_INCREMENTAL` | `"INCREMENTAL"` | Mode de càrrega incremental basat en watermark. |
| `MODE_FULL_RELOAD` | `"FULL_RELOAD"` | Mode de recàrrega sencera des de l'origen. |
| `MODE_SKIP` | `"SKIP"` | Mode que omet l'execució d'una taula. |
| `VALID_MODES` | `frozenset({...})` | Conjunt immutable amb els modes vàlids. |

---

## Noms Físics de Columna (`COL_*`)

| Constant | Valor Físic | Categoria |
|:---|:---|:---|
| `COL_SOURCE` | `"SOURCE"` | Identitat |
| `COL_TABLE_NAME` | `"TABLE_NAME"` | Identitat |
| `COL_AREA` | `"AREA"` | Identitat |
| `COL_IS_DIMENSION` | `"IS_DIMENSION"` | Identitat / Control |
| `COL_SILVER_TABLE` | `"SILVER_TABLE"` | Enrutament |
| `COL_BRONZE_PATH` | `"BRONZE_PATH"` | Enrutament |
| `COL_SOURCE_QUERY` | `"SOURCE_QUERY"` | Extracció |
| `COL_SOURCE_DATECOL` | `"SOURCE_DATECOL"` | Extracció |
| `COL_SOURCE_DATECOL_FORMAT` | `"SOURCE_DATECOL_FORMAT"` | Extracció |
| `COL_WINDOW_DAYS` | `"WINDOW_DAYS"` | Extracció / Finestra |
| `COL_PK_COLUMNS` | `"PK_COLUMNS"` | Merge / Clau Primària |
| `COL_ACTIVE` | `"ACTIVE"` | Control d'Execució |
| `COL_LOAD_MODE` | `"LOAD_MODE"` | Control d'Execució |
| `COL_NEXT_RUN_MODE` | `"NEXT_RUN_MODE"` | Control d'Execució |
| `COL_RUN_ORDER` | `"RUN_ORDER"` | Control d'Execució |
| `COL_WATERMARK_LAST` | `"WATERMARK_LAST_VALUE"` | Estat i Auditoria |
| `COL_LAST_RUN_AT` | `"LAST_RUN_AT"` | Estat i Auditoria |
| `COL_LAST_RUN_OK` | `"LAST_RUN_OK"` | Estat i Auditoria |
| `COL_LAST_ERROR` | `"LAST_ERROR"` | Estat i Auditoria |
| `COL_CREATED_AT` | `"CREATED_AT"` | Estat i Auditoria |
| `COL_UPDATED_AT` | `"UPDATED_AT"` | Estat i Auditoria |

---

## Exemple d'Ús Recomanat

```python
import bfg_watermark as wm
c = wm.constants

# Comprovació segura de mode efectiu
if cfg.effective_mode == c.MODE_FULL_RELOAD:
    print("Executant recàrrega completa...")
elif cfg.effective_mode == c.MODE_INCREMENTAL:
    print("Executant càrrega incremental...")

# Consultes SQL directes sense hardcodificar noms de columna
spark.sql(f"""
    SELECT {c.COL_TABLE_NAME}, {c.COL_WATERMARK_LAST}
    FROM {c.TABLE_NAME}
    WHERE {c.COL_ACTIVE} = true AND {c.COL_SOURCE} = 'SAGE'
""")
```
