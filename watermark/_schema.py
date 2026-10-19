# watermark/_schema.py
# ─────────────────────────────────────────────────────────────
# Schema definition for Watermark_Control.
# Provides centralized metadata for columns, default values,
# required validations, and dynamic SQL generation.
# ─────────────────────────────────────────────────────────────

from datetime import datetime
from . import _constants as c

CONTROL_TABLE_SCHEMA = {
    # ─────────────────────────────────────────────────────────────
    # 1. IDENTITAT (Obligatòries)
    # ─────────────────────────────────────────────────────────────
    "source": {
        "col": c.COL_SOURCE,
        "type": str,
        "required": True,
        "description": "Nom del sistema origen (ex: 'ORACLE', 'SAP')",
    },
    "table": {
        "col": c.COL_TABLE_NAME,
        "type": str,
        "required": True,
        "description": "Nom de la taula a l'origen",
    },
    "area": {
        "col": c.COL_AREA,
        "type": str,
        "required": True,
        "description": "Àrea funcional (ex: 'COMPRES', 'VENDES')",
    },

    # ─────────────────────────────────────────────────────────────
    # 2. ENRUTAMENT I EXTRACCIÓ (Obligatòries)
    # ─────────────────────────────────────────────────────────────
    "silver_table": {
        "col": c.COL_SILVER_TABLE,
        "type": str,
        "required": True,
        "description": "Nom de la taula de destinació a Silver",
    },
    "bronze_path": {
        "col": c.COL_BRONZE_PATH,
        "type": str,
        "required": True,
        "description": "Ruta al Lakehouse Bronze",
    },
    "source_query": {
        "col": c.COL_SOURCE_QUERY,
        "type": str,
        "required": True,
        "description": "Query SQL d'extracció a l'origen",
    },
    "source_datecol": {
        "col": c.COL_SOURCE_DATECOL,
        "type": str,
        "required": True,
        "description": "Columna temporal per al filtrat incremental",
    },
    "source_datecol_format": {
        "col": c.COL_SOURCE_DATECOL_FORMAT,
        "type": str,
        "required": True,
        "description": "Format de data per a la query d'extracció (ex: '%Y-%m-%d %H:%M:%S')",
    },
    "pk_columns": {
        "col": c.COL_PK_COLUMNS,
        "type": (list, tuple, set),
        "required": True,
        "description": "Llista de columnes que formen la clau primària",
    },

    # ─────────────────────────────────────────────────────────────
    # 3. CONTROL D'EXECUCIÓ (Opcionals amb valor per defecte)
    # ─────────────────────────────────────────────────────────────
    "window_days": {
        "col": c.COL_WINDOW_DAYS,
        "type": int,
        "required": False,
        "default": 0,
        "description": "Dies de marge retrospectiu a restar del watermark",
    },
    "active": {
        "col": c.COL_ACTIVE,
        "type": bool,
        "required": False,
        "default": True,
        "description": "Si la taula està activa per executar-se",
    },
    "run_order": {
        "col": c.COL_RUN_ORDER,
        "type": int,
        "required": False,
        "default": 1,
        "description": "Ordre o prioritat d'execució",
    },
    "load_mode": {
        "col": c.COL_LOAD_MODE,
        "type": str,
        "required": False,
        "default": c.MODE_INCREMENTAL,
        "valid_values": c.VALID_MODES,
        "description": "Mode de càrrega per defecte",
    },
    "next_run_mode": {
        "col": c.COL_NEXT_RUN_MODE,
        "type": (str, type(None)),
        "required": False,
        "default": None,
        "valid_values": c.VALID_MODES | {None},
        "description": "Override puntual per a la propera execució",
    },
    "is_dimension": {
        "col": c.COL_IS_DIMENSION,
        "type": bool,
        "required": False,
        "default": False,
        "description": "Indica si la taula és una dimensió",
    },
    "watermark_last": {
        "col": c.COL_WATERMARK_LAST,
        "type": (datetime, type(None)),
        "required": False,
        "default": None,
        "description": "Data de watermark inicial (opcional)",
    },
}

# ─────────────────────────────────────────────────────────────
# 4. COLUMNES DEL SISTEMA I AUDITORIA (Gestionades internament)
# ─────────────────────────────────────────────────────────────
SYSTEM_AUDIT_COLUMNS = {
    c.COL_LAST_RUN_AT: {"default": "NULL", "update_on_merge": False},
    c.COL_LAST_RUN_OK: {"default": "NULL", "update_on_merge": False},
    c.COL_LAST_ERROR:  {"default": "NULL", "update_on_merge": False},
    c.COL_CREATED_AT:  {"default": "CURRENT_TIMESTAMP()", "update_on_merge": False},
    c.COL_UPDATED_AT:  {"default": "CURRENT_TIMESTAMP()", "update_on_merge": True},
}

# Mapeig ràpid de paràmetres a columnes físiques (i àlies habituals)
PARAM_TO_COL = {param: meta["col"] for param, meta in CONTROL_TABLE_SCHEMA.items()}
PARAM_TO_COL["table_name"] = c.COL_TABLE_NAME
PARAM_TO_COL["watermark"] = c.COL_WATERMARK_LAST
PARAM_TO_COL["watermark_last_value"] = c.COL_WATERMARK_LAST

def get_column_name(field_name: str) -> str:
    """Retorna el nom de la columna física a Delta Lake per a qualsevol paràmetre o clau."""
    clean = field_name.strip()
    lower = clean.lower()
    if lower in PARAM_TO_COL:
        return PARAM_TO_COL[lower]
    return clean.upper()


def get_required_fields() -> list[str]:
    """Retorna els noms dels camps obligatoris definits a CONTROL_TABLE_SCHEMA."""
    return [field for field, meta in CONTROL_TABLE_SCHEMA.items() if meta.get("required")]


# Mapeig de columnes físiques a metadades
COL_TO_SCHEMA = {meta["col"]: meta for meta in CONTROL_TABLE_SCHEMA.values()}


def get_column_meta(field_or_col_name: str) -> dict | None:
    """Retorna la definició de metadades a CONTROL_TABLE_SCHEMA per a un paràmetre o columna física."""
    clean_col = get_column_name(field_or_col_name)
    return COL_TO_SCHEMA.get(clean_col)


def validate_column_value(col_name: str, val) -> tuple[str, any]:
    """
    Normalitza el nom de la columna i valida el seu valor contra CONTROL_TABLE_SCHEMA:
    - Si és required: no pot ser None, ni string buit, ni col·lecció buida.
    - Tipus: comprova que compleixi amb meta['type'] (evitant que bool passi com int).
    - Valors permesos: comprova meta['valid_values'] si està definit.
    - Neteja strings amb .strip().
    Retorna (clean_col, cleaned_val).
    """
    if not isinstance(col_name, str) or not col_name.strip():
        raise ValueError(f"Invalid column name: {col_name!r}")

    clean_col = get_column_name(col_name)

    if clean_col in (c.COL_CREATED_AT, c.COL_UPDATED_AT):
        raise ValueError(f"Cannot manually modify system audit column '{clean_col}'")

    meta = COL_TO_SCHEMA.get(clean_col)
    if meta is not None:
        # 1. Validació de camps obligatoris (no buits ni nulls)
        if meta.get("required"):
            if val is None:
                raise ValueError(f"Column '{clean_col}' is required and cannot be null/None.")
            if isinstance(val, str) and not val.strip():
                raise ValueError(f"Column '{clean_col}' is required and cannot be empty.")
            if isinstance(val, (list, tuple, set)):
                if len(val) == 0:
                    raise ValueError(f"Column '{clean_col}' is required and cannot be empty.")
                if not any(str(x).strip() for x in val):
                    raise ValueError(f"Column '{clean_col}' cannot contain only empty values.")

        # 2. Validació de tipus
        if val is not None:
            expected_type = meta.get("type")
            if expected_type:
                # En Python, bool és subclasse d'int. Evitem que True/False passin com a int.
                if expected_type is int and isinstance(val, bool):
                    is_type_valid = False
                else:
                    is_type_valid = isinstance(val, expected_type)

                if not is_type_valid:
                    expected_name = (
                        ", ".join(t.__name__ for t in expected_type)
                        if isinstance(expected_type, tuple)
                        else getattr(expected_type, "__name__", str(expected_type))
                    )
                    raise ValueError(
                        f"Invalid type for column '{clean_col}': expected {expected_name}, got {type(val).__name__}"
                    )

            # 3. Validació de valors permesos (ex: LOAD_MODE, NEXT_RUN_MODE)
            valid_vals = meta.get("valid_values")
            if valid_vals is not None and val not in valid_vals:
                valid_str = sorted(list(str(x) for x in valid_vals))
                raise ValueError(
                    f"Invalid value for column '{clean_col}': '{val}'. Must be one of: {valid_str}"
                )

    if isinstance(val, str):
        val = val.strip()

    return clean_col, val


def validate_record(inputs: dict) -> dict[str, any]:
    """
    Normalitza i valida un registre sencer de Watermark_Control contra CONTROL_TABLE_SCHEMA:
    - Aplica els valors per defecte de l'esquema per a camps no informats o amb valor None.
    - Valida que els camps obligatoris ('required') estiguin presents i no buits.
    - Valida tipus de dades i valors permesos mitjançant validate_column_value.
    - Admet camps addicionals (extra_fields) validant el seu nom i format.
    Retorna el diccionari normalitzat {NOM_COLUMNA_NET: valor_validat}.
    """
    raw = dict(inputs)
    cols_values = {}

    for field, meta in CONTROL_TABLE_SCHEMA.items():
        val = raw.pop(field, None)
        if val is None and "default" in meta:
            val = meta["default"]
        clean_col, clean_val = validate_column_value(field, val)
        cols_values[clean_col] = clean_val
 
    for field, val in raw.items():
        clean_col, clean_val = validate_column_value(field, val)
        cols_values[clean_col] = clean_val

    return cols_values

