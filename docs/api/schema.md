# Submòdul `schema`

El submòdul `bfg_watermark.schema` centralitza la definició de l'esquema de dades, les regles de validació, el sanejament de valors i la resolució d'àlies per a la taula `Watermark_Control`.

---

## Importació

```python
from bfg_watermark import schema

# O directament funcions i diccionaris:
from bfg_watermark.schema import (
    CONTROL_TABLE_SCHEMA,
    SYSTEM_AUDIT_COLUMNS,
    validate_record,
    validate_column_value,
    get_column_name,
    get_required_fields
)
```

---

## Estructures de Metadades

### `CONTROL_TABLE_SCHEMA`
Diccionari mestre que defineix cadascuna de les columnes configurables:
- **`col`**: Nom físic de la columna Delta (derivat de `constants`).
- **`type`**: Tipus Python admès (`str`, `int`, `bool`, `datetime`, o col·leccions).
- **`required`**: Booleà que indica si el camp és obligatori.
- **`default`**: Valor assignat automàticament si no s'especifica o és `None`.
- **`valid_values`**: Conjunt de valors permesos (ex: `VALID_MODES`).
- **`description`**: Descripció funcional del camp.

### `SYSTEM_AUDIT_COLUMNS`
Metadades de les columnes gestionades internament (`LAST_RUN_AT`, `LAST_RUN_OK`, `LAST_ERROR`, `CREATED_AT`, `UPDATED_AT`), establint si s'actualitzen automàticament en operacions de MERGE i els seus valors SQL per defecte.

---

## Funcions de Resolució i Validació

### `get_column_name(field_name: str) -> str`
Retorna el nom de la columna física a Delta Lake per a qualsevol paràmetre, clau o àlies habitual:
- `'table'` o `'table_name'` → `'TABLE_NAME'`
- `'watermark'` o `'watermark_last_value'` → `'WATERMARK_LAST_VALUE'`
- Noms en minúscules com `'area'` → `'AREA'`

### `get_required_fields() -> list[str]`
Retorna la llista de noms dels camps que són d'obligada informació (`required=True`):
```python
['source', 'table', 'area', 'silver_table', 'bronze_path', 'source_query', 'source_datecol', 'source_datecol_format', 'pk_columns']
```

### `get_column_meta(field_or_col_name: str) -> dict | None`
Retorna la definició de metadades associada a un paràmetre o nom físic de columna.

### `validate_column_value(col_name: str, val: any) -> tuple[str, any]`
Valida i saneja un valor individual:
- Normalitza el nom de la columna física.
- Rebutja modificacions de les columnes de sistema `CREATED_AT` i `UPDATED_AT`.
- Comprova obligatorietat (rebutja `None`, strings buits o col·leccions sense elements).
- Comprova tipus estricte (assegura que `bool` no passi com a `int`).
- Comprova que el valor figuri a `valid_values` si està definit.
- Aplica `.strip()` a strings.
- Retorna `(clean_col, cleaned_val)`.

### `validate_record(inputs: dict) -> dict[str, any]`
Valida un registre complet de configuració d'una taula:
- Aplica els valors per defecte de l'esquema per als camps opcionals no informats.
- Valida la presència de tots els camps obligatoris.
- Valida tipus i valors mitjançant `validate_column_value`.
- Admet i saneja camps addicionals (`extra_fields`).
- Retorna el diccionari normalitzat `{NOM_COLUMNA_FISICA: valor_validat}` llest per a la generació de SQL o MERGE.

---

## Exemple d'Ús

```python
from bfg_watermark import schema

# Resoldre un àlies
col_fisica = schema.get_column_name("silver_table")
print(col_fisica)  # Retorna 'SILVER_TABLE'

# Validar un registre manualment
inputs = {
    "source": "SAGE",
    "table": "PORDER",
    "area": "COMPRES",
    "silver_table": "silver_sage_porder",
    "bronze_path": "Files/Bronze/PORDER.parquet",
    "source_query": "SELECT * FROM porder",
    "source_datecol": "UPDDATTIM_0",
    "pk_columns": ["NUM_0"],
    # window_days agafarà el default: 0
    # active agafarà el default: True
}

clean_record = schema.validate_record(inputs)
print(clean_record["WINDOW_DAYS"])  # 0
print(clean_record["ACTIVE"])       # True
```
