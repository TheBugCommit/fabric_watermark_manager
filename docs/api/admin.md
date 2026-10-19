# `WatermarkAdmin`

`WatermarkAdmin` proporciona el conjunt complet d'eines d'administració, manteniment del cicle de vida (CRUD), canvis d'estat i diagnòstic per a la taula `Watermark_Control`.

Tots els mètodes operatius accepten filtres per `source`, `area` i/o `table` (com a mínim un filtre és obligatori per seguretat) i segellen automàticament `UPDATED_AT = CURRENT_TIMESTAMP()`.

---

## Inicialització

```python
from bfg_watermark import WatermarkAdmin
admin = WatermarkAdmin(spark)
```

---

## 1. Gestió del Cicle de Vida (CRUD de Taules)

Des de la versió **1.0.6**, pots registrar, actualitzar o eliminar taules directament des de Python sense necessitat d'escriure codi SQL manual.

### `insert_table(...)`
Insereix una nova taula a la taula de control Delta validant tots els paràmetres contra [`CONTROL_TABLE_SCHEMA`](schema.md).

```python
admin.insert_table(
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
    load_mode: str = "INCREMENTAL",
    overwrite: bool = False,
    table_name: str = None,
    **extra_fields
)
```

| Paràmetre | Tipus | Defecte | Descripció |
|:---|:---|:---:|:---|
| `source` | `str` | *Requerit* | Identificador del sistema font (ex: `'SAGE'`, `'SAP'`). |
| `table` / `table_name` | `str` | *Requerit* | Nom de la taula a l'origen. |
| `area` | `str` | *Requerit* | Domini funcional de negoci. |
| `silver_table` | `str` | *Requerit* | Nom de la taula Delta Plata destí. |
| `bronze_path` | `str` | *Requerit* | Ruta al fitxer parquet intermedi a Bronze. |
| `source_query` | `str` | *Requerit* | Query SQL base d'extracció. |
| `source_datecol` | `str` | *Requerit* | Columna temporal de control incremental. |
| `source_datecol_format` | `str` | `"%Y-%m-%d %H:%M:%S"` | Format per a la data al SQL. |
| `pk_columns` | `list` / `tuple` / `set` | *Requerit* | Llista de columnes clau primària per al MERGE. |
| `window_days` | `int` | `0` | Dies de marge de seguretat temporal. |
| `active` | `bool` | `True` | Estat d'activació inicial. |
| `run_order` | `int` | `1` | Prioritat o ordre d'execució. |
| `next_run_mode` | `str` \| `None` | `None` | Override puntual per a la propera càrrega. |
| `watermark_last` | `datetime` \| `None` | `None` | Watermark inicial. Si és `None`, forçarà recàrrega completa. |
| `is_dimension` | `bool` | `False` | Si la taula és una dimensió. |
| `load_mode` | `str` | `"INCREMENTAL"` | Mode de càrrega per defecte (`'INCREMENTAL'`, `'FULL_RELOAD'`, `'SKIP'`). |
| `overwrite` | `bool` | `False` | Si és `False` i la taula ja existeix, llança `ValueError`. Si és `True`, executa un MERGE actualitzant la definició **però preservant el watermark existent si `watermark_last` no s'especifica**. |
| `**extra_fields` | `Any` | - | Columnes suplementàries personalitzades. |

### `update_table(...)`
Actualitza una o diverses columnes per a les taules que compleixin els filtres `source`, `area` o `table`.

```python
admin.update_table(
    source: str = None,
    area: str = None,
    table: str = None,
    updates: dict = None,
    **kwargs
)
```

- Admet passar un diccionari `updates={"RUN_ORDER": 2}` o arguments per clau `admin.update_table(source="SAGE", table="PORDER", window_days=3)`.
- Valida tots els canvis contra l'esquema (`validate_column_value`) i actualitza `UPDATED_AT`.

### `delete_table(...)`
Esborra permanentment el registre d'una taula a `Watermark_Control`.

```python
admin.delete_table(source: str, table: str, area: str = None)
```

---

## 2. Activació i Desactivació

Permet activar o suspendre càrregues ràpidament:

```python
# Activar una taula concreta
admin.activate(source="SAGE", table="PORDER")

# Desactivar tota una àrea funcional
admin.deactivate(source="SAGE", area="STOCKS")
```

---

## 3. Mode de Càrrega Permanent

Canvia el valor permanent de `LOAD_MODE`:

```python
admin.set_mode("FULL_RELOAD", source="SAGE", table="TABCOUNTRY")
```

Modes vàlids: `'INCREMENTAL'`, `'FULL_RELOAD'`, `'SKIP'`.

---

## 4. Overrides Puntuals (Per a la Propera Execució)

Modifiquen el comportament exclusivament durant el cicle següent:

| Mètode | Descripció |
|:---|:---|
| `admin.force_full_reload(source=, area=, table=)` | Posa `NEXT_RUN_MODE = 'FULL_RELOAD'`. El Pipeline farà una recàrrega sencera i la sessió netejarà l'override a `NULL` un cop finalitzi correctament. |
| `admin.skip_next_run(source=, area=, table=)` | Posa `NEXT_RUN_MODE = 'SKIP'`. El proper cicle s'ometrà sense error. |
| `admin.resume_from_skip(source=, area=, table=)` | Reactiva taules que estiguin en skip: neteja `NEXT_RUN_MODE` (a `NULL`) i canvia `LOAD_MODE` (a `'INCREMENTAL'`). |
| `admin.reset_watermark(source=, area=, table=)` | Posa `WATERMARK_LAST_VALUE = NULL`. Forçarà automàticament un `FULL_RELOAD` al cicle següent. |

---

## 5. Diagnòstic i Salut del Sistema

Mètodes de consulta ràpida que retornen un `DataFrame` de Spark:

### `admin.status() -> DataFrame`
Visió global de totes les taules: modes, watermarks, timestamps d'execució i estats d'auditoria, ordenades per sistema, prioritat i nom de taula.

### `admin.errors() -> DataFrame`
Retorna les taules actives on la darrera execució va fallar (`LAST_RUN_OK = false`), ordenades per timestamp d'error descendent.

### `admin.stale(hours=25) -> DataFrame`
Retorna les taules actives que no s'han executat amb èxit en les darreres `hours` hores (per defecte `25`). Ideal per detectar taules encallades en els processos nocturns.

---

## 6. Delta Time Travel (Auditoria i Recuperació)

Aprofita les funcionalitats de Delta Lake per a la recuperació davant d'errors humans o corrupcions:

```python
# Veure l'historial de transaccions (DESCRIBE HISTORY)
display(admin.show_history(limit=20))

# Inspeccionar la taula en una versió anterior sense modificar res (només lectura)
display(admin.inspect_table(version=5))
# O per data:
display(admin.inspect_table(timestamp="2026-01-15 10:00:00"))

# Restaurar la taula sencera a un estat anterior (ACCIÓ DESTRUCTIVA / UNDO)
admin.recover_table(version=5)
```
