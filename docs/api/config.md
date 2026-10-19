# `WatermarkConfig`

`WatermarkConfig` encapsula la configuració, les metadades i l'estat d'execució d'una única taula a `Watermark_Control`.

!!! info "Instanciació"
    Els usuaris **mai no han d'instanciar directament aquesta classe**. Les instàncies s'obtenen mitjançant [`WatermarkQuery.build()`](query.md) o [`WatermarkQuery.one()`](query.md), o bé dins del bloc [`watermark_session`](session.md).

---

## Propietats d'Identitat i Enrutament

| Propietat | Tipus | Descripció |
|:---|:---|:---|
| `.source` | `str` | Nom del sistema d'origen (`SOURCE`). |
| `.table_name` | `str` | Nom físic de la taula a l'origen (`TABLE_NAME`). |
| `.area` | `str` | Àrea de negoci o domini funcional (`AREA`). |
| `.is_dimension` | `bool` | Indica si és una dimensió (`IS_DIMENSION`). |
| `.silver_table` | `str` | Nom de la taula Delta de destinació a Plata (`SILVER_TABLE`). |
| `.bronze_path` | `str` | Ruta al fitxer parquet intermedi a Bronze (`BRONZE_PATH`). |
| `.pk_columns` | `list[str]` | Llista Python de columnes que formen la clau primària (deserialitzades de JSON). |
| `.source_query` | `str\|None` | Query SQL base d'extracció a l'origen (`SOURCE_QUERY`). |
| `.source_datecol` | `str` | Nom de la columna de control temporal a l'origen (`SOURCE_DATECOL`). |
| `.source_datecol_format` | `str` | Format per a la data a la query SQL (defecte: `'%Y-%m-%d %H:%M:%S'`). |
| `.window_days` | `int` | Dies de marge de seguretat (defecte: `0`). |

---

## Propietats de Control i Estat

| Propietat | Tipus | Descripció |
|:---|:---|:---|
| `.active` | `bool` | Si la taula està marcada com a activa (`ACTIVE`). |
| `.load_mode` | `str` | Mode de càrrega permanent configurat (`LOAD_MODE`). |
| `.next_run_mode` | `str\|None` | Override puntual per a la propera execució (`NEXT_RUN_MODE`). |
| `.watermark_last` | `datetime\|None` | Data/hora de l'últim registre extret amb èxit (`WATERMARK_LAST_VALUE`). |
| `.last_run_at` | `datetime\|None` | Timestamp de l'última execució (`LAST_RUN_AT`). |
| `.last_run_ok` | `bool\|None` | Si la darrera execució va ser exitosa (`LAST_RUN_OK`). |
| `.last_error` | `str\|None` | Missatge d'error de l'última execució fallida (`LAST_ERROR`). |

---

## Propietats Calculades (Estratègia d'Execució)

Són les propietats que s'han d'utilitzar a la lògica del Notebook per decidir com actuar:

| Propietat | Tipus | Descripció |
|:---|:---|:---|
| `.effective_mode` | `str` | **Mode efectiu per a aquesta execució**. Resol la prioritat: `NEXT_RUN_MODE` > (Watermark `NULL` → `FULL_RELOAD`) > `LOAD_MODE`. Sempre s'ha d'usar aquesta propietat i no `load_mode`. |
| `.from_date` | `datetime\|None` | Data de tall calculada per al filtre incremental (`watermark_last - window_days`). Si és `FULL_RELOAD`, val `None`. |
| `.should_run` | `bool` | Retorna `False` si `effective_mode == 'SKIP'`. Útil per ometre ràpidament el processament. |
| `.is_first_run` | `bool` | Retorna `True` si `WATERMARK_LAST_VALUE` és `None` (primera càrrega mai feta). |

---

## Mètodes d'Acció

### `.build_source_query() -> str | None`
Genera el text SQL complet llest per enviar a l'activitat *Copy Data* de Fabric:
- Si `SOURCE_QUERY` no està configurat a la WT, retorna `None`.
- Si el mode efectiu és `FULL_RELOAD`, retorna la query base tal com està.
- Si és `INCREMENTAL`, afegeix automàticament el filtre temporal:
  `WHERE <source_datecol> >= '<from_date>' AND EXISTS (SELECT 1 FROM <table> WHERE <source_datecol> > '<watermark_last>')`
  Això permet que la consulta acabi immediatament sense descarregar res si no hi ha dades noves.

### `.set_new_watermark(value: datetime)`
Defineix el nou valor de watermark que la sessió transaccional enregistrarà en cas d'èxit. Si no es crida, la sessió desarà el valor previ o el timestamp actual.

---

## Mètodes Auxiliars i Accés Dinàmic

| Mètode / Accés | Retorn | Descripció |
|:---|:---|:---|
| `.get(key, default=None)` | `Any` | Retorna el valor de qualsevol columna de la fila de control (ignorant majúscules/minúscules). Retorna `default` si no existeix. |
| `.to_dict()` | `dict` | Retorna una còpia del diccionari complet de la fila amb totes les columnes subjacents. |
| Accés per atribut (`cfg.<col>`) | `Any` | Permet accedir directament a qualsevol columna estàndard o extra afegida a la taula Delta (ex: `cfg.created_at`, `cfg.my_custom_tag`), case-insensitive. |
| `__repr__()` | `str` | Representació amigable per a missatges de consola i logging. |

---

## Exemple d'Ús

```python
cfg = wm.WatermarkQuery(spark).source("SAGE").table("PORDER").one()

print("Taula:", cfg.table_name)
print("Mode efectiu:", cfg.effective_mode)
print("Data des de (from_date):", cfg.from_date)
print("Query generada:", cfg.build_source_query())

# Columnes del sistema o personalitzades
print("Data creació fila:", cfg.created_at)
print("Tag opcional:", cfg.get("CUSTOM_TAG", default="N/A"))
```
