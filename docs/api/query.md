# `WatermarkQuery`

`WatermarkQuery` és un builder fluent dissenyat per consultar la taula `Watermark_Control` des de Notebooks de Microsoft Fabric. Permet encadenar mètodes de filtrat i ordenació, i executar la consulta de manera eficient.

---

## Importació

```python
from bfg_watermark import WatermarkQuery
# O a través de la importació principal:
import bfg_watermark as wm
query = wm.WatermarkQuery(spark)
```

---

## Constructor

```python
WatermarkQuery(spark)
```

- **`spark`**: La instància activa de `SparkSession` del Notebook.

---

## Mètodes de Filtre (Encadenables)

Tots aquests mètodes retornen la mateixa instància de `WatermarkQuery`, permetent l'encadenament fluid:

| Mètode | Signatura | Descripció |
|:---|:---|:---|
| `.source(*sources)` | `source("SAGE")`<br>`source("SAGE", "ORACLE")` | Filtra per un o diversos sistemes origen. Si s'indiquen múltiples valors, s'aplica una clàusula `IN (...)` (OR). |
| `.area(*areas)` | `area("COMPRES")`<br>`area("COMPRES", "VENDES")` | Filtra per àrea funcional de negoci. Múltiples valors s'avaluen amb OR. |
| `.table(*tables)` | `table("PORDER")`<br>`table("PORDER", "PINVOICE")` | Filtra pel nom de la taula a l'origen (`TABLE_NAME`). |
| `.dimension(is_dim=True)` | `dimension(True)`<br>`dimension(False)` | Filtra pel flag `IS_DIMENSION`. |
| `.active_only()` | `active_only()` | Filtra únicament taules actives (`ACTIVE = true`). |
| `.runnable()` | `runnable()` | **Mètode recomanat per a Pipelines.** Aplica `.active_only()` i exclou qualsevol taula on el mode efectiu (`effective_mode`) sigui `SKIP`. |
| `.mode(*modes)` | `mode("INCREMENTAL")` | Filtra pel valor de la columna `LOAD_MODE` permanent. |
| `.where(**kwargs)` | `where(silver_table="...", active=True)` | **Filtre genèric per a qualsevol columna** (estàndard o afegida). Converteix `None` a `IS NULL`, booleans a `true/false`, escapa cadenes i resol automàticament els àlies de columnes via `schema.get_column_name()`. |
| `.ordered()` | `ordered()` | Ordena els registres per `SOURCE`, `RUN_ORDER` ascendent i `TABLE_NAME`. |

---

## Mètodes Terminals

Aquests mètodes executen la consulta contra Spark i retornen instàncies de [`WatermarkConfig`](config.md):

| Mètode | Retorn | Descripció |
|:---|:---|:---|
| `.build()` | `list[WatermarkConfig]` | Executa la query i retorna una llista d'objectes de configuració. Pot retornar una llista buida si no hi ha coincidències. |
| `.one()` | `WatermarkConfig` | Executa la query i retorna exactament un registre. Llança `ValueError` si es troben 0 files o més d'1. |

---

## Exemples d'Ús

### Consulta estàndard per a Pipelines
```python
import bfg_watermark as wm

configs = (
    wm.WatermarkQuery(spark)
    .source("SAGE")
    .runnable()
    .ordered()
    .build()
)

for cfg in configs:
    print(f"Taula: {cfg.table_name} -> {cfg.silver_table}")
```

### Obtenir la configuració d'una taula concreta
```python
cfg = (
    wm.WatermarkQuery(spark)
    .source("ORACLE")
    .table("CUSTOMERS")
    .one()
)
```

### Ús de `.where()` per a columnes estàndard o extres
```python
# Filtratge flexible per múltiples propietats
configs = (
    wm.WatermarkQuery(spark)
    .where(
        source="SAGE",
        area="COMPRES",
        active=True,
        window_days=2
    )
    .build()
)
```
