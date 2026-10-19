# `watermark_session`

`watermark_session` és el **context manager transaccional** que governa el cicle complet de vida d'una càrrega entre Bronze i Plata. Garanteix que la taula de control només s'actualitzi si la transformació s'ha completat sense errors, executant un únic `MERGE` atòmic a Delta Lake.

---

## Importació

```python
from bfg_watermark import watermark_session
# O bé:
import bfg_watermark as wm
```

---

## Signatures Acceptades

### 1. Amb instància de `WatermarkConfig` pre-carregada (Recomanat per a bucles)
```python
with wm.watermark_session(spark, config=cfg) as cfg:
    # Bloc de transformació
```

### 2. Amb càrrega directa per nom
```python
with wm.watermark_session(spark, source="SAGE", table="PORDER") as cfg:
    # Bloc de transformació
```
En aquest mode, la sessió busca la configuració amb `active_only().one()` de forma transparent.

---

## Comportament Garantit per Escenari

| Escenari | Acció realitzada per la sessió |
|:---|:---|
| **Èxit (sense errors)** | Executa un únic `MERGE` atòmic que: <br>1. Actualitza `WATERMARK_LAST_VALUE` al valor definit per `cfg.set_new_watermark()`.<br>2. Posa `LAST_RUN_OK = true` i `LAST_ERROR = NULL`.<br>3. Posa `LAST_RUN_AT = CURRENT_TIMESTAMP()`.<br>4. Neteja `NEXT_RUN_MODE = NULL` si hi havia un override puntual. |
| **Error / Excepció** | Executa un únic `MERGE` atòmic que: <br>1. **Conserva intacte el watermark anterior** (no avança la data).<br>2. Posa `LAST_RUN_OK = false`.<br>3. Enregistra l'error a `LAST_ERROR` (màxim 500 caràcters).<br>4. **Relança l'excepció** perquè Spark i el Pipeline de Fabric detectin la fallada. |
| **Mode `SKIP`** | Si `cfg.should_run` és `False`, fa un `yield` de l'objecte i surt immediatament del context sense fer cap escriptura a `Watermark_Control`. |

---

## Exemple Complet

```python
import bfg_watermark as wm
from delta.tables import DeltaTable
from pyspark.sql.functions import max as _max

def processar_taula(cfg: wm.WatermarkConfig):
    with wm.watermark_session(spark, config=cfg) as cfg:
        # 1. Comprovació de seguretat de mode SKIP
        if not cfg.should_run:
            print(f"Ometent {cfg.table_name} per mode SKIP.")
            return

        # 2. Llegir parquet de Bronze
        df_bronze = spark.read.parquet(cfg.bronze_path)

        # 3. Processar segons mode efectiu
        if cfg.effective_mode == wm.constants.MODE_FULL_RELOAD:
            (df_bronze.write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .saveAsTable(cfg.silver_table))

        elif cfg.effective_mode == wm.constants.MODE_INCREMENTAL:
            delta_target = DeltaTable.forName(spark, cfg.silver_table)
            merge_cond = " AND ".join([f"target.{k} = source.{k}" for k in cfg.pk_columns])

            (delta_target.alias("target")
                .merge(df_bronze.alias("source"), merge_cond)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute())

        # 4. Fixar el nou watermark amb la data màxima processada
        if cfg.source_datecol:
            max_val = df_bronze.agg(_max(cfg.source_datecol)).collect()[0][0]
            if max_val:
                cfg.set_new_watermark(max_val)

    # En sortir del bloc 'with', la sessió actualitza la taula de control automàticament!
```
