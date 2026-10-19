# Patrons de Codi Habituals

En aquesta secció es recullen els patrons d'implementació estàndard que s'utilitzen als Notebooks de Microsoft Fabric.

---

## 1. Patró Complet: Notebook Bronze → Plata

Aquest és el patró canònic per al Notebook de Transformació (`NB_Bronce_a_Plata_<Origen>`), el qual s'executa taula per taula dins del cicle del Pipeline:

```python
import bfg_watermark as wm
from delta.tables import DeltaTable
from pyspark.sql.functions import max as _max

def processar_taula(cfg: wm.WatermarkConfig):
    """
    Processa una sola taula:
    - Obre la sessió transaccional
    - Llegeix Bronze
    - Fa Overwrite si és FULL_RELOAD o MERGE si és INCREMENTAL
    - Avança el watermark si s'ha processat correctament
    """
    with wm.watermark_session(spark, config=cfg) as cfg:

        # Si el mode efectiu és SKIP, sortim immediatament
        if not cfg.should_run:
            print(f"[SKIP] Ometent {cfg.table_name}")
            return

        # 1. Llegir fitxer parquet de Bronze
        df_bronze = spark.read.format("parquet").load(cfg.bronze_path)

        # 2. Aplicar l'estratègia segons el mode efectiu
        if cfg.effective_mode == wm.constants.MODE_FULL_RELOAD:
            print(f"[FULL_RELOAD] Sobreescribint taula Plata: {cfg.silver_table}")
            (df_bronze.write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .saveAsTable(cfg.silver_table))

        elif cfg.effective_mode == wm.constants.MODE_INCREMENTAL:
            print(f"[INCREMENTAL] Executant MERGE a Plata: {cfg.silver_table}")
            delta_table = DeltaTable.forName(spark, cfg.silver_table)
            
            # Condició de creuament dinàmica basada en pk_columns
            merge_cond = " AND ".join([f"target.{k} = source.{k}" for k in cfg.pk_columns])

            (delta_table.alias("target")
                .merge(df_bronze.alias("source"), merge_cond)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute())

        # 3. Calcular la data màxima trobada a Bronze per actualitzar el watermark
        if cfg.source_datecol:
            max_date = df_bronze.agg(_max(cfg.source_datecol)).collect()[0][0]
            if max_date:
                cfg.set_new_watermark(max_date)

# -------------------------------------------------------------
# Bloc principal d'execució del Notebook
# -------------------------------------------------------------
configs = (
    wm.WatermarkQuery(spark)
    .source("SAGE")
    .runnable()
    .ordered()
    .build()
)

for cfg in configs:
    processar_taula(cfg)
```

---

## 2. Patró: Notebook Generador de Queries

Aquest patró s'utilitza al primer pas del Pipeline (`NB_Querys_<Origen>`) per generar el payload JSON que l'activitat `ForEach` del Pipeline d'Extracció consumirà:

```python
import bfg_watermark as wm
import json

# Obtenir només taules actives i executables avui
configs = (
    wm.WatermarkQuery(spark)
    .source("SAGE")
    .runnable()
    .ordered()
    .build()
)

output = [
    {
        "table": cfg.table_name,
        "query": cfg.build_source_query(),
        "bronze_path": cfg.bronze_path
    }
    for cfg in configs
    if cfg.build_source_query() is not None
]

# Sortir del Notebook retornant el JSON serialitzat
mssparkutils.notebook.exit(json.dumps(output))
```

---

## 3. Patró: Registre Programàtic de Taules amb `WatermarkAdmin`

A partir de la versió **1.0.6**, pots registrar una bateria de taules de forma totalment programàtica des d'un notebook d'inicialització:

```python
from bfg_watermark import WatermarkAdmin
admin = WatermarkAdmin(spark)

taules_a_donar_d_alta = [
    {
        "source": "SAGE",
        "table": "PORDER",
        "area": "COMPRES",
        "silver_table": "silver_sage_compres_porder",
        "bronze_path": "Files/Bronze/Sage/Compres/PORDER.parquet",
        "source_query": "SELECT UPDTICK_0, NUM_0, UPDDATTIM_0 FROM porder",
        "source_datecol": "UPDDATTIM_0",
        "pk_columns": ["NUM_0"],
        "window_days": 2,
        "load_mode": "INCREMENTAL"
    },
    {
        "source": "SAGE",
        "table": "TABCOUNTRY",
        "area": "MASTERS",
        "silver_table": "silver_sage_masters_tabcountry",
        "bronze_path": "Files/Bronze/Sage/Masters/TABCOUNTRY.parquet",
        "source_query": "SELECT CRY_0, CRYDES_0, UPDDATTIM_0 FROM tabcountry",
        "source_datecol": "UPDDATTIM_0",
        "pk_columns": ["CRY_0"],
        "is_dimension": True,
        "load_mode": "FULL_RELOAD"
    }
]

for t in taules_a_donar_d_alta:
    admin.insert_table(**t, overwrite=True)
```

---

## 4. Patró: Consultes Avançades amb `.where()` i Camps Extres

```python
import bfg_watermark as wm

# Cercar taules incrementals d'una àrea amb més d'un dia de finestra
configs = (
    wm.WatermarkQuery(spark)
    .where(source="SAGE", area="COMPRES", is_dimension=False)
    .ordered()
    .build()
)

for cfg in configs:
    # Lectura de columnes estàndard
    print(f"Taula: {cfg.table_name} | Clau primària: {cfg.pk_columns}")
    
    # Lectura de columnes del sistema o afegides
    print(f"  Darrera execució OK: {cfg.last_run_ok} ({cfg.last_run_at})")
    print(f"  Data creació registre: {cfg.created_at}")
```

---

## 5. Patró: Diagnòstic Ràpid i Monitorització

Pots utilitzar una cel·la d'un notebook interactiu per comprovar la salut de tot el sistema:

```python
from bfg_watermark import WatermarkAdmin
admin = WatermarkAdmin(spark)

# 1. Estat de salut general
print("--- Estat General de Taules ---")
display(admin.status())

# 2. Taules que han fallat
print("--- Errors Recents ---")
display(admin.errors())

# 3. Taules que porten més de 26 hores sense carregar
print("--- Taules Estancades (+26h) ---")
display(admin.stale(hours=26))
```
