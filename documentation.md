#  Watermark Manager - Documentació Oficial

El **Watermark Manager** és un *framework* dissenyat per a Microsoft Fabric per gestionar, orquestrar i controlar la càrrega de dades (Full Reload i Incremental) des de múltiples orígens cap a les capes Bronze i Silver del Lakehouse.

Utilitza una taula de control (`Watermark_Control`) per fer el seguiment de les dates d'última extracció, manejar excepcions, forçar re-càrregues completes o saltar execucions a nivell de taula, àrea o origen de forma dinàmica.

---

## 1. Empaquetar i Instal·lar la Llibreria a Fabric

Per poder utilitzar aquesta llibreria en diferents Notebooks sense haver de copiar el codi o manipular el `sys.path`, l'estratègia correcta a Fabric és empaquetar-la com a arxiu `.whl` (Wheel) i instal·lar-la en un **Environment**.

### Pas 1: Crear el fitxer `setup.py`
Assegura't de tenir un fitxer `setup.py` a la mateixa carpeta arrel (fora de la carpeta `watermark`) amb aquest contingut:
```python
from setuptools import setup, find_packages

setup(
    name="watermark",
    version="1.0.0",
    description="Llibreria d'orquestració i marques d'aigua per a Fabric",
    packages=find_packages(),
)
```

### Pas 2: Generar l'arxiu Wheel (.whl)
Obre un terminal a la carpeta on hi ha el `setup.py` i executa:
```bash
python setup.py bdist_wheel
```
Això crearà una carpeta `dist/` on hi trobaràs l'arxiu `watermark-1.0.0-py3-none-any.whl`.

### Pas 3: Instal·lar a Fabric
1. Ves a l'espai de treball de Fabric.
2. Crea o edita un **Environment** (Entorn).
3. A la secció de "Public Libraries" o "Custom Libraries", puja l'arxiu `.whl`.
4. Guarda i publica l'entorn.
5. Associa els teus Notebooks a aquest Environment. Ara podràs fer `import watermark` directament!

---

## 2. API i Funcionalitats Principals

La llibreria s'exposa de forma neta a través de l'`__init__.py`. Les eines principals són tres:

### A. `WatermarkQuery` (Cerca i llistat de taules)
S'utilitza per consultar l'estat actual i decidir quines taules han de processar-se avui. Permet generar el JSON per passar al pipeline de Data Factory.
```python
import _watermark as wm

# Obtenir només les taules actives i configurades de ORACLE i SAP
configs = wm.WatermarkQuery().source("ORACLE", "SAP").runnable().build()

for cfg in configs:
    print(cfg.table_name, cfg.effective_mode, cfg.source_query)
```

### B. `WatermarkAdmin` (Manteniment)
S'utilitza des de Notebooks d'administració per forçar comportaments en la següent execució sense haver de tocar codi SQL.
```python
import _watermark as wm

admin = wm.WatermarkAdmin()

# Activar una àrea sencera
admin.activate(area="COMPRES")

# Configurar el mode de càrrega d'una àrea
admin.set_mode(wm.MODE_INCREMENTAL, area="COMPRES")

# Forçar càrrega completa per a una taula d'Oracle en el següent run
admin.force_full_reload(source="ORACLE", table="PORDER")

# Saltar l'extracció d'una taula avui (si la BBDD origen cau, per exemple)
admin.skip_next_run(table="SALES_INVOICE")
```

### C. `watermark_session` (Context Manager d'Execució)
S'encarrega d'emmagatzemar l'èxit o el fracàs del procés Silver i estampar la nova data (Watermark).
```python
import _watermark as wm

cfg = wm.WatermarkQuery().source("ORACLE").table("PORDER").build(single=True)

with wm.watermark_session(config=cfg):
    # Llegim la capa bronze
    df = spark.read.parquet(cfg.bronze_path)
    
    # Transformacions silver
    df_clean = df.dropDuplicates(cfg.pk_columns.split(','))
    
    # Escrivim a la taula Delta
    df_clean.write.mode("append").saveAsTable(cfg.silver_table)
    
    # Informem a la configuració quin és el nou watermark a guardar
    max_date = df_clean.selectExpr(f"max({cfg.source_datecol})").collect()[0][0]
    cfg.set_new_watermark(max_date)

# Al sortir del `with`, l'estat i el nou watermark es guarden automàticament.
```

---

## 3. Escenari Real d'Arquitectura (Bronze i Silver)

Aquest és el flux recomanat i el patró de disseny a utilitzar amb Data Factory i Synapse Notebooks:

### Pas 1: Notebook d'Orquestració (L'Amo del Calabozo)
El disseny de referència recomana orquestrar per **Àrees** de negoci (ex: Compres, Vendes, Comptabilitat) en comptes de fer-ho tot de cop. 
Crea un Notebook (ex: `NB_Control_Extraction`) que rebi com a paràmetre l'Àrea a carregar, consulti quines tasques s'han de fer per aquesta àrea i retorni un Array JSON al Pipeline.
```python
# Paràmetre que passa Data Factory
PARAM_AREA = "COMPRES"

import _watermark as wm
import json

# Obtenim totes les taules de l'àrea especificada que estiguin actives i no estiguin "SKIPPED"
tasques = wm.WatermarkQuery().area(PARAM_AREA).runnable().build()

# Preparem la informació per al Pipeline de Data Factory
output = []
for t in tasques:
    output.append({
        "source": t.source,
        "table": t.table_name,
        "bronze_path": t.bronze_path,
        "query": t.build_source_query() # Màgia: Retorna la query filtrada per data (incremental) o neta (full)
    })

# Enviem el JSON al Data Factory perquè l'itèri
mssparkutils.notebook.exit(json.dumps(output))
```

### Pas 2: Data Factory Pipeline (Extracció a Bronze per Àrea)
1. Construeix el Pipeline passant per paràmetre quina **Àrea** vols executar (així pots tenir 1 sol pipeline reutilitzable).
2. Posa una activitat **Notebook** cridant el teu `NB_Control_Extraction` i passant-li l'Àrea.
3. Afegeix una activitat **ForEach** que itera sobre la sortida (l'array JSON) de l'activitat anterior.
4. Dins del ForEach, posa una activitat **Copy Data**:
   - **Origen**: Base de dades relacional. Com a "Query", passa dinàmicament el camp `@item().query` que ve del JSON.
   - **Destinació**: El Lakehouse a Files (Capa Bronze). La ruta serà `@item().bronze_path`.

### Pas 3: Notebook de Càrrega a Silver (Iteració per Àrea)
Un cop finalitzada l'extracció de tota l'àrea a Bronze, o bé de forma independent taula a taula, l'ideal és que un Notebook de processament **Silver** recorri aquestes taules. Es pot cridar amb la configuració de tota l'àrea:
```python
# Paràmetre que rep el Notebook des del Pipeline
PARAM_AREA = "COMPRES"

import _watermark as wm
import pyspark.sql.functions as F

# 1. Recuperem l'estat per tota aquesta àrea
taules = wm.WatermarkQuery().area(PARAM_AREA).runnable().build()

# 2. Iterem (watermark_session s'encarrega d'obrir i tancar transaccions per taula)
for cfg in taules:
    with wm.watermark_session(config=cfg):
        
        # 3. Llegim Bronze
        df_bronze = spark.read.format("parquet").load(cfg.bronze_path)
        
        # 4. Mode Full o Incremental?
        if cfg.effective_mode == 'FULL_RELOAD':
            # Sobreescriu Silver
            df_bronze.write.format("delta").mode("overwrite").saveAsTable(cfg.silver_table)
        else:
            # Lògica de Merge a Delta fent servir cfg.pk_columns
            pass

        # 5. Informem de la nova data màxima (Watermark) perquè la desi
        if cfg.source_datecol:
            max_date = df_bronze.select(F.max(cfg.source_datecol)).first()[0]
            cfg.set_new_watermark(max_date)

# Si el codi peta per a una taula concreta, watermark_session ho captura i passa a la següent.
```

---

## 4. Referència Completa de Mètodes

Tots els exemples assumeixen `import _watermark as wm`.

### `wm.WatermarkAdmin()`
*Classe per l'administració de la taula de control.*
- `activate(source, area, table)`: Posa `ACTIVE=True`. (ex: `admin.activate(area="COMPRES")`)
- `deactivate(source, area, table)`: Posa `ACTIVE=False`. Les taules no s'executaran.
- `set_mode(mode, source, area, table)`: Canvia el comportament per defecte permanentment (`wm.MODE_INCREMENTAL`, `wm.MODE_FULL_RELOAD`, `wm.MODE_SKIP`).
- `force_full_reload(source, area, table)`: Força que la següent execució sigui un `FULL_RELOAD`. (Només aplica 1 vegada).
- `skip_next_run(source, area, table)`: Força que la següent execució se salti i no faci res. (Només aplica 1 vegada).
- `resume_from_skip(source, area, table)`: Torna l'estat normal a taules que havien estat marcades per ser saltades manualment.
- `reset_watermark(source, area, table)`: Neteja la data del watermark, provocant un Full Reload inevitable la propera vegada.
- `failed()`: Retorna un DataFrame de Spark amb les taules que van fallar en la seva última execució.
- `stale(hours=25)`: Retorna un DataFrame amb les taules que porten més temps sense executar-se del compte (útil per alertes).

### `wm.WatermarkQuery()`
*Builder per filtrar i obtenir configuracions.*
- `.source(*sources)`: Filtra per un o més noms d'origen. (ex: `query.source("ORACLE")`)
- `.area(*areas)`: Filtra per una o més àrees. (ex: `query.area("COMPRES")`)
- `.table(*tables)`: Filtra pel nom de taula.
- `.dimension(is_dimension=True)`: Filtra taules que són dimensions (o facts si pases False).
- `.runnable(only=True)`: Si s'activa, exclou les taules desactivades i les que tenen el mode resolt en `SKIP`.
- `.build(single=False)`: Retorna una llista de `WatermarkConfig`. Si s'activa `single=True`, retorna un sol objecte o llança error.

### `wm.WatermarkConfig`
*Objecte retornat pel query builder que representa una taula.*
- **Propietats informatives**: `source`, `table_name`, `area`, `is_dimension`, `silver_table`, `bronze_path`, `pk_columns`, `source_query`, `source_datecol`, `window_days`.
- `effective_mode`: L'estratègia resolta actual ('FULL_RELOAD', 'INCREMENTAL', 'SKIP').
- `from_date`: La data límit per l'extracció, amb els `window_days` ja restats. És `None` si és mode full.
- `is_first_run`: Booleà que indica si és la primera vegada que es carrega la taula.
- `build_source_query(date_format)`: Construeix i retorna la Query SQL (p.ex per passar a Data Factory) injectant-hi el filtre de data necessari. Pots forçar el format de la data amb el paràmetre opcional `date_format`.
- `set_new_watermark(date_value)`: Mètode per sobreescriure la data que es guardarà quan el procés acabi amb èxit.

### `wm.watermark_session(config=cfg, source=..., table=...)`
*Context Manager per a encapsular l'execució d'una taula.*
```python
with wm.watermark_session(config=cfg):
    # El teu codi PySpark
```
