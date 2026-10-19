# `bfg_watermark` — Documentació Completa

> **Versió:** 1.0.6 · **Lakehouse de control:** `LH_BRONCE_PLATA` · **Taula de control:** `Watermark_Control`

---

## Índex

1. [Concepte: Què és un Watermark i per a què serveix?](#1-concepte)
2. [La Taula de Control `Watermark_Control`](#2-la-taula-de-control)
   - [A. Columnes d'Identitat](#a-columnes-didentitat)
   - [B. Columnes d'Enrutament](#b-columnes-denrutament)
   - [C. Columnes d'Extracció i Merge](#c-columnes-dextracció-i-merge)
   - [D. Columnes de Control d'Execució](#d-columnes-de-control-dexecució)
   - [E. Columnes d'Estat i Auditoria](#e-columnes-destat-i-auditoria)
   - [F. Esquema Centralitzat, Valors per Defecte i Columnes Personalitzades](#f-esquema-centralitzat-valors-per-defecte-i-columnes-personalitzades)
3. [Arquitectura: Com funciona el sistema de cap a cap](#3-arquitectura)
4. [La Llibreria `bfg_watermark`: Referència de l'API](#4-referència-de-lapi)
   - [A. `WatermarkQuery` — Consulta de la taula de control](#a-watermarkquery)
   - [B. `WatermarkConfig` — Configuració d'una taula](#b-watermarkconfig)
   - [C. `watermark_session` — Context Manager Transaccional](#c-watermark_session)
   - [D. `WatermarkAdmin` — Operacions d'Administració i CRUD](#d-watermarkadmin)
   - [E. Submòdul `constants` — Constants del Sistema i Columnes](#e-submòdul-constants)
   - [F. Submòdul `schema` — Metadades i Validació d'Esquema](#f-submòdul-schema)
5. [Patrons de codi habituals](#5-patrons-de-codi-habituals)
6. [Guia d'Operacions del Dia a Dia](#6-guia-doperacions-del-dia-a-dia)
7. [Escenaris Especials](#7-escenaris-especials)
8. [Configuració d'Entorn i Manteniment de la Llibreria](#8-configuració-dentorn)

---

## 1. Concepte

### El problema que resol

**La solució és el Watermark (marca d'aigua):** guardes la data de l'últim registre que vas processar exitosament, i la pròxima vegada **només demanes els registres més nous que aquella data**.

### Com funciona conceptualment

```
Primera vegada (FULL_RELOAD):
  Origen → descarrega TOT → Bronze → Plata
  Watermark guardat: 2025-01-15 08:30:00

Segona nit (INCREMENTAL):
  Origen → descarrega NOMÉS registres amb data > 2025-01-15 08:30:00
         → Bronze (parquet petit, ex: 2.000 registres) → Plata (MERGE)
  Watermark guardat: 2025-01-16 08:15:00

Tercera nit (INCREMENTAL):
  Origen → descarrega NOMÉS registres amb data > 2025-01-16 08:15:00
  ...
```

### La Watermark Table és el cervell del sistema

Tota la intel·ligència del procés viu a una única taula Delta al Lakehouse: **`Watermark_Control`**. Cada fila representa una taula del sistema origen, i conté:

- **Configuració estàtica:** com s'anomena la taula a l'origen, on es guarden les dades a Bronze i Plata, quines columnes formen la clau primària, etc.
- **Estat dinàmic:** quan va ser l'últim procés exitós, si hi va haver errors, quin mode s'ha d'aplicar la propera vegada, etc.

D'aquesta manera, els Notebooks de transformació **no contenen lògica hardcoded**: simplement pregunten a la taula de control "com t'he de tractar?" i actuen en conseqüència.

---

## 2. La Taula de Control `Watermark_Control`

La taula té **21 columnes** organitzades en 5 grups funcionals, governades de manera centralitzada per l'esquema de metadades de la llibreria (`CONTROL_TABLE_SCHEMA`).

### A. Columnes d'Identitat

Identifiquen unívocament cada registre de la taula. La clau primària és la combinació **(SOURCE + TABLE_NAME)**.

| Columna | Tipus | Exemple | Descripció |
|---------|-------|---------|------------|
| `SOURCE` | STRING | `'SAGE'` | Identificador del sistema d'origen des del qual es carrega la taula. Agrupa totes les taules que venen del mateix sistema extern (ex: `'SAGE'`, `'ORACLE'`, `'MSSQL'`). S'utilitza com a criteri principal de filtrat als Pipelines i a `WatermarkQuery.source(...)`. Obligatòria. |
| `TABLE_NAME` | STRING | `'PORDER'` | Nom exacte de la taula a l'origen. **No és el nom a Plata**, sinó el nom de l'objecte a la base de dades font. Juntament amb `SOURCE` forma la clau primària de la WT. Obligatòria. |
| `AREA` | STRING | `'COMPRES'` | Àrea de negoci o domini funcional al qual pertany la taula (ex: `'COMPRES'`, `'VENDES'`, `'STOCKS'`). Permet executar o saltar grups sencers de taules amb una sola instrucció d'Admin (`admin.deactivate(source="SAGE", area="COMPRES")`). Obligatòria. |
| `IS_DIMENSION` | BOOLEAN | `false` | Indica si la taula és una dimensió (`true`) o una taula de fets (`false`). Les dimensions acostumen a ser taules petites que es carreguen senceres cada vegada (`FULL_RELOAD`) i no necessiten lògica incremental. Opcional (defecte: `false`). |

### B. Columnes d'Enrutament

Indiquen al codi *on* ha d'anar a llegir les dades i *on* les ha d'escriure.

| Columna | Tipus | Exemple | Descripció |
|---------|-------|---------|------------|
| `SILVER_TABLE` | STRING | `'silver_sage_compres_porder'` | Nom complet de la taula Delta destí a la capa de Plata (Silver). El Notebook de transformació escriu aquí el resultat final. S'usa amb `cfg.silver_table` per fer el `saveAsTable()` o el `DeltaTable.forName()` al MERGE. Obligatòria. |
| `BRONZE_PATH` | STRING | `'Files/Bronze/Sage/Compres/PORDER.parquet'` | Ruta relativa (des de l'arrel del Lakehouse) on el Pipeline d'extracció guarda el fitxer `.parquet`. El Notebook Bronze→Plata llegeix exactament d'aquí amb `spark.read.parquet(cfg.bronze_path)`. Ha de coincidir amb la ruta configurada a l'activitat *Copy Data* del Pipeline. Obligatòria. |

### C. Columnes d'Extracció i Merge

Controlen *com* s'extreuen les dades de l'origen i com s'integren a Plata.

| Columna | Tipus | Exemple | Descripció |
|---------|-------|---------|------------|
| `SOURCE_QUERY` | STRING | `'SELECT UPDTICK_0, NUM_0, ... FROM porder'` | Query SQL completa (sense filtre de dates) que s'ha d'executar contra la font de dades. La funció `cfg.build_source_query()` agafa aquest SQL base i, si el mode és `INCREMENTAL`, hi afegeix automàticament el filtre `WHERE <datecol> >= '<from_date>'` i un `EXISTS` de control (per no extreure dades si no n'hi ha de més noves que el watermark). Si és `NULL`, el notebook gestiona la query manualment. Obligatòria en el registre de taules. |
| `SOURCE_DATECOL` | STRING | `'UPDDATTIM_0'` | Nom de la columna de data/timestamp a l'origen que s'usa com a criteri del filtre incremental. Ha de ser una columna que s'actualitza sempre que el registre canvia (típicament la columna d'última modificació del sistema origen, com `UPDDATTIM_0` a Sage). Obligatòria. |
| `SOURCE_DATECOL_FORMAT` | STRING | `'%Y-%m-%d %H:%M:%S'` | Format Python (`strftime`) per representar la data del filtre quan s'afegeix al SQL. Per defecte és `'%Y-%m-%d %H:%M:%S'`. Necessita ajust si la font espera un format diferent (ex: `TO_TIMESTAMP('%d/%m/%y %H:%M:%S', 'DD/MM/YY HH24:MI:SS')` per a Sage/Oracle). Obligatòria (amb defecte: `'%Y-%m-%d %H:%M:%S'`). |
| `WINDOW_DAYS` | INTEGER | `2` | Nombre de dies de solapament que s'apliquen al watermark per seguretat. Per exemple, si el watermark és `2025-01-15` i `WINDOW_DAYS = 2`, la query incremental filtrarà per `>= 2025-01-13`. Evita perdre registres en casos de problemes de rellotge a l'origen o càrregues parcials. Un valor de `0` vol dir "exactament des del watermark, sense solapament". Opcional (defecte: `0`). |
| `PK_COLUMNS` | STRING (JSON) | `'["NUM_0", "CPY_0"]'` | Array JSON amb els noms de les columnes que formen la clau primària de la taula a l'origen. El Notebook Bronze→Plata usa `cfg.pk_columns` per construir la condició del `MERGE` (ex: `target.NUM_0 = source.NUM_0 AND target.CPY_0 = source.CPY_0`). **Ha d'estar en format JSON vàlid amb cometes dobles.** Obligatòria. |

### D. Columnes de Control d'Execució

Controlen *si* i *com* s'executa la càrrega en cada cicle.

| Columna | Tipus | Valors vàlids | Descripció |
|---------|-------|---------------|------------|
| `ACTIVE` | BOOLEAN | `true` / `false` | Activa o desactiva completament la taula del cicle de càrrega. Les taules amb `ACTIVE = false` s'ignoren per la funció `.active_only()` i `.runnable()`. Útil per apagar permanentment taules que ja no s'utilitzen sense esborrar-les de la WT. Opcional (defecte: `true`). |
| `LOAD_MODE` | STRING | `INCREMENTAL`, `FULL_RELOAD`, `SKIP` | **Mode de càrrega permanent:** defineix el comportament per defecte en cada execució del Pipeline. `INCREMENTAL` = només les novetats des del darrer watermark; `FULL_RELOAD` = tot cada cop (adequat per a dimensions petites); `SKIP` = mai executar (taules desactivades funcionalment però actives a la WT). Opcional (defecte: `INCREMENTAL`). |
| `NEXT_RUN_MODE` | STRING \| NULL | `FULL_RELOAD`, `SKIP`, `NULL` | **Override puntual per a la propera execució:** si té valor, sobreescriu el `LOAD_MODE` **una sola vegada**. Un cop l'execució finalitza correctament, la sessió el posa automàticament a `NULL`. Típicament s'usa per forçar un `FULL_RELOAD` puntual (ex: l'esquema ha canviat) o un `SKIP` temporal (ex: l'origen té problemes). Opcional (defecte: `NULL`). |
| `RUN_ORDER` | INTEGER | `1`, `2`, `3`... | Prioritat d'execució dins del Pipeline. Les taules amb `RUN_ORDER` més baix s'executen primer quan el Query retorna resultats ordenats amb `.ordered()`. Útil quan unes taules depenen d'altres (ex: dimensions amb `RUN_ORDER = 1` i fets amb `RUN_ORDER = 2`). Opcional (defecte: `1`). |

**Lògica de prioritat del mode efectiu (calculada per `cfg.effective_mode`):**

```
1. Si NEXT_RUN_MODE té valor       → s'aplica NEXT_RUN_MODE
2. Si WATERMARK_LAST_VALUE és NULL → s'aplica FULL_RELOAD (primera càrrega ever)
3. En cas contrari                 → s'aplica LOAD_MODE
```

### E. Columnes d'Estat i Auditoria

Registren l'estat de l'última execució. **Escrites automàticament per la llibreria**, mai s'han de modificar manualment (el motor de validació en bloqueja l'edició manual directa per garantir la integritat del sistema).

| Columna | Tipus | Descripció |
|---------|-------|------------|
| `WATERMARK_LAST_VALUE` | TIMESTAMP | La data/hora del registre més recent processat en l'última execució exitosa. En el proper cicle incremental, la query filtra per registres posteriors a aquest valor (menys la finestra `WINDOW_DAYS`). **Si és NULL**, la taula mai s'ha carregat (o s'ha resetejat), i el sistema forçarà un `FULL_RELOAD`. Opcional a la inserció (defecte: `NULL`). |
| `LAST_RUN_AT` | TIMESTAMP | Data i hora exacta en que va acabar l'última execució (exitosa o fallida). Útil per detectar taules desactualitzades amb `admin.stale()`. Gestionada pel sistema. |
| `LAST_RUN_OK` | BOOLEAN | `true` si l'última execució va finalitzar sense errors, `false` si va fallar. Combinada amb `LAST_ERROR`, permet veure ràpidament l'estat de salut del sistema. Gestionada pel sistema. |
| `LAST_ERROR` | STRING | Missatge d'error (màxim 500 caràcters) de l'última execució fallida. És `NULL` quan `LAST_RUN_OK = true`. La sessió el neteja automàticament en la propera execució exitosa. Gestionada pel sistema. |
| `CREATED_AT` | TIMESTAMP | Data/hora de creació de la fila a la WT. S'estableix una sola vegada durant la inserció automàticament amb `CURRENT_TIMESTAMP()`. |
| `UPDATED_AT` | TIMESTAMP | Data/hora de l'última modificació del registre, ja sigui per una execució del Pipeline o per una operació manual via `WatermarkAdmin`. S'actualitza automàticament amb `CURRENT_TIMESTAMP()`. |

### F. Esquema Centralitzat, Valors per Defecte i Columnes Personalitzades

Des de la versió **1.0.6**, la taula `Watermark_Control` està governada pel mòdul intern `schema.py` (`CONTROL_TABLE_SCHEMA`), que aplica regles de validació estrictes tant en inserir com en modificar registres:

1. **Camps obligatoris estrictes (`required=True`):**
   - Els camps `source`, `table`, `area`, `silver_table`, `bronze_path`, `source_query`, `source_datecol`, `source_datecol_format` i `pk_columns` són requerits.
   - No poden ser `None`, ni cadenes buides o compostes només per espais en blanc, ni col·leccions (`pk_columns`) buides.
2. **Tipat estricte:**
   - Cada camp valida el seu tipus Python (`str`, `int`, `bool`, col·leccions o `datetime`). Per exemple, Python tracta `bool` com a subclasse d'`int`, però la validació de `bfg_watermark` impedeix expressament que un booleà com `True` sigui acceptat en lloc d'un enter a `window_days` o `run_order`.
3. **Valors per defecte automàtics:**
   - Si no s'especifiquen a la inserció, s'apliquen automàticament:
     - `window_days`: `0`
     - `active`: `True`
     - `run_order`: `1`
     - `load_mode`: `'INCREMENTAL'`
     - `next_run_mode`: `None`
     - `is_dimension`: `False`
     - `watermark_last`: `None`
4. **Columnes d'auditoria protegides:**
   - Les columnes `CREATED_AT` i `UPDATED_AT` estan protegides a `SYSTEM_AUDIT_COLUMNS`; no es permet la seva modificació manual directa per evitar corrupció de logs d'auditoria.
5. **Suport per a columnes personalitzades (`**extra_fields`):**
   - Si una organització afegeix columnes addicionals a la taula Delta (ex: `ENVIRONMENT`, `BUSINESS_OWNER`, `TAGS`, etc.), els mètodes `admin.insert_table(**extra_fields)` i `admin.update_table(**kwargs)` les accepten i persisteixen automàticament.
   - Aquestes columnes es poden filtrar amb `WatermarkQuery.where(nom_columna=valor)` i consultar mitjançant `cfg.get('nom_columna')` o directament per atribut `cfg.nom_columna`.

---

## 3. Arquitectura

El sistema s'organitza en **dos nivells de Pipelines** i **un sistema de Notebooks**, tots coordinats per la `Watermark_Control`.

### Visió general del flux

```
[Horari programat]
        |
        v
[PL_Programador_WT_<Origen>]     <- Pipeline Mestre
        |
        |---> [PL_<Origen>_WT]   <- Pipeline d'Extracció (Bronze)
        |           |
        |     [NB_Querys_<Origen>]  --> llegeix WT --> genera JSON de queries
        |           |
        |     [ForEach -> Copy Data] --> executa SQL --> guarda .parquet a Bronze
        |
        \---> [NB_Bronco_a_Plata_<Origen>]   <- Notebook de Transformació
                    |
              llegeix WT + parquets --> fa MERGE a Plata --> actualitza WT
```

### Pas 1: El Notebook de Queries (`NB_Querys_<Origen>`)

És el primer que s'executa. Utilitza `WatermarkQuery` per llegir la `Watermark_Control` i, per a cada taula executable, crida `cfg.build_source_query()` que retorna el SQL complet (amb el filtre de dates ja inclòs si és incremental).

Genera un JSON amb la llista de taules i les seves queries, que el Pipeline llegirà per iterar:

```python
import bfg_watermark as wm, json

configs = wm.WatermarkQuery(spark).source("SAGE").runnable().ordered().build()

output = [{"table": cfg.table_name, "query": cfg.build_source_query()} for cfg in configs]
mssparkutils.notebook.exit(json.dumps(output))
```

### Pas 2: El Pipeline d'Extracció (Bronze)

El Pipeline recull el JSON del pas anterior i itera (`ForEach`) sobre cada element. Per a cada taula:
- Una activitat **Copy Data** executa el SQL directament contra l'origen (via ODBC/Linked Service).
- Guarda el resultat com a fitxer `.parquet` a la ruta `BRONZE_PATH` del Lakehouse.

> **Important:** el Copy Data no sap si és incremental o FULL_RELOAD: simplement executa el SQL que li dóna el Notebook. Tota la intel·ligència del filtre ja estava al SQL construït al Pas 1 per `build_source_query()`.

### Pas 3: El Notebook de Transformació (`NB_Bronce_a_Plata_<Origen>`)

Quan el Pipeline d'Extracció acaba, el Mestre crida aquest Notebook. Per a cada taula:
1. Torna a llegir la WT per obtenir la configuració (`WatermarkQuery`).
2. Obre una `watermark_session` que protegeix tot el bloc.
3. Llegeix el `.parquet` de Bronze.
4. Si `FULL_RELOAD`: sobreescriu la taula de Plata completament.
5. Si `INCREMENTAL`: fa un MERGE (upsert) contra la taula Delta de Plata.
6. Calcula el valor màxim de la columna de data del Bronze i crida `cfg.set_new_watermark(max_date)`.
7. En sortir del `with`, la sessió actualitza la WT automàticament (un sol MERGE atòmic).

---

## 4. Referència de l'API

La llibreria s'importa com:
```python
import bfg_watermark as wm
# o selectivament:
from bfg_watermark import WatermarkQuery, WatermarkConfig, watermark_session, WatermarkAdmin
from bfg_watermark import constants as c
from bfg_watermark.constants import MODE_INCREMENTAL, MODE_FULL_RELOAD, MODE_SKIP
from bfg_watermark import schema
```

---

### A. `WatermarkQuery`

Builder fluent per consultar la `Watermark_Control`. Permet encadenar filtres i retorna objectes `WatermarkConfig`.

**Exemple d'ús típic:**
```python
# Totes les taules de SAGE executables, ordenades
configs = wm.WatermarkQuery(spark).source("SAGE").runnable().ordered().build()

# Una sola taula concreta
cfg = wm.WatermarkQuery(spark).source("SAGE").table("PORDER").one()

# Cerca amb filtre genèric per columnes estàndard o extres
configs = wm.WatermarkQuery(spark).where(silver_table="silver_sage_compres_porder", active=True).build()
```

**Mètodes de filtre (encadenables):**

| Mètode | Signatura | Descripció |
|--------|-----------|------------|
| `.source(*sources)` | `source("SAGE")` o `source("SAGE", "ORACLE")` | Filtra per sistema d'origen. Múltiples valors fan OR. |
| `.area(*areas)` | `area("COMPRES")` | Filtra per àrea de negoci. Múltiples valors fan OR. |
| `.table(*tables)` | `table("PORDER")` o `table("PORDER", "SORDER")` | Filtra per nom de taula. Múltiples valors fan OR. |
| `.dimension(is_dim)` | `dimension(True)` | Filtra pel flag `IS_DIMENSION`. Per defecte `True`. |
| `.active_only()` | `active_only()` | Exclou files amb `ACTIVE = false`. |
| `.runnable()` | `runnable()` | **Recomanat per Pipelines.** Aplica `.active_only()` i exclou taules on `effective_mode = SKIP`. La forma segura d'obtenir les taules que realment s'han d'executar avui. |
| `.mode(*modes)` | `mode("INCREMENTAL")` | Filtra pel `LOAD_MODE` permanent. |
| `.where(**kwargs)` | `where(silver_table="...", active=True, window_days=2)` | **Filtre genèric per a qualsevol columna** (estàndard o personalitzada/extra). Accepta `None` (`IS NULL`), booleans (`true`/`false`), enters/decimals i cadenes (amb escapat segur contra injecció SQL). Resol automàticament qualsevol àlies al nom físic de la columna via `schema.get_column_name()`. |
| `.ordered()` | `ordered()` | Ordena per `SOURCE`, `RUN_ORDER`, `TABLE_NAME`. |

**Mètodes terminals (executen la consulta):**

| Mètode | Retorna | Descripció |
|--------|---------|------------|
| `.build()` | `list[WatermarkConfig]` | Executa i retorna una llista. Pot ser buida. |
| `.one()` | `WatermarkConfig` | Executa i retorna exactament un resultat. Llança `ValueError` si n'hi ha 0 o més d'1. |

---

### B. `WatermarkConfig`

Representa la configuració i l'estat d'una sola taula. S'obté sempre a través de `WatermarkQuery`, mai s'instancia directament.

Encapsula una fila de la `Watermark_Control` i calcula automàticament l'estratègia a seguir per a l'execució actual.

**Propietats d'identitat i enrutament:**

| Propietat | Tipus | Descripció |
|-----------|-------|------------|
| `.source` | `str` | Valor de la columna `SOURCE`. |
| `.table_name` | `str` | Valor de la columna `TABLE_NAME`. |
| `.area` | `str` | Valor de la columna `AREA`. |
| `.is_dimension` | `bool` | Valor de la columna `IS_DIMENSION`. |
| `.silver_table` | `str` | Nom de la taula de destí a Plata. Usar amb `DeltaTable.forName(spark, cfg.silver_table)`. |
| `.bronze_path` | `str` | Ruta del fitxer `.parquet` a Bronze. Usar amb `spark.read.parquet(cfg.bronze_path)`. |
| `.pk_columns` | `list[str]` | Llista Python de columnes PK (deserialitzada del JSON). Usar per construir condicions de MERGE. |
| `.source_query` | `str\|None` | Query SQL base d'extracció a l'origen configurada a la columna `SOURCE_QUERY`. |
| `.source_datecol` | `str` | Nom de la columna de data a l'origen. |
| `.source_datecol_format` | `str` | Format de data per al filtre SQL (defecte: `'%Y-%m-%d %H:%M:%S'`). |
| `.window_days` | `int` | Nombre de dies de solapament temporal (defecte: `0`). |

**Propietats de control d'execució:**

| Propietat | Tipus | Descripció |
|-----------|-------|------------|
| `.active` | `bool` | Si la taula és activa a la WT. |
| `.load_mode` | `str` | Mode permanent (`LOAD_MODE`). |
| `.next_run_mode` | `str\|None` | Override puntual (`NEXT_RUN_MODE`). `None` si no n'hi ha. |
| `.watermark_last` | `datetime\|None` | Últim watermark guardat. `None` si és la primera càrrega. |
| `.last_run_at` | `datetime\|None` | Timestamp de l'última execució. |
| `.last_run_ok` | `bool\|None` | Si l'última execució va ser exitosa. |
| `.last_error` | `str\|None` | Missatge de l'últim error. |

**Propietats calculades (lògica de negoci — usar sempre aquestes, no llegir les columnes crues):**

| Propietat | Tipus | Descripció |
|-----------|-------|------------|
| `.effective_mode` | `str` | **El mode real per a AQUESTA execució**, seguint la prioritat: `NEXT_RUN_MODE` > (NULL watermark → `FULL_RELOAD`) > `LOAD_MODE`. Sempre usar aquest en el codi, mai `load_mode` directament. |
| `.from_date` | `datetime\|None` | Data de tall calculada (watermark - window_days). `None` si s'ha de fer `FULL_RELOAD`. Usar com a límit inferior en queries manuals. |
| `.should_run` | `bool` | `False` si `effective_mode == 'SKIP'`. Pot usar-se per sortir ràpid d'un notebook. |
| `.is_first_run` | `bool` | `True` si `WATERMARK_LAST_VALUE` és `NULL`. Primera càrrega de la taula. |

**Mètodes d'acció:**

| Mètode | Descripció |
|--------|------------|
| `.build_source_query()` | Retorna el SQL complet llest per executar: si és incremental, afegeix el filtre `WHERE <datecol> >= '<from_date>'` i una subquery `EXISTS` automàticament al `SOURCE_QUERY`. Aquest `EXISTS` permet que la query retorni 0 registres ràpidament si no hi ha novetats. Retorna `None` si `SOURCE_QUERY` no està configurat a la WT. |
| `.set_new_watermark(value)` | Sobreescriu el valor que es guardarà com a nou watermark quan la sessió finalitzi. Si no es crida, la sessió guarda `datetime.now()` per defecte. Típicament s'usa per guardar el màxim de la columna de data del Bronze (`max_date = df_bronze.agg(_max(cfg.source_datecol)).collect()[0][0]`). |

**Mètodes auxiliars i accés dinàmic a columnes:**

| Mètode / Accés | Retorna | Descripció |
|----------------|---------|------------|
| `.get(key, default=None)` | `Any` | Retorna el valor de qualsevol columna (estàndard o personalitzada). La cerca és insensible a majúscules i minúscules (`case-insensitive`). Retorna `default` si la columna no existeix. |
| `.to_dict()` | `dict` | Retorna una còpia del diccionari subjacent amb totes les columnes i valors del registre de la taula de control. |
| Accés per atribut (`__getattr__`) | `Any` | Permet accedir a qualsevol columna directament com a atribut de la instància (ex: `cfg.CREATED_AT`, `cfg.created_at`, o camps afegits com `cfg.custom_tag`), sense importar majúscules o minúscules. |
| `__repr__()` | `str` | Representació textual amigable per a depuració (`WatermarkConfig(source=..., table=..., mode=..., from=..., first_run=...)`). |

---

### C. `watermark_session`

**La peça clau del sistema.** Context manager que gestiona el cicle complet: obté la configuració, executa el codi de transformació, i actualitza la `Watermark_Control` en un sol MERGE atòmic al final.

```python
with wm.watermark_session(spark, config=cfg) as cfg:
    # Tot el codi de transformació va aquí
    df = spark.read.parquet(cfg.bronze_path)
    # ...
    cfg.set_new_watermark(max_date)
# <- Aquí la sessió actualitza la WT automàticament
```

**Signatures acceptades:**
```python
# Amb config pre-carregada (recomanat en bucles)
with watermark_session(spark, config=cfg) as cfg:

# Sense config (la sessió la carrega internament)
with watermark_session(spark, source="SAGE", table="PORDER") as cfg:
```

**Comportament garantit per a cada escenari:**

| Escenari | Què fa la sessió |
|----------|-----------------|
| Codi finalitza sense errors | Un sol MERGE: avança `WATERMARK_LAST_VALUE`, posa `LAST_RUN_OK = true`, neteja `LAST_ERROR`, neteja `NEXT_RUN_MODE` si era un override. |
| Codi llança una excepció | Un sol MERGE: manté `WATERMARK_LAST_VALUE` intacte (no avança), posa `LAST_RUN_OK = false`, guarda el missatge d'error a `LAST_ERROR`. Relança l'excepció per propagar-la. |
| Mode és SKIP | Fa un `yield` del `cfg` i surt immediatament sense escriure res a la WT. |

---

### D. `WatermarkAdmin`

Proporciona operacions administratives, de gestió del cicle de vida (CRUD), control d'execució i manteniment sobre la `Watermark_Control`.

```python
from bfg_watermark import WatermarkAdmin
admin = WatermarkAdmin(spark)
```

#### 1. Gestió del Cicle de Vida de Taules (CRUD)

Permet donar d'alta noves taules, modificar la seva configuració existent o donar-les de baixa sense necessitat d'escriure SQL manual.

| Mètode | Descripció |
|--------|------------|
| `admin.insert_table(...)` | Insereix una nova taula a la taula de control amb validació d'esquema completa (`CONTROL_TABLE_SCHEMA`). Si `overwrite=False` (defecte) i la taula `(SOURCE, TABLE_NAME)` ja existeix, llança un `ValueError`. Si `overwrite=True`, executa un `MERGE` atòmic que actualitza la definició però **preserva el watermark anterior** si no s'especifica un de nou, evitant reinicis accidentals del progrés incremental. |
| `admin.update_table(...)` | Actualitza una o més columnes per a les taules que coincideixin amb els filtres `source`, `area` o `table` (almenys un és obligatori). Valida els tipus i valors permesos, escapa els literals SQL i actualitza automàticament `UPDATED_AT = CURRENT_TIMESTAMP()`. Admet tant un diccionari `updates={...}` com arguments per clau `**kwargs`. |
| `admin.delete_table(source, table, area=None)` | Esborra permanentment el registre d'una taula de la `Watermark_Control`. Requereix obligatòriament `source` i `table` no buits. Opcionalment admet `area`. |

**Detall de paràmetres de `insert_table`:**

```python
admin.insert_table(
    source="SAGE",
    table="PORDER",                                   # o table_name="PORDER"
    area="COMPRES",
    silver_table="silver_sage_compres_porder",
    bronze_path="Files/Bronze/Sage/Compres/PORDER.parquet",
    source_query="SELECT UPDTICK_0, NUM_0, UPDDATTIM_0 FROM porder",
    source_datecol="UPDDATTIM_0",
    source_datecol_format="%Y-%m-%d %H:%M:%S",        # opcional (defecte: "%Y-%m-%d %H:%M:%S")
    pk_columns=["NUM_0", "CPY_0"],                    # list, tuple o set (obligatòria)
    window_days=2,                                    # opcional (defecte: 0)
    active=True,                                      # opcional (defecte: True)
    run_order=1,                                      # opcional (defecte: 1)
    next_run_mode=None,                               # opcional (defecte: None)
    watermark_last=None,                              # opcional (defecte: None)
    is_dimension=False,                               # opcional (defecte: False)
    load_mode="INCREMENTAL",                          # opcional (defecte: "INCREMENTAL")
    overwrite=False,                                  # opcional (defecte: False)
    **extra_fields                                    # columnes addicionals personalitzades
)
```

**Exemples d'ús de `update_table`:**

```python
# Actualitzar mitjançant diccionari
admin.update_table(source="SAGE", table="PORDER", updates={"WINDOW_DAYS": 3, "RUN_ORDER": 2})

# Actualitzar mitjançant paràmetres amb nom (kwargs)
admin.update_table(source="SAGE", table="PORDER", window_days=3, run_order=2)

# Actualitzar totes les taules d'una àrea
admin.update_table(source="SAGE", area="COMPRES", updates={"LOAD_MODE": "FULL_RELOAD"})
```

#### 2. Activació i Desactivació

| Mètode | Descripció |
|--------|------------|
| `admin.activate(source=, area=, table=)` | Posa `ACTIVE = true` per a les taules que coincideixin amb els filtres. |
| `admin.deactivate(source=, area=, table=)` | Posa `ACTIVE = false`. Les taules desactivades s'ignoren en `.runnable()` i `.active_only()`. |

#### 3. Mode de càrrega permanent

| Mètode | Descripció |
|--------|------------|
| `admin.set_mode(mode, source=, area=, table=)` | Canvia el `LOAD_MODE` permanent. `mode` ha de ser `'INCREMENTAL'`, `'FULL_RELOAD'` o `'SKIP'`. |

#### 4. Overrides puntuals (per a la propera execució)

| Mètode | Descripció |
|--------|------------|
| `admin.force_full_reload(source=, area=, table=)` | Posa `NEXT_RUN_MODE = 'FULL_RELOAD'`. La propera execució del Pipeline farà una recàrrega completa i automàticament netejarà l'override en finalitzar amb èxit. |
| `admin.skip_next_run(source=, area=, table=)` | Posa `NEXT_RUN_MODE = 'SKIP'`. La propera execució s'obviarà completament sense error. |
| `admin.resume_from_skip(source=, area=, table=)` | Restaura les taules que estiguin en estat de skip: neteja `NEXT_RUN_MODE` si era `'SKIP'` (a `NULL`) i canvia `LOAD_MODE` si era `'SKIP'` (a `'INCREMENTAL'`). |
| `admin.reset_watermark(source=, area=, table=)` | Posa `WATERMARK_LAST_VALUE = NULL`. La propera execució forçarà un `FULL_RELOAD` automàtic (el watermark NULL sempre activa recàrrega completa tot i que el `LOAD_MODE` sigui `INCREMENTAL`). |

#### 5. Diagnòstic i Estat

| Mètode | Retorna | Descripció |
|--------|---------|------------|
| `admin.status()` | DataFrame | Visió completa de totes les taules: mode, estat, watermark, timestamps d'última execució i auditoria. Ordenades per `SOURCE`, `RUN_ORDER`, `TABLE_NAME`. |
| `admin.errors()` | DataFrame | Filtra les taules actives amb `LAST_RUN_OK = false`. Ordenades per data d'error descendent per identificar ràpidament incidències. |
| `admin.stale(hours=25)` | DataFrame | Taules actives sense execució exitosa en les últimes `hours` hores (defecte: `25`). Detecta taules endarrerides o no executades. |

#### 6. Delta Time Travel (auditoria i recuperació)

| Mètode | Descripció |
|--------|------------|
| `admin.show_history(limit=20)` | Mostra l'historial de transaccions Delta de `Watermark_Control` (`DESCRIBE HISTORY`) amb usuari, operació i timestamp. |
| `admin.inspect_table(version=, timestamp=)` | Llegeix la taula `Watermark_Control` tal com era en una versió Delta o timestamp concret, **en mode només lectura (sense modificar-la)**. |
| `admin.recover_table(version=, timestamp=)` | **Acció destructiva / Undo complet:** restaura la taula `Watermark_Control` sencera a un estat anterior mitjançant `RESTORE TABLE`. |

---

### E. Submòdul `constants`

El submòdul `bfg_watermark.constants` (o accessible com `wm.constants`) centralitza tots els noms d'objectes Delta, modes i columnes físiques per evitar l'ús de cadenes de text directes al codi.

**Importació recomanada:**
```python
from bfg_watermark import constants as c
# o selectivament:
from bfg_watermark.constants import TABLE_NAME, MODE_INCREMENTAL, MODE_FULL_RELOAD, MODE_SKIP
```

**Constants principals disponibles:**

| Constant | Valor | Descripció |
|----------|-------|------------|
| `TABLE_NAME` | `"Watermark_Control"` | Nom de la taula Delta de control. |
| `MODE_INCREMENTAL` | `"INCREMENTAL"` | Càrrega incremental per watermark. |
| `MODE_FULL_RELOAD` | `"FULL_RELOAD"` | Recàrrega completa des de l'origen. |
| `MODE_SKIP` | `"SKIP"` | Omissió de l'execució de la taula. |
| `VALID_MODES` | `frozenset({...})` | Conjunt immutable amb els modes vàlids. |
| `COL_SOURCE` | `"SOURCE"` | Nom físic de la columna SOURCE. |
| `COL_TABLE_NAME` | `"TABLE_NAME"` | Nom físic de la columna TABLE_NAME. |
| `COL_AREA` | `"AREA"` | Nom físic de la columna AREA. |
| `COL_IS_DIMENSION` | `"IS_DIMENSION"` | Nom físic de la columna IS_DIMENSION. |
| `COL_SILVER_TABLE` | `"SILVER_TABLE"` | Nom físic de la columna SILVER_TABLE. |
| `COL_BRONZE_PATH` | `"BRONZE_PATH"` | Nom físic de la columna BRONZE_PATH. |
| `COL_SOURCE_QUERY` | `"SOURCE_QUERY"` | Nom físic de la columna SOURCE_QUERY. |
| `COL_SOURCE_DATECOL` | `"SOURCE_DATECOL"` | Nom físic de la columna SOURCE_DATECOL. |
| `COL_SOURCE_DATECOL_FORMAT` | `"SOURCE_DATECOL_FORMAT"` | Nom físic de la columna SOURCE_DATECOL_FORMAT. |
| `COL_WINDOW_DAYS` | `"WINDOW_DAYS"` | Nom físic de la columna WINDOW_DAYS. |
| `COL_PK_COLUMNS` | `"PK_COLUMNS"` | Nom físic de la columna PK_COLUMNS. |
| `COL_ACTIVE` | `"ACTIVE"` | Nom físic de la columna ACTIVE. |
| `COL_LOAD_MODE` | `"LOAD_MODE"` | Nom físic de la columna LOAD_MODE. |
| `COL_NEXT_RUN_MODE` | `"NEXT_RUN_MODE"` | Nom físic de la columna NEXT_RUN_MODE. |
| `COL_RUN_ORDER` | `"RUN_ORDER"` | Nom físic de la columna RUN_ORDER. |
| `COL_WATERMARK_LAST` | `"WATERMARK_LAST_VALUE"` | Nom físic de la columna WATERMARK_LAST_VALUE. |
| `COL_LAST_RUN_AT` | `"LAST_RUN_AT"` | Nom físic de la columna LAST_RUN_AT. |
| `COL_LAST_RUN_OK` | `"LAST_RUN_OK"` | Nom físic de la columna LAST_RUN_OK. |
| `COL_LAST_ERROR` | `"LAST_ERROR"` | Nom físic de la columna LAST_ERROR. |
| `COL_CREATED_AT` | `"CREATED_AT"` | Nom físic de la columna CREATED_AT. |
| `COL_UPDATED_AT` | `"UPDATED_AT"` | Nom físic de la columna UPDATED_AT. |

---

### F. Submòdul `schema`

El submòdul `bfg_watermark.schema` (o accessible com `wm.schema`) encapsula la font de veritat sobre l'estructura de la taula de control, la validació de tipus de dades i la construcció dinàmica de registres.

**Importació:**
```python
from bfg_watermark import schema
# o directament:
from bfg_watermark.schema import CONTROL_TABLE_SCHEMA, validate_record, validate_column_value, get_column_name
```

**Elements principals disponibles:**

| Element | Tipus / Signatura | Descripció |
|---------|-------------------|------------|
| `CONTROL_TABLE_SCHEMA` | `dict` | Diccionari central amb les metadades de totes les columnes configurables: nom físic (`col`), tipus admès (`type`), requeriment (`required`), valor per defecte (`default`), valors vàlids (`valid_values`) i descripció. |
| `SYSTEM_AUDIT_COLUMNS` | `dict` | Metadades de les columnes d'auditoria del sistema (`LAST_RUN_AT`, `LAST_RUN_OK`, `LAST_ERROR`, `CREATED_AT`, `UPDATED_AT`), establint els seus valors SQL per defecte i la protecció en operacions de MERGE. |
| `get_column_name(field_name)` | `(str) -> str` | Normalitza i tradueix qualsevol àlies habitual (ex: `'table'`, `'table_name'`, `'watermark'`) al seu nom físic Delta (ex: `'TABLE_NAME'`, `'WATERMARK_LAST_VALUE'`). |
| `get_required_fields()` | `() -> list[str]` | Retorna la llista de noms dels camps que són d'obligada informació (`required=True`). |
| `get_column_meta(field_or_col_name)` | `(str) -> dict \| None` | Retorna el diccionari de metadades associat a un paràmetre o nom de columna. |
| `validate_column_value(col_name, val)` | `(str, Any) -> tuple[str, Any]` | Normalitza el nom de la columna i valida el seu valor contra l'esquema: comprova obligatorietat (rebutja valors nuls, cadenes buides o llistes buides), verifica el tipus (rebutjant `bool` com a `int`), comprova valors vàlids i bloqueja modificacions manuals de `CREATED_AT` i `UPDATED_AT`. Retorna `(clean_col, cleaned_val)`. |
| `validate_record(inputs)` | `(dict) -> dict[str, Any]` | Valida i normalitza un diccionari sencer de configuració d'una taula. Aplica valors per defecte de l'esquema per als camps absents o amb valor `None`, valida camps requerits, admet columnes addicionals (`extra_fields`) i retorna el diccionari llest per a la inserció o MERGE SQL. |

---

## 5. Patrons de codi habituals

### Patró: Registre i inicialització de taules amb `admin.insert_table`

A partir de la versió 1.0.6, es poden donar d'alta taules de forma programàtica garantint la validació de tipus i camps requerits:

```python
from bfg_watermark import WatermarkAdmin
admin = WatermarkAdmin(spark)

# Alta d'una taula incremental
admin.insert_table(
    source="SAGE",
    table="PORDER",
    area="COMPRES",
    silver_table="silver_sage_compres_porder",
    bronze_path="Files/Bronze/Sage/Compres/PORDER.parquet",
    source_query="SELECT UPDTICK_0, NUM_0, UPDDATTIM_0 FROM porder",
    source_datecol="UPDDATTIM_0",
    pk_columns=["NUM_0", "CPY_0"],
    window_days=2,
    run_order=1,
    load_mode="INCREMENTAL",
    overwrite=True  # Actualitza si ja existeix sense perdre el watermark anterior
)

# Alta d'una dimensió petita en FULL_RELOAD
admin.insert_table(
    source="SAGE",
    table="TABCOUNTRY",
    area="MASTERS",
    silver_table="silver_sage_masters_tabcountry",
    bronze_path="Files/Bronze/Sage/Masters/TABCOUNTRY.parquet",
    source_query="SELECT CRY_0, CRYDES_0, UPDDATTIM_0 FROM tabcountry",
    source_datecol="UPDDATTIM_0",
    pk_columns=["CRY_0"],
    is_dimension=True,
    load_mode="FULL_RELOAD",
    overwrite=True
)
```

### Patró complet: Notebook Bronze → Plata

```python
import bfg_watermark as wm
from delta.tables import DeltaTable
from pyspark.sql.functions import max as _max

def processar_taula(cfg: wm.WatermarkConfig):
    """Processa una sola taula: Bronze -> Plata amb gestió de watermark."""
    with wm.watermark_session(spark, config=cfg) as cfg:

        if not cfg.should_run:
            return  # mode SKIP: la sessió ja ho gestiona, no cal fer res

        # Llegir de Bronze
        df_bronze = spark.read.format("parquet").load(cfg.bronze_path)

        if cfg.effective_mode == wm.constants.MODE_FULL_RELOAD:
            # Sobreescriure tota la taula de Plata
            (df_bronze.write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .saveAsTable(cfg.silver_table))

        elif cfg.effective_mode == wm.constants.MODE_INCREMENTAL:
            # MERGE incremental contra la taula Delta de Plata
            delta_table = DeltaTable.forName(spark, cfg.silver_table)
            merge_cond = " AND ".join([f"target.{k} = source.{k}" for k in cfg.pk_columns])

            (delta_table.alias("target")
                .merge(df_bronze.alias("source"), merge_cond)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute())

        # Avançar el watermark al valor màxim trobat a Bronze
        if cfg.source_datecol:
            max_date = df_bronze.agg(_max(cfg.source_datecol)).collect()[0][0]
            if max_date:
                cfg.set_new_watermark(max_date)


# Obtenir totes les taules de SAGE executables i processar-les
configs = wm.WatermarkQuery(spark).source("SAGE").runnable().ordered().build()
for cfg in configs:
    processar_taula(cfg)
```

### Patró: Notebook de Queries (genera JSON per al Pipeline)

```python
import bfg_watermark as wm
import json

configs = wm.WatermarkQuery(spark).source("SAGE").runnable().ordered().build()

output = [
    {
        "table": cfg.table_name,
        "query": cfg.build_source_query(),
        "bronze_path": cfg.bronze_path
    }
    for cfg in configs
    if cfg.build_source_query() is not None  # taules sense SOURCE_QUERY es gestionen manualment
]

mssparkutils.notebook.exit(json.dumps(output))
```

### Patró: Filtrat dinàmic amb `.where()` i accés a columnes extres

```python
import bfg_watermark as wm

# Cercar taules amb filtres avançats
query = wm.WatermarkQuery(spark)
configs = query.where(area="COMPRES", active=True, is_dimension=False).ordered().build()

for cfg in configs:
    # Accés estàndard
    print(f"Taula: {cfg.table_name}, Mode: {cfg.effective_mode}")
    
    # Accés auxiliar o dinàmic a columnes del sistema o personalitzades
    created = cfg.get("CREATED_AT")
    tags = cfg.get("TAGS", default="sense tags")
    
    # Accés per atribut directe (case-insensitive)
    print(f"  Creat el: {cfg.created_at}, Tags: {tags}")
    
    # Diccionari complet de la fila
    raw_dict = cfg.to_dict()
```

### Patró: Diagnòstic ràpid del sistema

```python
from bfg_watermark import WatermarkAdmin
admin = WatermarkAdmin(spark)

# Estat global de totes les taules
display(admin.status())

# Taules que han fallat
display(admin.errors())

# Taules no actualitzades en les últimes 26 hores
display(admin.stale(hours=26))
```

---

## 6. Guia d'Operacions del Dia a Dia

### Afegir una taula nova

Per donar d'alta una taula a la `Watermark_Control` es pot utilitzar el mètode `admin.insert_table()`, ja sigui des del notebook `NB_Init_Watermark_Table` o directament des d'un notebook de manteniment:

```python
admin = WatermarkAdmin(spark)

admin.insert_table(
    source="SAGE",
    table="PINVOICE",
    area="COMPRES",
    silver_table="silver_sage_compres_pinvoice",
    bronze_path="Files/Bronze/Sage/Compres/PINVOICE.parquet",
    source_query="SELECT NUM_0, BPR_0, UPDDATTIM_0 FROM pinvoice",
    source_datecol="UPDDATTIM_0",
    pk_columns=["NUM_0"],
    window_days=2,
    load_mode="INCREMENTAL",
    overwrite=True
)
```

1. **Si la taula és petita o normal:** no cal fer res més. El proper cicle del Pipeline la trobarà amb `WATERMARK_LAST_VALUE = NULL`, farà un `FULL_RELOAD` automàtic i guardarà el watermark inicial.
2. **Si la taula és gran (historial massiu de milions de registres):** consulta la secció 7A.

### Modificar columnes de configuració d'una taula (`admin.update_table`)

Quan cal canviar una propietat d'una o diverses taules (com ara els dies de finestra `WINDOW_DAYS`, l'ordre d'execució `RUN_ORDER`, o la ruta destí `SILVER_TABLE`):

```python
admin = WatermarkAdmin(spark)

# Modificar paràmetres concrets d'una taula
admin.update_table(
    source="SAGE",
    table="PORDER",
    window_days=5,
    run_order=2
)

# Canviar el mode permanent de tota una àrea funcional
admin.update_table(
    source="SAGE",
    area="COMPRES",
    updates={"LOAD_MODE": "FULL_RELOAD"}
)
```

Tots els canvis passen pel validador d'esquema i actualitzen automàticament el camp `UPDATED_AT`.

### Donar de baixa o esborrar una taula

Segons la necessitat, disposes de dues opcions:

1. **Desactivació temporal o permanent (Recomanat):** La taula es manté a la `Watermark_Control` amb tot el seu historial i watermark, però s'ignora completament durant les càrregues.
   ```python
   admin.deactivate(source="SAGE", table="PORDER")
   # Per reactivar-la en qualsevol moment:
   admin.activate(source="SAGE", table="PORDER")
   ```

2. **Esborrat físic complet:** Si la taula ha deixat d'existir a l'origen i es vol eliminar definitivament del control:
   ```python
   admin.delete_table(source="SAGE", table="PORDER")
   ```

### Forçar la recàrrega completa d'una taula existent

```python
admin = WatermarkAdmin(spark)
admin.force_full_reload(source="SAGE", table="PORDER")
# La propera nit, el Pipeline carregarà tota la taula des de zero
```

Útil quan s'ha canviat l'esquema de la taula Silver i cal regenerar-la, o quan se sospita que hi ha inconsistències entre Bronze i Plata.

### Bloquejar temporalment una taula problemàtica

```python
admin = WatermarkAdmin(spark)
admin.skip_next_run(source="SAGE", table="SINVOICE")
# Demà el Pipeline saltarà aquesta taula. La resta seguirà amb normalitat.

# Quan l'origen estigui reparat, per tornar a activar-la:
admin.resume_from_skip(source="SAGE", table="SINVOICE")
```

### Desactivar tota una àrea de taules indefinidament

```python
admin = WatermarkAdmin(spark)
admin.deactivate(source="SAGE", area="COMPRES")
# Totes les taules de l'àrea COMPRES de SAGE s'ignoraran
```

---

## 7. Escenaris Especials

### A. Càrrega inicial massiva (historial gran)

**Problema:** una taula nova té 30+ milions de registres. Executar el Pipeline sencer faria que l'activitat *Copy Data* consumís masses CUs i potser fallés per timeout.

**Solució:**

1. **Extracció manual:** des del teu ordinador (via DBeaver, SQL Server Management Studio, etc.), exporta totes les dades de la taula a un fitxer `.parquet`.
2. **Puja a Bronze:** ves a l'explorador de fitxers de MS Fabric i puja el `.parquet` exactament a la ruta `BRONZE_PATH` que figura a la WT per aquesta taula.
3. **Verifica el mode:** assegura't que la taula tingui `NEXT_RUN_MODE = 'FULL_RELOAD'` (o que `WATERMARK_LAST_VALUE` sigui `NULL`). Ho pots fer amb:
   ```python
   admin.force_full_reload(source="SAGE", table="PORDER")
   # o be:
   admin.reset_watermark(source="SAGE", table="PORDER")
   ```
4. **Executa NOMÉS el Notebook de transformació:** ves al notebook `NB_Bronce_a_Plata_<Origen>` i executa'l de forma aïllada des de MS Fabric (no el Pipeline sencer).
   - El Notebook detectarà el mode `FULL_RELOAD`, llegirà el teu enorme `.parquet` i l'introduirà a Plata.
   - En acabar bé, la sessió netejarà el `NEXT_RUN_MODE` i avançarà el watermark.
   - La propera nit, el Pipeline correrà en mode `INCREMENTAL` amb normalitat.

### B. Executar només algunes taules (SKIP selectiu)

Si per incidència puntual vols executar el Pipeline però ometre taules concretes:

```python
admin = WatermarkAdmin(spark)
admin.skip_next_run(source="SAGE", table="SINVOICE")
admin.skip_next_run(source="SAGE", table="SINVOICED")
# Ara executa el Pipeline normalment: processara totes les taules EXCEPTE les marcades
```

### C. Recuperar la `Watermark_Control` d'un estat corromput

Si algú ha modificat la `Watermark_Control` per error (ex: ha esborrat files, ha posat watermarks incorrectes):

```python
admin = WatermarkAdmin(spark)

# Primer, veure l'historial de versions per identificar quan estava be
display(admin.show_history())

# Inspeccionar com era la taula en la versio 5 sense modificar-la
display(admin.inspect_table(version=5))

# Si sembla correcta, restaurar-la (ACCIO DESTRUCTIVA - undo total)
admin.recover_table(version=5)
# o per timestamp:
admin.recover_table(timestamp="2025-01-15 08:00:00")
```

---

## 8. Configuració d'Entorn

### Lakehouse per defecte

La taula `Watermark_Control` viu al Lakehouse **`LH_BRONCE_PLATA`**.

**Qualsevol Notebook que usi `bfg_watermark` ha de tenir `LH_BRONCE_PLATA` configurat com a Lakehouse per defecte** (a la pestanya de configuració del Notebook a Fabric).

Si el teu Notebook necessita `LH_ORO` o un altre Lakehouse com a defecte, afegeix aquest codi al principi:

```python
spark.catalog.setCurrentDatabase("LH_BRONCE_PLATA")
```

### Actualitzar i empaquetar la llibreria

Si modifiques el codi Python de `bfg_watermark`:

1. **Localment:** executa `python setup.py bdist_wheel` a la carpeta del projecte. Es generarà un fitxer `.whl` a la carpeta `dist/`.
2. **A Fabric:** ves a **Environments**, obre el teu `DEFAULT_ENV`.
3. Ves a la pestanya **Custom Libraries**, puja el nou `.whl` substituint l'anterior, i fes **Publish**.
4. Tots els Notebooks que usen l'entorn prendran automàticament la nova versió.

### Constants de mode disponibles

```python
from bfg_watermark.constants import MODE_INCREMENTAL, MODE_FULL_RELOAD, MODE_SKIP
# o bé mitjançant el submòdul constants:
from bfg_watermark import constants as c
# c.MODE_INCREMENTAL, c.MODE_FULL_RELOAD, c.MODE_SKIP
# Valors: "INCREMENTAL", "FULL_RELOAD", "SKIP"
```

Usar sempre les constants, mai strings literals lliures, per evitar errors de tipografia.