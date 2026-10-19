# `bfg_watermark`

> **Versió actual:** `1.0.6`  
> **Lakehouse de control:** `LH_BRONCE_PLATA`  
> **Taula Delta:** `Watermark_Control`

---

## Què és `bfg_watermark`?

**`bfg_watermark`** és la llibreria estàndard en Python dissenyada per a **Microsoft Fabric** que orquestra, automatitza i controla les càrregues incrementals i completes entre sistemes origen (ex: Sage, Oracle, SAP, bases de dades relacionals) i les capes **Bronze** i **Plata (Silver)** del Lakehouse.

El sistema es basa en el concepte de **Watermark (marca d'aigua)**: registrar la data i hora de l'últim registre extret amb èxit per demanar, en les execucions posteriors, **únicament els registres creats o modificats a partir d'aquell instant**.

```
Primera execució (FULL_RELOAD):
  Origen → Descàrrega completa → Bronze → Plata (Delta)
  Watermark guardat: 2026-01-15 08:30:00

Segona execució (INCREMENTAL):
  Origen → Descàrrega només registres amb data >= 2026-01-15 08:30:00 - WINDOW_DAYS
         → Bronze (fitxer parquet reduït) → Plata (MERGE / Upsert atòmic)
  Watermark actualitzat: 2026-01-16 08:15:00
```

---

## Característiques Clau

- **Control Centralitzat:** Tota la configuració d'extracció, enrutament, claus primàries i estat d'auditoria resideix en una única taula Delta: `Watermark_Control`.
- **Sense lògica hardcoded als Notebooks:** Els notebooks consulten la taula de control mitjançant un builder fluent (`WatermarkQuery`) i s'adapten dinàmicament a l'estratègia requerida.
- **Transaccionalitat Atòmica Garantida:** Mitjançant el context manager `watermark_session`, l'actualització del watermark, la neteja d'overrides o el registre d'errors s'efectua en una única operació `MERGE` atòmica a Delta Lake.
- **Gestió Integral del Cicle de Vida (CRUD):** Des de la versió **1.0.6**, `WatermarkAdmin` permet donar d'alta (`insert_table`), actualitzar (`update_table`) o donar de baixa (`delete_table`) taules amb validació d'esquema estricta (`schema.py`).
- **Resiliència davant d'errors:** Si una transformació falla, el watermark no s'avança, l'error es registra a `LAST_ERROR` i la propera execució reprendrà el procés des del punt correcte.
- **Suport per a Delta Time Travel:** Permet auditar versions anteriors (`show_history`), inspeccionar estats passats (`inspect_table`) i desfer canvis destructius (`recover_table`).

---

## Instal·lació i Importació

Dins dels Notebooks de Microsoft Fabric associats al teu entorn (on la llibreria està instal·lada com a Custom Library):

```python
import bfg_watermark as wm

# O bé classes i submòduls específics:
from bfg_watermark import WatermarkQuery, WatermarkConfig, watermark_session, WatermarkAdmin
from bfg_watermark import constants as c
from bfg_watermark import schema
```

---

## Inici Ràpid (Quick Start)

### 1. Consultar taules executables
```python
import bfg_watermark as wm

# Obtenir totes les taules de SAGE llestes per executar
configs = wm.WatermarkQuery(spark).source("SAGE").runnable().ordered().build()

for cfg in configs:
    print(f"Taula: {cfg.table_name} | Mode efectiu: {cfg.effective_mode}")
```

### 2. Processar una taula amb `watermark_session`
```python
with wm.watermark_session(spark, config=cfg) as cfg:
    if not cfg.should_run:
        pass # La sessió ja gestiona automàticament el mode SKIP
    else:
        df_bronze = spark.read.parquet(cfg.bronze_path)
        
        # Executar el MERGE o Overwrite a Plata segons cfg.effective_mode...
        
        # Actualitzar el nou watermark amb la data màxima trobada
        max_date = df_bronze.agg({"UPDDATTIM_0": "max"}).collect()[0][0]
        cfg.set_new_watermark(max_date)
# En sortir del bloc with, la sessió fa el MERGE atòmic a Watermark_Control
```

### 3. Operacions administratives
```python
from bfg_watermark import WatermarkAdmin
admin = WatermarkAdmin(spark)

# Forçar una recàrrega completa puntual
admin.force_full_reload(source="SAGE", table="PORDER")

# Consultar l'estat global del sistema
display(admin.status())
```

---

## Estructura de la Documentació

- [**La Taula de Control (`Watermark_Control`)**](control-table.md): Detall de les 21 columnes, tipus, regles de validació, valors per defecte i columnes addicionals.
- [**Arquitectura del Sistema**](architecture.md): Disseny dels dos nivells de Pipelines i funcionament coordinat Notebooks-Pipelines.
- [**Referència de l'API**](api/query.md):
    - [`WatermarkQuery`](api/query.md): Consulta fluenta i filtrat de taules.
    - [`WatermarkConfig`](api/config.md): Configuració i estratègia calculada d'una taula.
    - [`watermark_session`](api/session.md): Context manager transaccional.
    - [`WatermarkAdmin`](api/admin.md): Operacions administratives, CRUD i diagnòstic.
    - [`constants`](api/constants.md): Noms físics i constants del sistema.
    - [`schema`](api/schema.md): Esquema centralitzat i validador de registres.
- [**Guies Pràctiques**](patterns.md):
    - [Patrons de Codi](patterns.md): Exemples complets de Notebooks Bronze→Plata i generadors de queries.
    - [Operacions del Dia a Dia](operations.md): Guia d'ús freqüent (altes, modificacions, bloquejos temporals).
    - [Escenaris Especials](special-scenarios.md): Càrregues massives inicials i recuperació de desastres.
    - [Configuració a Microsoft Fabric](fabric-setup.md): Requisits de Lakehouse i empaquetament `.whl`.
