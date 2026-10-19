# Guia d'Operacions del Dia a Dia

Aquesta guia descriu els procediments habituals de manteniment i operació que els enginyers de dades han de realitzar sobre la plataforma de càrregues.

---

## 1. Donar d'Alta una Taula Nova

Per incorporar una nova taula a la plataforma de càrrega, utilitza el mètode [`admin.insert_table()`](api/admin.md#insert_table):

```python
from bfg_watermark import WatermarkAdmin
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

- **Si la taula té un volum moderat:** No cal cap intervenció addicional. Com que `WATERMARK_LAST_VALUE` s'inicia com a `NULL`, el proper cicle del Pipeline detectarà automàticament que és una primera càrrega (`cfg.is_first_run == True`), aplicarà un `FULL_RELOAD` i enregistrarà el watermark inicial.
- **Si la taula conté milions de registres històrics:** Consulta el procediment d'alta massiva a [Escenaris Especials](special-scenarios.md#1-carrega-inicial-massiva-historics-grans).

---

## 2. Modificar la Configuració d'una Taula

Quan necessitis canviar propietats com la finestra temporal (`WINDOW_DAYS`), l'ordre de prioritat (`RUN_ORDER`), el mode o la taula de destí, empra [`admin.update_table()`](api/admin.md#update_table):

```python
admin = WatermarkAdmin(spark)

# Canviar el marge de seguretat a 5 dies i la prioritat a 2
admin.update_table(
    source="SAGE",
    table="PORDER",
    window_days=5,
    run_order=2
)

# Canviar el mode permanent de tot un domini de negoci
admin.update_table(
    source="SAGE",
    area="COMPRES",
    updates={"LOAD_MODE": "FULL_RELOAD"}
)
```

Tots els canvis es validen contra l'esquema i actualitzen automàticament el camp d'auditoria `UPDATED_AT`.

---

## 3. Donar de Baixa o Suspendre una Taula

Segons si es tracta d'una aturada temporal o d'una retirada definitiva:

### Opció A: Desactivació (Recomanada)
La taula continua existint a `Watermark_Control` amb tot el seu historial i l'últim watermark preservat, però els Pipelines l'ometen automàticament (`.runnable()` i `.active_only()`):

```python
# Desactivar
admin.deactivate(source="SAGE", table="PORDER")

# Reactivar en qualsevol moment
admin.activate(source="SAGE", table="PORDER")
```

### Opció B: Esborrat Definitiu
Si la taula s'ha eliminat físicament de la base de dades origen i cal suprimir el seu registre:

```python
admin.delete_table(source="SAGE", table="PORDER")
```

---

## 4. Forçar una Recàrrega Completa Puntual

Si s'ha modificat l'esquema de la taula Silver (s'han afegit o canviat columnes) o cal reconstruir la taula Delta per consistència:

```python
admin = WatermarkAdmin(spark)
admin.force_full_reload(source="SAGE", table="PORDER")
```

**Què passarà?**
1. La taula tindrà `NEXT_RUN_MODE = 'FULL_RELOAD'`.
2. En la propera execució nocturna, el Pipeline farà una extracció completa de tota la taula i sobreescriurà Plata.
3. Un cop finalitzada amb èxit, la sessió **netejarà automàticament `NEXT_RUN_MODE` a `NULL`**, de manera que l'endemà continuarà en mode incremental normal sense cap intervenció manual.

---

## 5. Bloquejar Temporalment una Taula Problemàtica

Si el sistema d'origen d'una taula té un bloqueig o s'estan fent tasques de manteniment i no vols que el Pipeline nocturn falli:

```python
admin = WatermarkAdmin(spark)

# Ometre la taula en el proper cicle
admin.skip_next_run(source="SAGE", table="SINVOICE")

# Quan l'origen torni a estar disponible, desbloqueja-la:
admin.resume_from_skip(source="SAGE", table="SINVOICE")
```

---

## 6. Reiniciar el Watermark

Si vols forçar una recàrrega completa esborrant el registre temporal d'èxit:

```python
admin.reset_watermark(source="SAGE", table="PORDER")
```
Posa `WATERMARK_LAST_VALUE = NULL`. La propera execució actuarà com si fos una primera càrrega.
