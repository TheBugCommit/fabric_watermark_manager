# Configuració a Microsoft Fabric

Aquesta guia detalla com configurar l'entorn de Microsoft Fabric per utilitzar `bfg_watermark` i com empaquetar noves versions de la llibreria.

---

## 1. Configuració del Lakehouse

La taula Delta `Watermark_Control` resideix per conveni al Lakehouse **`LH_BRONCE_PLATA`**.

### Lakehouse per defecte al Notebook
Qualsevol Notebook de Fabric que utilitzi `bfg_watermark` ha de tenir configurat **`LH_BRONCE_PLATA`** com a Lakehouse per defecte (a la pestanya de Lakehouses de l'extrem esquerre del Notebook a Fabric).

### Ús amb altres Lakehouses per defecte
Si el teu notebook té assignat per defecte un altre Lakehouse (ex: `LH_ORO`), afegeix aquesta instrucció a la primera cel·la del Notebook abans d'utilitzar la llibreria:

```python
spark.catalog.setCurrentDatabase("LH_BRONCE_PLATA")
```

---

## 2. Compilació i Empaquetament de la Llibreria

Quan es realitzin canvis al codi font de `bfg_watermark`, cal generar un paquet distribuïble Python Wheel (`.whl`).

### Pas 1: Incrementar la Versió
Edita el fitxer `setup.py` a l'arrel del repositori i actualitza el camp `version`:

```python
setup(
    name="bfg_watermark",
    version="1.0.6",  # Actualitza la versió
    description="Watermark control library for Microsoft Fabric",
    packages=find_packages(),
    python_requires=">=3.8",
)
```
Actualitza també la variable `__version__` a `bfg_watermark/__init__.py`.

### Pas 2: Compilar el fitxer `.whl`
Des del terminal a la carpeta arrel del projecte:

```bash
python setup.py bdist_wheel
```

Aquest procés generarà el fitxer empaquetat a la carpeta `dist/`, per exemple:
`dist/bfg_watermark-1.0.6-py3-none-any.whl`

---

## 3. Desplegament a l'Entorn de Microsoft Fabric

Perquè tots els Notebooks i Pipelines puguin fer servir la llibreria sense necessitat de fer `!pip install`:

1. Obre el teu espai de treball (**Workspace**) a Microsoft Fabric.
2. Localitza l'element de tipus **Environment** (típicament anomenat `DEFAULT_ENV` o l'entorn corporatiu de dades).
3. Entra a la pestanya **Custom Libraries**.
4. Fes clic a **Add from file** i puja el fitxer `.whl` generat a la carpeta `dist/`.
5. Si ja existia una versió anterior, esborra-la per evitar conflictes.
6. Fes clic al botó **Publish** de la barra superior.
7. Un cop l'entorn s'hagi publicat amb èxit (triga entre 2 i 5 minuts), tots els Notebooks associats a l'entorn disposaran de la nova versió automàticament.

---

## 4. Comprovació de Versió Instal·lada

Dins d'una cel·la d'un Notebook a Fabric, pots comprovar que la versió activa és la correcta:

```python
import bfg_watermark as wm
print("Versió bfg_watermark:", wm.__version__)
```
Hauria de retornar:
```
Versió bfg_watermark: 1.0.6
```
