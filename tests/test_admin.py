# ==============================================================================
# 🧪 TEST SUITE DE WatermarkAdmin: insert_table, update_table, delete_table
# ==============================================================================
import watermark as wm
# Si estàs provant el paquet genèric pots fer: import watermark as wm

admin = wm.WatermarkAdmin(spark)

# Constants de prova (taula inventada per no tocar dades de producció)
TEST_SOURCE = "TEST_SYSTEM"
TEST_TABLE  = "TEST_TABLE_DEV"
TEST_AREA   = "TEST_AREA"

test_results = []

def run_test(name, func):
    """Executa un test i registra si ha passat o fallat."""
    try:
        func()
        print(f"  ✅ [PASS] {name}")
        test_results.append((name, True, None))
    except Exception as e:
        print(f"  ❌ [FAIL] {name}: {e}")
        test_results.append((name, False, str(e)))

print("=" * 75)
print(f"🚀 INICIANT PROVES DE WatermarkAdmin CONTRA Watermark_Control")
print(f"   Taula de test fictícia: {TEST_SOURCE}.{TEST_TABLE}")
print("=" * 75)

try:
    # ── 0. Neteja prèvia ──────────────────────────────────────────────────────
    print("\n[PAS 0] Neteja prèvia de seguretat si la taula ja existia...")
    try:
        admin.delete_table(source=TEST_SOURCE, table=TEST_TABLE)
    except Exception:
        pass

    # ── 1. PROVES DE insert_table ─────────────────────────────────────────────
    print("\n" + "─" * 60)
    print("📦 1. PROVES DE insert_table")
    print("─" * 60)

    def test_insert_basic():
        """1.1 Inserció bàsica: camps requerits + valors per defecte de l'esquema."""
        admin.insert_table(
            source=TEST_SOURCE,
            table=TEST_TABLE,
            area=TEST_AREA,
            silver_table="silver.test_table_dev",
            bronze_path="Files/bronze/test_table_dev",
            source_query="SELECT * FROM TEST_TABLE_DEV",
            source_datecol="UPDATED_AT",
            pk_columns=["ID", "CODE"],
            # window_days, active, run_order, load_mode agafen defaults de l'esquema
        )
        # Verifiquem a Spark que la fila s'ha inserit correctament amb els defaults
        df = spark.sql(f"""
            SELECT SOURCE, TABLE_NAME, AREA, ACTIVE, RUN_ORDER, WINDOW_DAYS, LOAD_MODE 
            FROM Watermark_Control 
            WHERE SOURCE = '{TEST_SOURCE}' AND TABLE_NAME = '{TEST_TABLE}'
        """)
        assert df.count() == 1, "La fila no s'ha trobat a Watermark_Control!"
        row = df.first()
        assert row["ACTIVE"] is True, "ACTIVE hauria de ser True per defecte"
        assert row["RUN_ORDER"] == 1, "RUN_ORDER hauria de ser 1 per defecte"
        assert row["WINDOW_DAYS"] == 0, "WINDOW_DAYS hauria de ser 0 per defecte"
        assert row["LOAD_MODE"] == "INCREMENTAL", "LOAD_MODE hauria de ser INCREMENTAL per defecte"

    run_test("1.1 Inserció bàsica amb valors per defecte de l'esquema", test_insert_basic)

    def test_insert_duplicate_error():
        """1.2 Error en inserir duplicat: si overwrite=False ha de llançar ValueError."""
        try:
            admin.insert_table(
                source=TEST_SOURCE,
                table=TEST_TABLE,
                area=TEST_AREA,
                silver_table="silver.test_table_dev",
                bronze_path="Files/bronze/test_table_dev",
                source_query="SELECT * FROM TEST_TABLE_DEV",
                source_datecol="UPDATED_AT",
                pk_columns=["ID"],
                overwrite=False  # Rebutja si ja existeix
            )
            raise AssertionError("Hauria d'haver llançat ValueError per duplicat!")
        except ValueError as e:
            assert "already exists" in str(e).lower()

    run_test("1.2 Prevenció de duplicats (overwrite=False llança ValueError)", test_insert_duplicate_error)

    def test_insert_required_validation():
        """1.3 Error en camp requerit buit o None (validació per schema)."""
        try:
            admin.insert_table(
                source=TEST_SOURCE,
                table="TMP_ERR",
                area="   ",  # Buit!
                silver_table="silver.test",
                bronze_path="Files/bronze/test",
                source_query="SELECT 1",
                source_datecol="DT",
                pk_columns=["ID"]
            )
            raise AssertionError("Hauria d'haver fallat per area buida!")
        except ValueError as e:
            assert "required" in str(e).lower()

    run_test("1.3 Rebuig d'inserció amb camp obligatori buit (area='   ')", test_insert_required_validation)

    def test_insert_invalid_type():
        """1.4 Error de tipus invàlid (window_days espera int, passem str)."""
        try:
            admin.insert_table(
                source=TEST_SOURCE,
                table="TMP_ERR",
                area=TEST_AREA,
                silver_table="silver.test",
                bronze_path="Files/bronze/test",
                source_query="SELECT 1",
                source_datecol="DT",
                pk_columns=["ID"],
                window_days="cinc"  # Tipus incorrecte!
            )
            raise AssertionError("Hauria d'haver fallat per window_days='cinc'!")
        except ValueError as e:
            assert "invalid type" in str(e).lower()

    run_test("1.4 Rebuig d'inserció amb tipus invàlid (window_days='cinc')", test_insert_invalid_type)

    def test_insert_overwrite_true():
        """1.5 Inserció amb overwrite=True, provant àlies table_name i camp extra."""
        admin.insert_table(
            source=TEST_SOURCE,
            table_name=TEST_TABLE,  # Provant l'àlies table_name
            area=TEST_AREA,
            silver_table="silver.test_table_dev_v2",
            bronze_path="Files/bronze/test_table_dev",
            source_query="SELECT * FROM TEST_TABLE_DEV",
            source_datecol="UPDATED_AT",
            pk_columns=["ID", "CODE"],
            window_days=3,
            run_order=2,
            overwrite=True,  # Actualitza la fila existent amb MERGE
            custom_metadata="DEV_EXP_1"  # Camp extra admès
        )
        row = spark.sql(f"""
            SELECT SILVER_TABLE, WINDOW_DAYS, RUN_ORDER 
            FROM Watermark_Control 
            WHERE SOURCE = '{TEST_SOURCE}' AND TABLE_NAME = '{TEST_TABLE}'
        """).first()
        assert row["SILVER_TABLE"] == "silver.test_table_dev_v2"
        assert row["WINDOW_DAYS"] == 3
        assert row["RUN_ORDER"] == 2

    run_test("1.5 Inserció amb overwrite=True (MERGE) i àlies table_name", test_insert_overwrite_true)

    # ── 2. PROVES DE update_table ─────────────────────────────────────────────
    print("\n" + "─" * 60)
    print("✏️ 2. PROVES DE update_table")
    print("─" * 60)

    def test_update_via_dict():
        """2.1 Actualització mitjançant diccionari updates={...}."""
        admin.update_table(
            source=TEST_SOURCE,
            table=TEST_TABLE,
            updates={"RUN_ORDER": 5, "WINDOW_DAYS": 14}
        )
        row = spark.sql(f"""
            SELECT RUN_ORDER, WINDOW_DAYS 
            FROM Watermark_Control 
            WHERE SOURCE = '{TEST_SOURCE}' AND TABLE_NAME = '{TEST_TABLE}'
        """).first()
        assert row["RUN_ORDER"] == 5
        assert row["WINDOW_DAYS"] == 14

    run_test("2.1 Actualització mitjançant diccionari updates={...}", test_update_via_dict)

    def test_update_via_kwargs():
        """2.2 Actualització mitjançant arguments directes per paraula clau (**kwargs)."""
        admin.update_table(
            source=TEST_SOURCE,
            table=TEST_TABLE,
            active=False,
            silver_table="silver.test_table_dev_final"
        )
        row = spark.sql(f"""
            SELECT ACTIVE, SILVER_TABLE 
            FROM Watermark_Control 
            WHERE SOURCE = '{TEST_SOURCE}' AND TABLE_NAME = '{TEST_TABLE}'
        """).first()
        assert row["ACTIVE"] is False
        assert row["SILVER_TABLE"] == "silver.test_table_dev_final"

    run_test("2.2 Actualització mitjançant paràmetres directes (**kwargs)", test_update_via_kwargs)

    def test_update_empty_required():
        """2.3 Rebuig d'actualització que intenti buidar un camp obligatori."""
        try:
            admin.update_table(
                source=TEST_SOURCE,
                table=TEST_TABLE,
                updates={"AREA": ""}  # Camp obligatori buit
            )
            raise AssertionError("Hauria d'haver fallat per AREA=''!")
        except ValueError as e:
            assert "required" in str(e).lower()

    run_test("2.3 Rebuig d'actualització amb camp obligatori buit (AREA='')", test_update_empty_required)

    def test_update_invalid_type():
        """2.4 Rebuig d'actualització amb tipus erroni (ex: string per a enter)."""
        try:
            admin.update_table(
                source=TEST_SOURCE,
                table=TEST_TABLE,
                window_days="deu_dies"
            )
            raise AssertionError("Hauria d'haver fallat per tipus invàlid!")
        except ValueError as e:
            assert "invalid type" in str(e).lower()

    run_test("2.4 Rebuig d'actualització amb tipus erroni (window_days='deu_dies')", test_update_invalid_type)

    def test_update_invalid_mode():
        """2.5 Rebuig d'actualització amb mode no vàlid."""
        try:
            admin.update_table(
                source=TEST_SOURCE,
                table=TEST_TABLE,
                load_mode="MODE_INEXISTENT"
            )
            raise AssertionError("Hauria d'haver fallat per load_mode invàlid!")
        except ValueError as e:
            assert "invalid value" in str(e).lower()

    run_test("2.5 Rebuig d'actualització amb mode invàlid (load_mode='MODE_INEXISTENT')", test_update_invalid_mode)

    def test_update_audit_protection():
        """2.6 Protecció de columnes del sistema (CREATED_AT / UPDATED_AT)."""
        try:
            admin.update_table(
                source=TEST_SOURCE,
                table=TEST_TABLE,
                CREATED_AT="2026-01-01 00:00:00"
            )
            raise AssertionError("Hauria d'haver protegit CREATED_AT!")
        except ValueError as e:
            assert "system audit column" in str(e).lower()

    run_test("2.6 Protecció de columnes d'auditoria (bloqueig de CREATED_AT)", test_update_audit_protection)

    def test_update_without_filter():
        """2.7 Rebuig d'actualització sense cap filtre (sense source, area ni table)."""
        try:
            admin.update_table(active=True)
            raise AssertionError("Hauria d'haver demanat mínim un filtre!")
        except ValueError as e:
            assert "must provide at least one filter" in str(e).lower()

    run_test("2.7 Rebuig d'actualització sense cap filtre", test_update_without_filter)

    # ── 3. PROVES DE delete_table ─────────────────────────────────────────────
    print("\n" + "─" * 60)
    print("🗑️ 3. PROVES DE delete_table")
    print("─" * 60)

    def test_delete_table():
        """3.1 Eliminació física de la taula de test."""
        admin.delete_table(source=TEST_SOURCE, table=TEST_TABLE)
        # Verifiquem que ja no existeix a Spark
        cnt = spark.sql(f"""
            SELECT 1 FROM Watermark_Control 
            WHERE SOURCE = '{TEST_SOURCE}' AND TABLE_NAME = '{TEST_TABLE}'
        """).count()
        assert cnt == 0, "La taula encara apareix a Watermark_Control després del DELETE!"

    run_test("3.1 Eliminació física de la taula (delete_table)", test_delete_table)

    def test_delete_missing_args():
        """3.2 Rebuig de delete_table sense paràmetres obligatoris."""
        try:
            admin.delete_table(source=None, table_name=None)
            raise AssertionError("Hauria d'haver fallat per arguments buits!")
        except ValueError as e:
            assert "required" in str(e).lower()

    run_test("3.2 Rebuig de delete_table sense paràmetres obligatoris", test_delete_missing_args)

finally:
    # ── 4. NETEJA FINAL DE SEGURETAT I RESUM ──────────────────────────────────
    print("\n[NETEJA] Executant neteja final a Watermark_Control per seguretat...")
    try:
        admin.delete_table(source=TEST_SOURCE, table=TEST_TABLE)
    except Exception:
        pass

    print("\n" + "=" * 75)
    total_tests = len(test_results)
    passed_tests = sum(1 for _, ok, _ in test_results if ok)
    failed_tests = total_tests - passed_tests

    if failed_tests == 0:
        print(f"🎉 TOTS ELS TESTS HAN PASSAT AMB ÈXIT! ({passed_tests}/{total_tests})")
        print("   Totes les validacions d'insert_table, update_table i delete_table")
        print("   han funcionat perfectament contra Watermark_Control!")
    else:
        print(f"⚠️ S'HAN DETECTAT FALLADES: {failed_tests} de {total_tests} tests han fallat.")
        for name, ok, err in test_results:
            if not ok:
                print(f"   - {name}: {err}")
    print("=" * 75)
