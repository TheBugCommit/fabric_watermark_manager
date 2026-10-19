# ==============================================================================
# 🧪 TEST SUITE: WatermarkQuery i WatermarkConfig CONTRA Watermark_Control REAL
# ==============================================================================
# Aquest script està dissenyat per executar-se directament en una cel·la d'un
# Notebook de Microsoft Fabric amb la sessió de spark activa.
#
# Prova el 100% dels mètodes de WatermarkQuery:
#   .source(), .area(), .table(), .dimension(), .active_only(),
#   .runnable(), .mode(), .where(), .ordered(), .build(), .one()
#
# I el 100% de propietats i mètodes de WatermarkConfig:
#   - Identitat: source, table_name, area, is_dimension
#   - Enrutament: silver_table, bronze_path, source_datecol, pk_columns
#   - Estat: active, load_mode, next_run_mode, watermark_last, last_run_at, last_run_ok, last_error
#   - Finestra/Query: source_query, source_datecol_format, window_days
#   - Estratègia: effective_mode, from_date, should_run, is_first_run, build_source_query()
#   - Helpers: set_new_watermark(), .get(), .to_dict(), __getattr__, __repr__
#
# Seguretat: Utilitza un SOURCE de test ("TEST_QRY_SYS") i s'esborra tot a 'finally'.
# ==============================================================================

import sys
from datetime import datetime, timedelta

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

try:
    import watermark as wm
except ImportError:
    import watermark as wm

try:
    _ = spark
except NameError:
    spark = None

if spark is None:
    print("ℹ️ Sessió de 'spark' no detectada. Aquest script està pensat per a Notebooks de Fabric.")
else:
    admin = wm.WatermarkAdmin(spark)

    TEST_SOURCE = "TEST_QRY_SYS"
    test_results = []

    def run_test(name, func):
        """Executa una prova unitària i registra si ha passat o fallat."""
        try:
            func()
            print(f"  ✅ [PASS] {name}")
            test_results.append((name, True, None))
        except Exception as e:
            print(f"  ❌ [FAIL] {name}: {e}")
            test_results.append((name, False, str(e)))

    print("=" * 80)
    print(f"🚀 INICIANT BATERIA DE PROVES: WatermarkQuery I WatermarkConfig CONTRA WT REAL")
    print("=" * 80)

    try:
        # ── 0. NETEJA PRÈVIA DE SEGURETAT ─────────────────────────────────────
        print("\n[PAS 0] Neteja prèvia de seguretat a la Watermark_Control...")
        spark.sql(f"DELETE FROM {wm.constants.TABLE_NAME} WHERE {wm.constants.COL_SOURCE} = '{TEST_SOURCE}'")

        # ── SETUP: INSERCIÓ DE TAULES DE PROVA ────────────────────────────────
        print("\n[SETUP] Inserint taules de prova a Watermark_Control amb WatermarkAdmin...")

        # Taula 1: T_ACCOUNTS (Dimensió, FULL_RELOAD, watermark null)
        admin.insert_table(
            source=TEST_SOURCE,
            table="T_ACCOUNTS",
            area="FINANCE",
            silver_table="silver.fin_accounts",
            bronze_path="Files/bronze/fin_accounts",
            source_query="SELECT * FROM FIN_ACCOUNTS",
            source_datecol="UPDATED_AT",
            pk_columns=["ACCOUNT_CODE"],
            run_order=5,
            is_dimension=True,
            load_mode=wm.constants.MODE_FULL_RELOAD,
            overwrite=True,
        )

        # Taula 2: T_ORDERS (Incremental amb watermark i finestra de 2 dies)
        admin.insert_table(
            source=TEST_SOURCE,
            table="T_ORDERS",
            area="SALES",
            silver_table="silver.sales_orders",
            bronze_path="Files/bronze/sales_orders",
            source_query="SELECT * FROM RAW_ORDERS",
            source_datecol="UPDATED_AT",
            source_datecol_format="%Y-%m-%d %H:%M:%S",
            pk_columns=["ORDER_ID", "LINE_ID"],
            window_days=2,
            active=True,
            run_order=10,
            is_dimension=False,
            load_mode=wm.constants.MODE_INCREMENTAL,
            watermark_last=datetime(2025, 6, 15, 12, 0, 0),
            overwrite=True,
        )

        # Taula 3: T_INCR_WHERE (Incremental amb clàusula WHERE preexistent a la query)
        admin.insert_table(
            source=TEST_SOURCE,
            table="T_INCR_WHERE",
            area="SALES",
            silver_table="silver.sales_where",
            bronze_path="Files/bronze/sales_where",
            source_query="SELECT * FROM ORDERS WHERE STATUS = 'SHIPPED'",
            source_datecol="SHIP_DATE",
            source_datecol_format="%Y-%m-%d",
            pk_columns=["ORDER_ID"],
            window_days=1,
            active=True,
            run_order=15,
            is_dimension=False,
            load_mode=wm.constants.MODE_INCREMENTAL,
            watermark_last=datetime(2025, 8, 20, 10, 0, 0),
            overwrite=True,
        )

        # Taula 4: T_CUSTOMERS (Incremental però watermark NULL -> primera execució / full reload)
        admin.insert_table(
            source=TEST_SOURCE,
            table="T_CUSTOMERS",
            area="SALES",
            silver_table="silver.sales_customers",
            bronze_path="Files/bronze/sales_customers",
            source_query="SELECT * FROM RAW_CUSTOMERS WHERE ACTIVE_FLAG = 1",
            source_datecol="MODIFIED_DATE",
            pk_columns=["CUSTOMER_ID"],
            window_days=0,
            active=True,
            run_order=20,
            is_dimension=False,
            load_mode=wm.constants.MODE_INCREMENTAL,
            watermark_last=None,
            overwrite=True,
        )

        # Taula 5: T_BUDGET (Override NEXT_RUN_MODE = SKIP)
        admin.insert_table(
            source=TEST_SOURCE,
            table="T_BUDGET",
            area="FINANCE",
            silver_table="silver.fin_budget",
            bronze_path="Files/bronze/fin_budget",
            source_query="SELECT * FROM RAW_BUDGET",
            source_datecol="LAST_CHANGE",
            pk_columns=["COST_CENTER", "YEAR"],
            window_days=5,
            active=True,
            run_order=30,
            is_dimension=False,
            load_mode=wm.constants.MODE_INCREMENTAL,
            next_run_mode=wm.constants.MODE_SKIP,
            watermark_last=datetime(2025, 5, 1, 0, 0, 0),
            overwrite=True,
        )

        # Taula 6: T_LOGS (Inactiva: ACTIVE = False)
        admin.insert_table(
            source=TEST_SOURCE,
            table="T_LOGS",
            area="SYSTEM",
            silver_table="silver.sys_logs",
            bronze_path="Files/bronze/sys_logs",
            source_query="SELECT * FROM LOGS",
            source_datecol="LOG_TIME",
            pk_columns=["LOG_ID"],
            window_days=0,
            active=False,
            run_order=50,
            is_dimension=False,
            load_mode=wm.constants.MODE_INCREMENTAL,
            watermark_last=datetime(2025, 1, 1, 0, 0, 0),
            overwrite=True,
        )

        # Enriquiment d'auditoria per a T_ORDERS i query buida per a T_ACCOUNTS
        spark.sql(f"""
            UPDATE {wm.constants.TABLE_NAME}
            SET {wm.constants.COL_LAST_RUN_AT} = TIMESTAMP('2025-06-15 12:05:00'),
                {wm.constants.COL_LAST_RUN_OK} = true,
                {wm.constants.COL_LAST_ERROR}  = NULL
            WHERE {wm.constants.COL_SOURCE} = '{TEST_SOURCE}' AND {wm.constants.COL_TABLE_NAME} = 'T_ORDERS'
        """)
        spark.sql(f"""
            UPDATE {wm.constants.TABLE_NAME}
            SET {wm.constants.COL_SOURCE_QUERY} = NULL
            WHERE {wm.constants.COL_SOURCE} = '{TEST_SOURCE}' AND {wm.constants.COL_TABLE_NAME} = 'T_ACCOUNTS'
        """)

        print("  ✅ [SETUP] 6 taules inserides i configurades a Watermark_Control.")

        # ── 1. PROVES DE WatermarkQuery: FILTRES I EXECUTORS ─────────────────
        print("\n" + "─" * 50)
        print("🔍 1. PROVES DE WatermarkQuery (Mètodes de Filtre i Execució)")
        print("─" * 50)

        def test_query_source():
            # Single source
            res = wm.WatermarkQuery(spark).source(TEST_SOURCE).build()
            assert len(res) == 6, f"Esperats 6 registres, obtinguts {len(res)}"
            assert all(isinstance(x, wm.WatermarkConfig) for x in res)

            # Multiple sources
            res_mult = wm.WatermarkQuery(spark).source(TEST_SOURCE, "NON_EXISTENT").build()
            assert len(res_mult) == 6

            # Non-existent source
            res_none = wm.WatermarkQuery(spark).source("NON_EXISTENT").build()
            assert len(res_none) == 0
        run_test("1.1 .source() amb 1 valor, múltiples valors i font inexistent", test_query_source)

        def test_query_area():
            # Single area
            res_fin = wm.WatermarkQuery(spark).source(TEST_SOURCE).area("FINANCE").build()
            names = {x.table_name for x in res_fin}
            assert names == {"T_ACCOUNTS", "T_BUDGET"}, f"Obtingut: {names}"

            # Multiple areas
            res_mult = wm.WatermarkQuery(spark).source(TEST_SOURCE).area("SALES", "FINANCE").build()
            assert len(res_mult) == 5

            # Unknown area
            res_unk = wm.WatermarkQuery(spark).source(TEST_SOURCE).area("UNKNOWN").build()
            assert len(res_unk) == 0
        run_test("1.2 .area() amb 1 valor, múltiples valors i àrea inexistent", test_query_area)

        def test_query_table():
            # Single table
            cfg = wm.WatermarkQuery(spark).source(TEST_SOURCE).table("T_ORDERS").one()
            assert cfg.table_name == "T_ORDERS"

            # Multiple tables
            res = wm.WatermarkQuery(spark).source(TEST_SOURCE).table("T_ORDERS", "T_ACCOUNTS").build()
            names = {x.table_name for x in res}
            assert names == {"T_ORDERS", "T_ACCOUNTS"}
        run_test("1.3 .table() amb 1 taula i múltiples taules", test_query_table)

        def test_query_dimension():
            # dimension(True)
            res_dim = wm.WatermarkQuery(spark).source(TEST_SOURCE).dimension(True).build()
            assert len(res_dim) == 1
            assert res_dim[0].table_name == "T_ACCOUNTS"
            assert res_dim[0].is_dimension is True

            # dimension(False)
            res_nodim = wm.WatermarkQuery(spark).source(TEST_SOURCE).dimension(False).build()
            assert len(res_nodim) == 5
            assert all(not x.is_dimension for x in res_nodim)
        run_test("1.4 .dimension(True) i .dimension(False)", test_query_dimension)

        def test_query_active_only():
            res = wm.WatermarkQuery(spark).source(TEST_SOURCE).active_only().build()
            assert len(res) == 5
            assert not any(x.table_name == "T_LOGS" for x in res)
            assert all(x.active for x in res)
        run_test("1.5 .active_only() exclou taules inactives (ACTIVE = false)", test_query_active_only)

        def test_query_runnable():
            res = wm.WatermarkQuery(spark).source(TEST_SOURCE).runnable().build()
            # Exclou T_LOGS (inactiva) i T_BUDGET (NEXT_RUN_MODE = SKIP)
            names = {x.table_name for x in res}
            assert names == {"T_ACCOUNTS", "T_ORDERS", "T_INCR_WHERE", "T_CUSTOMERS"}, f"Obtingut: {names}"
        run_test("1.6 .runnable() exclou inactives i efectivament SKIP", test_query_runnable)

        def test_query_mode():
            # Single mode FULL_RELOAD
            res_full = wm.WatermarkQuery(spark).source(TEST_SOURCE).mode("FULL_RELOAD").build()
            assert len(res_full) == 1
            assert res_full[0].table_name == "T_ACCOUNTS"

            # Multiple modes
            res_all = wm.WatermarkQuery(spark).source(TEST_SOURCE).mode("INCREMENTAL", "FULL_RELOAD").build()
            assert len(res_all) == 6
        run_test("1.7 .mode() amb un o múltiples modes permanents", test_query_mode)

        def test_query_where():
            # Integer filter
            res_int = wm.WatermarkQuery(spark).source(TEST_SOURCE).where(window_days=2).build()
            assert len(res_int) == 1 and res_int[0].table_name == "T_ORDERS"

            # String filter
            res_str = wm.WatermarkQuery(spark).source(TEST_SOURCE).where(silver_table="silver.sales_orders").build()
            assert len(res_str) == 1 and res_str[0].table_name == "T_ORDERS"

            # Null filter
            res_null = wm.WatermarkQuery(spark).source(TEST_SOURCE).where(watermark_last=None).build()
            assert {x.table_name for x in res_null} == {"T_ACCOUNTS", "T_CUSTOMERS"}

            # Boolean filter
            res_bool = wm.WatermarkQuery(spark).source(TEST_SOURCE).where(active=False).build()
            assert len(res_bool) == 1 and res_bool[0].table_name == "T_LOGS"
        run_test("1.8 .where() genèric per int, str, None (IS NULL) i bool", test_query_where)

        def test_query_ordered():
            res = wm.WatermarkQuery(spark).source(TEST_SOURCE).ordered().build()
            names = [x.table_name for x in res]
            expected = ["T_ACCOUNTS", "T_ORDERS", "T_INCR_WHERE", "T_CUSTOMERS", "T_BUDGET", "T_LOGS"]
            assert names == expected, f"Esperat {expected}, obtingut {names}"
        run_test("1.9 .ordered() ordena correctament per RUN_ORDER i TABLE_NAME", test_query_ordered)

        def test_query_one_success_and_errors():
            # Success
            cfg = wm.WatermarkQuery(spark).source(TEST_SOURCE).table("T_ORDERS").one()
            assert cfg.table_name == "T_ORDERS"

            # Error: 0 rows found
            try:
                wm.WatermarkQuery(spark).source(TEST_SOURCE).table("NON_EXISTENT").one()
                raise AssertionError("Hauria d'haver llançat ValueError per 0 files!")
            except ValueError as e:
                assert "No row found" in str(e)

            # Error: Multiple rows found
            try:
                wm.WatermarkQuery(spark).source(TEST_SOURCE).one()
                raise AssertionError("Hauria d'haver llançat ValueError per múltiples files!")
            except ValueError as e:
                assert "Expected exactly 1 row" in str(e)
        run_test("1.10 .one() retorna 1 registre o llança ValueError si 0 o >1", test_query_one_success_and_errors)

        def test_query_chained():
            res = (
                wm.WatermarkQuery(spark)
                .source(TEST_SOURCE)
                .area("SALES")
                .runnable()
                .ordered()
                .build()
            )
            names = [x.table_name for x in res]
            assert names == ["T_ORDERS", "T_INCR_WHERE", "T_CUSTOMERS"], f"Obtingut: {names}"
        run_test("1.11 Enllaçat complex: .source().area().runnable().ordered().build()", test_query_chained)


        # ── 2. PROVES DE WatermarkConfig: PROPIETATS I MÈTODES ───────────────
        print("\n" + "─" * 50)
        print("⚙️  2. PROVES DE WatermarkConfig (Propietats i Mètodes)")
        print("─" * 50)

        cfg_orders = wm.WatermarkQuery(spark).source(TEST_SOURCE).table("T_ORDERS").one()
        cfg_accounts = wm.WatermarkQuery(spark).source(TEST_SOURCE).table("T_ACCOUNTS").one()
        cfg_incr_where = wm.WatermarkQuery(spark).source(TEST_SOURCE).table("T_INCR_WHERE").one()
        cfg_customers = wm.WatermarkQuery(spark).source(TEST_SOURCE).table("T_CUSTOMERS").one()
        cfg_budget = wm.WatermarkQuery(spark).source(TEST_SOURCE).table("T_BUDGET").one()
        cfg_logs = wm.WatermarkQuery(spark).source(TEST_SOURCE).table("T_LOGS").one()

        def test_config_identity():
            assert cfg_orders.source == TEST_SOURCE
            assert cfg_orders.table_name == "T_ORDERS"
            assert cfg_orders.area == "SALES"
            assert cfg_orders.is_dimension is False
            assert cfg_accounts.is_dimension is True
        run_test("2.1 Propietats d'identitat (source, table_name, area, is_dimension)", test_config_identity)

        def test_config_routing():
            assert cfg_orders.silver_table == "silver.sales_orders"
            assert cfg_orders.bronze_path == "Files/bronze/sales_orders"
            assert cfg_orders.source_datecol == "UPDATED_AT"
            assert cfg_orders.pk_columns == ["ORDER_ID", "LINE_ID"]
            assert isinstance(cfg_orders.pk_columns, list)
        run_test("2.2 Propietats d'enrutament (silver_table, bronze_path, source_datecol, pk_columns)", test_config_routing)

        def test_config_load_behaviour():
            assert cfg_orders.active is True
            assert cfg_logs.active is False
            assert cfg_orders.load_mode == "INCREMENTAL"
            assert cfg_orders.next_run_mode is None
            assert cfg_budget.next_run_mode == "SKIP"
            assert cfg_orders.watermark_last == datetime(2025, 6, 15, 12, 0, 0)
            assert cfg_orders.last_run_at == datetime(2025, 6, 15, 12, 5, 0)
            assert cfg_orders.last_run_ok is True
            assert cfg_orders.last_error is None
        run_test("2.3 Propietats d'estat i execució (active, load_mode, next_run_mode, watermark_last, audit)", test_config_load_behaviour)

        def test_config_source_windows():
            assert cfg_orders.source_query == "SELECT * FROM RAW_ORDERS"
            assert cfg_orders.source_datecol_format == "%Y-%m-%d %H:%M:%S"
            assert cfg_orders.window_days == 2
            assert cfg_incr_where.source_datecol_format == "%Y-%m-%d"
            assert cfg_accounts.source_query is None
        run_test("2.4 Propietats d'extracció (source_query, source_datecol_format, window_days)", test_config_source_windows)

        def test_config_effective_mode():
            # Normal incremental
            assert cfg_orders.effective_mode == "INCREMENTAL"
            # Watermark null forces FULL_RELOAD even if LOAD_MODE is INCREMENTAL
            assert cfg_customers.effective_mode == "FULL_RELOAD"
            # NEXT_RUN_MODE overrides LOAD_MODE
            assert cfg_budget.effective_mode == "SKIP"
            # Standard full reload
            assert cfg_accounts.effective_mode == "FULL_RELOAD"
        run_test("2.5 Resolució de effective_mode (prioritats NEXT_RUN_MODE > NULL watermark > LOAD_MODE)", test_config_effective_mode)

        def test_config_from_date():
            # Incremental with window: 2025-06-15 12:00:00 - 2 days = 2025-06-13 12:00:00
            assert cfg_orders.from_date == datetime(2025, 6, 13, 12, 0, 0)
            # Incremental with window 1: 2025-08-20 10:00:00 - 1 day = 2025-08-19 10:00:00
            assert cfg_incr_where.from_date == datetime(2025, 8, 19, 10, 0, 0)
            # Full reload because watermark is None -> from_date is None
            assert cfg_customers.from_date is None
            # Full reload mode -> from_date is None
            assert cfg_accounts.from_date is None
            # Skip mode -> from_date is None
            assert cfg_budget.from_date is None
        run_test("2.6 Càlcul de from_date (watermark - window_days o None en càrrega completa/skip)", test_config_from_date)

        def test_config_should_run_and_first_run():
            # should_run
            assert cfg_orders.should_run is True
            assert cfg_customers.should_run is True
            assert cfg_accounts.should_run is True
            assert cfg_budget.should_run is False  # effective_mode == SKIP

            # is_first_run
            assert cfg_orders.is_first_run is False     # has watermark
            assert cfg_customers.is_first_run is True    # watermark is None
            assert cfg_accounts.is_first_run is True     # watermark is None
        run_test("2.7 Indicadors de flux (should_run, is_first_run)", test_config_should_run_and_first_run)

        def test_config_build_source_query():
            # Incremental sense WHERE previ -> afegeix WHERE + EXISTS
            q1 = cfg_orders.build_source_query()
            assert q1 == "SELECT * FROM RAW_ORDERS WHERE UPDATED_AT >= 2025-06-13 12:00:00 AND EXISTS (SELECT 1 FROM T_ORDERS WHERE UPDATED_AT > 2025-06-15 12:00:00)", f"Obtingut: {q1}"

            # Incremental amb WHERE previ -> afegeix AND + EXISTS
            q2 = cfg_incr_where.build_source_query()
            assert q2 == "SELECT * FROM ORDERS WHERE STATUS = 'SHIPPED' AND SHIP_DATE >= 2025-08-19 AND EXISTS (SELECT 1 FROM T_INCR_WHERE WHERE SHIP_DATE > 2025-08-20)", f"Obtingut: {q2}"

            # Full reload (first run) -> retorna la query base sense filtrar
            q3 = cfg_customers.build_source_query()
            assert q3 == "SELECT * FROM RAW_CUSTOMERS WHERE ACTIVE_FLAG = 1", f"Obtingut: {q3}"

            # None source_query -> retorna None
            q4 = cfg_accounts.build_source_query()
            assert q4 is None, f"Obtingut: {q4}"
        run_test("2.8 build_source_query() en incremental (WHERE/AND + EXISTS), full reload i query buida", test_config_build_source_query)

        def test_config_set_new_watermark():
            new_ts = datetime(2025, 12, 31, 23, 59, 59)
            cfg_orders.set_new_watermark(new_ts)
            assert cfg_orders._new_watermark == new_ts
        run_test("2.9 set_new_watermark() assigna el nou valor de watermark", test_config_set_new_watermark)

        def test_config_dynamic_access_and_helpers():
            # .get() exact match
            assert cfg_orders.get("SILVER_TABLE") == "silver.sales_orders"
            # .get() case-insensitive fallback
            assert cfg_orders.get("silver_table") == "silver.sales_orders"
            # .get() with default
            assert cfg_orders.get("NON_EXISTING", "DEFAULT_VAL") == "DEFAULT_VAL"

            # .to_dict()
            d = cfg_orders.to_dict()
            assert isinstance(d, dict)
            assert d["TABLE_NAME"] == "T_ORDERS"

            # __getattr__ dynamic access
            assert cfg_orders.RUN_ORDER == 10
            assert cfg_orders.run_order == 10

            try:
                _ = cfg_orders.COMPLETELY_NON_EXISTENT_FIELD
                raise AssertionError("Hauria d'haver llançat AttributeError!")
            except AttributeError:
                pass

            # __repr__
            r = repr(cfg_orders)
            assert "WatermarkConfig" in r and "T_ORDERS" in r
        run_test("2.10 Accés dinàmic (.get(), .to_dict(), __getattr__, __repr__)", test_config_dynamic_access_and_helpers)

    finally:
        # ── 3. NETEJA FINAL GARANTIDA ─────────────────────────────────────────
        print("\n" + "─" * 50)
        print("🧹 3. NETEJA DE DADES DE TEST (finally)")
        print("─" * 50)
        spark.sql(f"DELETE FROM {wm.constants.TABLE_NAME} WHERE {wm.constants.COL_SOURCE} = '{TEST_SOURCE}'")
        remaining = wm.WatermarkQuery(spark).source(TEST_SOURCE).build()
        print(f"  ✅ [PASS] Neteja completada. Taules restants a la WT de test: {len(remaining)}")

    # ── RESUM FINAL ───────────────────────────────────────────────────────────
    print("\n" + "=" * 80)
    total_tests = len(test_results)
    passed = sum(1 for _, ok, _ in test_results if ok)
    failed = sum(1 for _, ok, _ in test_results if not ok)
    print(f"📊 RESUM FINAL DE PROVES: {passed}/{total_tests} correctes, {failed} fallades.")
    if failed == 0:
        print("🎉 TOTES LES PROVES DE WatermarkQuery I WatermarkConfig S'HAN SUPERAT AMB ÈXIT!")
    else:
        print("⚠️ S'HAN TROBAT ERRORS EN LES PROVES SEGÜENTS:")
        for name, ok, err in test_results:
            if not ok:
                print(f"   - {name}: {err}")
    print("=" * 80)
