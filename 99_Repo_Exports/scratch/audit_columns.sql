DO $$
DECLARE
    rec_table RECORD;
    rec_column RECORD;
    v_sql TEXT;
    v_total BIGINT;
    v_non_null BIGINT;
    v_percent NUMERIC;
BEGIN
    FOR rec_table IN 
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
        ORDER BY table_name
    LOOP
        v_sql := 'SELECT COUNT(*) FROM public.' || quote_ident(rec_table.table_name);
        EXECUTE v_sql INTO v_total;
        
        RAISE NOTICE 'Table: % (Total rows: %)', rec_table.table_name, v_total;
        
        IF v_total > 0 THEN
            FOR rec_column IN 
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = 'public' AND table_name = rec_table.table_name
                ORDER BY column_name
            LOOP
                v_sql := 'SELECT COUNT(' || quote_ident(rec_column.column_name) || ') FROM public.' || quote_ident(rec_table.table_name);
                EXECUTE v_sql INTO v_non_null;
                
                v_percent := ROUND((v_non_null::NUMERIC / v_total::NUMERIC) * 100, 2);
                RAISE NOTICE '  Column: % | Filled: % | Missing: % | % %%', 
                    RPAD(rec_column.column_name, 35), 
                    RPAD(v_non_null::TEXT, 10), 
                    RPAD((v_total - v_non_null)::TEXT, 10),
                    v_percent;
            END LOOP;
        ELSE
            RAISE NOTICE '  Table is empty.';
        END IF;
    END LOOP;
END;
$$;
