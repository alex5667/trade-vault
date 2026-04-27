--
-- PostgreSQL database dump
--

\restrict z7BD4H2cloFKfdb8aqXbsWFwJg6EtIX1JOrcZHT9hSmmBueWgz3IuUWj7daYi8Q

-- Dumped from database version 15.15 (Ubuntu 15.15-1.pgdg22.04+1)
-- Dumped by pg_dump version 16.13 (Ubuntu 16.13-0ubuntu0.24.04.1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: timescaledb; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS timescaledb WITH SCHEMA public;


--
-- Name: EXTENSION timescaledb; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION timescaledb IS 'Enables scalable inserts and complex queries for time-series data (Community Edition)';


--
-- Name: timescaledb_toolkit; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS timescaledb_toolkit WITH SCHEMA public;


--
-- Name: EXTENSION timescaledb_toolkit; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION timescaledb_toolkit IS 'Library of analytical hyperfunctions, time-series pipelining, and other SQL utilities';


--
-- Name: populate_exit_ts(); Type: FUNCTION; Schema: public; Owner: trading
--

CREATE FUNCTION public.populate_exit_ts() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    NEW.exit_ts := to_timestamp(NEW.exit_ts_ms / 1000.0);
    RETURN NEW;
END;
$$;


ALTER FUNCTION public.populate_exit_ts() OWNER TO trading;

--
-- Name: populate_exit_ts_p0(); Type: FUNCTION; Schema: public; Owner: trading
--

CREATE FUNCTION public.populate_exit_ts_p0() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    NEW.exit_ts := to_timestamp(NEW.exit_ts_ms / 1000.0);
    RETURN NEW;
END;
$$;


ALTER FUNCTION public.populate_exit_ts_p0() OWNER TO trading;

--
-- Name: populate_ticks_ts(); Type: FUNCTION; Schema: public; Owner: trading
--

CREATE FUNCTION public.populate_ticks_ts() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    NEW.ts := to_timestamp(NEW.ts_ms / 1000.0);
    RETURN NEW;
END;
$$;


ALTER FUNCTION public.populate_ticks_ts() OWNER TO trading;

--
-- Name: populate_trades_closed_ts(); Type: FUNCTION; Schema: public; Owner: trading
--

CREATE FUNCTION public.populate_trades_closed_ts() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    NEW.entry_ts := to_timestamp(NEW.entry_ts_ms / 1000.0);
    NEW.exit_ts := to_timestamp(NEW.exit_ts_ms / 1000.0);
    RETURN NEW;
END;
$$;


ALTER FUNCTION public.populate_trades_closed_ts() OWNER TO trading;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: _compressed_hypertable_573; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal._compressed_hypertable_573 (
);


ALTER TABLE _timescaledb_internal._compressed_hypertable_573 OWNER TO trading;

--
-- Name: _compressed_hypertable_575; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal._compressed_hypertable_575 (
);


ALTER TABLE _timescaledb_internal._compressed_hypertable_575 OWNER TO trading;

--
-- Name: of_gate_metrics; Type: TABLE; Schema: public; Owner: trading
--

CREATE TABLE public.of_gate_metrics (
    stream_id text NOT NULL,
    ts_ms bigint NOT NULL,
    ts timestamp with time zone NOT NULL,
    symbol text NOT NULL,
    schema_version integer DEFAULT 1 NOT NULL,
    scenario_v4 text DEFAULT 'unknown'::text NOT NULL,
    ok integer DEFAULT 0 NOT NULL,
    ok_soft integer DEFAULT 0 NOT NULL,
    reason_code text DEFAULT 'na'::text NOT NULL,
    missing_legs jsonb,
    payload_json jsonb NOT NULL
);


ALTER TABLE public.of_gate_metrics OWNER TO trading;

--
-- Name: _direct_view_2816; Type: VIEW; Schema: _timescaledb_internal; Owner: trading
--

CREATE VIEW _timescaledb_internal._direct_view_2816 AS
 SELECT public.time_bucket('00:05:00'::interval, of_gate_metrics.ts) AS bucket,
    of_gate_metrics.symbol,
    of_gate_metrics.scenario_v4,
    count(*) AS eligible,
    sum(of_gate_metrics.ok) AS ok_hard,
    sum(of_gate_metrics.ok_soft) AS ok_soft
   FROM public.of_gate_metrics
  GROUP BY (public.time_bucket('00:05:00'::interval, of_gate_metrics.ts)), of_gate_metrics.symbol, of_gate_metrics.scenario_v4;


ALTER VIEW _timescaledb_internal._direct_view_2816 OWNER TO trading;

--
-- Name: _direct_view_2817; Type: VIEW; Schema: _timescaledb_internal; Owner: trading
--

CREATE VIEW _timescaledb_internal._direct_view_2817 AS
 SELECT public.time_bucket('01:00:00'::interval, of_gate_metrics.ts) AS bucket,
    of_gate_metrics.symbol,
    of_gate_metrics.scenario_v4,
    count(*) AS eligible,
    sum(of_gate_metrics.ok) AS ok_hard,
    sum(of_gate_metrics.ok_soft) AS ok_soft
   FROM public.of_gate_metrics
  GROUP BY (public.time_bucket('01:00:00'::interval, of_gate_metrics.ts)), of_gate_metrics.symbol, of_gate_metrics.scenario_v4;


ALTER VIEW _timescaledb_internal._direct_view_2817 OWNER TO trading;

--
-- Name: _hyper_2814_40_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal._hyper_2814_40_chunk (
    CONSTRAINT constraint_28 CHECK (((ts >= '2026-02-19 00:00:00+00'::timestamp with time zone) AND (ts < '2026-02-26 00:00:00+00'::timestamp with time zone)))
)
INHERITS (public.of_gate_metrics);


ALTER TABLE _timescaledb_internal._hyper_2814_40_chunk OWNER TO trading;

--
-- Name: _hyper_2814_41_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal._hyper_2814_41_chunk (
    CONSTRAINT constraint_29 CHECK (((ts >= '2026-02-26 00:00:00+00'::timestamp with time zone) AND (ts < '2026-03-05 00:00:00+00'::timestamp with time zone)))
)
INHERITS (public.of_gate_metrics);


ALTER TABLE _timescaledb_internal._hyper_2814_41_chunk OWNER TO trading;

--
-- Name: _hyper_2814_57_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal._hyper_2814_57_chunk (
    CONSTRAINT constraint_39 CHECK (((ts >= '2026-03-05 00:00:00+00'::timestamp with time zone) AND (ts < '2026-03-12 00:00:00+00'::timestamp with time zone)))
)
INHERITS (public.of_gate_metrics);


ALTER TABLE _timescaledb_internal._hyper_2814_57_chunk OWNER TO trading;

--
-- Name: _materialized_hypertable_2816; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal._materialized_hypertable_2816 (
    bucket timestamp with time zone NOT NULL,
    symbol text,
    scenario_v4 text,
    eligible bigint,
    ok_hard bigint,
    ok_soft bigint
);


ALTER TABLE _timescaledb_internal._materialized_hypertable_2816 OWNER TO trading;

--
-- Name: _hyper_2816_42_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal._hyper_2816_42_chunk (
    CONSTRAINT constraint_30 CHECK (((bucket >= '2026-02-26 00:00:00+00'::timestamp with time zone) AND (bucket < '2026-05-07 00:00:00+00'::timestamp with time zone)))
)
INHERITS (_timescaledb_internal._materialized_hypertable_2816);


ALTER TABLE _timescaledb_internal._hyper_2816_42_chunk OWNER TO trading;

--
-- Name: _hyper_2816_45_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal._hyper_2816_45_chunk (
    CONSTRAINT constraint_33 CHECK (((bucket >= '2025-12-18 00:00:00+00'::timestamp with time zone) AND (bucket < '2026-02-26 00:00:00+00'::timestamp with time zone)))
)
INHERITS (_timescaledb_internal._materialized_hypertable_2816);


ALTER TABLE _timescaledb_internal._hyper_2816_45_chunk OWNER TO trading;

--
-- Name: _materialized_hypertable_2817; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal._materialized_hypertable_2817 (
    bucket timestamp with time zone NOT NULL,
    symbol text,
    scenario_v4 text,
    eligible bigint,
    ok_hard bigint,
    ok_soft bigint
);


ALTER TABLE _timescaledb_internal._materialized_hypertable_2817 OWNER TO trading;

--
-- Name: _hyper_2817_43_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal._hyper_2817_43_chunk (
    CONSTRAINT constraint_31 CHECK (((bucket >= '2026-02-26 00:00:00+00'::timestamp with time zone) AND (bucket < '2026-05-07 00:00:00+00'::timestamp with time zone)))
)
INHERITS (_timescaledb_internal._materialized_hypertable_2817);


ALTER TABLE _timescaledb_internal._hyper_2817_43_chunk OWNER TO trading;

--
-- Name: _hyper_2817_44_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal._hyper_2817_44_chunk (
    CONSTRAINT constraint_32 CHECK (((bucket >= '2025-12-18 00:00:00+00'::timestamp with time zone) AND (bucket < '2026-02-26 00:00:00+00'::timestamp with time zone)))
)
INHERITS (_timescaledb_internal._materialized_hypertable_2817);


ALTER TABLE _timescaledb_internal._hyper_2817_44_chunk OWNER TO trading;

--
-- Name: candles_archive; Type: TABLE; Schema: public; Owner: trading
--

CREATE TABLE public.candles_archive (
    id bigint NOT NULL,
    symbol character varying(20) NOT NULL,
    timeframe character varying(10) NOT NULL,
    open_time timestamp with time zone NOT NULL,
    close_time timestamp with time zone NOT NULL,
    open numeric(20,8) NOT NULL,
    high numeric(20,8) NOT NULL,
    low numeric(20,8) NOT NULL,
    close numeric(20,8) NOT NULL,
    volume numeric(20,8),
    quote_volume numeric(20,8),
    trades integer,
    taker_buy_base numeric(20,8),
    taker_buy_quote numeric(20,8),
    archived_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.candles_archive OWNER TO trading;

--
-- Name: TABLE candles_archive; Type: COMMENT; Schema: public; Owner: trading
--

COMMENT ON TABLE public.candles_archive IS 'Historical candles data archived from Redis stream';


--
-- Name: _hyper_572_10_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal._hyper_572_10_chunk (
    CONSTRAINT constraint_10 CHECK (((open_time >= '2026-02-13 00:00:00+00'::timestamp with time zone) AND (open_time < '2026-02-14 00:00:00+00'::timestamp with time zone)))
)
INHERITS (public.candles_archive);


ALTER TABLE _timescaledb_internal._hyper_572_10_chunk OWNER TO trading;

--
-- Name: _hyper_572_11_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal._hyper_572_11_chunk (
    CONSTRAINT constraint_11 CHECK (((open_time >= '2026-02-14 00:00:00+00'::timestamp with time zone) AND (open_time < '2026-02-15 00:00:00+00'::timestamp with time zone)))
)
INHERITS (public.candles_archive);


ALTER TABLE _timescaledb_internal._hyper_572_11_chunk OWNER TO trading;

--
-- Name: _hyper_572_12_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal._hyper_572_12_chunk (
    CONSTRAINT constraint_12 CHECK (((open_time >= '2026-02-15 00:00:00+00'::timestamp with time zone) AND (open_time < '2026-02-16 00:00:00+00'::timestamp with time zone)))
)
INHERITS (public.candles_archive);


ALTER TABLE _timescaledb_internal._hyper_572_12_chunk OWNER TO trading;

--
-- Name: _hyper_572_13_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal._hyper_572_13_chunk (
    CONSTRAINT constraint_13 CHECK (((open_time >= '2026-02-16 00:00:00+00'::timestamp with time zone) AND (open_time < '2026-02-17 00:00:00+00'::timestamp with time zone)))
)
INHERITS (public.candles_archive);


ALTER TABLE _timescaledb_internal._hyper_572_13_chunk OWNER TO trading;

--
-- Name: _hyper_572_15_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal._hyper_572_15_chunk (
    CONSTRAINT constraint_14 CHECK (((open_time >= '2026-02-17 00:00:00+00'::timestamp with time zone) AND (open_time < '2026-02-18 00:00:00+00'::timestamp with time zone)))
)
INHERITS (public.candles_archive);


ALTER TABLE _timescaledb_internal._hyper_572_15_chunk OWNER TO trading;

--
-- Name: _hyper_572_17_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal._hyper_572_17_chunk (
    CONSTRAINT constraint_15 CHECK (((open_time >= '2026-02-18 00:00:00+00'::timestamp with time zone) AND (open_time < '2026-02-19 00:00:00+00'::timestamp with time zone)))
)
INHERITS (public.candles_archive);


ALTER TABLE _timescaledb_internal._hyper_572_17_chunk OWNER TO trading;

--
-- Name: _hyper_572_20_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal._hyper_572_20_chunk (
    CONSTRAINT constraint_17 CHECK (((open_time >= '2026-02-19 00:00:00+00'::timestamp with time zone) AND (open_time < '2026-02-20 00:00:00+00'::timestamp with time zone)))
)
INHERITS (public.candles_archive);


ALTER TABLE _timescaledb_internal._hyper_572_20_chunk OWNER TO trading;

--
-- Name: _hyper_572_22_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal._hyper_572_22_chunk (
    CONSTRAINT constraint_18 CHECK (((open_time >= '2026-02-20 00:00:00+00'::timestamp with time zone) AND (open_time < '2026-02-21 00:00:00+00'::timestamp with time zone)))
)
INHERITS (public.candles_archive);


ALTER TABLE _timescaledb_internal._hyper_572_22_chunk OWNER TO trading;

--
-- Name: _hyper_572_24_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal._hyper_572_24_chunk (
    CONSTRAINT constraint_19 CHECK (((open_time >= '2026-02-21 00:00:00+00'::timestamp with time zone) AND (open_time < '2026-02-22 00:00:00+00'::timestamp with time zone)))
)
INHERITS (public.candles_archive);


ALTER TABLE _timescaledb_internal._hyper_572_24_chunk OWNER TO trading;

--
-- Name: _hyper_572_26_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal._hyper_572_26_chunk (
    CONSTRAINT constraint_20 CHECK (((open_time >= '2026-02-22 00:00:00+00'::timestamp with time zone) AND (open_time < '2026-02-23 00:00:00+00'::timestamp with time zone)))
)
INHERITS (public.candles_archive);


ALTER TABLE _timescaledb_internal._hyper_572_26_chunk OWNER TO trading;

--
-- Name: _hyper_572_28_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal._hyper_572_28_chunk (
    CONSTRAINT constraint_21 CHECK (((open_time >= '2026-02-23 00:00:00+00'::timestamp with time zone) AND (open_time < '2026-02-24 00:00:00+00'::timestamp with time zone)))
)
INHERITS (public.candles_archive);


ALTER TABLE _timescaledb_internal._hyper_572_28_chunk OWNER TO trading;

--
-- Name: _hyper_572_31_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal._hyper_572_31_chunk (
    CONSTRAINT constraint_22 CHECK (((open_time >= '2026-02-24 00:00:00+00'::timestamp with time zone) AND (open_time < '2026-02-25 00:00:00+00'::timestamp with time zone)))
)
INHERITS (public.candles_archive);


ALTER TABLE _timescaledb_internal._hyper_572_31_chunk OWNER TO trading;

--
-- Name: _hyper_572_32_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal._hyper_572_32_chunk (
    CONSTRAINT constraint_23 CHECK (((open_time >= '2026-02-25 00:00:00+00'::timestamp with time zone) AND (open_time < '2026-02-26 00:00:00+00'::timestamp with time zone)))
)
INHERITS (public.candles_archive);


ALTER TABLE _timescaledb_internal._hyper_572_32_chunk OWNER TO trading;

--
-- Name: _hyper_572_36_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal._hyper_572_36_chunk (
    CONSTRAINT constraint_25 CHECK (((open_time >= '2026-02-26 00:00:00+00'::timestamp with time zone) AND (open_time < '2026-02-27 00:00:00+00'::timestamp with time zone)))
)
INHERITS (public.candles_archive);


ALTER TABLE _timescaledb_internal._hyper_572_36_chunk OWNER TO trading;

--
-- Name: _hyper_572_38_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal._hyper_572_38_chunk (
    CONSTRAINT constraint_26 CHECK (((open_time >= '2026-02-27 00:00:00+00'::timestamp with time zone) AND (open_time < '2026-02-28 00:00:00+00'::timestamp with time zone)))
)
INHERITS (public.candles_archive);


ALTER TABLE _timescaledb_internal._hyper_572_38_chunk OWNER TO trading;

--
-- Name: _hyper_572_39_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal._hyper_572_39_chunk (
    CONSTRAINT constraint_27 CHECK (((open_time >= '2026-02-28 00:00:00+00'::timestamp with time zone) AND (open_time < '2026-03-01 00:00:00+00'::timestamp with time zone)))
)
INHERITS (public.candles_archive);


ALTER TABLE _timescaledb_internal._hyper_572_39_chunk OWNER TO trading;

--
-- Name: _hyper_572_48_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal._hyper_572_48_chunk (
    CONSTRAINT constraint_35 CHECK (((open_time >= '2026-03-01 00:00:00+00'::timestamp with time zone) AND (open_time < '2026-03-02 00:00:00+00'::timestamp with time zone)))
)
INHERITS (public.candles_archive);


ALTER TABLE _timescaledb_internal._hyper_572_48_chunk OWNER TO trading;

--
-- Name: _hyper_572_4_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal._hyper_572_4_chunk (
    CONSTRAINT constraint_4 CHECK (((open_time >= '2026-02-08 00:00:00+00'::timestamp with time zone) AND (open_time < '2026-02-09 00:00:00+00'::timestamp with time zone)))
)
INHERITS (public.candles_archive);


ALTER TABLE _timescaledb_internal._hyper_572_4_chunk OWNER TO trading;

--
-- Name: _hyper_572_51_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal._hyper_572_51_chunk (
    CONSTRAINT constraint_36 CHECK (((open_time >= '2026-03-02 00:00:00+00'::timestamp with time zone) AND (open_time < '2026-03-03 00:00:00+00'::timestamp with time zone)))
)
INHERITS (public.candles_archive);


ALTER TABLE _timescaledb_internal._hyper_572_51_chunk OWNER TO trading;

--
-- Name: _hyper_572_53_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal._hyper_572_53_chunk (
    CONSTRAINT constraint_37 CHECK (((open_time >= '2026-03-03 00:00:00+00'::timestamp with time zone) AND (open_time < '2026-03-04 00:00:00+00'::timestamp with time zone)))
)
INHERITS (public.candles_archive);


ALTER TABLE _timescaledb_internal._hyper_572_53_chunk OWNER TO trading;

--
-- Name: _hyper_572_55_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal._hyper_572_55_chunk (
    CONSTRAINT constraint_38 CHECK (((open_time >= '2026-03-04 00:00:00+00'::timestamp with time zone) AND (open_time < '2026-03-05 00:00:00+00'::timestamp with time zone)))
)
INHERITS (public.candles_archive);


ALTER TABLE _timescaledb_internal._hyper_572_55_chunk OWNER TO trading;

--
-- Name: _hyper_572_58_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal._hyper_572_58_chunk (
    CONSTRAINT constraint_40 CHECK (((open_time >= '2026-03-05 00:00:00+00'::timestamp with time zone) AND (open_time < '2026-03-06 00:00:00+00'::timestamp with time zone)))
)
INHERITS (public.candles_archive);


ALTER TABLE _timescaledb_internal._hyper_572_58_chunk OWNER TO trading;

--
-- Name: _hyper_572_5_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal._hyper_572_5_chunk (
    CONSTRAINT constraint_5 CHECK (((open_time >= '2026-02-09 00:00:00+00'::timestamp with time zone) AND (open_time < '2026-02-10 00:00:00+00'::timestamp with time zone)))
)
INHERITS (public.candles_archive);


ALTER TABLE _timescaledb_internal._hyper_572_5_chunk OWNER TO trading;

--
-- Name: _hyper_572_61_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal._hyper_572_61_chunk (
    CONSTRAINT constraint_42 CHECK (((open_time >= '2026-03-06 00:00:00+00'::timestamp with time zone) AND (open_time < '2026-03-07 00:00:00+00'::timestamp with time zone)))
)
INHERITS (public.candles_archive);


ALTER TABLE _timescaledb_internal._hyper_572_61_chunk OWNER TO trading;

--
-- Name: _hyper_572_6_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal._hyper_572_6_chunk (
    CONSTRAINT constraint_6 CHECK (((open_time >= '2026-02-10 00:00:00+00'::timestamp with time zone) AND (open_time < '2026-02-11 00:00:00+00'::timestamp with time zone)))
)
INHERITS (public.candles_archive);


ALTER TABLE _timescaledb_internal._hyper_572_6_chunk OWNER TO trading;

--
-- Name: _hyper_572_7_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal._hyper_572_7_chunk (
    CONSTRAINT constraint_7 CHECK (((open_time >= '2026-02-11 00:00:00+00'::timestamp with time zone) AND (open_time < '2026-02-12 00:00:00+00'::timestamp with time zone)))
)
INHERITS (public.candles_archive);


ALTER TABLE _timescaledb_internal._hyper_572_7_chunk OWNER TO trading;

--
-- Name: _hyper_572_8_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal._hyper_572_8_chunk (
    CONSTRAINT constraint_8 CHECK (((open_time >= '2026-02-12 00:00:00+00'::timestamp with time zone) AND (open_time < '2026-02-13 00:00:00+00'::timestamp with time zone)))
)
INHERITS (public.candles_archive);


ALTER TABLE _timescaledb_internal._hyper_572_8_chunk OWNER TO trading;

--
-- Name: trades_closed_p0; Type: TABLE; Schema: public; Owner: trading
--

CREATE TABLE public.trades_closed_p0 (
    order_id text NOT NULL,
    exit_ts_ms bigint NOT NULL,
    exit_ts timestamp with time zone NOT NULL,
    scenario text,
    regime text,
    session text,
    entry_reason text,
    mae_bps double precision,
    mfe_bps double precision,
    time_to_mfe_ms bigint,
    hold_ms bigint,
    spread_bps_at_entry double precision,
    slippage_bps_est double precision,
    book_age_ms bigint,
    features_json jsonb,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    is_virtual boolean DEFAULT false,
    meta_enforce_cov_bucket text DEFAULT ''::text,
    meta_enforce_applied integer DEFAULT '-1'::integer,
    policy_mode text,
    policy_raw text
);


ALTER TABLE public.trades_closed_p0 OWNER TO trading;

--
-- Name: _hyper_7_19_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal._hyper_7_19_chunk (
    CONSTRAINT constraint_16 CHECK (((exit_ts >= '2026-02-19 00:00:00+00'::timestamp with time zone) AND (exit_ts < '2026-02-26 00:00:00+00'::timestamp with time zone)))
)
INHERITS (public.trades_closed_p0);


ALTER TABLE _timescaledb_internal._hyper_7_19_chunk OWNER TO trading;

--
-- Name: _hyper_7_1_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal._hyper_7_1_chunk (
    CONSTRAINT constraint_1 CHECK (((exit_ts >= '2026-01-22 00:00:00+00'::timestamp with time zone) AND (exit_ts < '2026-01-29 00:00:00+00'::timestamp with time zone)))
)
INHERITS (public.trades_closed_p0);


ALTER TABLE _timescaledb_internal._hyper_7_1_chunk OWNER TO trading;

--
-- Name: _hyper_7_2_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal._hyper_7_2_chunk (
    CONSTRAINT constraint_2 CHECK (((exit_ts >= '2026-01-29 00:00:00+00'::timestamp with time zone) AND (exit_ts < '2026-02-05 00:00:00+00'::timestamp with time zone)))
)
INHERITS (public.trades_closed_p0);


ALTER TABLE _timescaledb_internal._hyper_7_2_chunk OWNER TO trading;

--
-- Name: _hyper_7_35_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal._hyper_7_35_chunk (
    CONSTRAINT constraint_24 CHECK (((exit_ts >= '2026-02-26 00:00:00+00'::timestamp with time zone) AND (exit_ts < '2026-03-05 00:00:00+00'::timestamp with time zone)))
)
INHERITS (public.trades_closed_p0);


ALTER TABLE _timescaledb_internal._hyper_7_35_chunk OWNER TO trading;

--
-- Name: _hyper_7_3_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal._hyper_7_3_chunk (
    CONSTRAINT constraint_3 CHECK (((exit_ts >= '2026-02-05 00:00:00+00'::timestamp with time zone) AND (exit_ts < '2026-02-12 00:00:00+00'::timestamp with time zone)))
)
INHERITS (public.trades_closed_p0);


ALTER TABLE _timescaledb_internal._hyper_7_3_chunk OWNER TO trading;

--
-- Name: _hyper_7_59_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal._hyper_7_59_chunk (
    CONSTRAINT constraint_41 CHECK (((exit_ts >= '2026-03-05 00:00:00+00'::timestamp with time zone) AND (exit_ts < '2026-03-12 00:00:00+00'::timestamp with time zone)))
)
INHERITS (public.trades_closed_p0);


ALTER TABLE _timescaledb_internal._hyper_7_59_chunk OWNER TO trading;

--
-- Name: _hyper_7_9_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal._hyper_7_9_chunk (
    CONSTRAINT constraint_9 CHECK (((exit_ts >= '2026-02-12 00:00:00+00'::timestamp with time zone) AND (exit_ts < '2026-02-19 00:00:00+00'::timestamp with time zone)))
)
INHERITS (public.trades_closed_p0);


ALTER TABLE _timescaledb_internal._hyper_7_9_chunk OWNER TO trading;

--
-- Name: _partial_view_2816; Type: VIEW; Schema: _timescaledb_internal; Owner: trading
--

CREATE VIEW _timescaledb_internal._partial_view_2816 AS
 SELECT public.time_bucket('00:05:00'::interval, of_gate_metrics.ts) AS bucket,
    of_gate_metrics.symbol,
    of_gate_metrics.scenario_v4,
    count(*) AS eligible,
    sum(of_gate_metrics.ok) AS ok_hard,
    sum(of_gate_metrics.ok_soft) AS ok_soft
   FROM public.of_gate_metrics
  GROUP BY (public.time_bucket('00:05:00'::interval, of_gate_metrics.ts)), of_gate_metrics.symbol, of_gate_metrics.scenario_v4;


ALTER VIEW _timescaledb_internal._partial_view_2816 OWNER TO trading;

--
-- Name: _partial_view_2817; Type: VIEW; Schema: _timescaledb_internal; Owner: trading
--

CREATE VIEW _timescaledb_internal._partial_view_2817 AS
 SELECT public.time_bucket('01:00:00'::interval, of_gate_metrics.ts) AS bucket,
    of_gate_metrics.symbol,
    of_gate_metrics.scenario_v4,
    count(*) AS eligible,
    sum(of_gate_metrics.ok) AS ok_hard,
    sum(of_gate_metrics.ok_soft) AS ok_soft
   FROM public.of_gate_metrics
  GROUP BY (public.time_bucket('01:00:00'::interval, of_gate_metrics.ts)), of_gate_metrics.symbol, of_gate_metrics.scenario_v4;


ALTER VIEW _timescaledb_internal._partial_view_2817 OWNER TO trading;

--
-- Name: compress_hyper_573_14_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal.compress_hyper_573_14_chunk (
    _ts_meta_count integer,
    symbol character varying(20),
    timeframe character varying(10),
    id _timescaledb_internal.compressed_data,
    _ts_meta_min_1 timestamp with time zone,
    _ts_meta_max_1 timestamp with time zone,
    open_time _timescaledb_internal.compressed_data,
    close_time _timescaledb_internal.compressed_data,
    open _timescaledb_internal.compressed_data,
    high _timescaledb_internal.compressed_data,
    low _timescaledb_internal.compressed_data,
    close _timescaledb_internal.compressed_data,
    volume _timescaledb_internal.compressed_data,
    quote_volume _timescaledb_internal.compressed_data,
    trades _timescaledb_internal.compressed_data,
    taker_buy_base _timescaledb_internal.compressed_data,
    taker_buy_quote _timescaledb_internal.compressed_data,
    _ts_meta_min_2 timestamp with time zone,
    _ts_meta_max_2 timestamp with time zone,
    archived_at _timescaledb_internal.compressed_data
)
WITH (toast_tuple_target='128');
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_14_chunk ALTER COLUMN _ts_meta_count SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_14_chunk ALTER COLUMN symbol SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_14_chunk ALTER COLUMN timeframe SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_14_chunk ALTER COLUMN id SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_14_chunk ALTER COLUMN _ts_meta_min_1 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_14_chunk ALTER COLUMN _ts_meta_max_1 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_14_chunk ALTER COLUMN open_time SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_14_chunk ALTER COLUMN close_time SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_14_chunk ALTER COLUMN open SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_14_chunk ALTER COLUMN open SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_14_chunk ALTER COLUMN high SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_14_chunk ALTER COLUMN high SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_14_chunk ALTER COLUMN low SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_14_chunk ALTER COLUMN low SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_14_chunk ALTER COLUMN close SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_14_chunk ALTER COLUMN close SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_14_chunk ALTER COLUMN volume SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_14_chunk ALTER COLUMN volume SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_14_chunk ALTER COLUMN quote_volume SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_14_chunk ALTER COLUMN quote_volume SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_14_chunk ALTER COLUMN trades SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_14_chunk ALTER COLUMN taker_buy_base SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_14_chunk ALTER COLUMN taker_buy_base SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_14_chunk ALTER COLUMN taker_buy_quote SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_14_chunk ALTER COLUMN taker_buy_quote SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_14_chunk ALTER COLUMN _ts_meta_min_2 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_14_chunk ALTER COLUMN _ts_meta_max_2 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_14_chunk ALTER COLUMN archived_at SET STATISTICS 0;


ALTER TABLE _timescaledb_internal.compress_hyper_573_14_chunk OWNER TO trading;

--
-- Name: compress_hyper_573_16_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal.compress_hyper_573_16_chunk (
    _ts_meta_count integer,
    symbol character varying(20),
    timeframe character varying(10),
    id _timescaledb_internal.compressed_data,
    _ts_meta_min_1 timestamp with time zone,
    _ts_meta_max_1 timestamp with time zone,
    open_time _timescaledb_internal.compressed_data,
    close_time _timescaledb_internal.compressed_data,
    open _timescaledb_internal.compressed_data,
    high _timescaledb_internal.compressed_data,
    low _timescaledb_internal.compressed_data,
    close _timescaledb_internal.compressed_data,
    volume _timescaledb_internal.compressed_data,
    quote_volume _timescaledb_internal.compressed_data,
    trades _timescaledb_internal.compressed_data,
    taker_buy_base _timescaledb_internal.compressed_data,
    taker_buy_quote _timescaledb_internal.compressed_data,
    _ts_meta_min_2 timestamp with time zone,
    _ts_meta_max_2 timestamp with time zone,
    archived_at _timescaledb_internal.compressed_data
)
WITH (toast_tuple_target='128');
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_16_chunk ALTER COLUMN _ts_meta_count SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_16_chunk ALTER COLUMN symbol SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_16_chunk ALTER COLUMN timeframe SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_16_chunk ALTER COLUMN id SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_16_chunk ALTER COLUMN _ts_meta_min_1 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_16_chunk ALTER COLUMN _ts_meta_max_1 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_16_chunk ALTER COLUMN open_time SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_16_chunk ALTER COLUMN close_time SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_16_chunk ALTER COLUMN open SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_16_chunk ALTER COLUMN open SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_16_chunk ALTER COLUMN high SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_16_chunk ALTER COLUMN high SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_16_chunk ALTER COLUMN low SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_16_chunk ALTER COLUMN low SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_16_chunk ALTER COLUMN close SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_16_chunk ALTER COLUMN close SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_16_chunk ALTER COLUMN volume SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_16_chunk ALTER COLUMN volume SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_16_chunk ALTER COLUMN quote_volume SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_16_chunk ALTER COLUMN quote_volume SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_16_chunk ALTER COLUMN trades SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_16_chunk ALTER COLUMN taker_buy_base SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_16_chunk ALTER COLUMN taker_buy_base SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_16_chunk ALTER COLUMN taker_buy_quote SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_16_chunk ALTER COLUMN taker_buy_quote SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_16_chunk ALTER COLUMN _ts_meta_min_2 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_16_chunk ALTER COLUMN _ts_meta_max_2 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_16_chunk ALTER COLUMN archived_at SET STATISTICS 0;


ALTER TABLE _timescaledb_internal.compress_hyper_573_16_chunk OWNER TO trading;

--
-- Name: compress_hyper_573_18_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal.compress_hyper_573_18_chunk (
    _ts_meta_count integer,
    symbol character varying(20),
    timeframe character varying(10),
    id _timescaledb_internal.compressed_data,
    _ts_meta_min_1 timestamp with time zone,
    _ts_meta_max_1 timestamp with time zone,
    open_time _timescaledb_internal.compressed_data,
    close_time _timescaledb_internal.compressed_data,
    open _timescaledb_internal.compressed_data,
    high _timescaledb_internal.compressed_data,
    low _timescaledb_internal.compressed_data,
    close _timescaledb_internal.compressed_data,
    volume _timescaledb_internal.compressed_data,
    quote_volume _timescaledb_internal.compressed_data,
    trades _timescaledb_internal.compressed_data,
    taker_buy_base _timescaledb_internal.compressed_data,
    taker_buy_quote _timescaledb_internal.compressed_data,
    _ts_meta_min_2 timestamp with time zone,
    _ts_meta_max_2 timestamp with time zone,
    archived_at _timescaledb_internal.compressed_data
)
WITH (toast_tuple_target='128');
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_18_chunk ALTER COLUMN _ts_meta_count SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_18_chunk ALTER COLUMN symbol SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_18_chunk ALTER COLUMN timeframe SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_18_chunk ALTER COLUMN id SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_18_chunk ALTER COLUMN _ts_meta_min_1 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_18_chunk ALTER COLUMN _ts_meta_max_1 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_18_chunk ALTER COLUMN open_time SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_18_chunk ALTER COLUMN close_time SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_18_chunk ALTER COLUMN open SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_18_chunk ALTER COLUMN open SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_18_chunk ALTER COLUMN high SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_18_chunk ALTER COLUMN high SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_18_chunk ALTER COLUMN low SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_18_chunk ALTER COLUMN low SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_18_chunk ALTER COLUMN close SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_18_chunk ALTER COLUMN close SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_18_chunk ALTER COLUMN volume SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_18_chunk ALTER COLUMN volume SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_18_chunk ALTER COLUMN quote_volume SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_18_chunk ALTER COLUMN quote_volume SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_18_chunk ALTER COLUMN trades SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_18_chunk ALTER COLUMN taker_buy_base SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_18_chunk ALTER COLUMN taker_buy_base SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_18_chunk ALTER COLUMN taker_buy_quote SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_18_chunk ALTER COLUMN taker_buy_quote SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_18_chunk ALTER COLUMN _ts_meta_min_2 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_18_chunk ALTER COLUMN _ts_meta_max_2 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_18_chunk ALTER COLUMN archived_at SET STATISTICS 0;


ALTER TABLE _timescaledb_internal.compress_hyper_573_18_chunk OWNER TO trading;

--
-- Name: compress_hyper_573_21_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal.compress_hyper_573_21_chunk (
    _ts_meta_count integer,
    symbol character varying(20),
    timeframe character varying(10),
    id _timescaledb_internal.compressed_data,
    _ts_meta_min_1 timestamp with time zone,
    _ts_meta_max_1 timestamp with time zone,
    open_time _timescaledb_internal.compressed_data,
    close_time _timescaledb_internal.compressed_data,
    open _timescaledb_internal.compressed_data,
    high _timescaledb_internal.compressed_data,
    low _timescaledb_internal.compressed_data,
    close _timescaledb_internal.compressed_data,
    volume _timescaledb_internal.compressed_data,
    quote_volume _timescaledb_internal.compressed_data,
    trades _timescaledb_internal.compressed_data,
    taker_buy_base _timescaledb_internal.compressed_data,
    taker_buy_quote _timescaledb_internal.compressed_data,
    _ts_meta_min_2 timestamp with time zone,
    _ts_meta_max_2 timestamp with time zone,
    archived_at _timescaledb_internal.compressed_data
)
WITH (toast_tuple_target='128');
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_21_chunk ALTER COLUMN _ts_meta_count SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_21_chunk ALTER COLUMN symbol SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_21_chunk ALTER COLUMN timeframe SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_21_chunk ALTER COLUMN id SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_21_chunk ALTER COLUMN _ts_meta_min_1 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_21_chunk ALTER COLUMN _ts_meta_max_1 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_21_chunk ALTER COLUMN open_time SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_21_chunk ALTER COLUMN close_time SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_21_chunk ALTER COLUMN open SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_21_chunk ALTER COLUMN open SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_21_chunk ALTER COLUMN high SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_21_chunk ALTER COLUMN high SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_21_chunk ALTER COLUMN low SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_21_chunk ALTER COLUMN low SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_21_chunk ALTER COLUMN close SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_21_chunk ALTER COLUMN close SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_21_chunk ALTER COLUMN volume SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_21_chunk ALTER COLUMN volume SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_21_chunk ALTER COLUMN quote_volume SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_21_chunk ALTER COLUMN quote_volume SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_21_chunk ALTER COLUMN trades SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_21_chunk ALTER COLUMN taker_buy_base SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_21_chunk ALTER COLUMN taker_buy_base SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_21_chunk ALTER COLUMN taker_buy_quote SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_21_chunk ALTER COLUMN taker_buy_quote SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_21_chunk ALTER COLUMN _ts_meta_min_2 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_21_chunk ALTER COLUMN _ts_meta_max_2 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_21_chunk ALTER COLUMN archived_at SET STATISTICS 0;


ALTER TABLE _timescaledb_internal.compress_hyper_573_21_chunk OWNER TO trading;

--
-- Name: compress_hyper_573_23_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal.compress_hyper_573_23_chunk (
    _ts_meta_count integer,
    symbol character varying(20),
    timeframe character varying(10),
    id _timescaledb_internal.compressed_data,
    _ts_meta_min_1 timestamp with time zone,
    _ts_meta_max_1 timestamp with time zone,
    open_time _timescaledb_internal.compressed_data,
    close_time _timescaledb_internal.compressed_data,
    open _timescaledb_internal.compressed_data,
    high _timescaledb_internal.compressed_data,
    low _timescaledb_internal.compressed_data,
    close _timescaledb_internal.compressed_data,
    volume _timescaledb_internal.compressed_data,
    quote_volume _timescaledb_internal.compressed_data,
    trades _timescaledb_internal.compressed_data,
    taker_buy_base _timescaledb_internal.compressed_data,
    taker_buy_quote _timescaledb_internal.compressed_data,
    _ts_meta_min_2 timestamp with time zone,
    _ts_meta_max_2 timestamp with time zone,
    archived_at _timescaledb_internal.compressed_data
)
WITH (toast_tuple_target='128');
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_23_chunk ALTER COLUMN _ts_meta_count SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_23_chunk ALTER COLUMN symbol SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_23_chunk ALTER COLUMN timeframe SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_23_chunk ALTER COLUMN id SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_23_chunk ALTER COLUMN _ts_meta_min_1 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_23_chunk ALTER COLUMN _ts_meta_max_1 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_23_chunk ALTER COLUMN open_time SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_23_chunk ALTER COLUMN close_time SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_23_chunk ALTER COLUMN open SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_23_chunk ALTER COLUMN open SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_23_chunk ALTER COLUMN high SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_23_chunk ALTER COLUMN high SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_23_chunk ALTER COLUMN low SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_23_chunk ALTER COLUMN low SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_23_chunk ALTER COLUMN close SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_23_chunk ALTER COLUMN close SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_23_chunk ALTER COLUMN volume SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_23_chunk ALTER COLUMN volume SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_23_chunk ALTER COLUMN quote_volume SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_23_chunk ALTER COLUMN quote_volume SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_23_chunk ALTER COLUMN trades SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_23_chunk ALTER COLUMN taker_buy_base SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_23_chunk ALTER COLUMN taker_buy_base SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_23_chunk ALTER COLUMN taker_buy_quote SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_23_chunk ALTER COLUMN taker_buy_quote SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_23_chunk ALTER COLUMN _ts_meta_min_2 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_23_chunk ALTER COLUMN _ts_meta_max_2 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_23_chunk ALTER COLUMN archived_at SET STATISTICS 0;


ALTER TABLE _timescaledb_internal.compress_hyper_573_23_chunk OWNER TO trading;

--
-- Name: compress_hyper_573_25_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal.compress_hyper_573_25_chunk (
    _ts_meta_count integer,
    symbol character varying(20),
    timeframe character varying(10),
    id _timescaledb_internal.compressed_data,
    _ts_meta_min_1 timestamp with time zone,
    _ts_meta_max_1 timestamp with time zone,
    open_time _timescaledb_internal.compressed_data,
    close_time _timescaledb_internal.compressed_data,
    open _timescaledb_internal.compressed_data,
    high _timescaledb_internal.compressed_data,
    low _timescaledb_internal.compressed_data,
    close _timescaledb_internal.compressed_data,
    volume _timescaledb_internal.compressed_data,
    quote_volume _timescaledb_internal.compressed_data,
    trades _timescaledb_internal.compressed_data,
    taker_buy_base _timescaledb_internal.compressed_data,
    taker_buy_quote _timescaledb_internal.compressed_data,
    _ts_meta_min_2 timestamp with time zone,
    _ts_meta_max_2 timestamp with time zone,
    archived_at _timescaledb_internal.compressed_data
)
WITH (toast_tuple_target='128');
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_25_chunk ALTER COLUMN _ts_meta_count SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_25_chunk ALTER COLUMN symbol SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_25_chunk ALTER COLUMN timeframe SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_25_chunk ALTER COLUMN id SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_25_chunk ALTER COLUMN _ts_meta_min_1 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_25_chunk ALTER COLUMN _ts_meta_max_1 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_25_chunk ALTER COLUMN open_time SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_25_chunk ALTER COLUMN close_time SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_25_chunk ALTER COLUMN open SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_25_chunk ALTER COLUMN open SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_25_chunk ALTER COLUMN high SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_25_chunk ALTER COLUMN high SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_25_chunk ALTER COLUMN low SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_25_chunk ALTER COLUMN low SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_25_chunk ALTER COLUMN close SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_25_chunk ALTER COLUMN close SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_25_chunk ALTER COLUMN volume SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_25_chunk ALTER COLUMN volume SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_25_chunk ALTER COLUMN quote_volume SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_25_chunk ALTER COLUMN quote_volume SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_25_chunk ALTER COLUMN trades SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_25_chunk ALTER COLUMN taker_buy_base SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_25_chunk ALTER COLUMN taker_buy_base SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_25_chunk ALTER COLUMN taker_buy_quote SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_25_chunk ALTER COLUMN taker_buy_quote SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_25_chunk ALTER COLUMN _ts_meta_min_2 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_25_chunk ALTER COLUMN _ts_meta_max_2 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_25_chunk ALTER COLUMN archived_at SET STATISTICS 0;


ALTER TABLE _timescaledb_internal.compress_hyper_573_25_chunk OWNER TO trading;

--
-- Name: compress_hyper_573_27_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal.compress_hyper_573_27_chunk (
    _ts_meta_count integer,
    symbol character varying(20),
    timeframe character varying(10),
    id _timescaledb_internal.compressed_data,
    _ts_meta_min_1 timestamp with time zone,
    _ts_meta_max_1 timestamp with time zone,
    open_time _timescaledb_internal.compressed_data,
    close_time _timescaledb_internal.compressed_data,
    open _timescaledb_internal.compressed_data,
    high _timescaledb_internal.compressed_data,
    low _timescaledb_internal.compressed_data,
    close _timescaledb_internal.compressed_data,
    volume _timescaledb_internal.compressed_data,
    quote_volume _timescaledb_internal.compressed_data,
    trades _timescaledb_internal.compressed_data,
    taker_buy_base _timescaledb_internal.compressed_data,
    taker_buy_quote _timescaledb_internal.compressed_data,
    _ts_meta_min_2 timestamp with time zone,
    _ts_meta_max_2 timestamp with time zone,
    archived_at _timescaledb_internal.compressed_data
)
WITH (toast_tuple_target='128');
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_27_chunk ALTER COLUMN _ts_meta_count SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_27_chunk ALTER COLUMN symbol SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_27_chunk ALTER COLUMN timeframe SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_27_chunk ALTER COLUMN id SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_27_chunk ALTER COLUMN _ts_meta_min_1 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_27_chunk ALTER COLUMN _ts_meta_max_1 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_27_chunk ALTER COLUMN open_time SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_27_chunk ALTER COLUMN close_time SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_27_chunk ALTER COLUMN open SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_27_chunk ALTER COLUMN open SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_27_chunk ALTER COLUMN high SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_27_chunk ALTER COLUMN high SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_27_chunk ALTER COLUMN low SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_27_chunk ALTER COLUMN low SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_27_chunk ALTER COLUMN close SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_27_chunk ALTER COLUMN close SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_27_chunk ALTER COLUMN volume SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_27_chunk ALTER COLUMN volume SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_27_chunk ALTER COLUMN quote_volume SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_27_chunk ALTER COLUMN quote_volume SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_27_chunk ALTER COLUMN trades SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_27_chunk ALTER COLUMN taker_buy_base SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_27_chunk ALTER COLUMN taker_buy_base SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_27_chunk ALTER COLUMN taker_buy_quote SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_27_chunk ALTER COLUMN taker_buy_quote SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_27_chunk ALTER COLUMN _ts_meta_min_2 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_27_chunk ALTER COLUMN _ts_meta_max_2 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_27_chunk ALTER COLUMN archived_at SET STATISTICS 0;


ALTER TABLE _timescaledb_internal.compress_hyper_573_27_chunk OWNER TO trading;

--
-- Name: compress_hyper_573_29_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal.compress_hyper_573_29_chunk (
    _ts_meta_count integer,
    symbol character varying(20),
    timeframe character varying(10),
    id _timescaledb_internal.compressed_data,
    _ts_meta_min_1 timestamp with time zone,
    _ts_meta_max_1 timestamp with time zone,
    open_time _timescaledb_internal.compressed_data,
    close_time _timescaledb_internal.compressed_data,
    open _timescaledb_internal.compressed_data,
    high _timescaledb_internal.compressed_data,
    low _timescaledb_internal.compressed_data,
    close _timescaledb_internal.compressed_data,
    volume _timescaledb_internal.compressed_data,
    quote_volume _timescaledb_internal.compressed_data,
    trades _timescaledb_internal.compressed_data,
    taker_buy_base _timescaledb_internal.compressed_data,
    taker_buy_quote _timescaledb_internal.compressed_data,
    _ts_meta_min_2 timestamp with time zone,
    _ts_meta_max_2 timestamp with time zone,
    archived_at _timescaledb_internal.compressed_data
)
WITH (toast_tuple_target='128');
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_29_chunk ALTER COLUMN _ts_meta_count SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_29_chunk ALTER COLUMN symbol SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_29_chunk ALTER COLUMN timeframe SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_29_chunk ALTER COLUMN id SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_29_chunk ALTER COLUMN _ts_meta_min_1 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_29_chunk ALTER COLUMN _ts_meta_max_1 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_29_chunk ALTER COLUMN open_time SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_29_chunk ALTER COLUMN close_time SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_29_chunk ALTER COLUMN open SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_29_chunk ALTER COLUMN open SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_29_chunk ALTER COLUMN high SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_29_chunk ALTER COLUMN high SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_29_chunk ALTER COLUMN low SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_29_chunk ALTER COLUMN low SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_29_chunk ALTER COLUMN close SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_29_chunk ALTER COLUMN close SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_29_chunk ALTER COLUMN volume SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_29_chunk ALTER COLUMN volume SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_29_chunk ALTER COLUMN quote_volume SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_29_chunk ALTER COLUMN quote_volume SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_29_chunk ALTER COLUMN trades SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_29_chunk ALTER COLUMN taker_buy_base SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_29_chunk ALTER COLUMN taker_buy_base SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_29_chunk ALTER COLUMN taker_buy_quote SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_29_chunk ALTER COLUMN taker_buy_quote SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_29_chunk ALTER COLUMN _ts_meta_min_2 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_29_chunk ALTER COLUMN _ts_meta_max_2 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_29_chunk ALTER COLUMN archived_at SET STATISTICS 0;


ALTER TABLE _timescaledb_internal.compress_hyper_573_29_chunk OWNER TO trading;

--
-- Name: compress_hyper_573_30_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal.compress_hyper_573_30_chunk (
    _ts_meta_count integer,
    symbol character varying(20),
    timeframe character varying(10),
    id _timescaledb_internal.compressed_data,
    _ts_meta_min_1 timestamp with time zone,
    _ts_meta_max_1 timestamp with time zone,
    open_time _timescaledb_internal.compressed_data,
    close_time _timescaledb_internal.compressed_data,
    open _timescaledb_internal.compressed_data,
    high _timescaledb_internal.compressed_data,
    low _timescaledb_internal.compressed_data,
    close _timescaledb_internal.compressed_data,
    volume _timescaledb_internal.compressed_data,
    quote_volume _timescaledb_internal.compressed_data,
    trades _timescaledb_internal.compressed_data,
    taker_buy_base _timescaledb_internal.compressed_data,
    taker_buy_quote _timescaledb_internal.compressed_data,
    _ts_meta_min_2 timestamp with time zone,
    _ts_meta_max_2 timestamp with time zone,
    archived_at _timescaledb_internal.compressed_data
)
WITH (toast_tuple_target='128');
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_30_chunk ALTER COLUMN _ts_meta_count SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_30_chunk ALTER COLUMN symbol SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_30_chunk ALTER COLUMN timeframe SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_30_chunk ALTER COLUMN id SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_30_chunk ALTER COLUMN _ts_meta_min_1 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_30_chunk ALTER COLUMN _ts_meta_max_1 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_30_chunk ALTER COLUMN open_time SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_30_chunk ALTER COLUMN close_time SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_30_chunk ALTER COLUMN open SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_30_chunk ALTER COLUMN open SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_30_chunk ALTER COLUMN high SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_30_chunk ALTER COLUMN high SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_30_chunk ALTER COLUMN low SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_30_chunk ALTER COLUMN low SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_30_chunk ALTER COLUMN close SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_30_chunk ALTER COLUMN close SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_30_chunk ALTER COLUMN volume SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_30_chunk ALTER COLUMN volume SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_30_chunk ALTER COLUMN quote_volume SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_30_chunk ALTER COLUMN quote_volume SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_30_chunk ALTER COLUMN trades SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_30_chunk ALTER COLUMN taker_buy_base SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_30_chunk ALTER COLUMN taker_buy_base SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_30_chunk ALTER COLUMN taker_buy_quote SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_30_chunk ALTER COLUMN taker_buy_quote SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_30_chunk ALTER COLUMN _ts_meta_min_2 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_30_chunk ALTER COLUMN _ts_meta_max_2 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_30_chunk ALTER COLUMN archived_at SET STATISTICS 0;


ALTER TABLE _timescaledb_internal.compress_hyper_573_30_chunk OWNER TO trading;

--
-- Name: compress_hyper_573_33_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal.compress_hyper_573_33_chunk (
    _ts_meta_count integer,
    symbol character varying(20),
    timeframe character varying(10),
    id _timescaledb_internal.compressed_data,
    _ts_meta_min_1 timestamp with time zone,
    _ts_meta_max_1 timestamp with time zone,
    open_time _timescaledb_internal.compressed_data,
    close_time _timescaledb_internal.compressed_data,
    open _timescaledb_internal.compressed_data,
    high _timescaledb_internal.compressed_data,
    low _timescaledb_internal.compressed_data,
    close _timescaledb_internal.compressed_data,
    volume _timescaledb_internal.compressed_data,
    quote_volume _timescaledb_internal.compressed_data,
    trades _timescaledb_internal.compressed_data,
    taker_buy_base _timescaledb_internal.compressed_data,
    taker_buy_quote _timescaledb_internal.compressed_data,
    _ts_meta_min_2 timestamp with time zone,
    _ts_meta_max_2 timestamp with time zone,
    archived_at _timescaledb_internal.compressed_data
)
WITH (toast_tuple_target='128');
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_33_chunk ALTER COLUMN _ts_meta_count SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_33_chunk ALTER COLUMN symbol SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_33_chunk ALTER COLUMN timeframe SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_33_chunk ALTER COLUMN id SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_33_chunk ALTER COLUMN _ts_meta_min_1 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_33_chunk ALTER COLUMN _ts_meta_max_1 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_33_chunk ALTER COLUMN open_time SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_33_chunk ALTER COLUMN close_time SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_33_chunk ALTER COLUMN open SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_33_chunk ALTER COLUMN open SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_33_chunk ALTER COLUMN high SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_33_chunk ALTER COLUMN high SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_33_chunk ALTER COLUMN low SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_33_chunk ALTER COLUMN low SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_33_chunk ALTER COLUMN close SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_33_chunk ALTER COLUMN close SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_33_chunk ALTER COLUMN volume SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_33_chunk ALTER COLUMN volume SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_33_chunk ALTER COLUMN quote_volume SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_33_chunk ALTER COLUMN quote_volume SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_33_chunk ALTER COLUMN trades SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_33_chunk ALTER COLUMN taker_buy_base SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_33_chunk ALTER COLUMN taker_buy_base SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_33_chunk ALTER COLUMN taker_buy_quote SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_33_chunk ALTER COLUMN taker_buy_quote SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_33_chunk ALTER COLUMN _ts_meta_min_2 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_33_chunk ALTER COLUMN _ts_meta_max_2 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_33_chunk ALTER COLUMN archived_at SET STATISTICS 0;


ALTER TABLE _timescaledb_internal.compress_hyper_573_33_chunk OWNER TO trading;

--
-- Name: compress_hyper_573_34_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal.compress_hyper_573_34_chunk (
    _ts_meta_count integer,
    symbol character varying(20),
    timeframe character varying(10),
    id _timescaledb_internal.compressed_data,
    _ts_meta_min_1 timestamp with time zone,
    _ts_meta_max_1 timestamp with time zone,
    open_time _timescaledb_internal.compressed_data,
    close_time _timescaledb_internal.compressed_data,
    open _timescaledb_internal.compressed_data,
    high _timescaledb_internal.compressed_data,
    low _timescaledb_internal.compressed_data,
    close _timescaledb_internal.compressed_data,
    volume _timescaledb_internal.compressed_data,
    quote_volume _timescaledb_internal.compressed_data,
    trades _timescaledb_internal.compressed_data,
    taker_buy_base _timescaledb_internal.compressed_data,
    taker_buy_quote _timescaledb_internal.compressed_data,
    _ts_meta_min_2 timestamp with time zone,
    _ts_meta_max_2 timestamp with time zone,
    archived_at _timescaledb_internal.compressed_data
)
WITH (toast_tuple_target='128');
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_34_chunk ALTER COLUMN _ts_meta_count SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_34_chunk ALTER COLUMN symbol SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_34_chunk ALTER COLUMN timeframe SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_34_chunk ALTER COLUMN id SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_34_chunk ALTER COLUMN _ts_meta_min_1 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_34_chunk ALTER COLUMN _ts_meta_max_1 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_34_chunk ALTER COLUMN open_time SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_34_chunk ALTER COLUMN close_time SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_34_chunk ALTER COLUMN open SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_34_chunk ALTER COLUMN open SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_34_chunk ALTER COLUMN high SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_34_chunk ALTER COLUMN high SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_34_chunk ALTER COLUMN low SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_34_chunk ALTER COLUMN low SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_34_chunk ALTER COLUMN close SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_34_chunk ALTER COLUMN close SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_34_chunk ALTER COLUMN volume SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_34_chunk ALTER COLUMN volume SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_34_chunk ALTER COLUMN quote_volume SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_34_chunk ALTER COLUMN quote_volume SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_34_chunk ALTER COLUMN trades SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_34_chunk ALTER COLUMN taker_buy_base SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_34_chunk ALTER COLUMN taker_buy_base SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_34_chunk ALTER COLUMN taker_buy_quote SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_34_chunk ALTER COLUMN taker_buy_quote SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_34_chunk ALTER COLUMN _ts_meta_min_2 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_34_chunk ALTER COLUMN _ts_meta_max_2 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_34_chunk ALTER COLUMN archived_at SET STATISTICS 0;


ALTER TABLE _timescaledb_internal.compress_hyper_573_34_chunk OWNER TO trading;

--
-- Name: compress_hyper_573_37_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal.compress_hyper_573_37_chunk (
    _ts_meta_count integer,
    symbol character varying(20),
    timeframe character varying(10),
    id _timescaledb_internal.compressed_data,
    _ts_meta_min_1 timestamp with time zone,
    _ts_meta_max_1 timestamp with time zone,
    open_time _timescaledb_internal.compressed_data,
    close_time _timescaledb_internal.compressed_data,
    open _timescaledb_internal.compressed_data,
    high _timescaledb_internal.compressed_data,
    low _timescaledb_internal.compressed_data,
    close _timescaledb_internal.compressed_data,
    volume _timescaledb_internal.compressed_data,
    quote_volume _timescaledb_internal.compressed_data,
    trades _timescaledb_internal.compressed_data,
    taker_buy_base _timescaledb_internal.compressed_data,
    taker_buy_quote _timescaledb_internal.compressed_data,
    _ts_meta_min_2 timestamp with time zone,
    _ts_meta_max_2 timestamp with time zone,
    archived_at _timescaledb_internal.compressed_data
)
WITH (toast_tuple_target='128');
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_37_chunk ALTER COLUMN _ts_meta_count SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_37_chunk ALTER COLUMN symbol SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_37_chunk ALTER COLUMN timeframe SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_37_chunk ALTER COLUMN id SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_37_chunk ALTER COLUMN _ts_meta_min_1 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_37_chunk ALTER COLUMN _ts_meta_max_1 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_37_chunk ALTER COLUMN open_time SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_37_chunk ALTER COLUMN close_time SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_37_chunk ALTER COLUMN open SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_37_chunk ALTER COLUMN open SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_37_chunk ALTER COLUMN high SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_37_chunk ALTER COLUMN high SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_37_chunk ALTER COLUMN low SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_37_chunk ALTER COLUMN low SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_37_chunk ALTER COLUMN close SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_37_chunk ALTER COLUMN close SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_37_chunk ALTER COLUMN volume SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_37_chunk ALTER COLUMN volume SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_37_chunk ALTER COLUMN quote_volume SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_37_chunk ALTER COLUMN quote_volume SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_37_chunk ALTER COLUMN trades SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_37_chunk ALTER COLUMN taker_buy_base SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_37_chunk ALTER COLUMN taker_buy_base SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_37_chunk ALTER COLUMN taker_buy_quote SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_37_chunk ALTER COLUMN taker_buy_quote SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_37_chunk ALTER COLUMN _ts_meta_min_2 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_37_chunk ALTER COLUMN _ts_meta_max_2 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_37_chunk ALTER COLUMN archived_at SET STATISTICS 0;


ALTER TABLE _timescaledb_internal.compress_hyper_573_37_chunk OWNER TO trading;

--
-- Name: compress_hyper_573_46_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal.compress_hyper_573_46_chunk (
    _ts_meta_count integer,
    symbol character varying(20),
    timeframe character varying(10),
    id _timescaledb_internal.compressed_data,
    _ts_meta_min_1 timestamp with time zone,
    _ts_meta_max_1 timestamp with time zone,
    open_time _timescaledb_internal.compressed_data,
    close_time _timescaledb_internal.compressed_data,
    open _timescaledb_internal.compressed_data,
    high _timescaledb_internal.compressed_data,
    low _timescaledb_internal.compressed_data,
    close _timescaledb_internal.compressed_data,
    volume _timescaledb_internal.compressed_data,
    quote_volume _timescaledb_internal.compressed_data,
    trades _timescaledb_internal.compressed_data,
    taker_buy_base _timescaledb_internal.compressed_data,
    taker_buy_quote _timescaledb_internal.compressed_data,
    _ts_meta_min_2 timestamp with time zone,
    _ts_meta_max_2 timestamp with time zone,
    archived_at _timescaledb_internal.compressed_data
)
WITH (toast_tuple_target='128');
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_46_chunk ALTER COLUMN _ts_meta_count SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_46_chunk ALTER COLUMN symbol SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_46_chunk ALTER COLUMN timeframe SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_46_chunk ALTER COLUMN id SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_46_chunk ALTER COLUMN _ts_meta_min_1 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_46_chunk ALTER COLUMN _ts_meta_max_1 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_46_chunk ALTER COLUMN open_time SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_46_chunk ALTER COLUMN close_time SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_46_chunk ALTER COLUMN open SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_46_chunk ALTER COLUMN open SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_46_chunk ALTER COLUMN high SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_46_chunk ALTER COLUMN high SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_46_chunk ALTER COLUMN low SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_46_chunk ALTER COLUMN low SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_46_chunk ALTER COLUMN close SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_46_chunk ALTER COLUMN close SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_46_chunk ALTER COLUMN volume SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_46_chunk ALTER COLUMN volume SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_46_chunk ALTER COLUMN quote_volume SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_46_chunk ALTER COLUMN quote_volume SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_46_chunk ALTER COLUMN trades SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_46_chunk ALTER COLUMN taker_buy_base SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_46_chunk ALTER COLUMN taker_buy_base SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_46_chunk ALTER COLUMN taker_buy_quote SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_46_chunk ALTER COLUMN taker_buy_quote SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_46_chunk ALTER COLUMN _ts_meta_min_2 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_46_chunk ALTER COLUMN _ts_meta_max_2 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_46_chunk ALTER COLUMN archived_at SET STATISTICS 0;


ALTER TABLE _timescaledb_internal.compress_hyper_573_46_chunk OWNER TO trading;

--
-- Name: compress_hyper_573_49_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal.compress_hyper_573_49_chunk (
    _ts_meta_count integer,
    symbol character varying(20),
    timeframe character varying(10),
    id _timescaledb_internal.compressed_data,
    _ts_meta_min_1 timestamp with time zone,
    _ts_meta_max_1 timestamp with time zone,
    open_time _timescaledb_internal.compressed_data,
    close_time _timescaledb_internal.compressed_data,
    open _timescaledb_internal.compressed_data,
    high _timescaledb_internal.compressed_data,
    low _timescaledb_internal.compressed_data,
    close _timescaledb_internal.compressed_data,
    volume _timescaledb_internal.compressed_data,
    quote_volume _timescaledb_internal.compressed_data,
    trades _timescaledb_internal.compressed_data,
    taker_buy_base _timescaledb_internal.compressed_data,
    taker_buy_quote _timescaledb_internal.compressed_data,
    _ts_meta_min_2 timestamp with time zone,
    _ts_meta_max_2 timestamp with time zone,
    archived_at _timescaledb_internal.compressed_data
)
WITH (toast_tuple_target='128');
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_49_chunk ALTER COLUMN _ts_meta_count SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_49_chunk ALTER COLUMN symbol SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_49_chunk ALTER COLUMN timeframe SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_49_chunk ALTER COLUMN id SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_49_chunk ALTER COLUMN _ts_meta_min_1 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_49_chunk ALTER COLUMN _ts_meta_max_1 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_49_chunk ALTER COLUMN open_time SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_49_chunk ALTER COLUMN close_time SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_49_chunk ALTER COLUMN open SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_49_chunk ALTER COLUMN open SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_49_chunk ALTER COLUMN high SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_49_chunk ALTER COLUMN high SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_49_chunk ALTER COLUMN low SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_49_chunk ALTER COLUMN low SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_49_chunk ALTER COLUMN close SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_49_chunk ALTER COLUMN close SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_49_chunk ALTER COLUMN volume SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_49_chunk ALTER COLUMN volume SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_49_chunk ALTER COLUMN quote_volume SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_49_chunk ALTER COLUMN quote_volume SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_49_chunk ALTER COLUMN trades SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_49_chunk ALTER COLUMN taker_buy_base SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_49_chunk ALTER COLUMN taker_buy_base SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_49_chunk ALTER COLUMN taker_buy_quote SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_49_chunk ALTER COLUMN taker_buy_quote SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_49_chunk ALTER COLUMN _ts_meta_min_2 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_49_chunk ALTER COLUMN _ts_meta_max_2 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_49_chunk ALTER COLUMN archived_at SET STATISTICS 0;


ALTER TABLE _timescaledb_internal.compress_hyper_573_49_chunk OWNER TO trading;

--
-- Name: compress_hyper_573_52_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal.compress_hyper_573_52_chunk (
    _ts_meta_count integer,
    symbol character varying(20),
    timeframe character varying(10),
    id _timescaledb_internal.compressed_data,
    _ts_meta_min_1 timestamp with time zone,
    _ts_meta_max_1 timestamp with time zone,
    open_time _timescaledb_internal.compressed_data,
    close_time _timescaledb_internal.compressed_data,
    open _timescaledb_internal.compressed_data,
    high _timescaledb_internal.compressed_data,
    low _timescaledb_internal.compressed_data,
    close _timescaledb_internal.compressed_data,
    volume _timescaledb_internal.compressed_data,
    quote_volume _timescaledb_internal.compressed_data,
    trades _timescaledb_internal.compressed_data,
    taker_buy_base _timescaledb_internal.compressed_data,
    taker_buy_quote _timescaledb_internal.compressed_data,
    _ts_meta_min_2 timestamp with time zone,
    _ts_meta_max_2 timestamp with time zone,
    archived_at _timescaledb_internal.compressed_data
)
WITH (toast_tuple_target='128');
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_52_chunk ALTER COLUMN _ts_meta_count SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_52_chunk ALTER COLUMN symbol SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_52_chunk ALTER COLUMN timeframe SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_52_chunk ALTER COLUMN id SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_52_chunk ALTER COLUMN _ts_meta_min_1 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_52_chunk ALTER COLUMN _ts_meta_max_1 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_52_chunk ALTER COLUMN open_time SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_52_chunk ALTER COLUMN close_time SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_52_chunk ALTER COLUMN open SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_52_chunk ALTER COLUMN open SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_52_chunk ALTER COLUMN high SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_52_chunk ALTER COLUMN high SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_52_chunk ALTER COLUMN low SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_52_chunk ALTER COLUMN low SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_52_chunk ALTER COLUMN close SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_52_chunk ALTER COLUMN close SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_52_chunk ALTER COLUMN volume SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_52_chunk ALTER COLUMN volume SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_52_chunk ALTER COLUMN quote_volume SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_52_chunk ALTER COLUMN quote_volume SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_52_chunk ALTER COLUMN trades SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_52_chunk ALTER COLUMN taker_buy_base SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_52_chunk ALTER COLUMN taker_buy_base SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_52_chunk ALTER COLUMN taker_buy_quote SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_52_chunk ALTER COLUMN taker_buy_quote SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_52_chunk ALTER COLUMN _ts_meta_min_2 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_52_chunk ALTER COLUMN _ts_meta_max_2 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_52_chunk ALTER COLUMN archived_at SET STATISTICS 0;


ALTER TABLE _timescaledb_internal.compress_hyper_573_52_chunk OWNER TO trading;

--
-- Name: compress_hyper_573_54_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal.compress_hyper_573_54_chunk (
    _ts_meta_count integer,
    symbol character varying(20),
    timeframe character varying(10),
    id _timescaledb_internal.compressed_data,
    _ts_meta_min_1 timestamp with time zone,
    _ts_meta_max_1 timestamp with time zone,
    open_time _timescaledb_internal.compressed_data,
    close_time _timescaledb_internal.compressed_data,
    open _timescaledb_internal.compressed_data,
    high _timescaledb_internal.compressed_data,
    low _timescaledb_internal.compressed_data,
    close _timescaledb_internal.compressed_data,
    volume _timescaledb_internal.compressed_data,
    quote_volume _timescaledb_internal.compressed_data,
    trades _timescaledb_internal.compressed_data,
    taker_buy_base _timescaledb_internal.compressed_data,
    taker_buy_quote _timescaledb_internal.compressed_data,
    _ts_meta_min_2 timestamp with time zone,
    _ts_meta_max_2 timestamp with time zone,
    archived_at _timescaledb_internal.compressed_data
)
WITH (toast_tuple_target='128');
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_54_chunk ALTER COLUMN _ts_meta_count SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_54_chunk ALTER COLUMN symbol SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_54_chunk ALTER COLUMN timeframe SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_54_chunk ALTER COLUMN id SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_54_chunk ALTER COLUMN _ts_meta_min_1 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_54_chunk ALTER COLUMN _ts_meta_max_1 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_54_chunk ALTER COLUMN open_time SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_54_chunk ALTER COLUMN close_time SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_54_chunk ALTER COLUMN open SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_54_chunk ALTER COLUMN open SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_54_chunk ALTER COLUMN high SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_54_chunk ALTER COLUMN high SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_54_chunk ALTER COLUMN low SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_54_chunk ALTER COLUMN low SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_54_chunk ALTER COLUMN close SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_54_chunk ALTER COLUMN close SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_54_chunk ALTER COLUMN volume SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_54_chunk ALTER COLUMN volume SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_54_chunk ALTER COLUMN quote_volume SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_54_chunk ALTER COLUMN quote_volume SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_54_chunk ALTER COLUMN trades SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_54_chunk ALTER COLUMN taker_buy_base SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_54_chunk ALTER COLUMN taker_buy_base SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_54_chunk ALTER COLUMN taker_buy_quote SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_54_chunk ALTER COLUMN taker_buy_quote SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_54_chunk ALTER COLUMN _ts_meta_min_2 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_54_chunk ALTER COLUMN _ts_meta_max_2 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_54_chunk ALTER COLUMN archived_at SET STATISTICS 0;


ALTER TABLE _timescaledb_internal.compress_hyper_573_54_chunk OWNER TO trading;

--
-- Name: compress_hyper_573_56_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal.compress_hyper_573_56_chunk (
    _ts_meta_count integer,
    symbol character varying(20),
    timeframe character varying(10),
    id _timescaledb_internal.compressed_data,
    _ts_meta_min_1 timestamp with time zone,
    _ts_meta_max_1 timestamp with time zone,
    open_time _timescaledb_internal.compressed_data,
    close_time _timescaledb_internal.compressed_data,
    open _timescaledb_internal.compressed_data,
    high _timescaledb_internal.compressed_data,
    low _timescaledb_internal.compressed_data,
    close _timescaledb_internal.compressed_data,
    volume _timescaledb_internal.compressed_data,
    quote_volume _timescaledb_internal.compressed_data,
    trades _timescaledb_internal.compressed_data,
    taker_buy_base _timescaledb_internal.compressed_data,
    taker_buy_quote _timescaledb_internal.compressed_data,
    _ts_meta_min_2 timestamp with time zone,
    _ts_meta_max_2 timestamp with time zone,
    archived_at _timescaledb_internal.compressed_data
)
WITH (toast_tuple_target='128');
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_56_chunk ALTER COLUMN _ts_meta_count SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_56_chunk ALTER COLUMN symbol SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_56_chunk ALTER COLUMN timeframe SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_56_chunk ALTER COLUMN id SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_56_chunk ALTER COLUMN _ts_meta_min_1 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_56_chunk ALTER COLUMN _ts_meta_max_1 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_56_chunk ALTER COLUMN open_time SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_56_chunk ALTER COLUMN close_time SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_56_chunk ALTER COLUMN open SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_56_chunk ALTER COLUMN open SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_56_chunk ALTER COLUMN high SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_56_chunk ALTER COLUMN high SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_56_chunk ALTER COLUMN low SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_56_chunk ALTER COLUMN low SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_56_chunk ALTER COLUMN close SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_56_chunk ALTER COLUMN close SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_56_chunk ALTER COLUMN volume SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_56_chunk ALTER COLUMN volume SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_56_chunk ALTER COLUMN quote_volume SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_56_chunk ALTER COLUMN quote_volume SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_56_chunk ALTER COLUMN trades SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_56_chunk ALTER COLUMN taker_buy_base SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_56_chunk ALTER COLUMN taker_buy_base SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_56_chunk ALTER COLUMN taker_buy_quote SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_56_chunk ALTER COLUMN taker_buy_quote SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_56_chunk ALTER COLUMN _ts_meta_min_2 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_56_chunk ALTER COLUMN _ts_meta_max_2 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_56_chunk ALTER COLUMN archived_at SET STATISTICS 0;


ALTER TABLE _timescaledb_internal.compress_hyper_573_56_chunk OWNER TO trading;

--
-- Name: compress_hyper_573_60_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal.compress_hyper_573_60_chunk (
    _ts_meta_count integer,
    symbol character varying(20),
    timeframe character varying(10),
    id _timescaledb_internal.compressed_data,
    _ts_meta_min_1 timestamp with time zone,
    _ts_meta_max_1 timestamp with time zone,
    open_time _timescaledb_internal.compressed_data,
    close_time _timescaledb_internal.compressed_data,
    open _timescaledb_internal.compressed_data,
    high _timescaledb_internal.compressed_data,
    low _timescaledb_internal.compressed_data,
    close _timescaledb_internal.compressed_data,
    volume _timescaledb_internal.compressed_data,
    quote_volume _timescaledb_internal.compressed_data,
    trades _timescaledb_internal.compressed_data,
    taker_buy_base _timescaledb_internal.compressed_data,
    taker_buy_quote _timescaledb_internal.compressed_data,
    _ts_meta_min_2 timestamp with time zone,
    _ts_meta_max_2 timestamp with time zone,
    archived_at _timescaledb_internal.compressed_data
)
WITH (toast_tuple_target='128');
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_60_chunk ALTER COLUMN _ts_meta_count SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_60_chunk ALTER COLUMN symbol SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_60_chunk ALTER COLUMN timeframe SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_60_chunk ALTER COLUMN id SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_60_chunk ALTER COLUMN _ts_meta_min_1 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_60_chunk ALTER COLUMN _ts_meta_max_1 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_60_chunk ALTER COLUMN open_time SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_60_chunk ALTER COLUMN close_time SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_60_chunk ALTER COLUMN open SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_60_chunk ALTER COLUMN open SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_60_chunk ALTER COLUMN high SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_60_chunk ALTER COLUMN high SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_60_chunk ALTER COLUMN low SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_60_chunk ALTER COLUMN low SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_60_chunk ALTER COLUMN close SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_60_chunk ALTER COLUMN close SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_60_chunk ALTER COLUMN volume SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_60_chunk ALTER COLUMN volume SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_60_chunk ALTER COLUMN quote_volume SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_60_chunk ALTER COLUMN quote_volume SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_60_chunk ALTER COLUMN trades SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_60_chunk ALTER COLUMN taker_buy_base SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_60_chunk ALTER COLUMN taker_buy_base SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_60_chunk ALTER COLUMN taker_buy_quote SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_60_chunk ALTER COLUMN taker_buy_quote SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_60_chunk ALTER COLUMN _ts_meta_min_2 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_60_chunk ALTER COLUMN _ts_meta_max_2 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_60_chunk ALTER COLUMN archived_at SET STATISTICS 0;


ALTER TABLE _timescaledb_internal.compress_hyper_573_60_chunk OWNER TO trading;

--
-- Name: compress_hyper_573_62_chunk; Type: TABLE; Schema: _timescaledb_internal; Owner: trading
--

CREATE TABLE _timescaledb_internal.compress_hyper_573_62_chunk (
    _ts_meta_count integer,
    symbol character varying(20),
    timeframe character varying(10),
    id _timescaledb_internal.compressed_data,
    _ts_meta_min_1 timestamp with time zone,
    _ts_meta_max_1 timestamp with time zone,
    open_time _timescaledb_internal.compressed_data,
    close_time _timescaledb_internal.compressed_data,
    open _timescaledb_internal.compressed_data,
    high _timescaledb_internal.compressed_data,
    low _timescaledb_internal.compressed_data,
    close _timescaledb_internal.compressed_data,
    volume _timescaledb_internal.compressed_data,
    quote_volume _timescaledb_internal.compressed_data,
    trades _timescaledb_internal.compressed_data,
    taker_buy_base _timescaledb_internal.compressed_data,
    taker_buy_quote _timescaledb_internal.compressed_data,
    _ts_meta_min_2 timestamp with time zone,
    _ts_meta_max_2 timestamp with time zone,
    archived_at _timescaledb_internal.compressed_data
)
WITH (toast_tuple_target='128');
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_62_chunk ALTER COLUMN _ts_meta_count SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_62_chunk ALTER COLUMN symbol SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_62_chunk ALTER COLUMN timeframe SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_62_chunk ALTER COLUMN id SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_62_chunk ALTER COLUMN _ts_meta_min_1 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_62_chunk ALTER COLUMN _ts_meta_max_1 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_62_chunk ALTER COLUMN open_time SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_62_chunk ALTER COLUMN close_time SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_62_chunk ALTER COLUMN open SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_62_chunk ALTER COLUMN open SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_62_chunk ALTER COLUMN high SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_62_chunk ALTER COLUMN high SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_62_chunk ALTER COLUMN low SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_62_chunk ALTER COLUMN low SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_62_chunk ALTER COLUMN close SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_62_chunk ALTER COLUMN close SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_62_chunk ALTER COLUMN volume SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_62_chunk ALTER COLUMN volume SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_62_chunk ALTER COLUMN quote_volume SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_62_chunk ALTER COLUMN quote_volume SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_62_chunk ALTER COLUMN trades SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_62_chunk ALTER COLUMN taker_buy_base SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_62_chunk ALTER COLUMN taker_buy_base SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_62_chunk ALTER COLUMN taker_buy_quote SET STATISTICS 0;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_62_chunk ALTER COLUMN taker_buy_quote SET STORAGE EXTENDED;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_62_chunk ALTER COLUMN _ts_meta_min_2 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_62_chunk ALTER COLUMN _ts_meta_max_2 SET STATISTICS 1000;
ALTER TABLE ONLY _timescaledb_internal.compress_hyper_573_62_chunk ALTER COLUMN archived_at SET STATISTICS 0;


ALTER TABLE _timescaledb_internal.compress_hyper_573_62_chunk OWNER TO trading;

--
-- Name: archive_metadata; Type: TABLE; Schema: public; Owner: trading
--

CREATE TABLE public.archive_metadata (
    stream_name character varying(100) NOT NULL,
    last_archived_id character varying(100),
    last_archived_at timestamp with time zone,
    records_archived bigint DEFAULT 0,
    last_error text,
    last_error_at timestamp with time zone
);


ALTER TABLE public.archive_metadata OWNER TO trading;

--
-- Name: TABLE archive_metadata; Type: COMMENT; Schema: public; Owner: trading
--

COMMENT ON TABLE public.archive_metadata IS 'Track archiving progress and errors';


--
-- Name: atr_archive; Type: TABLE; Schema: public; Owner: trading
--

CREATE TABLE public.atr_archive (
    id bigint NOT NULL,
    symbol character varying(20) NOT NULL,
    timeframe character varying(10) NOT NULL,
    atr numeric(20,8) NOT NULL,
    period integer DEFAULT 14,
    close_price numeric(20,8),
    ts timestamp with time zone NOT NULL,
    count integer,
    source character varying(20) DEFAULT 'py'::character varying,
    archived_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.atr_archive OWNER TO trading;

--
-- Name: TABLE atr_archive; Type: COMMENT; Schema: public; Owner: trading
--

COMMENT ON TABLE public.atr_archive IS 'Historical ATR calculations archived from Redis keys';


--
-- Name: atr_archive_id_seq; Type: SEQUENCE; Schema: public; Owner: trading
--

CREATE SEQUENCE public.atr_archive_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.atr_archive_id_seq OWNER TO trading;

--
-- Name: atr_archive_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: trading
--

ALTER SEQUENCE public.atr_archive_id_seq OWNED BY public.atr_archive.id;


--
-- Name: calendar_events; Type: TABLE; Schema: public; Owner: trading
--

CREATE TABLE public.calendar_events (
    uid text NOT NULL,
    event_ts_ms bigint NOT NULL,
    ingested_ts_ms bigint NOT NULL,
    country text NOT NULL,
    currency text NOT NULL,
    title text NOT NULL,
    importance integer NOT NULL,
    grade_id integer NOT NULL,
    forecast text NOT NULL,
    previous text NOT NULL,
    unit text NOT NULL,
    source text NOT NULL,
    payload_json jsonb NOT NULL,
    inserted_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.calendar_events OWNER TO trading;

--
-- Name: calendar_features_scope; Type: TABLE; Schema: public; Owner: trading
--

CREATE TABLE public.calendar_features_scope (
    scope text NOT NULL,
    ts_ms bigint NOT NULL,
    next_event_ts_ms bigint NOT NULL,
    event_grade_id integer NOT NULL,
    event_ref text NOT NULL,
    event_tminus_sec integer NOT NULL,
    inserted_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.calendar_features_scope OWNER TO trading;

--
-- Name: calibration_state; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.calibration_state (
    symbol text NOT NULL,
    regime text NOT NULL,
    kind text NOT NULL,
    ts_ms bigint NOT NULL,
    state_json jsonb NOT NULL,
    updated_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.calibration_state OWNER TO postgres;

--
-- Name: candles_archive_id_seq; Type: SEQUENCE; Schema: public; Owner: trading
--

CREATE SEQUENCE public.candles_archive_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.candles_archive_id_seq OWNER TO trading;

--
-- Name: candles_archive_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: trading
--

ALTER SEQUENCE public.candles_archive_id_seq OWNED BY public.candles_archive.id;


--
-- Name: daily_metrics; Type: TABLE; Schema: public; Owner: trading
--

CREATE TABLE public.daily_metrics (
    id bigint NOT NULL,
    date date NOT NULL,
    source text,
    symbol text NOT NULL,
    trades_count integer DEFAULT 0,
    wins integer DEFAULT 0,
    losses integer DEFAULT 0,
    breakeven integer DEFAULT 0,
    pnl_net_sum double precision DEFAULT 0.0,
    pnl_net_avg double precision DEFAULT 0.0,
    pnl_net_std double precision DEFAULT 0.0,
    expectancy_r double precision DEFAULT 0.0,
    payoff_r double precision DEFAULT 0.0,
    payoff_usd double precision DEFAULT 0.0,
    kelly_r double precision DEFAULT 0.0,
    wr double precision DEFAULT 0.0,
    sharpe double precision DEFAULT 0.0,
    sortino double precision DEFAULT 0.0,
    mdd_usd double precision DEFAULT 0.0,
    wr_fixed double precision DEFAULT 0.0,
    expectancy_fixed_r double precision DEFAULT 0.0,
    payoff_fixed_r double precision DEFAULT 0.0,
    payoff_fixed_usd double precision DEFAULT 0.0,
    delta_expectancy_r double precision DEFAULT 0.0,
    created_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.daily_metrics OWNER TO trading;

--
-- Name: daily_metrics_id_seq; Type: SEQUENCE; Schema: public; Owner: trading
--

CREATE SEQUENCE public.daily_metrics_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.daily_metrics_id_seq OWNER TO trading;

--
-- Name: daily_metrics_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: trading
--

ALTER SEQUENCE public.daily_metrics_id_seq OWNED BY public.daily_metrics.id;


--
-- Name: edge_gate_events; Type: TABLE; Schema: public; Owner: trading
--

CREATE TABLE public.edge_gate_events (
    id bigint NOT NULL,
    signal_id text NOT NULL,
    symbol text NOT NULL,
    gate_name text DEFAULT 'edge_cost'::text NOT NULL,
    gate_version integer DEFAULT 2 NOT NULL,
    stage text DEFAULT 'pre_emit'::text NOT NULL,
    ts_ms bigint NOT NULL,
    ts timestamp with time zone GENERATED ALWAYS AS (to_timestamp((((ts_ms)::numeric / 1000.0))::double precision)) STORED,
    passed boolean NOT NULL,
    veto_code text,
    edge_source text DEFAULT 'none'::text NOT NULL,
    exp_bps double precision NOT NULL,
    req_bps double precision NOT NULL,
    margin_bps double precision NOT NULL,
    edge_ratio double precision NOT NULL,
    k double precision NOT NULL,
    fees_bps double precision NOT NULL,
    slip_bps double precision NOT NULL,
    buf_bps double precision NOT NULL,
    total_costs_bps double precision NOT NULL,
    ctx jsonb,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.edge_gate_events OWNER TO trading;

--
-- Name: edge_gate_events_id_seq; Type: SEQUENCE; Schema: public; Owner: trading
--

CREATE SEQUENCE public.edge_gate_events_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.edge_gate_events_id_seq OWNER TO trading;

--
-- Name: edge_gate_events_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: trading
--

ALTER SEQUENCE public.edge_gate_events_id_seq OWNED BY public.edge_gate_events.id;


--
-- Name: entry_policy_audit; Type: TABLE; Schema: public; Owner: trading
--

CREATE TABLE public.entry_policy_audit (
    stream_id text NOT NULL,
    ts_ms bigint NOT NULL,
    ts timestamp with time zone NOT NULL,
    sid text,
    symbol text,
    tf text,
    strategy text,
    source text,
    decision text NOT NULL,
    arm text,
    ab_group text,
    scenario text,
    regime text,
    of_confirm_score double precision,
    coh double precision,
    leader_conf double precision,
    spread_z double precision,
    pressure_sps double precision,
    obi_age_ms bigint,
    payload_json jsonb NOT NULL,
    ingested_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.entry_policy_audit OWNER TO trading;

--
-- Name: entry_tag_metrics; Type: TABLE; Schema: public; Owner: trading
--

CREATE TABLE public.entry_tag_metrics (
    id bigint NOT NULL,
    date date NOT NULL,
    source text,
    symbol text NOT NULL,
    entry_tag text NOT NULL,
    trades_count integer DEFAULT 0,
    wins integer DEFAULT 0,
    losses integer DEFAULT 0,
    breakeven integer DEFAULT 0,
    pnl_net_sum double precision DEFAULT 0.0,
    pnl_net_avg double precision DEFAULT 0.0,
    expectancy_r double precision DEFAULT 0.0,
    payoff_r double precision DEFAULT 0.0,
    payoff_usd double precision DEFAULT 0.0,
    wr double precision DEFAULT 0.0,
    wr_fixed double precision DEFAULT 0.0,
    expectancy_fixed_r double precision DEFAULT 0.0,
    payoff_fixed_r double precision DEFAULT 0.0,
    payoff_fixed_usd double precision DEFAULT 0.0,
    delta_expectancy_r double precision DEFAULT 0.0,
    giveback_avg_usd double precision DEFAULT 0.0,
    giveback_avg_r double precision DEFAULT 0.0,
    giveback_avg_ratio double precision DEFAULT 0.0,
    giveback_share double precision DEFAULT 0.0,
    missed_avg_usd double precision DEFAULT 0.0,
    missed_avg_r double precision DEFAULT 0.0,
    missed_avg_ratio double precision DEFAULT 0.0,
    missed_share double precision DEFAULT 0.0,
    mfe_avg_r double precision DEFAULT 0.0,
    mae_avg_r double precision DEFAULT 0.0,
    trailing_share double precision DEFAULT 0.0,
    trailing_close_share double precision DEFAULT 0.0,
    trailing_wr double precision DEFAULT 0.0,
    trailing_expectancy_r double precision DEFAULT 0.0,
    trailing_expectancy_fixed_r double precision DEFAULT 0.0,
    trailing_delta_expectancy_r double precision DEFAULT 0.0,
    created_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.entry_tag_metrics OWNER TO trading;

--
-- Name: entry_tag_metrics_id_seq; Type: SEQUENCE; Schema: public; Owner: trading
--

CREATE SEQUENCE public.entry_tag_metrics_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.entry_tag_metrics_id_seq OWNER TO trading;

--
-- Name: entry_tag_metrics_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: trading
--

ALTER SEQUENCE public.entry_tag_metrics_id_seq OWNED BY public.entry_tag_metrics.id;


--
-- Name: signal_performance; Type: TABLE; Schema: public; Owner: trading
--

CREATE TABLE public.signal_performance (
    signal_id uuid NOT NULL,
    ts_signal timestamp with time zone NOT NULL,
    symbol text NOT NULL,
    side text NOT NULL,
    setup_type text NOT NULL,
    ts_entry timestamp with time zone,
    ts_exit timestamp with time zone,
    price_at_signal double precision NOT NULL,
    entry_price double precision,
    exit_price double precision,
    stop_price double precision,
    realized_r double precision,
    mfe_r double precision,
    mae_r double precision,
    ttd_bars integer,
    ttd_seconds double precision,
    outcome text NOT NULL,
    bars_to_entry integer,
    bars_to_exit integer,
    notes text
);


ALTER TABLE public.signal_performance OWNER TO trading;

--
-- Name: TABLE signal_performance; Type: COMMENT; Schema: public; Owner: trading
--

COMMENT ON TABLE public.signal_performance IS 'Post-execution performance metrics and TTD analysis';


--
-- Name: signals; Type: TABLE; Schema: public; Owner: trading
--

CREATE TABLE public.signals (
    signal_id uuid NOT NULL,
    ts_signal timestamp with time zone NOT NULL,
    symbol text NOT NULL,
    side text NOT NULL,
    setup_type text NOT NULL,
    price_at_signal double precision NOT NULL,
    final_score double precision NOT NULL,
    atr_1m double precision,
    atr_5m double precision,
    tick_size double precision,
    contract_size double precision,
    extra_json jsonb DEFAULT '{}'::jsonb,
    signal_family text,
    experiment_id text,
    experiment_variant text,
    filter_flags jsonb,
    session text,
    regime text,
    delta_spike_z double precision,
    obi double precision,
    weak_progress double precision,
    atr_quantile double precision,
    signal_type text,
    pnl_r double precision,
    raw_ctx jsonb
);


ALTER TABLE public.signals OWNER TO trading;

--
-- Name: TABLE signals; Type: COMMENT; Schema: public; Owner: trading
--

COMMENT ON TABLE public.signals IS 'Base signal events with context data';


--
-- Name: COLUMN signals.experiment_id; Type: COMMENT; Schema: public; Owner: trading
--

COMMENT ON COLUMN public.signals.experiment_id IS 'ID of experiment this signal participated in';


--
-- Name: COLUMN signals.experiment_variant; Type: COMMENT; Schema: public; Owner: trading
--

COMMENT ON COLUMN public.signals.experiment_variant IS 'Which variant (control/treatment) this signal was assigned to';


--
-- Name: COLUMN signals.filter_flags; Type: COMMENT; Schema: public; Owner: trading
--

COMMENT ON COLUMN public.signals.filter_flags IS 'JSON object with results of filter applications for experiment analysis';


--
-- Name: COLUMN signals.delta_spike_z; Type: COMMENT; Schema: public; Owner: trading
--

COMMENT ON COLUMN public.signals.delta_spike_z IS 'Delta spike Z-score for signal strength';


--
-- Name: COLUMN signals.obi; Type: COMMENT; Schema: public; Owner: trading
--

COMMENT ON COLUMN public.signals.obi IS 'Order Book Imbalance metric';


--
-- Name: COLUMN signals.weak_progress; Type: COMMENT; Schema: public; Owner: trading
--

COMMENT ON COLUMN public.signals.weak_progress IS 'Weak progress indicator (range vs ATR)';


--
-- Name: COLUMN signals.atr_quantile; Type: COMMENT; Schema: public; Owner: trading
--

COMMENT ON COLUMN public.signals.atr_quantile IS 'ATR quantile for volatility assessment';


--
-- Name: experiment_signal_summary; Type: VIEW; Schema: public; Owner: trading
--

CREATE VIEW public.experiment_signal_summary AS
 SELECT s.signal_id,
    s.ts_signal,
    s.symbol,
    s.side,
    s.setup_type,
    s.experiment_id,
    s.experiment_variant,
    s.filter_flags,
    sp.realized_r,
    sp.outcome,
        CASE
            WHEN (sp.realized_r >= (0.2)::double precision) THEN 1
            ELSE 0
        END AS is_winner,
        CASE
            WHEN (sp.outcome = ANY (ARRAY['realized'::text, 'stopped'::text])) THEN 1
            ELSE 0
        END AS was_traded
   FROM (public.signals s
     LEFT JOIN public.signal_performance sp ON ((s.signal_id = sp.signal_id)))
  WHERE (s.experiment_id IS NOT NULL);


ALTER VIEW public.experiment_signal_summary OWNER TO trading;

--
-- Name: VIEW experiment_signal_summary; Type: COMMENT; Schema: public; Owner: trading
--

COMMENT ON VIEW public.experiment_signal_summary IS 'Unified view for experiment analysis with trade outcomes';


--
-- Name: market_daily_ohlc; Type: TABLE; Schema: public; Owner: trading
--

CREATE TABLE public.market_daily_ohlc (
    symbol text NOT NULL,
    date date NOT NULL,
    open numeric,
    high numeric,
    low numeric,
    close numeric,
    volume numeric,
    inserted_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.market_daily_ohlc OWNER TO trading;

--
-- Name: microbars; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.microbars (
    symbol text NOT NULL,
    ts_ms bigint NOT NULL,
    o double precision NOT NULL,
    h double precision NOT NULL,
    l double precision NOT NULL,
    c double precision NOT NULL,
    v double precision NOT NULL,
    cvd double precision NOT NULL,
    inserted_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.microbars OWNER TO postgres;

--
-- Name: trades_closed; Type: TABLE; Schema: public; Owner: trading
--

CREATE TABLE public.trades_closed (
    id bigint NOT NULL,
    order_id text NOT NULL,
    sid text,
    strategy text,
    source text,
    symbol text NOT NULL,
    tf text,
    direction text,
    entry_ts_ms bigint NOT NULL,
    exit_ts_ms bigint NOT NULL,
    entry_ts timestamp with time zone,
    exit_ts timestamp with time zone,
    entry_price double precision NOT NULL,
    exit_price double precision NOT NULL,
    lot double precision NOT NULL,
    notional_usd double precision,
    pnl_net double precision NOT NULL,
    pnl_gross double precision NOT NULL,
    fees double precision NOT NULL,
    pnl_pct double precision,
    pnl_if_fixed_exit double precision,
    baseline_exit_reason text,
    baseline_exit_ts_ms bigint,
    baseline_exit_price double precision,
    tp1_hit boolean,
    tp2_hit boolean,
    tp3_hit boolean,
    tp_hits integer,
    tp_before_sl integer,
    trailing_started boolean,
    trailing_active boolean,
    trailing_moves integer,
    trailing_profile text,
    mfe_pnl double precision,
    mae_pnl double precision,
    giveback double precision,
    missed_profit double precision,
    one_r_money double precision,
    r_multiple double precision,
    duration_ms bigint,
    close_reason text,
    close_reason_raw text,
    close_reason_detail text DEFAULT ''::text,
    entry_tag text,
    max_favorable_price double precision,
    max_favorable_ts bigint,
    is_final_close boolean,
    remaining_qty double precision,
    status text,
    health_l2_stale_ratio_tick double precision,
    health_l2_stale_ratio_now double precision,
    health_avg_l2_age_ms double precision,
    health_avg_l2_age_tick_ms double precision,
    health_signal_emit_rate double precision,
    health_dlq_rate double precision,
    created_at timestamp with time zone DEFAULT now(),
    config_json jsonb,
    is_virtual boolean DEFAULT false,
    strong_gate_ok boolean,
    meta_enforce_cov_bucket text DEFAULT ''::text,
    meta_enforce_applied integer DEFAULT '-1'::integer,
    policy_mode text,
    policy_raw text,
    ind_delta_z double precision GENERATED ALWAYS AS ((((config_json -> 'indicators'::text) ->> 'delta_z'::text))::double precision) STORED,
    ind_obi double precision GENERATED ALWAYS AS ((((config_json -> 'indicators'::text) ->> 'obi'::text))::double precision) STORED,
    ind_weak_progress boolean GENERATED ALWAYS AS ((((config_json -> 'indicators'::text) ->> 'weak_progress'::text) = 'true'::text)) STORED,
    ind_atr_th_bps double precision GENERATED ALWAYS AS ((((config_json -> 'indicators'::text) ->> 'atr_unified_th_bps'::text))::double precision) STORED
);


ALTER TABLE public.trades_closed OWNER TO trading;

--
-- Name: v_exec_slippage_eval; Type: VIEW; Schema: public; Owner: trading
--

CREATE VIEW public.v_exec_slippage_eval AS
 WITH base AS (
         SELECT p0.exit_ts AS ts,
            t.symbol AS sym,
            COALESCE(NULLIF((p0.features_json ->> 'exec_regime_bucket'::text), ''::text), 'NORMAL'::text) AS exec_regime_bucket,
            COALESCE(NULLIF(((p0.features_json ->> 'spread_bps_submit'::text))::double precision, (0)::double precision), NULLIF(p0.spread_bps_at_entry, (0)::double precision), NULLIF(((p0.features_json ->> 'spread_bps'::text))::double precision, (0)::double precision), (0.0)::double precision) AS spread_bps,
            COALESCE(NULLIF(((p0.features_json ->> 'impact_proxy'::text))::double precision, (0)::double precision), (0.0)::double precision) AS impact_proxy,
            COALESCE(NULLIF(((p0.features_json ->> 'mid_px_submit'::text))::double precision, (0)::double precision), NULLIF(t.entry_price, (0)::double precision), (0.0)::double precision) AS mid_px_submit,
            COALESCE(NULLIF(((p0.features_json ->> 'fill_px'::text))::double precision, (0)::double precision), NULLIF(t.entry_price, (0)::double precision), (0.0)::double precision) AS fill_px,
            COALESCE(NULLIF(t.notional_usd, (0)::double precision), NULLIF(((p0.features_json ->> 'size_usd'::text))::double precision, (0)::double precision), (0.0)::double precision) AS size_usd,
            COALESCE(NULLIF(((p0.features_json ->> 'expected_slippage_bps'::text))::double precision, (0)::double precision), (0.0)::double precision) AS expected_slip_model_bps,
            COALESCE(NULLIF(((p0.features_json ->> 'expected_slippage_decomp_bps'::text))::double precision, (0)::double precision), (0.0)::double precision) AS expected_slip_decomp_bps,
            COALESCE(NULLIF(((p0.features_json ->> 'slip_decomp_coeff_bps'::text))::double precision, (0)::double precision), (0.0)::double precision) AS slip_decomp_coeff_bps,
            COALESCE(NULLIF(((p0.features_json ->> 'slip_decomp_spread_bps'::text))::double precision, (0)::double precision), (0.0)::double precision) AS slip_decomp_spread_bps,
            COALESCE(NULLIF(((p0.features_json ->> 'slip_decomp_impact_bps'::text))::double precision, (0)::double precision), (0.0)::double precision) AS slip_decomp_impact_bps,
            NULLIF(((p0.features_json ->> 'edge_bps'::text))::double precision, (0)::double precision) AS edge_bps,
            upper(COALESCE(NULLIF(t.direction, ''::text), ''::text)) AS dir,
            COALESCE(NULLIF(((p0.features_json ->> 'taker_flow_imb_z'::text))::double precision, (0)::double precision), (0.0)::double precision) AS taker_flow_imb_z,
            COALESCE(NULLIF((p0.features_json ->> 'liq_regime_label'::text), ''::text), 'na'::text) AS liq_regime_label,
            COALESCE(NULLIF((p0.features_json ->> 'vol_regime_label'::text), ''::text), 'na'::text) AS vol_regime_label,
            p0.features_json
           FROM (public.trades_closed_p0 p0
             JOIN public.trades_closed t ON ((t.order_id = p0.order_id)))
          WHERE (p0.exit_ts > (now() - '60 days'::interval))
        ), inner_calc AS (
         SELECT base.ts,
            base.sym,
            base.exec_regime_bucket,
            base.spread_bps,
            base.impact_proxy,
            base.mid_px_submit,
            base.fill_px,
            base.size_usd,
            base.expected_slip_model_bps,
            base.expected_slip_decomp_bps,
            base.slip_decomp_coeff_bps,
            base.slip_decomp_spread_bps,
            base.slip_decomp_impact_bps,
            base.edge_bps,
            base.dir,
            base.taker_flow_imb_z,
            base.liq_regime_label,
            base.vol_regime_label,
            base.features_json,
                CASE
                    WHEN ((base.mid_px_submit <= (0)::double precision) OR (base.fill_px <= (0)::double precision)) THEN (0.0)::double precision
                    WHEN (base.dir = 'LONG'::text) THEN GREATEST((0.0)::double precision, (((base.fill_px - base.mid_px_submit) / base.mid_px_submit) * (10000.0)::double precision))
                    WHEN (base.dir = 'SHORT'::text) THEN GREATEST((0.0)::double precision, (((base.mid_px_submit - base.fill_px) / base.mid_px_submit) * (10000.0)::double precision))
                    ELSE (0.0)::double precision
                END AS realized_slip_worse_bps,
                CASE
                    WHEN (base.edge_bps IS NULL) THEN NULL::double precision
                    ELSE (base.edge_bps - base.expected_slip_decomp_bps)
                END AS edge_minus_expected_bps,
                CASE
                    WHEN (base.edge_bps IS NULL) THEN NULL::double precision
                    ELSE (base.edge_bps - base.expected_slip_model_bps)
                END AS edge_minus_expected_model_bps
           FROM base
        ), calc AS (
         SELECT x.ts,
            x.sym,
            x.exec_regime_bucket,
            x.spread_bps,
            x.impact_proxy,
            x.mid_px_submit,
            x.fill_px,
            x.size_usd,
            x.expected_slip_model_bps,
            x.expected_slip_decomp_bps,
            x.slip_decomp_coeff_bps,
            x.slip_decomp_spread_bps,
            x.slip_decomp_impact_bps,
            x.edge_bps,
            x.dir,
            x.taker_flow_imb_z,
            x.liq_regime_label,
            x.vol_regime_label,
            x.features_json,
            x.realized_slip_worse_bps,
            x.edge_minus_expected_bps,
            x.edge_minus_expected_model_bps,
            (x.realized_slip_worse_bps - x.expected_slip_decomp_bps) AS slippage_residual_bps,
            (x.realized_slip_worse_bps - x.expected_slip_model_bps) AS slippage_residual_model_bps
           FROM inner_calc x
        )
 SELECT calc.ts,
    calc.sym,
    calc.exec_regime_bucket,
    calc.spread_bps,
    calc.impact_proxy,
    calc.mid_px_submit,
    calc.fill_px,
    calc.size_usd,
    calc.expected_slip_model_bps,
    calc.expected_slip_decomp_bps,
    calc.slip_decomp_coeff_bps,
    calc.slip_decomp_spread_bps,
    calc.slip_decomp_impact_bps,
    calc.edge_bps,
    calc.realized_slip_worse_bps,
    calc.edge_minus_expected_bps AS edge_minus_expected_slip_decomp_bps,
    calc.edge_minus_expected_bps,
    calc.edge_minus_expected_model_bps,
    calc.taker_flow_imb_z,
    calc.liq_regime_label,
    calc.vol_regime_label,
    calc.features_json,
    calc.slippage_residual_bps,
    calc.slippage_residual_model_bps
   FROM calc;


ALTER VIEW public.v_exec_slippage_eval OWNER TO trading;

--
-- Name: mv_exec_slippage_eval_1h_stats; Type: MATERIALIZED VIEW; Schema: public; Owner: trading
--

CREATE MATERIALIZED VIEW public.mv_exec_slippage_eval_1h_stats AS
 SELECT v_exec_slippage_eval.sym,
    public.time_bucket('01:00:00'::interval, v_exec_slippage_eval.ts) AS t,
    v_exec_slippage_eval.exec_regime_bucket,
    count(*) AS n,
    percentile_cont((0.95)::double precision) WITHIN GROUP (ORDER BY v_exec_slippage_eval.slippage_residual_bps) AS resid_p95_bps,
    percentile_cont((0.99)::double precision) WITHIN GROUP (ORDER BY v_exec_slippage_eval.slippage_residual_bps) AS resid_p99_bps,
    avg(
        CASE
            WHEN (v_exec_slippage_eval.edge_minus_expected_bps < (0)::double precision) THEN 1.0
            ELSE 0.0
        END) AS edge_neg_share
   FROM public.v_exec_slippage_eval
  GROUP BY v_exec_slippage_eval.sym, (public.time_bucket('01:00:00'::interval, v_exec_slippage_eval.ts)), v_exec_slippage_eval.exec_regime_bucket
  WITH NO DATA;


ALTER MATERIALIZED VIEW public.mv_exec_slippage_eval_1h_stats OWNER TO trading;

--
-- Name: mv_exec_slippage_eval_1h_stats_v2; Type: MATERIALIZED VIEW; Schema: public; Owner: trading
--

CREATE MATERIALIZED VIEW public.mv_exec_slippage_eval_1h_stats_v2 AS
 SELECT public.time_bucket('01:00:00'::interval, v_exec_slippage_eval.ts) AS t,
    v_exec_slippage_eval.sym,
    v_exec_slippage_eval.exec_regime_bucket,
    count(*) AS n,
    percentile_cont((0.95)::double precision) WITHIN GROUP (ORDER BY v_exec_slippage_eval.slippage_residual_bps) AS resid_p95_bps,
    percentile_cont((0.99)::double precision) WITHIN GROUP (ORDER BY v_exec_slippage_eval.slippage_residual_bps) AS resid_p99_bps,
    avg(
        CASE
            WHEN (v_exec_slippage_eval.edge_minus_expected_bps < (0)::double precision) THEN 1
            ELSE 0
        END) AS edge_neg_share,
    percentile_cont((0.95)::double precision) WITHIN GROUP (ORDER BY v_exec_slippage_eval.slippage_residual_model_bps) AS resid_model_p95_bps,
    percentile_cont((0.99)::double precision) WITHIN GROUP (ORDER BY v_exec_slippage_eval.slippage_residual_model_bps) AS resid_model_p99_bps,
    avg(
        CASE
            WHEN (v_exec_slippage_eval.edge_minus_expected_model_bps < (0)::double precision) THEN 1
            ELSE 0
        END) AS edge_neg_share_model
   FROM public.v_exec_slippage_eval
  GROUP BY (public.time_bucket('01:00:00'::interval, v_exec_slippage_eval.ts)), v_exec_slippage_eval.sym, v_exec_slippage_eval.exec_regime_bucket
  WITH NO DATA;


ALTER MATERIALIZED VIEW public.mv_exec_slippage_eval_1h_stats_v2 OWNER TO trading;

--
-- Name: news_analysis; Type: TABLE; Schema: public; Owner: trading
--

CREATE TABLE public.news_analysis (
    uid text NOT NULL,
    symbol text NOT NULL,
    ts_ms bigint NOT NULL,
    source text NOT NULL,
    risk double precision NOT NULL,
    surprise double precision NOT NULL,
    tags_mask bigint NOT NULL,
    primary_tag integer NOT NULL,
    payload_json jsonb NOT NULL,
    inserted_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.news_analysis OWNER TO trading;

--
-- Name: news_features_symbol; Type: TABLE; Schema: public; Owner: trading
--

CREATE TABLE public.news_features_symbol (
    symbol text NOT NULL,
    ts_ms bigint NOT NULL,
    risk double precision NOT NULL,
    surprise double precision NOT NULL,
    tags_mask bigint NOT NULL,
    primary_tag integer NOT NULL,
    ref text NOT NULL,
    inserted_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.news_features_symbol OWNER TO trading;

--
-- Name: of_gate_metrics_quarantine; Type: TABLE; Schema: public; Owner: trading
--

CREATE TABLE public.of_gate_metrics_quarantine (
    stream_id text NOT NULL,
    ts_ms bigint NOT NULL,
    ts timestamp with time zone NOT NULL,
    src_stream text,
    src_stream_id text,
    dq_code text,
    err text,
    payload_json jsonb,
    source_stream text,
    symbol text,
    scenario_v4 text,
    schema_version integer,
    ok smallint,
    ok_soft smallint
);


ALTER TABLE public.of_gate_metrics_quarantine OWNER TO trading;

--
-- Name: of_gate_ok_rate_1h; Type: VIEW; Schema: public; Owner: trading
--

CREATE VIEW public.of_gate_ok_rate_1h AS
 SELECT _materialized_hypertable_2817.bucket,
    _materialized_hypertable_2817.symbol,
    _materialized_hypertable_2817.scenario_v4,
    _materialized_hypertable_2817.eligible,
    _materialized_hypertable_2817.ok_hard,
    _materialized_hypertable_2817.ok_soft
   FROM _timescaledb_internal._materialized_hypertable_2817;


ALTER VIEW public.of_gate_ok_rate_1h OWNER TO trading;

--
-- Name: of_gate_ok_rate_5m; Type: VIEW; Schema: public; Owner: trading
--

CREATE VIEW public.of_gate_ok_rate_5m AS
 SELECT _materialized_hypertable_2816.bucket,
    _materialized_hypertable_2816.symbol,
    _materialized_hypertable_2816.scenario_v4,
    _materialized_hypertable_2816.eligible,
    _materialized_hypertable_2816.ok_hard,
    _materialized_hypertable_2816.ok_soft
   FROM _timescaledb_internal._materialized_hypertable_2816;


ALTER VIEW public.of_gate_ok_rate_5m OWNER TO trading;

--
-- Name: position_events; Type: TABLE; Schema: public; Owner: trading
--

CREATE TABLE public.position_events (
    stream_id text NOT NULL,
    ts_ms bigint NOT NULL,
    ts timestamp with time zone NOT NULL,
    position_id text,
    sid text,
    symbol text,
    event_type text NOT NULL,
    meta_json jsonb,
    payload_json jsonb NOT NULL,
    ingested_at timestamp with time zone DEFAULT now() NOT NULL
)
WITH (autovacuum_vacuum_scale_factor='0.01', autovacuum_analyze_scale_factor='0.005', autovacuum_vacuum_cost_delay='2', autovacuum_vacuum_threshold='100');


ALTER TABLE public.position_events OWNER TO trading;

--
-- Name: regime_quantiles; Type: TABLE; Schema: public; Owner: trading
--

CREATE TABLE public.regime_quantiles (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    symbol text NOT NULL,
    timeframe text NOT NULL,
    adx_p40 double precision,
    adx_p60 double precision,
    adx_p75 double precision,
    atrp_p25 double precision,
    atrp_p50 double precision,
    atrp_p75 double precision,
    sample_count integer,
    created_at timestamp with time zone DEFAULT now(),
    computed_at timestamp with time zone DEFAULT now(),
    atrp_p90 double precision DEFAULT 0.0 NOT NULL,
    window_days integer DEFAULT 14 NOT NULL,
    src_time_min timestamp with time zone,
    src_time_max timestamp with time zone
);


ALTER TABLE public.regime_quantiles OWNER TO trading;

--
-- Name: regime_snapshot; Type: TABLE; Schema: public; Owner: trading
--

CREATE TABLE public.regime_snapshot (
    id bigint NOT NULL,
    symbol text NOT NULL,
    timeframe text NOT NULL,
    ts timestamp with time zone NOT NULL,
    adx double precision,
    "atrPct" double precision,
    regime text,
    trend_score double precision DEFAULT 0.0,
    range_score double precision DEFAULT 0.0,
    atr_value double precision,
    atr_quantile double precision,
    volatility_state text,
    is_trending boolean,
    created_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.regime_snapshot OWNER TO trading;

--
-- Name: regime_snapshot_id_seq; Type: SEQUENCE; Schema: public; Owner: trading
--

CREATE SEQUENCE public.regime_snapshot_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.regime_snapshot_id_seq OWNER TO trading;

--
-- Name: regime_snapshot_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: trading
--

ALTER SEQUENCE public.regime_snapshot_id_seq OWNED BY public.regime_snapshot.id;


--
-- Name: signal_confidence_scores; Type: TABLE; Schema: public; Owner: trading
--

CREATE TABLE public.signal_confidence_scores (
    stream_id text NOT NULL,
    ts_ms bigint NOT NULL,
    ts timestamp with time zone NOT NULL,
    sid text NOT NULL,
    symbol text NOT NULL,
    schema_version integer NOT NULL,
    producer text NOT NULL,
    confidence_raw double precision NOT NULL,
    confidence_final double precision,
    evidence_json jsonb NOT NULL,
    context_json jsonb
);


ALTER TABLE public.signal_confidence_scores OWNER TO trading;

--
-- Name: signal_exec_summary; Type: TABLE; Schema: public; Owner: trading
--

CREATE TABLE public.signal_exec_summary (
    signal_id bigint NOT NULL,
    symbol text NOT NULL,
    family text NOT NULL,
    opened_at timestamp with time zone NOT NULL,
    closed_at timestamp with time zone NOT NULL,
    result_r double precision NOT NULL,
    mfe_r double precision,
    mae_r double precision,
    ttd_sec double precision,
    extra_json jsonb
);


ALTER TABLE public.signal_exec_summary OWNER TO trading;

--
-- Name: signal_execution_plan; Type: TABLE; Schema: public; Owner: trading
--

CREATE TABLE public.signal_execution_plan (
    signal_id uuid NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    entry_zone_low double precision NOT NULL,
    entry_zone_high double precision NOT NULL,
    stop_price double precision NOT NULL,
    tp_levels double precision[] NOT NULL,
    partials double precision[] NOT NULL,
    pos_risk_r double precision NOT NULL,
    risk_usd double precision NOT NULL,
    position_size double precision NOT NULL,
    expiry_bars integer NOT NULL
);


ALTER TABLE public.signal_execution_plan OWNER TO trading;

--
-- Name: TABLE signal_execution_plan; Type: COMMENT; Schema: public; Owner: trading
--

COMMENT ON TABLE public.signal_execution_plan IS 'Detailed execution plans for signals';


--
-- Name: signal_ttd_config; Type: TABLE; Schema: public; Owner: trading
--

CREATE TABLE public.signal_ttd_config (
    symbol text NOT NULL,
    setup_type text NOT NULL,
    ttd_q50_bars integer NOT NULL,
    ttd_q75_bars integer NOT NULL,
    ttd_q90_bars integer NOT NULL,
    expiry_bars integer NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.signal_ttd_config OWNER TO trading;

--
-- Name: TABLE signal_ttd_config; Type: COMMENT; Schema: public; Owner: trading
--

COMMENT ON TABLE public.signal_ttd_config IS 'TTD quantiles and expiry settings by symbol/setup';


--
-- Name: signal_execution_summary; Type: VIEW; Schema: public; Owner: trading
--

CREATE VIEW public.signal_execution_summary AS
 SELECT s.signal_id,
    s.symbol,
    s.side,
    s.setup_type,
    s.ts_signal,
    s.price_at_signal,
    s.final_score,
    ep.entry_zone_low,
    ep.entry_zone_high,
    ep.stop_price,
    ep.tp_levels,
    ep.position_size,
    ep.expiry_bars,
    sp.ts_entry,
    sp.ts_exit,
    sp.entry_price,
    sp.exit_price,
    sp.realized_r,
    sp.mfe_r,
    sp.mae_r,
    sp.ttd_bars,
    sp.ttd_seconds,
    sp.outcome,
    tc.expiry_bars AS config_expiry_bars
   FROM (((public.signals s
     LEFT JOIN public.signal_execution_plan ep ON ((s.signal_id = ep.signal_id)))
     LEFT JOIN public.signal_performance sp ON ((s.signal_id = sp.signal_id)))
     LEFT JOIN public.signal_ttd_config tc ON (((s.symbol = tc.symbol) AND (s.setup_type = tc.setup_type))));


ALTER VIEW public.signal_execution_summary OWNER TO trading;

--
-- Name: VIEW signal_execution_summary; Type: COMMENT; Schema: public; Owner: trading
--

COMMENT ON VIEW public.signal_execution_summary IS 'Unified view of signals, plans, and performance';


--
-- Name: signal_experiment; Type: TABLE; Schema: public; Owner: trading
--

CREATE TABLE public.signal_experiment (
    experiment_id text NOT NULL,
    name text NOT NULL,
    filter_name text NOT NULL,
    signal_family text NOT NULL,
    direction integer DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    start_at timestamp with time zone NOT NULL,
    end_at timestamp with time zone,
    status text DEFAULT 'draft'::text NOT NULL,
    target_metric text NOT NULL,
    config jsonb
);


ALTER TABLE public.signal_experiment OWNER TO trading;

--
-- Name: TABLE signal_experiment; Type: COMMENT; Schema: public; Owner: trading
--

COMMENT ON TABLE public.signal_experiment IS 'Definitions of A/B experiments for signal filters and features';


--
-- Name: signal_experiment_snapshot; Type: TABLE; Schema: public; Owner: trading
--

CREATE TABLE public.signal_experiment_snapshot (
    experiment_id text NOT NULL,
    as_of timestamp with time zone NOT NULL,
    variant text NOT NULL,
    signals_total integer NOT NULL,
    traded_total integer NOT NULL,
    winners_total integer NOT NULL,
    losers_total integer NOT NULL,
    expectancy_r double precision,
    sharpe_r double precision,
    max_dd_r double precision,
    cl_ratio double precision,
    winrate double precision,
    "precision" double precision,
    recall double precision,
    f1 double precision,
    extra jsonb
);


ALTER TABLE public.signal_experiment_snapshot OWNER TO trading;

--
-- Name: TABLE signal_experiment_snapshot; Type: COMMENT; Schema: public; Owner: trading
--

COMMENT ON TABLE public.signal_experiment_snapshot IS 'Pre-calculated metrics snapshots for experiment evaluation';


--
-- Name: signal_family_baseline; Type: TABLE; Schema: public; Owner: trading
--

CREATE TABLE public.signal_family_baseline (
    symbol text NOT NULL,
    family text NOT NULL,
    metric text NOT NULL,
    window_size integer NOT NULL,
    horizon_days integer NOT NULL,
    p05 double precision,
    p10 double precision,
    p25 double precision,
    p50 double precision,
    p75 double precision,
    p90 double precision,
    p95 double precision,
    sample_size integer NOT NULL,
    computed_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.signal_family_baseline OWNER TO trading;

--
-- Name: signal_family_regime_state; Type: TABLE; Schema: public; Owner: trading
--

CREATE TABLE public.signal_family_regime_state (
    ts_state timestamp with time zone NOT NULL,
    family text NOT NULL,
    venue text NOT NULL,
    symbol text NOT NULL,
    timeframe text NOT NULL,
    status text NOT NULL,
    wr_window double precision,
    exp_r_window double precision,
    dd_r_window double precision,
    trades_window integer,
    reason text,
    disable_until timestamp with time zone,
    threshold_mult double precision,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.signal_family_regime_state OWNER TO trading;

--
-- Name: signal_local_calibration; Type: TABLE; Schema: public; Owner: trading
--

CREATE TABLE public.signal_local_calibration (
    symbol text NOT NULL,
    session text NOT NULL,
    regime text NOT NULL,
    metric text NOT NULL,
    q90 double precision,
    q95 double precision,
    q98 double precision,
    chosen_threshold double precision,
    count_samples bigint NOT NULL,
    cdf_points jsonb NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.signal_local_calibration OWNER TO trading;

--
-- Name: TABLE signal_local_calibration; Type: COMMENT; Schema: public; Owner: trading
--

COMMENT ON TABLE public.signal_local_calibration IS 'Local calibration thresholds and CDFs for signal metrics by symbol/session/regime clusters';


--
-- Name: signal_quality_offline; Type: TABLE; Schema: public; Owner: trading
--

CREATE TABLE public.signal_quality_offline (
    id bigint NOT NULL,
    symbol text NOT NULL,
    signal_type text NOT NULL,
    side text NOT NULL,
    session text NOT NULL,
    regime text NOT NULL,
    feature_bucket text NOT NULL,
    horizon text DEFAULT 'R_main'::text NOT NULL,
    n_signals integer NOT NULL,
    win_rate double precision NOT NULL,
    expectancy_r double precision NOT NULL,
    var_r double precision NOT NULL,
    cvar_r double precision NOT NULL,
    quality_score double precision NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.signal_quality_offline OWNER TO trading;

--
-- Name: TABLE signal_quality_offline; Type: COMMENT; Schema: public; Owner: trading
--

COMMENT ON TABLE public.signal_quality_offline IS 'Offline signal quality assessment by feature clusters';


--
-- Name: COLUMN signal_quality_offline.feature_bucket; Type: COMMENT; Schema: public; Owner: trading
--

COMMENT ON COLUMN public.signal_quality_offline.feature_bucket IS 'Feature cluster key (dz:bin|obi:bin|wp:bin|atr:bin)';


--
-- Name: signal_quality_offline_id_seq; Type: SEQUENCE; Schema: public; Owner: trading
--

CREATE SEQUENCE public.signal_quality_offline_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.signal_quality_offline_id_seq OWNER TO trading;

--
-- Name: signal_quality_offline_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: trading
--

ALTER SEQUENCE public.signal_quality_offline_id_seq OWNED BY public.signal_quality_offline.id;


--
-- Name: signal_quality_online; Type: TABLE; Schema: public; Owner: trading
--

CREATE TABLE public.signal_quality_online (
    id bigint NOT NULL,
    symbol text NOT NULL,
    signal_type text NOT NULL,
    side text NOT NULL,
    horizon text DEFAULT 'R_main'::text NOT NULL,
    n_recent integer NOT NULL,
    win_rate_recent double precision NOT NULL,
    expectancy_r_recent double precision NOT NULL,
    var_r_recent double precision NOT NULL,
    cvar_r_recent double precision NOT NULL,
    quality_score_online double precision NOT NULL,
    status text DEFAULT 'ok'::text NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.signal_quality_online OWNER TO trading;

--
-- Name: TABLE signal_quality_online; Type: COMMENT; Schema: public; Owner: trading
--

COMMENT ON TABLE public.signal_quality_online IS 'Online rolling signal quality assessment by type';


--
-- Name: COLUMN signal_quality_online.status; Type: COMMENT; Schema: public; Owner: trading
--

COMMENT ON COLUMN public.signal_quality_online.status IS 'Quality status: ok/degraded/disabled';


--
-- Name: signal_quality_online_id_seq; Type: SEQUENCE; Schema: public; Owner: trading
--

CREATE SEQUENCE public.signal_quality_online_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.signal_quality_online_id_seq OWNER TO trading;

--
-- Name: signal_quality_online_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: trading
--

ALTER SEQUENCE public.signal_quality_online_id_seq OWNED BY public.signal_quality_online.id;


--
-- Name: ticks; Type: TABLE; Schema: public; Owner: trading
--

CREATE TABLE public.ticks (
    id bigint NOT NULL,
    source text,
    symbol text NOT NULL,
    ts_ms bigint NOT NULL,
    ts timestamp with time zone,
    price double precision,
    bid double precision,
    ask double precision,
    volume double precision,
    side text,
    meta jsonb,
    created_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.ticks OWNER TO trading;

--
-- Name: ticks_id_seq; Type: SEQUENCE; Schema: public; Owner: trading
--

CREATE SEQUENCE public.ticks_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.ticks_id_seq OWNER TO trading;

--
-- Name: ticks_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: trading
--

ALTER SEQUENCE public.ticks_id_seq OWNED BY public.ticks.id;


--
-- Name: trade_kpi_liqmap_v1; Type: TABLE; Schema: public; Owner: trading
--

CREATE TABLE public.trade_kpi_liqmap_v1 (
    stream_id text NOT NULL,
    ts_ms bigint NOT NULL,
    ts timestamp with time zone NOT NULL,
    trade_id text NOT NULL,
    symbol text NOT NULL,
    side text NOT NULL,
    regime text NOT NULL,
    sl_hit_near_liqmap_peak smallint,
    tp1_anchored smallint,
    tp1_anchored_and_hit smallint,
    sl_liqmap_peak_dist_bps double precision,
    sl_liqmap_peak_usd double precision,
    liqmap_kpi jsonb NOT NULL,
    payload_json jsonb NOT NULL
);


ALTER TABLE public.trade_kpi_liqmap_v1 OWNER TO trading;

--
-- Name: trades_closed_id_seq; Type: SEQUENCE; Schema: public; Owner: trading
--

CREATE SEQUENCE public.trades_closed_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.trades_closed_id_seq OWNER TO trading;

--
-- Name: trades_closed_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: trading
--

ALTER SEQUENCE public.trades_closed_id_seq OWNED BY public.trades_closed.id;


--
-- Name: _hyper_2814_40_chunk schema_version; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_2814_40_chunk ALTER COLUMN schema_version SET DEFAULT 1;


--
-- Name: _hyper_2814_40_chunk scenario_v4; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_2814_40_chunk ALTER COLUMN scenario_v4 SET DEFAULT 'unknown'::text;


--
-- Name: _hyper_2814_40_chunk ok; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_2814_40_chunk ALTER COLUMN ok SET DEFAULT 0;


--
-- Name: _hyper_2814_40_chunk ok_soft; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_2814_40_chunk ALTER COLUMN ok_soft SET DEFAULT 0;


--
-- Name: _hyper_2814_40_chunk reason_code; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_2814_40_chunk ALTER COLUMN reason_code SET DEFAULT 'na'::text;


--
-- Name: _hyper_2814_41_chunk schema_version; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_2814_41_chunk ALTER COLUMN schema_version SET DEFAULT 1;


--
-- Name: _hyper_2814_41_chunk scenario_v4; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_2814_41_chunk ALTER COLUMN scenario_v4 SET DEFAULT 'unknown'::text;


--
-- Name: _hyper_2814_41_chunk ok; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_2814_41_chunk ALTER COLUMN ok SET DEFAULT 0;


--
-- Name: _hyper_2814_41_chunk ok_soft; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_2814_41_chunk ALTER COLUMN ok_soft SET DEFAULT 0;


--
-- Name: _hyper_2814_41_chunk reason_code; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_2814_41_chunk ALTER COLUMN reason_code SET DEFAULT 'na'::text;


--
-- Name: _hyper_2814_57_chunk schema_version; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_2814_57_chunk ALTER COLUMN schema_version SET DEFAULT 1;


--
-- Name: _hyper_2814_57_chunk scenario_v4; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_2814_57_chunk ALTER COLUMN scenario_v4 SET DEFAULT 'unknown'::text;


--
-- Name: _hyper_2814_57_chunk ok; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_2814_57_chunk ALTER COLUMN ok SET DEFAULT 0;


--
-- Name: _hyper_2814_57_chunk ok_soft; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_2814_57_chunk ALTER COLUMN ok_soft SET DEFAULT 0;


--
-- Name: _hyper_2814_57_chunk reason_code; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_2814_57_chunk ALTER COLUMN reason_code SET DEFAULT 'na'::text;


--
-- Name: _hyper_572_10_chunk id; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_10_chunk ALTER COLUMN id SET DEFAULT nextval('public.candles_archive_id_seq'::regclass);


--
-- Name: _hyper_572_10_chunk archived_at; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_10_chunk ALTER COLUMN archived_at SET DEFAULT now();


--
-- Name: _hyper_572_11_chunk id; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_11_chunk ALTER COLUMN id SET DEFAULT nextval('public.candles_archive_id_seq'::regclass);


--
-- Name: _hyper_572_11_chunk archived_at; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_11_chunk ALTER COLUMN archived_at SET DEFAULT now();


--
-- Name: _hyper_572_12_chunk id; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_12_chunk ALTER COLUMN id SET DEFAULT nextval('public.candles_archive_id_seq'::regclass);


--
-- Name: _hyper_572_12_chunk archived_at; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_12_chunk ALTER COLUMN archived_at SET DEFAULT now();


--
-- Name: _hyper_572_13_chunk id; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_13_chunk ALTER COLUMN id SET DEFAULT nextval('public.candles_archive_id_seq'::regclass);


--
-- Name: _hyper_572_13_chunk archived_at; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_13_chunk ALTER COLUMN archived_at SET DEFAULT now();


--
-- Name: _hyper_572_15_chunk id; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_15_chunk ALTER COLUMN id SET DEFAULT nextval('public.candles_archive_id_seq'::regclass);


--
-- Name: _hyper_572_15_chunk archived_at; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_15_chunk ALTER COLUMN archived_at SET DEFAULT now();


--
-- Name: _hyper_572_17_chunk id; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_17_chunk ALTER COLUMN id SET DEFAULT nextval('public.candles_archive_id_seq'::regclass);


--
-- Name: _hyper_572_17_chunk archived_at; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_17_chunk ALTER COLUMN archived_at SET DEFAULT now();


--
-- Name: _hyper_572_20_chunk id; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_20_chunk ALTER COLUMN id SET DEFAULT nextval('public.candles_archive_id_seq'::regclass);


--
-- Name: _hyper_572_20_chunk archived_at; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_20_chunk ALTER COLUMN archived_at SET DEFAULT now();


--
-- Name: _hyper_572_22_chunk id; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_22_chunk ALTER COLUMN id SET DEFAULT nextval('public.candles_archive_id_seq'::regclass);


--
-- Name: _hyper_572_22_chunk archived_at; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_22_chunk ALTER COLUMN archived_at SET DEFAULT now();


--
-- Name: _hyper_572_24_chunk id; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_24_chunk ALTER COLUMN id SET DEFAULT nextval('public.candles_archive_id_seq'::regclass);


--
-- Name: _hyper_572_24_chunk archived_at; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_24_chunk ALTER COLUMN archived_at SET DEFAULT now();


--
-- Name: _hyper_572_26_chunk id; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_26_chunk ALTER COLUMN id SET DEFAULT nextval('public.candles_archive_id_seq'::regclass);


--
-- Name: _hyper_572_26_chunk archived_at; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_26_chunk ALTER COLUMN archived_at SET DEFAULT now();


--
-- Name: _hyper_572_28_chunk id; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_28_chunk ALTER COLUMN id SET DEFAULT nextval('public.candles_archive_id_seq'::regclass);


--
-- Name: _hyper_572_28_chunk archived_at; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_28_chunk ALTER COLUMN archived_at SET DEFAULT now();


--
-- Name: _hyper_572_31_chunk id; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_31_chunk ALTER COLUMN id SET DEFAULT nextval('public.candles_archive_id_seq'::regclass);


--
-- Name: _hyper_572_31_chunk archived_at; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_31_chunk ALTER COLUMN archived_at SET DEFAULT now();


--
-- Name: _hyper_572_32_chunk id; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_32_chunk ALTER COLUMN id SET DEFAULT nextval('public.candles_archive_id_seq'::regclass);


--
-- Name: _hyper_572_32_chunk archived_at; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_32_chunk ALTER COLUMN archived_at SET DEFAULT now();


--
-- Name: _hyper_572_36_chunk id; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_36_chunk ALTER COLUMN id SET DEFAULT nextval('public.candles_archive_id_seq'::regclass);


--
-- Name: _hyper_572_36_chunk archived_at; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_36_chunk ALTER COLUMN archived_at SET DEFAULT now();


--
-- Name: _hyper_572_38_chunk id; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_38_chunk ALTER COLUMN id SET DEFAULT nextval('public.candles_archive_id_seq'::regclass);


--
-- Name: _hyper_572_38_chunk archived_at; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_38_chunk ALTER COLUMN archived_at SET DEFAULT now();


--
-- Name: _hyper_572_39_chunk id; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_39_chunk ALTER COLUMN id SET DEFAULT nextval('public.candles_archive_id_seq'::regclass);


--
-- Name: _hyper_572_39_chunk archived_at; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_39_chunk ALTER COLUMN archived_at SET DEFAULT now();


--
-- Name: _hyper_572_48_chunk id; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_48_chunk ALTER COLUMN id SET DEFAULT nextval('public.candles_archive_id_seq'::regclass);


--
-- Name: _hyper_572_48_chunk archived_at; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_48_chunk ALTER COLUMN archived_at SET DEFAULT now();


--
-- Name: _hyper_572_4_chunk id; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_4_chunk ALTER COLUMN id SET DEFAULT nextval('public.candles_archive_id_seq'::regclass);


--
-- Name: _hyper_572_4_chunk archived_at; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_4_chunk ALTER COLUMN archived_at SET DEFAULT now();


--
-- Name: _hyper_572_51_chunk id; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_51_chunk ALTER COLUMN id SET DEFAULT nextval('public.candles_archive_id_seq'::regclass);


--
-- Name: _hyper_572_51_chunk archived_at; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_51_chunk ALTER COLUMN archived_at SET DEFAULT now();


--
-- Name: _hyper_572_53_chunk id; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_53_chunk ALTER COLUMN id SET DEFAULT nextval('public.candles_archive_id_seq'::regclass);


--
-- Name: _hyper_572_53_chunk archived_at; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_53_chunk ALTER COLUMN archived_at SET DEFAULT now();


--
-- Name: _hyper_572_55_chunk id; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_55_chunk ALTER COLUMN id SET DEFAULT nextval('public.candles_archive_id_seq'::regclass);


--
-- Name: _hyper_572_55_chunk archived_at; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_55_chunk ALTER COLUMN archived_at SET DEFAULT now();


--
-- Name: _hyper_572_58_chunk id; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_58_chunk ALTER COLUMN id SET DEFAULT nextval('public.candles_archive_id_seq'::regclass);


--
-- Name: _hyper_572_58_chunk archived_at; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_58_chunk ALTER COLUMN archived_at SET DEFAULT now();


--
-- Name: _hyper_572_5_chunk id; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_5_chunk ALTER COLUMN id SET DEFAULT nextval('public.candles_archive_id_seq'::regclass);


--
-- Name: _hyper_572_5_chunk archived_at; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_5_chunk ALTER COLUMN archived_at SET DEFAULT now();


--
-- Name: _hyper_572_61_chunk id; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_61_chunk ALTER COLUMN id SET DEFAULT nextval('public.candles_archive_id_seq'::regclass);


--
-- Name: _hyper_572_61_chunk archived_at; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_61_chunk ALTER COLUMN archived_at SET DEFAULT now();


--
-- Name: _hyper_572_6_chunk id; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_6_chunk ALTER COLUMN id SET DEFAULT nextval('public.candles_archive_id_seq'::regclass);


--
-- Name: _hyper_572_6_chunk archived_at; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_6_chunk ALTER COLUMN archived_at SET DEFAULT now();


--
-- Name: _hyper_572_7_chunk id; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_7_chunk ALTER COLUMN id SET DEFAULT nextval('public.candles_archive_id_seq'::regclass);


--
-- Name: _hyper_572_7_chunk archived_at; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_7_chunk ALTER COLUMN archived_at SET DEFAULT now();


--
-- Name: _hyper_572_8_chunk id; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_8_chunk ALTER COLUMN id SET DEFAULT nextval('public.candles_archive_id_seq'::regclass);


--
-- Name: _hyper_572_8_chunk archived_at; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_8_chunk ALTER COLUMN archived_at SET DEFAULT now();


--
-- Name: _hyper_7_19_chunk created_at; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_7_19_chunk ALTER COLUMN created_at SET DEFAULT now();


--
-- Name: _hyper_7_19_chunk updated_at; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_7_19_chunk ALTER COLUMN updated_at SET DEFAULT now();


--
-- Name: _hyper_7_19_chunk is_virtual; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_7_19_chunk ALTER COLUMN is_virtual SET DEFAULT false;


--
-- Name: _hyper_7_19_chunk meta_enforce_cov_bucket; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_7_19_chunk ALTER COLUMN meta_enforce_cov_bucket SET DEFAULT ''::text;


--
-- Name: _hyper_7_19_chunk meta_enforce_applied; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_7_19_chunk ALTER COLUMN meta_enforce_applied SET DEFAULT '-1'::integer;


--
-- Name: _hyper_7_1_chunk created_at; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_7_1_chunk ALTER COLUMN created_at SET DEFAULT now();


--
-- Name: _hyper_7_1_chunk updated_at; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_7_1_chunk ALTER COLUMN updated_at SET DEFAULT now();


--
-- Name: _hyper_7_1_chunk is_virtual; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_7_1_chunk ALTER COLUMN is_virtual SET DEFAULT false;


--
-- Name: _hyper_7_1_chunk meta_enforce_cov_bucket; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_7_1_chunk ALTER COLUMN meta_enforce_cov_bucket SET DEFAULT ''::text;


--
-- Name: _hyper_7_1_chunk meta_enforce_applied; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_7_1_chunk ALTER COLUMN meta_enforce_applied SET DEFAULT '-1'::integer;


--
-- Name: _hyper_7_2_chunk created_at; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_7_2_chunk ALTER COLUMN created_at SET DEFAULT now();


--
-- Name: _hyper_7_2_chunk updated_at; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_7_2_chunk ALTER COLUMN updated_at SET DEFAULT now();


--
-- Name: _hyper_7_2_chunk is_virtual; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_7_2_chunk ALTER COLUMN is_virtual SET DEFAULT false;


--
-- Name: _hyper_7_2_chunk meta_enforce_cov_bucket; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_7_2_chunk ALTER COLUMN meta_enforce_cov_bucket SET DEFAULT ''::text;


--
-- Name: _hyper_7_2_chunk meta_enforce_applied; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_7_2_chunk ALTER COLUMN meta_enforce_applied SET DEFAULT '-1'::integer;


--
-- Name: _hyper_7_35_chunk created_at; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_7_35_chunk ALTER COLUMN created_at SET DEFAULT now();


--
-- Name: _hyper_7_35_chunk updated_at; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_7_35_chunk ALTER COLUMN updated_at SET DEFAULT now();


--
-- Name: _hyper_7_35_chunk is_virtual; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_7_35_chunk ALTER COLUMN is_virtual SET DEFAULT false;


--
-- Name: _hyper_7_35_chunk meta_enforce_cov_bucket; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_7_35_chunk ALTER COLUMN meta_enforce_cov_bucket SET DEFAULT ''::text;


--
-- Name: _hyper_7_35_chunk meta_enforce_applied; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_7_35_chunk ALTER COLUMN meta_enforce_applied SET DEFAULT '-1'::integer;


--
-- Name: _hyper_7_3_chunk created_at; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_7_3_chunk ALTER COLUMN created_at SET DEFAULT now();


--
-- Name: _hyper_7_3_chunk updated_at; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_7_3_chunk ALTER COLUMN updated_at SET DEFAULT now();


--
-- Name: _hyper_7_3_chunk is_virtual; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_7_3_chunk ALTER COLUMN is_virtual SET DEFAULT false;


--
-- Name: _hyper_7_3_chunk meta_enforce_cov_bucket; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_7_3_chunk ALTER COLUMN meta_enforce_cov_bucket SET DEFAULT ''::text;


--
-- Name: _hyper_7_3_chunk meta_enforce_applied; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_7_3_chunk ALTER COLUMN meta_enforce_applied SET DEFAULT '-1'::integer;


--
-- Name: _hyper_7_59_chunk created_at; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_7_59_chunk ALTER COLUMN created_at SET DEFAULT now();


--
-- Name: _hyper_7_59_chunk updated_at; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_7_59_chunk ALTER COLUMN updated_at SET DEFAULT now();


--
-- Name: _hyper_7_59_chunk is_virtual; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_7_59_chunk ALTER COLUMN is_virtual SET DEFAULT false;


--
-- Name: _hyper_7_59_chunk meta_enforce_cov_bucket; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_7_59_chunk ALTER COLUMN meta_enforce_cov_bucket SET DEFAULT ''::text;


--
-- Name: _hyper_7_59_chunk meta_enforce_applied; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_7_59_chunk ALTER COLUMN meta_enforce_applied SET DEFAULT '-1'::integer;


--
-- Name: _hyper_7_9_chunk created_at; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_7_9_chunk ALTER COLUMN created_at SET DEFAULT now();


--
-- Name: _hyper_7_9_chunk updated_at; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_7_9_chunk ALTER COLUMN updated_at SET DEFAULT now();


--
-- Name: _hyper_7_9_chunk is_virtual; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_7_9_chunk ALTER COLUMN is_virtual SET DEFAULT false;


--
-- Name: _hyper_7_9_chunk meta_enforce_cov_bucket; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_7_9_chunk ALTER COLUMN meta_enforce_cov_bucket SET DEFAULT ''::text;


--
-- Name: _hyper_7_9_chunk meta_enforce_applied; Type: DEFAULT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_7_9_chunk ALTER COLUMN meta_enforce_applied SET DEFAULT '-1'::integer;


--
-- Name: atr_archive id; Type: DEFAULT; Schema: public; Owner: trading
--

ALTER TABLE ONLY public.atr_archive ALTER COLUMN id SET DEFAULT nextval('public.atr_archive_id_seq'::regclass);


--
-- Name: candles_archive id; Type: DEFAULT; Schema: public; Owner: trading
--

ALTER TABLE ONLY public.candles_archive ALTER COLUMN id SET DEFAULT nextval('public.candles_archive_id_seq'::regclass);


--
-- Name: daily_metrics id; Type: DEFAULT; Schema: public; Owner: trading
--

ALTER TABLE ONLY public.daily_metrics ALTER COLUMN id SET DEFAULT nextval('public.daily_metrics_id_seq'::regclass);


--
-- Name: edge_gate_events id; Type: DEFAULT; Schema: public; Owner: trading
--

ALTER TABLE ONLY public.edge_gate_events ALTER COLUMN id SET DEFAULT nextval('public.edge_gate_events_id_seq'::regclass);


--
-- Name: entry_tag_metrics id; Type: DEFAULT; Schema: public; Owner: trading
--

ALTER TABLE ONLY public.entry_tag_metrics ALTER COLUMN id SET DEFAULT nextval('public.entry_tag_metrics_id_seq'::regclass);


--
-- Name: regime_snapshot id; Type: DEFAULT; Schema: public; Owner: trading
--

ALTER TABLE ONLY public.regime_snapshot ALTER COLUMN id SET DEFAULT nextval('public.regime_snapshot_id_seq'::regclass);


--
-- Name: signal_quality_offline id; Type: DEFAULT; Schema: public; Owner: trading
--

ALTER TABLE ONLY public.signal_quality_offline ALTER COLUMN id SET DEFAULT nextval('public.signal_quality_offline_id_seq'::regclass);


--
-- Name: signal_quality_online id; Type: DEFAULT; Schema: public; Owner: trading
--

ALTER TABLE ONLY public.signal_quality_online ALTER COLUMN id SET DEFAULT nextval('public.signal_quality_online_id_seq'::regclass);


--
-- Name: ticks id; Type: DEFAULT; Schema: public; Owner: trading
--

ALTER TABLE ONLY public.ticks ALTER COLUMN id SET DEFAULT nextval('public.ticks_id_seq'::regclass);


--
-- Name: trades_closed id; Type: DEFAULT; Schema: public; Owner: trading
--

ALTER TABLE ONLY public.trades_closed ALTER COLUMN id SET DEFAULT nextval('public.trades_closed_id_seq'::regclass);


--
-- Name: _hyper_572_10_chunk 10_10_candles_archive_pkey; Type: CONSTRAINT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_10_chunk
    ADD CONSTRAINT "10_10_candles_archive_pkey" PRIMARY KEY (symbol, timeframe, open_time);


--
-- Name: _hyper_572_11_chunk 11_11_candles_archive_pkey; Type: CONSTRAINT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_11_chunk
    ADD CONSTRAINT "11_11_candles_archive_pkey" PRIMARY KEY (symbol, timeframe, open_time);


--
-- Name: _hyper_572_12_chunk 12_12_candles_archive_pkey; Type: CONSTRAINT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_12_chunk
    ADD CONSTRAINT "12_12_candles_archive_pkey" PRIMARY KEY (symbol, timeframe, open_time);


--
-- Name: _hyper_572_13_chunk 13_13_candles_archive_pkey; Type: CONSTRAINT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_13_chunk
    ADD CONSTRAINT "13_13_candles_archive_pkey" PRIMARY KEY (symbol, timeframe, open_time);


--
-- Name: _hyper_572_15_chunk 15_14_candles_archive_pkey; Type: CONSTRAINT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_15_chunk
    ADD CONSTRAINT "15_14_candles_archive_pkey" PRIMARY KEY (symbol, timeframe, open_time);


--
-- Name: _hyper_572_17_chunk 17_15_candles_archive_pkey; Type: CONSTRAINT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_17_chunk
    ADD CONSTRAINT "17_15_candles_archive_pkey" PRIMARY KEY (symbol, timeframe, open_time);


--
-- Name: _hyper_7_19_chunk 19_16_trades_closed_p0_pkey; Type: CONSTRAINT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_7_19_chunk
    ADD CONSTRAINT "19_16_trades_closed_p0_pkey" PRIMARY KEY (order_id, exit_ts);


--
-- Name: _hyper_7_1_chunk 1_1_trades_closed_p0_pkey; Type: CONSTRAINT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_7_1_chunk
    ADD CONSTRAINT "1_1_trades_closed_p0_pkey" PRIMARY KEY (order_id, exit_ts);


--
-- Name: _hyper_572_20_chunk 20_17_candles_archive_pkey; Type: CONSTRAINT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_20_chunk
    ADD CONSTRAINT "20_17_candles_archive_pkey" PRIMARY KEY (symbol, timeframe, open_time);


--
-- Name: _hyper_572_22_chunk 22_18_candles_archive_pkey; Type: CONSTRAINT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_22_chunk
    ADD CONSTRAINT "22_18_candles_archive_pkey" PRIMARY KEY (symbol, timeframe, open_time);


--
-- Name: _hyper_572_24_chunk 24_19_candles_archive_pkey; Type: CONSTRAINT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_24_chunk
    ADD CONSTRAINT "24_19_candles_archive_pkey" PRIMARY KEY (symbol, timeframe, open_time);


--
-- Name: _hyper_572_26_chunk 26_20_candles_archive_pkey; Type: CONSTRAINT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_26_chunk
    ADD CONSTRAINT "26_20_candles_archive_pkey" PRIMARY KEY (symbol, timeframe, open_time);


--
-- Name: _hyper_572_28_chunk 28_21_candles_archive_pkey; Type: CONSTRAINT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_28_chunk
    ADD CONSTRAINT "28_21_candles_archive_pkey" PRIMARY KEY (symbol, timeframe, open_time);


--
-- Name: _hyper_7_2_chunk 2_2_trades_closed_p0_pkey; Type: CONSTRAINT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_7_2_chunk
    ADD CONSTRAINT "2_2_trades_closed_p0_pkey" PRIMARY KEY (order_id, exit_ts);


--
-- Name: _hyper_572_31_chunk 31_22_candles_archive_pkey; Type: CONSTRAINT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_31_chunk
    ADD CONSTRAINT "31_22_candles_archive_pkey" PRIMARY KEY (symbol, timeframe, open_time);


--
-- Name: _hyper_572_32_chunk 32_23_candles_archive_pkey; Type: CONSTRAINT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_32_chunk
    ADD CONSTRAINT "32_23_candles_archive_pkey" PRIMARY KEY (symbol, timeframe, open_time);


--
-- Name: _hyper_7_35_chunk 35_24_trades_closed_p0_pkey; Type: CONSTRAINT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_7_35_chunk
    ADD CONSTRAINT "35_24_trades_closed_p0_pkey" PRIMARY KEY (order_id, exit_ts);


--
-- Name: _hyper_572_36_chunk 36_25_candles_archive_pkey; Type: CONSTRAINT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_36_chunk
    ADD CONSTRAINT "36_25_candles_archive_pkey" PRIMARY KEY (symbol, timeframe, open_time);


--
-- Name: _hyper_572_38_chunk 38_26_candles_archive_pkey; Type: CONSTRAINT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_38_chunk
    ADD CONSTRAINT "38_26_candles_archive_pkey" PRIMARY KEY (symbol, timeframe, open_time);


--
-- Name: _hyper_572_39_chunk 39_27_candles_archive_pkey; Type: CONSTRAINT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_39_chunk
    ADD CONSTRAINT "39_27_candles_archive_pkey" PRIMARY KEY (symbol, timeframe, open_time);


--
-- Name: _hyper_7_3_chunk 3_3_trades_closed_p0_pkey; Type: CONSTRAINT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_7_3_chunk
    ADD CONSTRAINT "3_3_trades_closed_p0_pkey" PRIMARY KEY (order_id, exit_ts);


--
-- Name: _hyper_2814_40_chunk 40_28_of_gate_metrics_pkey; Type: CONSTRAINT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_2814_40_chunk
    ADD CONSTRAINT "40_28_of_gate_metrics_pkey" PRIMARY KEY (stream_id, ts);


--
-- Name: _hyper_2814_41_chunk 41_29_of_gate_metrics_pkey; Type: CONSTRAINT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_2814_41_chunk
    ADD CONSTRAINT "41_29_of_gate_metrics_pkey" PRIMARY KEY (stream_id, ts);


--
-- Name: _hyper_572_48_chunk 48_31_candles_archive_pkey; Type: CONSTRAINT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_48_chunk
    ADD CONSTRAINT "48_31_candles_archive_pkey" PRIMARY KEY (symbol, timeframe, open_time);


--
-- Name: _hyper_572_4_chunk 4_4_candles_archive_pkey; Type: CONSTRAINT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_4_chunk
    ADD CONSTRAINT "4_4_candles_archive_pkey" PRIMARY KEY (symbol, timeframe, open_time);


--
-- Name: _hyper_572_51_chunk 51_32_candles_archive_pkey; Type: CONSTRAINT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_51_chunk
    ADD CONSTRAINT "51_32_candles_archive_pkey" PRIMARY KEY (symbol, timeframe, open_time);


--
-- Name: _hyper_572_53_chunk 53_33_candles_archive_pkey; Type: CONSTRAINT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_53_chunk
    ADD CONSTRAINT "53_33_candles_archive_pkey" PRIMARY KEY (symbol, timeframe, open_time);


--
-- Name: _hyper_572_55_chunk 55_34_candles_archive_pkey; Type: CONSTRAINT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_55_chunk
    ADD CONSTRAINT "55_34_candles_archive_pkey" PRIMARY KEY (symbol, timeframe, open_time);


--
-- Name: _hyper_2814_57_chunk 57_35_of_gate_metrics_pkey; Type: CONSTRAINT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_2814_57_chunk
    ADD CONSTRAINT "57_35_of_gate_metrics_pkey" PRIMARY KEY (stream_id, ts);


--
-- Name: _hyper_572_58_chunk 58_36_candles_archive_pkey; Type: CONSTRAINT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_58_chunk
    ADD CONSTRAINT "58_36_candles_archive_pkey" PRIMARY KEY (symbol, timeframe, open_time);


--
-- Name: _hyper_7_59_chunk 59_37_trades_closed_p0_pkey; Type: CONSTRAINT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_7_59_chunk
    ADD CONSTRAINT "59_37_trades_closed_p0_pkey" PRIMARY KEY (order_id, exit_ts);


--
-- Name: _hyper_572_5_chunk 5_5_candles_archive_pkey; Type: CONSTRAINT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_5_chunk
    ADD CONSTRAINT "5_5_candles_archive_pkey" PRIMARY KEY (symbol, timeframe, open_time);


--
-- Name: _hyper_572_61_chunk 61_38_candles_archive_pkey; Type: CONSTRAINT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_61_chunk
    ADD CONSTRAINT "61_38_candles_archive_pkey" PRIMARY KEY (symbol, timeframe, open_time);


--
-- Name: _hyper_572_6_chunk 6_6_candles_archive_pkey; Type: CONSTRAINT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_6_chunk
    ADD CONSTRAINT "6_6_candles_archive_pkey" PRIMARY KEY (symbol, timeframe, open_time);


--
-- Name: _hyper_572_7_chunk 7_7_candles_archive_pkey; Type: CONSTRAINT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_7_chunk
    ADD CONSTRAINT "7_7_candles_archive_pkey" PRIMARY KEY (symbol, timeframe, open_time);


--
-- Name: _hyper_572_8_chunk 8_8_candles_archive_pkey; Type: CONSTRAINT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_572_8_chunk
    ADD CONSTRAINT "8_8_candles_archive_pkey" PRIMARY KEY (symbol, timeframe, open_time);


--
-- Name: _hyper_7_9_chunk 9_9_trades_closed_p0_pkey; Type: CONSTRAINT; Schema: _timescaledb_internal; Owner: trading
--

ALTER TABLE ONLY _timescaledb_internal._hyper_7_9_chunk
    ADD CONSTRAINT "9_9_trades_closed_p0_pkey" PRIMARY KEY (order_id, exit_ts);


--
-- Name: archive_metadata archive_metadata_pkey; Type: CONSTRAINT; Schema: public; Owner: trading
--

ALTER TABLE ONLY public.archive_metadata
    ADD CONSTRAINT archive_metadata_pkey PRIMARY KEY (stream_name);


--
-- Name: atr_archive atr_archive_pkey; Type: CONSTRAINT; Schema: public; Owner: trading
--

ALTER TABLE ONLY public.atr_archive
    ADD CONSTRAINT atr_archive_pkey PRIMARY KEY (symbol, timeframe, ts);


--
-- Name: calendar_events calendar_events_pkey; Type: CONSTRAINT; Schema: public; Owner: trading
--

ALTER TABLE ONLY public.calendar_events
    ADD CONSTRAINT calendar_events_pkey PRIMARY KEY (uid);


--
-- Name: calendar_features_scope calendar_features_scope_pkey; Type: CONSTRAINT; Schema: public; Owner: trading
--

ALTER TABLE ONLY public.calendar_features_scope
    ADD CONSTRAINT calendar_features_scope_pkey PRIMARY KEY (scope, ts_ms);


--
-- Name: calibration_state calibration_state_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.calibration_state
    ADD CONSTRAINT calibration_state_pkey PRIMARY KEY (symbol, regime, kind);


--
-- Name: candles_archive candles_archive_pkey; Type: CONSTRAINT; Schema: public; Owner: trading
--

ALTER TABLE ONLY public.candles_archive
    ADD CONSTRAINT candles_archive_pkey PRIMARY KEY (symbol, timeframe, open_time);


--
-- Name: daily_metrics daily_metrics_pkey; Type: CONSTRAINT; Schema: public; Owner: trading
--

ALTER TABLE ONLY public.daily_metrics
    ADD CONSTRAINT daily_metrics_pkey PRIMARY KEY (id);


--
-- Name: entry_policy_audit entry_policy_audit_pkey; Type: CONSTRAINT; Schema: public; Owner: trading
--

ALTER TABLE ONLY public.entry_policy_audit
    ADD CONSTRAINT entry_policy_audit_pkey PRIMARY KEY (stream_id);


--
-- Name: entry_tag_metrics entry_tag_metrics_pkey; Type: CONSTRAINT; Schema: public; Owner: trading
--

ALTER TABLE ONLY public.entry_tag_metrics
    ADD CONSTRAINT entry_tag_metrics_pkey PRIMARY KEY (id);


--
-- Name: market_daily_ohlc market_daily_ohlc_pkey; Type: CONSTRAINT; Schema: public; Owner: trading
--

ALTER TABLE ONLY public.market_daily_ohlc
    ADD CONSTRAINT market_daily_ohlc_pkey PRIMARY KEY (symbol, date);


--
-- Name: microbars microbars_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.microbars
    ADD CONSTRAINT microbars_pkey PRIMARY KEY (symbol, ts_ms);


--
-- Name: news_analysis news_analysis_pkey; Type: CONSTRAINT; Schema: public; Owner: trading
--

ALTER TABLE ONLY public.news_analysis
    ADD CONSTRAINT news_analysis_pkey PRIMARY KEY (uid, symbol);


--
-- Name: news_features_symbol news_features_symbol_pkey; Type: CONSTRAINT; Schema: public; Owner: trading
--

ALTER TABLE ONLY public.news_features_symbol
    ADD CONSTRAINT news_features_symbol_pkey PRIMARY KEY (symbol, ts_ms);


--
-- Name: of_gate_metrics of_gate_metrics_pkey; Type: CONSTRAINT; Schema: public; Owner: trading
--

ALTER TABLE ONLY public.of_gate_metrics
    ADD CONSTRAINT of_gate_metrics_pkey PRIMARY KEY (stream_id, ts);


--
-- Name: of_gate_metrics_quarantine of_gate_metrics_quarantine_pkey; Type: CONSTRAINT; Schema: public; Owner: trading
--

ALTER TABLE ONLY public.of_gate_metrics_quarantine
    ADD CONSTRAINT of_gate_metrics_quarantine_pkey PRIMARY KEY (stream_id, ts);


--
-- Name: position_events position_events_pkey; Type: CONSTRAINT; Schema: public; Owner: trading
--

ALTER TABLE ONLY public.position_events
    ADD CONSTRAINT position_events_pkey PRIMARY KEY (stream_id);


--
-- Name: regime_quantiles regime_quantiles_pkey; Type: CONSTRAINT; Schema: public; Owner: trading
--

ALTER TABLE ONLY public.regime_quantiles
    ADD CONSTRAINT regime_quantiles_pkey PRIMARY KEY (id);


--
-- Name: regime_quantiles regime_quantiles_symbol_timeframe_key; Type: CONSTRAINT; Schema: public; Owner: trading
--

ALTER TABLE ONLY public.regime_quantiles
    ADD CONSTRAINT regime_quantiles_symbol_timeframe_key UNIQUE (symbol, timeframe);


--
-- Name: regime_snapshot regime_snapshot_pkey; Type: CONSTRAINT; Schema: public; Owner: trading
--

ALTER TABLE ONLY public.regime_snapshot
    ADD CONSTRAINT regime_snapshot_pkey PRIMARY KEY (id);


--
-- Name: regime_snapshot regime_snapshot_symbol_timeframe_ts_key; Type: CONSTRAINT; Schema: public; Owner: trading
--

ALTER TABLE ONLY public.regime_snapshot
    ADD CONSTRAINT regime_snapshot_symbol_timeframe_ts_key UNIQUE (symbol, timeframe, ts);


--
-- Name: signal_confidence_scores signal_confidence_scores_pkey; Type: CONSTRAINT; Schema: public; Owner: trading
--

ALTER TABLE ONLY public.signal_confidence_scores
    ADD CONSTRAINT signal_confidence_scores_pkey PRIMARY KEY (stream_id, ts);


--
-- Name: signal_exec_summary signal_exec_summary_pkey; Type: CONSTRAINT; Schema: public; Owner: trading
--

ALTER TABLE ONLY public.signal_exec_summary
    ADD CONSTRAINT signal_exec_summary_pkey PRIMARY KEY (signal_id);


--
-- Name: signal_execution_plan signal_execution_plan_pkey; Type: CONSTRAINT; Schema: public; Owner: trading
--

ALTER TABLE ONLY public.signal_execution_plan
    ADD CONSTRAINT signal_execution_plan_pkey PRIMARY KEY (signal_id);


--
-- Name: signal_experiment signal_experiment_pkey; Type: CONSTRAINT; Schema: public; Owner: trading
--

ALTER TABLE ONLY public.signal_experiment
    ADD CONSTRAINT signal_experiment_pkey PRIMARY KEY (experiment_id);


--
-- Name: signal_experiment_snapshot signal_experiment_snapshot_pkey; Type: CONSTRAINT; Schema: public; Owner: trading
--

ALTER TABLE ONLY public.signal_experiment_snapshot
    ADD CONSTRAINT signal_experiment_snapshot_pkey PRIMARY KEY (experiment_id, as_of, variant);


--
-- Name: signal_family_baseline signal_family_baseline_pkey; Type: CONSTRAINT; Schema: public; Owner: trading
--

ALTER TABLE ONLY public.signal_family_baseline
    ADD CONSTRAINT signal_family_baseline_pkey PRIMARY KEY (symbol, family, metric, window_size, horizon_days);


--
-- Name: signal_local_calibration signal_local_calibration_pkey; Type: CONSTRAINT; Schema: public; Owner: trading
--

ALTER TABLE ONLY public.signal_local_calibration
    ADD CONSTRAINT signal_local_calibration_pkey PRIMARY KEY (symbol, session, regime, metric);


--
-- Name: signal_performance signal_performance_pkey; Type: CONSTRAINT; Schema: public; Owner: trading
--

ALTER TABLE ONLY public.signal_performance
    ADD CONSTRAINT signal_performance_pkey PRIMARY KEY (signal_id);


--
-- Name: signal_quality_offline signal_quality_offline_pkey; Type: CONSTRAINT; Schema: public; Owner: trading
--

ALTER TABLE ONLY public.signal_quality_offline
    ADD CONSTRAINT signal_quality_offline_pkey PRIMARY KEY (id);


--
-- Name: signal_quality_offline signal_quality_offline_symbol_signal_type_side_session_regi_key; Type: CONSTRAINT; Schema: public; Owner: trading
--

ALTER TABLE ONLY public.signal_quality_offline
    ADD CONSTRAINT signal_quality_offline_symbol_signal_type_side_session_regi_key UNIQUE (symbol, signal_type, side, session, regime, feature_bucket, horizon);


--
-- Name: signal_quality_online signal_quality_online_pkey; Type: CONSTRAINT; Schema: public; Owner: trading
--

ALTER TABLE ONLY public.signal_quality_online
    ADD CONSTRAINT signal_quality_online_pkey PRIMARY KEY (id);


--
-- Name: signal_quality_online signal_quality_online_symbol_signal_type_side_horizon_key; Type: CONSTRAINT; Schema: public; Owner: trading
--

ALTER TABLE ONLY public.signal_quality_online
    ADD CONSTRAINT signal_quality_online_symbol_signal_type_side_horizon_key UNIQUE (symbol, signal_type, side, horizon);


--
-- Name: signal_ttd_config signal_ttd_config_pkey; Type: CONSTRAINT; Schema: public; Owner: trading
--

ALTER TABLE ONLY public.signal_ttd_config
    ADD CONSTRAINT signal_ttd_config_pkey PRIMARY KEY (symbol, setup_type);


--
-- Name: signals signals_pkey; Type: CONSTRAINT; Schema: public; Owner: trading
--

ALTER TABLE ONLY public.signals
    ADD CONSTRAINT signals_pkey PRIMARY KEY (signal_id);


--
-- Name: ticks ticks_pkey; Type: CONSTRAINT; Schema: public; Owner: trading
--

ALTER TABLE ONLY public.ticks
    ADD CONSTRAINT ticks_pkey PRIMARY KEY (id);


--
-- Name: trade_kpi_liqmap_v1 trade_kpi_liqmap_v1_pkey; Type: CONSTRAINT; Schema: public; Owner: trading
--

ALTER TABLE ONLY public.trade_kpi_liqmap_v1
    ADD CONSTRAINT trade_kpi_liqmap_v1_pkey PRIMARY KEY (stream_id, ts);


--
-- Name: trades_closed trades_closed_order_id_key; Type: CONSTRAINT; Schema: public; Owner: trading
--

ALTER TABLE ONLY public.trades_closed
    ADD CONSTRAINT trades_closed_order_id_key UNIQUE (order_id);


--
-- Name: trades_closed_p0 trades_closed_p0_pkey; Type: CONSTRAINT; Schema: public; Owner: trading
--

ALTER TABLE ONLY public.trades_closed_p0
    ADD CONSTRAINT trades_closed_p0_pkey PRIMARY KEY (order_id, exit_ts);


--
-- Name: trades_closed trades_closed_pkey; Type: CONSTRAINT; Schema: public; Owner: trading
--

ALTER TABLE ONLY public.trades_closed
    ADD CONSTRAINT trades_closed_pkey PRIMARY KEY (id);


--
-- Name: _hyper_2814_40_chunk_of_gate_metrics_reason_ts_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_2814_40_chunk_of_gate_metrics_reason_ts_idx ON _timescaledb_internal._hyper_2814_40_chunk USING btree (reason_code, ts DESC);


--
-- Name: _hyper_2814_40_chunk_of_gate_metrics_scenario_ts_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_2814_40_chunk_of_gate_metrics_scenario_ts_idx ON _timescaledb_internal._hyper_2814_40_chunk USING btree (scenario_v4, ts DESC);


--
-- Name: _hyper_2814_40_chunk_of_gate_metrics_symbol_ts_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_2814_40_chunk_of_gate_metrics_symbol_ts_idx ON _timescaledb_internal._hyper_2814_40_chunk USING btree (symbol, ts DESC);


--
-- Name: _hyper_2814_40_chunk_of_gate_metrics_ts_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_2814_40_chunk_of_gate_metrics_ts_idx ON _timescaledb_internal._hyper_2814_40_chunk USING btree (ts DESC);


--
-- Name: _hyper_2814_41_chunk_of_gate_metrics_reason_ts_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_2814_41_chunk_of_gate_metrics_reason_ts_idx ON _timescaledb_internal._hyper_2814_41_chunk USING btree (reason_code, ts DESC);


--
-- Name: _hyper_2814_41_chunk_of_gate_metrics_scenario_ts_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_2814_41_chunk_of_gate_metrics_scenario_ts_idx ON _timescaledb_internal._hyper_2814_41_chunk USING btree (scenario_v4, ts DESC);


--
-- Name: _hyper_2814_41_chunk_of_gate_metrics_symbol_ts_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_2814_41_chunk_of_gate_metrics_symbol_ts_idx ON _timescaledb_internal._hyper_2814_41_chunk USING btree (symbol, ts DESC);


--
-- Name: _hyper_2814_41_chunk_of_gate_metrics_ts_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_2814_41_chunk_of_gate_metrics_ts_idx ON _timescaledb_internal._hyper_2814_41_chunk USING btree (ts DESC);


--
-- Name: _hyper_2814_57_chunk_of_gate_metrics_reason_ts_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_2814_57_chunk_of_gate_metrics_reason_ts_idx ON _timescaledb_internal._hyper_2814_57_chunk USING btree (reason_code, ts DESC);


--
-- Name: _hyper_2814_57_chunk_of_gate_metrics_scenario_ts_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_2814_57_chunk_of_gate_metrics_scenario_ts_idx ON _timescaledb_internal._hyper_2814_57_chunk USING btree (scenario_v4, ts DESC);


--
-- Name: _hyper_2814_57_chunk_of_gate_metrics_symbol_ts_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_2814_57_chunk_of_gate_metrics_symbol_ts_idx ON _timescaledb_internal._hyper_2814_57_chunk USING btree (symbol, ts DESC);


--
-- Name: _hyper_2814_57_chunk_of_gate_metrics_ts_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_2814_57_chunk_of_gate_metrics_ts_idx ON _timescaledb_internal._hyper_2814_57_chunk USING btree (ts DESC);


--
-- Name: _hyper_2816_42_chunk__materialized_hypertable_2816_bucket_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_2816_42_chunk__materialized_hypertable_2816_bucket_idx ON _timescaledb_internal._hyper_2816_42_chunk USING btree (bucket DESC);


--
-- Name: _hyper_2816_42_chunk__materialized_hypertable_2816_scenario_v4_; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_2816_42_chunk__materialized_hypertable_2816_scenario_v4_ ON _timescaledb_internal._hyper_2816_42_chunk USING btree (scenario_v4, bucket DESC);


--
-- Name: _hyper_2816_42_chunk__materialized_hypertable_2816_symbol_bucke; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_2816_42_chunk__materialized_hypertable_2816_symbol_bucke ON _timescaledb_internal._hyper_2816_42_chunk USING btree (symbol, bucket DESC);


--
-- Name: _hyper_2816_45_chunk__materialized_hypertable_2816_bucket_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_2816_45_chunk__materialized_hypertable_2816_bucket_idx ON _timescaledb_internal._hyper_2816_45_chunk USING btree (bucket DESC);


--
-- Name: _hyper_2816_45_chunk__materialized_hypertable_2816_scenario_v4_; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_2816_45_chunk__materialized_hypertable_2816_scenario_v4_ ON _timescaledb_internal._hyper_2816_45_chunk USING btree (scenario_v4, bucket DESC);


--
-- Name: _hyper_2816_45_chunk__materialized_hypertable_2816_symbol_bucke; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_2816_45_chunk__materialized_hypertable_2816_symbol_bucke ON _timescaledb_internal._hyper_2816_45_chunk USING btree (symbol, bucket DESC);


--
-- Name: _hyper_2817_43_chunk__materialized_hypertable_2817_bucket_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_2817_43_chunk__materialized_hypertable_2817_bucket_idx ON _timescaledb_internal._hyper_2817_43_chunk USING btree (bucket DESC);


--
-- Name: _hyper_2817_43_chunk__materialized_hypertable_2817_scenario_v4_; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_2817_43_chunk__materialized_hypertable_2817_scenario_v4_ ON _timescaledb_internal._hyper_2817_43_chunk USING btree (scenario_v4, bucket DESC);


--
-- Name: _hyper_2817_43_chunk__materialized_hypertable_2817_symbol_bucke; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_2817_43_chunk__materialized_hypertable_2817_symbol_bucke ON _timescaledb_internal._hyper_2817_43_chunk USING btree (symbol, bucket DESC);


--
-- Name: _hyper_2817_44_chunk__materialized_hypertable_2817_bucket_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_2817_44_chunk__materialized_hypertable_2817_bucket_idx ON _timescaledb_internal._hyper_2817_44_chunk USING btree (bucket DESC);


--
-- Name: _hyper_2817_44_chunk__materialized_hypertable_2817_scenario_v4_; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_2817_44_chunk__materialized_hypertable_2817_scenario_v4_ ON _timescaledb_internal._hyper_2817_44_chunk USING btree (scenario_v4, bucket DESC);


--
-- Name: _hyper_2817_44_chunk__materialized_hypertable_2817_symbol_bucke; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_2817_44_chunk__materialized_hypertable_2817_symbol_bucke ON _timescaledb_internal._hyper_2817_44_chunk USING btree (symbol, bucket DESC);


--
-- Name: _hyper_572_10_chunk_candles_archive_open_time_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_10_chunk_candles_archive_open_time_idx ON _timescaledb_internal._hyper_572_10_chunk USING btree (open_time DESC);


--
-- Name: _hyper_572_10_chunk_idx_candles_archived_at; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_10_chunk_idx_candles_archived_at ON _timescaledb_internal._hyper_572_10_chunk USING btree (archived_at) WHERE (archived_at IS NOT NULL);


--
-- Name: _hyper_572_10_chunk_idx_candles_symbol_tf_time; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_10_chunk_idx_candles_symbol_tf_time ON _timescaledb_internal._hyper_572_10_chunk USING btree (symbol, timeframe, open_time DESC);


--
-- Name: _hyper_572_11_chunk_candles_archive_open_time_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_11_chunk_candles_archive_open_time_idx ON _timescaledb_internal._hyper_572_11_chunk USING btree (open_time DESC);


--
-- Name: _hyper_572_11_chunk_idx_candles_archived_at; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_11_chunk_idx_candles_archived_at ON _timescaledb_internal._hyper_572_11_chunk USING btree (archived_at) WHERE (archived_at IS NOT NULL);


--
-- Name: _hyper_572_11_chunk_idx_candles_symbol_tf_time; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_11_chunk_idx_candles_symbol_tf_time ON _timescaledb_internal._hyper_572_11_chunk USING btree (symbol, timeframe, open_time DESC);


--
-- Name: _hyper_572_12_chunk_candles_archive_open_time_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_12_chunk_candles_archive_open_time_idx ON _timescaledb_internal._hyper_572_12_chunk USING btree (open_time DESC);


--
-- Name: _hyper_572_12_chunk_idx_candles_archived_at; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_12_chunk_idx_candles_archived_at ON _timescaledb_internal._hyper_572_12_chunk USING btree (archived_at) WHERE (archived_at IS NOT NULL);


--
-- Name: _hyper_572_12_chunk_idx_candles_symbol_tf_time; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_12_chunk_idx_candles_symbol_tf_time ON _timescaledb_internal._hyper_572_12_chunk USING btree (symbol, timeframe, open_time DESC);


--
-- Name: _hyper_572_13_chunk_candles_archive_open_time_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_13_chunk_candles_archive_open_time_idx ON _timescaledb_internal._hyper_572_13_chunk USING btree (open_time DESC);


--
-- Name: _hyper_572_13_chunk_idx_candles_archived_at; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_13_chunk_idx_candles_archived_at ON _timescaledb_internal._hyper_572_13_chunk USING btree (archived_at) WHERE (archived_at IS NOT NULL);


--
-- Name: _hyper_572_13_chunk_idx_candles_symbol_tf_time; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_13_chunk_idx_candles_symbol_tf_time ON _timescaledb_internal._hyper_572_13_chunk USING btree (symbol, timeframe, open_time DESC);


--
-- Name: _hyper_572_15_chunk_candles_archive_open_time_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_15_chunk_candles_archive_open_time_idx ON _timescaledb_internal._hyper_572_15_chunk USING btree (open_time DESC);


--
-- Name: _hyper_572_15_chunk_idx_candles_archived_at; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_15_chunk_idx_candles_archived_at ON _timescaledb_internal._hyper_572_15_chunk USING btree (archived_at) WHERE (archived_at IS NOT NULL);


--
-- Name: _hyper_572_15_chunk_idx_candles_symbol_tf_time; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_15_chunk_idx_candles_symbol_tf_time ON _timescaledb_internal._hyper_572_15_chunk USING btree (symbol, timeframe, open_time DESC);


--
-- Name: _hyper_572_17_chunk_candles_archive_open_time_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_17_chunk_candles_archive_open_time_idx ON _timescaledb_internal._hyper_572_17_chunk USING btree (open_time DESC);


--
-- Name: _hyper_572_17_chunk_idx_candles_archived_at; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_17_chunk_idx_candles_archived_at ON _timescaledb_internal._hyper_572_17_chunk USING btree (archived_at) WHERE (archived_at IS NOT NULL);


--
-- Name: _hyper_572_17_chunk_idx_candles_symbol_tf_time; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_17_chunk_idx_candles_symbol_tf_time ON _timescaledb_internal._hyper_572_17_chunk USING btree (symbol, timeframe, open_time DESC);


--
-- Name: _hyper_572_20_chunk_candles_archive_open_time_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_20_chunk_candles_archive_open_time_idx ON _timescaledb_internal._hyper_572_20_chunk USING btree (open_time DESC);


--
-- Name: _hyper_572_20_chunk_idx_candles_archived_at; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_20_chunk_idx_candles_archived_at ON _timescaledb_internal._hyper_572_20_chunk USING btree (archived_at) WHERE (archived_at IS NOT NULL);


--
-- Name: _hyper_572_20_chunk_idx_candles_symbol_tf_time; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_20_chunk_idx_candles_symbol_tf_time ON _timescaledb_internal._hyper_572_20_chunk USING btree (symbol, timeframe, open_time DESC);


--
-- Name: _hyper_572_22_chunk_candles_archive_open_time_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_22_chunk_candles_archive_open_time_idx ON _timescaledb_internal._hyper_572_22_chunk USING btree (open_time DESC);


--
-- Name: _hyper_572_22_chunk_idx_candles_archived_at; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_22_chunk_idx_candles_archived_at ON _timescaledb_internal._hyper_572_22_chunk USING btree (archived_at) WHERE (archived_at IS NOT NULL);


--
-- Name: _hyper_572_22_chunk_idx_candles_symbol_tf_time; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_22_chunk_idx_candles_symbol_tf_time ON _timescaledb_internal._hyper_572_22_chunk USING btree (symbol, timeframe, open_time DESC);


--
-- Name: _hyper_572_24_chunk_candles_archive_open_time_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_24_chunk_candles_archive_open_time_idx ON _timescaledb_internal._hyper_572_24_chunk USING btree (open_time DESC);


--
-- Name: _hyper_572_24_chunk_idx_candles_archived_at; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_24_chunk_idx_candles_archived_at ON _timescaledb_internal._hyper_572_24_chunk USING btree (archived_at) WHERE (archived_at IS NOT NULL);


--
-- Name: _hyper_572_24_chunk_idx_candles_symbol_tf_time; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_24_chunk_idx_candles_symbol_tf_time ON _timescaledb_internal._hyper_572_24_chunk USING btree (symbol, timeframe, open_time DESC);


--
-- Name: _hyper_572_26_chunk_candles_archive_open_time_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_26_chunk_candles_archive_open_time_idx ON _timescaledb_internal._hyper_572_26_chunk USING btree (open_time DESC);


--
-- Name: _hyper_572_26_chunk_idx_candles_archived_at; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_26_chunk_idx_candles_archived_at ON _timescaledb_internal._hyper_572_26_chunk USING btree (archived_at) WHERE (archived_at IS NOT NULL);


--
-- Name: _hyper_572_26_chunk_idx_candles_symbol_tf_time; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_26_chunk_idx_candles_symbol_tf_time ON _timescaledb_internal._hyper_572_26_chunk USING btree (symbol, timeframe, open_time DESC);


--
-- Name: _hyper_572_28_chunk_candles_archive_open_time_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_28_chunk_candles_archive_open_time_idx ON _timescaledb_internal._hyper_572_28_chunk USING btree (open_time DESC);


--
-- Name: _hyper_572_28_chunk_idx_candles_archived_at; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_28_chunk_idx_candles_archived_at ON _timescaledb_internal._hyper_572_28_chunk USING btree (archived_at) WHERE (archived_at IS NOT NULL);


--
-- Name: _hyper_572_28_chunk_idx_candles_symbol_tf_time; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_28_chunk_idx_candles_symbol_tf_time ON _timescaledb_internal._hyper_572_28_chunk USING btree (symbol, timeframe, open_time DESC);


--
-- Name: _hyper_572_31_chunk_candles_archive_open_time_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_31_chunk_candles_archive_open_time_idx ON _timescaledb_internal._hyper_572_31_chunk USING btree (open_time DESC);


--
-- Name: _hyper_572_31_chunk_idx_candles_archived_at; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_31_chunk_idx_candles_archived_at ON _timescaledb_internal._hyper_572_31_chunk USING btree (archived_at) WHERE (archived_at IS NOT NULL);


--
-- Name: _hyper_572_31_chunk_idx_candles_symbol_tf_time; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_31_chunk_idx_candles_symbol_tf_time ON _timescaledb_internal._hyper_572_31_chunk USING btree (symbol, timeframe, open_time DESC);


--
-- Name: _hyper_572_32_chunk_candles_archive_open_time_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_32_chunk_candles_archive_open_time_idx ON _timescaledb_internal._hyper_572_32_chunk USING btree (open_time DESC);


--
-- Name: _hyper_572_32_chunk_idx_candles_archived_at; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_32_chunk_idx_candles_archived_at ON _timescaledb_internal._hyper_572_32_chunk USING btree (archived_at) WHERE (archived_at IS NOT NULL);


--
-- Name: _hyper_572_32_chunk_idx_candles_symbol_tf_time; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_32_chunk_idx_candles_symbol_tf_time ON _timescaledb_internal._hyper_572_32_chunk USING btree (symbol, timeframe, open_time DESC);


--
-- Name: _hyper_572_36_chunk_candles_archive_open_time_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_36_chunk_candles_archive_open_time_idx ON _timescaledb_internal._hyper_572_36_chunk USING btree (open_time DESC);


--
-- Name: _hyper_572_36_chunk_idx_candles_archived_at; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_36_chunk_idx_candles_archived_at ON _timescaledb_internal._hyper_572_36_chunk USING btree (archived_at) WHERE (archived_at IS NOT NULL);


--
-- Name: _hyper_572_36_chunk_idx_candles_symbol_tf_time; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_36_chunk_idx_candles_symbol_tf_time ON _timescaledb_internal._hyper_572_36_chunk USING btree (symbol, timeframe, open_time DESC);


--
-- Name: _hyper_572_38_chunk_candles_archive_open_time_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_38_chunk_candles_archive_open_time_idx ON _timescaledb_internal._hyper_572_38_chunk USING btree (open_time DESC);


--
-- Name: _hyper_572_38_chunk_idx_candles_archived_at; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_38_chunk_idx_candles_archived_at ON _timescaledb_internal._hyper_572_38_chunk USING btree (archived_at) WHERE (archived_at IS NOT NULL);


--
-- Name: _hyper_572_38_chunk_idx_candles_symbol_tf_time; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_38_chunk_idx_candles_symbol_tf_time ON _timescaledb_internal._hyper_572_38_chunk USING btree (symbol, timeframe, open_time DESC);


--
-- Name: _hyper_572_39_chunk_candles_archive_open_time_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_39_chunk_candles_archive_open_time_idx ON _timescaledb_internal._hyper_572_39_chunk USING btree (open_time DESC);


--
-- Name: _hyper_572_39_chunk_idx_candles_archived_at; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_39_chunk_idx_candles_archived_at ON _timescaledb_internal._hyper_572_39_chunk USING btree (archived_at) WHERE (archived_at IS NOT NULL);


--
-- Name: _hyper_572_39_chunk_idx_candles_symbol_tf_time; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_39_chunk_idx_candles_symbol_tf_time ON _timescaledb_internal._hyper_572_39_chunk USING btree (symbol, timeframe, open_time DESC);


--
-- Name: _hyper_572_48_chunk_candles_archive_open_time_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_48_chunk_candles_archive_open_time_idx ON _timescaledb_internal._hyper_572_48_chunk USING btree (open_time DESC);


--
-- Name: _hyper_572_48_chunk_idx_candles_archived_at; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_48_chunk_idx_candles_archived_at ON _timescaledb_internal._hyper_572_48_chunk USING btree (archived_at) WHERE (archived_at IS NOT NULL);


--
-- Name: _hyper_572_48_chunk_idx_candles_symbol_tf_time; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_48_chunk_idx_candles_symbol_tf_time ON _timescaledb_internal._hyper_572_48_chunk USING btree (symbol, timeframe, open_time DESC);


--
-- Name: _hyper_572_4_chunk_candles_archive_open_time_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_4_chunk_candles_archive_open_time_idx ON _timescaledb_internal._hyper_572_4_chunk USING btree (open_time DESC);


--
-- Name: _hyper_572_4_chunk_idx_candles_archived_at; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_4_chunk_idx_candles_archived_at ON _timescaledb_internal._hyper_572_4_chunk USING btree (archived_at) WHERE (archived_at IS NOT NULL);


--
-- Name: _hyper_572_4_chunk_idx_candles_symbol_tf_time; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_4_chunk_idx_candles_symbol_tf_time ON _timescaledb_internal._hyper_572_4_chunk USING btree (symbol, timeframe, open_time DESC);


--
-- Name: _hyper_572_51_chunk_candles_archive_open_time_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_51_chunk_candles_archive_open_time_idx ON _timescaledb_internal._hyper_572_51_chunk USING btree (open_time DESC);


--
-- Name: _hyper_572_51_chunk_idx_candles_archived_at; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_51_chunk_idx_candles_archived_at ON _timescaledb_internal._hyper_572_51_chunk USING btree (archived_at) WHERE (archived_at IS NOT NULL);


--
-- Name: _hyper_572_51_chunk_idx_candles_symbol_tf_time; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_51_chunk_idx_candles_symbol_tf_time ON _timescaledb_internal._hyper_572_51_chunk USING btree (symbol, timeframe, open_time DESC);


--
-- Name: _hyper_572_53_chunk_candles_archive_open_time_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_53_chunk_candles_archive_open_time_idx ON _timescaledb_internal._hyper_572_53_chunk USING btree (open_time DESC);


--
-- Name: _hyper_572_53_chunk_idx_candles_archived_at; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_53_chunk_idx_candles_archived_at ON _timescaledb_internal._hyper_572_53_chunk USING btree (archived_at) WHERE (archived_at IS NOT NULL);


--
-- Name: _hyper_572_53_chunk_idx_candles_symbol_tf_time; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_53_chunk_idx_candles_symbol_tf_time ON _timescaledb_internal._hyper_572_53_chunk USING btree (symbol, timeframe, open_time DESC);


--
-- Name: _hyper_572_55_chunk_candles_archive_open_time_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_55_chunk_candles_archive_open_time_idx ON _timescaledb_internal._hyper_572_55_chunk USING btree (open_time DESC);


--
-- Name: _hyper_572_55_chunk_idx_candles_archived_at; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_55_chunk_idx_candles_archived_at ON _timescaledb_internal._hyper_572_55_chunk USING btree (archived_at) WHERE (archived_at IS NOT NULL);


--
-- Name: _hyper_572_55_chunk_idx_candles_symbol_tf_time; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_55_chunk_idx_candles_symbol_tf_time ON _timescaledb_internal._hyper_572_55_chunk USING btree (symbol, timeframe, open_time DESC);


--
-- Name: _hyper_572_58_chunk_candles_archive_open_time_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_58_chunk_candles_archive_open_time_idx ON _timescaledb_internal._hyper_572_58_chunk USING btree (open_time DESC);


--
-- Name: _hyper_572_58_chunk_idx_candles_archived_at; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_58_chunk_idx_candles_archived_at ON _timescaledb_internal._hyper_572_58_chunk USING btree (archived_at) WHERE (archived_at IS NOT NULL);


--
-- Name: _hyper_572_58_chunk_idx_candles_symbol_tf_time; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_58_chunk_idx_candles_symbol_tf_time ON _timescaledb_internal._hyper_572_58_chunk USING btree (symbol, timeframe, open_time DESC);


--
-- Name: _hyper_572_5_chunk_candles_archive_open_time_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_5_chunk_candles_archive_open_time_idx ON _timescaledb_internal._hyper_572_5_chunk USING btree (open_time DESC);


--
-- Name: _hyper_572_5_chunk_idx_candles_archived_at; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_5_chunk_idx_candles_archived_at ON _timescaledb_internal._hyper_572_5_chunk USING btree (archived_at) WHERE (archived_at IS NOT NULL);


--
-- Name: _hyper_572_5_chunk_idx_candles_symbol_tf_time; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_5_chunk_idx_candles_symbol_tf_time ON _timescaledb_internal._hyper_572_5_chunk USING btree (symbol, timeframe, open_time DESC);


--
-- Name: _hyper_572_61_chunk_candles_archive_open_time_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_61_chunk_candles_archive_open_time_idx ON _timescaledb_internal._hyper_572_61_chunk USING btree (open_time DESC);


--
-- Name: _hyper_572_61_chunk_idx_candles_archived_at; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_61_chunk_idx_candles_archived_at ON _timescaledb_internal._hyper_572_61_chunk USING btree (archived_at) WHERE (archived_at IS NOT NULL);


--
-- Name: _hyper_572_61_chunk_idx_candles_symbol_tf_time; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_61_chunk_idx_candles_symbol_tf_time ON _timescaledb_internal._hyper_572_61_chunk USING btree (symbol, timeframe, open_time DESC);


--
-- Name: _hyper_572_6_chunk_candles_archive_open_time_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_6_chunk_candles_archive_open_time_idx ON _timescaledb_internal._hyper_572_6_chunk USING btree (open_time DESC);


--
-- Name: _hyper_572_6_chunk_idx_candles_archived_at; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_6_chunk_idx_candles_archived_at ON _timescaledb_internal._hyper_572_6_chunk USING btree (archived_at) WHERE (archived_at IS NOT NULL);


--
-- Name: _hyper_572_6_chunk_idx_candles_symbol_tf_time; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_6_chunk_idx_candles_symbol_tf_time ON _timescaledb_internal._hyper_572_6_chunk USING btree (symbol, timeframe, open_time DESC);


--
-- Name: _hyper_572_7_chunk_candles_archive_open_time_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_7_chunk_candles_archive_open_time_idx ON _timescaledb_internal._hyper_572_7_chunk USING btree (open_time DESC);


--
-- Name: _hyper_572_7_chunk_idx_candles_archived_at; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_7_chunk_idx_candles_archived_at ON _timescaledb_internal._hyper_572_7_chunk USING btree (archived_at) WHERE (archived_at IS NOT NULL);


--
-- Name: _hyper_572_7_chunk_idx_candles_symbol_tf_time; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_7_chunk_idx_candles_symbol_tf_time ON _timescaledb_internal._hyper_572_7_chunk USING btree (symbol, timeframe, open_time DESC);


--
-- Name: _hyper_572_8_chunk_candles_archive_open_time_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_8_chunk_candles_archive_open_time_idx ON _timescaledb_internal._hyper_572_8_chunk USING btree (open_time DESC);


--
-- Name: _hyper_572_8_chunk_idx_candles_archived_at; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_8_chunk_idx_candles_archived_at ON _timescaledb_internal._hyper_572_8_chunk USING btree (archived_at) WHERE (archived_at IS NOT NULL);


--
-- Name: _hyper_572_8_chunk_idx_candles_symbol_tf_time; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_572_8_chunk_idx_candles_symbol_tf_time ON _timescaledb_internal._hyper_572_8_chunk USING btree (symbol, timeframe, open_time DESC);


--
-- Name: _hyper_7_19_chunk_idx_trades_closed_p0_order_exitms; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_7_19_chunk_idx_trades_closed_p0_order_exitms ON _timescaledb_internal._hyper_7_19_chunk USING btree (order_id, exit_ts_ms);


--
-- Name: _hyper_7_19_chunk_idx_trades_closed_p0_order_id; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_7_19_chunk_idx_trades_closed_p0_order_id ON _timescaledb_internal._hyper_7_19_chunk USING btree (order_id);


--
-- Name: _hyper_7_19_chunk_idx_trades_closed_p0_regime_exit; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_7_19_chunk_idx_trades_closed_p0_regime_exit ON _timescaledb_internal._hyper_7_19_chunk USING btree (regime, exit_ts DESC);


--
-- Name: _hyper_7_19_chunk_idx_trades_closed_p0_scenario_exit; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_7_19_chunk_idx_trades_closed_p0_scenario_exit ON _timescaledb_internal._hyper_7_19_chunk USING btree (scenario, exit_ts DESC);


--
-- Name: _hyper_7_19_chunk_idx_trades_closed_p0_session_exit; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_7_19_chunk_idx_trades_closed_p0_session_exit ON _timescaledb_internal._hyper_7_19_chunk USING btree (session, exit_ts DESC);


--
-- Name: _hyper_7_19_chunk_trades_closed_p0_exit_ts_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_7_19_chunk_trades_closed_p0_exit_ts_idx ON _timescaledb_internal._hyper_7_19_chunk USING btree (exit_ts DESC);


--
-- Name: _hyper_7_1_chunk_idx_trades_closed_p0_order_exitms; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_7_1_chunk_idx_trades_closed_p0_order_exitms ON _timescaledb_internal._hyper_7_1_chunk USING btree (order_id, exit_ts_ms);


--
-- Name: _hyper_7_1_chunk_idx_trades_closed_p0_order_id; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_7_1_chunk_idx_trades_closed_p0_order_id ON _timescaledb_internal._hyper_7_1_chunk USING btree (order_id);


--
-- Name: _hyper_7_1_chunk_idx_trades_closed_p0_regime_exit; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_7_1_chunk_idx_trades_closed_p0_regime_exit ON _timescaledb_internal._hyper_7_1_chunk USING btree (regime, exit_ts DESC);


--
-- Name: _hyper_7_1_chunk_idx_trades_closed_p0_scenario_exit; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_7_1_chunk_idx_trades_closed_p0_scenario_exit ON _timescaledb_internal._hyper_7_1_chunk USING btree (scenario, exit_ts DESC);


--
-- Name: _hyper_7_1_chunk_idx_trades_closed_p0_session_exit; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_7_1_chunk_idx_trades_closed_p0_session_exit ON _timescaledb_internal._hyper_7_1_chunk USING btree (session, exit_ts DESC);


--
-- Name: _hyper_7_1_chunk_trades_closed_p0_exit_ts_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_7_1_chunk_trades_closed_p0_exit_ts_idx ON _timescaledb_internal._hyper_7_1_chunk USING btree (exit_ts DESC);


--
-- Name: _hyper_7_2_chunk_idx_trades_closed_p0_order_exitms; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_7_2_chunk_idx_trades_closed_p0_order_exitms ON _timescaledb_internal._hyper_7_2_chunk USING btree (order_id, exit_ts_ms);


--
-- Name: _hyper_7_2_chunk_idx_trades_closed_p0_order_id; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_7_2_chunk_idx_trades_closed_p0_order_id ON _timescaledb_internal._hyper_7_2_chunk USING btree (order_id);


--
-- Name: _hyper_7_2_chunk_idx_trades_closed_p0_regime_exit; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_7_2_chunk_idx_trades_closed_p0_regime_exit ON _timescaledb_internal._hyper_7_2_chunk USING btree (regime, exit_ts DESC);


--
-- Name: _hyper_7_2_chunk_idx_trades_closed_p0_scenario_exit; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_7_2_chunk_idx_trades_closed_p0_scenario_exit ON _timescaledb_internal._hyper_7_2_chunk USING btree (scenario, exit_ts DESC);


--
-- Name: _hyper_7_2_chunk_idx_trades_closed_p0_session_exit; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_7_2_chunk_idx_trades_closed_p0_session_exit ON _timescaledb_internal._hyper_7_2_chunk USING btree (session, exit_ts DESC);


--
-- Name: _hyper_7_2_chunk_trades_closed_p0_exit_ts_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_7_2_chunk_trades_closed_p0_exit_ts_idx ON _timescaledb_internal._hyper_7_2_chunk USING btree (exit_ts DESC);


--
-- Name: _hyper_7_35_chunk_idx_trades_closed_p0_order_exitms; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_7_35_chunk_idx_trades_closed_p0_order_exitms ON _timescaledb_internal._hyper_7_35_chunk USING btree (order_id, exit_ts_ms);


--
-- Name: _hyper_7_35_chunk_idx_trades_closed_p0_order_id; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_7_35_chunk_idx_trades_closed_p0_order_id ON _timescaledb_internal._hyper_7_35_chunk USING btree (order_id);


--
-- Name: _hyper_7_35_chunk_idx_trades_closed_p0_regime_exit; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_7_35_chunk_idx_trades_closed_p0_regime_exit ON _timescaledb_internal._hyper_7_35_chunk USING btree (regime, exit_ts DESC);


--
-- Name: _hyper_7_35_chunk_idx_trades_closed_p0_scenario_exit; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_7_35_chunk_idx_trades_closed_p0_scenario_exit ON _timescaledb_internal._hyper_7_35_chunk USING btree (scenario, exit_ts DESC);


--
-- Name: _hyper_7_35_chunk_idx_trades_closed_p0_session_exit; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_7_35_chunk_idx_trades_closed_p0_session_exit ON _timescaledb_internal._hyper_7_35_chunk USING btree (session, exit_ts DESC);


--
-- Name: _hyper_7_35_chunk_trades_closed_p0_exit_ts_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_7_35_chunk_trades_closed_p0_exit_ts_idx ON _timescaledb_internal._hyper_7_35_chunk USING btree (exit_ts DESC);


--
-- Name: _hyper_7_3_chunk_idx_trades_closed_p0_order_exitms; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_7_3_chunk_idx_trades_closed_p0_order_exitms ON _timescaledb_internal._hyper_7_3_chunk USING btree (order_id, exit_ts_ms);


--
-- Name: _hyper_7_3_chunk_idx_trades_closed_p0_order_id; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_7_3_chunk_idx_trades_closed_p0_order_id ON _timescaledb_internal._hyper_7_3_chunk USING btree (order_id);


--
-- Name: _hyper_7_3_chunk_idx_trades_closed_p0_regime_exit; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_7_3_chunk_idx_trades_closed_p0_regime_exit ON _timescaledb_internal._hyper_7_3_chunk USING btree (regime, exit_ts DESC);


--
-- Name: _hyper_7_3_chunk_idx_trades_closed_p0_scenario_exit; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_7_3_chunk_idx_trades_closed_p0_scenario_exit ON _timescaledb_internal._hyper_7_3_chunk USING btree (scenario, exit_ts DESC);


--
-- Name: _hyper_7_3_chunk_idx_trades_closed_p0_session_exit; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_7_3_chunk_idx_trades_closed_p0_session_exit ON _timescaledb_internal._hyper_7_3_chunk USING btree (session, exit_ts DESC);


--
-- Name: _hyper_7_3_chunk_trades_closed_p0_exit_ts_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_7_3_chunk_trades_closed_p0_exit_ts_idx ON _timescaledb_internal._hyper_7_3_chunk USING btree (exit_ts DESC);


--
-- Name: _hyper_7_59_chunk_idx_trades_closed_p0_order_exitms; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_7_59_chunk_idx_trades_closed_p0_order_exitms ON _timescaledb_internal._hyper_7_59_chunk USING btree (order_id, exit_ts_ms);


--
-- Name: _hyper_7_59_chunk_idx_trades_closed_p0_order_id; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_7_59_chunk_idx_trades_closed_p0_order_id ON _timescaledb_internal._hyper_7_59_chunk USING btree (order_id);


--
-- Name: _hyper_7_59_chunk_idx_trades_closed_p0_regime_exit; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_7_59_chunk_idx_trades_closed_p0_regime_exit ON _timescaledb_internal._hyper_7_59_chunk USING btree (regime, exit_ts DESC);


--
-- Name: _hyper_7_59_chunk_idx_trades_closed_p0_scenario_exit; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_7_59_chunk_idx_trades_closed_p0_scenario_exit ON _timescaledb_internal._hyper_7_59_chunk USING btree (scenario, exit_ts DESC);


--
-- Name: _hyper_7_59_chunk_idx_trades_closed_p0_session_exit; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_7_59_chunk_idx_trades_closed_p0_session_exit ON _timescaledb_internal._hyper_7_59_chunk USING btree (session, exit_ts DESC);


--
-- Name: _hyper_7_59_chunk_trades_closed_p0_exit_ts_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_7_59_chunk_trades_closed_p0_exit_ts_idx ON _timescaledb_internal._hyper_7_59_chunk USING btree (exit_ts DESC);


--
-- Name: _hyper_7_9_chunk_idx_trades_closed_p0_order_exitms; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_7_9_chunk_idx_trades_closed_p0_order_exitms ON _timescaledb_internal._hyper_7_9_chunk USING btree (order_id, exit_ts_ms);


--
-- Name: _hyper_7_9_chunk_idx_trades_closed_p0_order_id; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_7_9_chunk_idx_trades_closed_p0_order_id ON _timescaledb_internal._hyper_7_9_chunk USING btree (order_id);


--
-- Name: _hyper_7_9_chunk_idx_trades_closed_p0_regime_exit; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_7_9_chunk_idx_trades_closed_p0_regime_exit ON _timescaledb_internal._hyper_7_9_chunk USING btree (regime, exit_ts DESC);


--
-- Name: _hyper_7_9_chunk_idx_trades_closed_p0_scenario_exit; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_7_9_chunk_idx_trades_closed_p0_scenario_exit ON _timescaledb_internal._hyper_7_9_chunk USING btree (scenario, exit_ts DESC);


--
-- Name: _hyper_7_9_chunk_idx_trades_closed_p0_session_exit; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_7_9_chunk_idx_trades_closed_p0_session_exit ON _timescaledb_internal._hyper_7_9_chunk USING btree (session, exit_ts DESC);


--
-- Name: _hyper_7_9_chunk_trades_closed_p0_exit_ts_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _hyper_7_9_chunk_trades_closed_p0_exit_ts_idx ON _timescaledb_internal._hyper_7_9_chunk USING btree (exit_ts DESC);


--
-- Name: _materialized_hypertable_2816_bucket_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _materialized_hypertable_2816_bucket_idx ON _timescaledb_internal._materialized_hypertable_2816 USING btree (bucket DESC);


--
-- Name: _materialized_hypertable_2816_scenario_v4_bucket_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _materialized_hypertable_2816_scenario_v4_bucket_idx ON _timescaledb_internal._materialized_hypertable_2816 USING btree (scenario_v4, bucket DESC);


--
-- Name: _materialized_hypertable_2816_symbol_bucket_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _materialized_hypertable_2816_symbol_bucket_idx ON _timescaledb_internal._materialized_hypertable_2816 USING btree (symbol, bucket DESC);


--
-- Name: _materialized_hypertable_2817_bucket_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _materialized_hypertable_2817_bucket_idx ON _timescaledb_internal._materialized_hypertable_2817 USING btree (bucket DESC);


--
-- Name: _materialized_hypertable_2817_scenario_v4_bucket_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _materialized_hypertable_2817_scenario_v4_bucket_idx ON _timescaledb_internal._materialized_hypertable_2817 USING btree (scenario_v4, bucket DESC);


--
-- Name: _materialized_hypertable_2817_symbol_bucket_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX _materialized_hypertable_2817_symbol_bucket_idx ON _timescaledb_internal._materialized_hypertable_2817 USING btree (symbol, bucket DESC);


--
-- Name: compress_hyper_573_14_chunk_symbol_timeframe__ts_meta_min_1_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX compress_hyper_573_14_chunk_symbol_timeframe__ts_meta_min_1_idx ON _timescaledb_internal.compress_hyper_573_14_chunk USING btree (symbol, timeframe, _ts_meta_min_1 DESC, _ts_meta_max_1 DESC, _ts_meta_min_2, _ts_meta_max_2);


--
-- Name: compress_hyper_573_16_chunk_symbol_timeframe__ts_meta_min_1_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX compress_hyper_573_16_chunk_symbol_timeframe__ts_meta_min_1_idx ON _timescaledb_internal.compress_hyper_573_16_chunk USING btree (symbol, timeframe, _ts_meta_min_1 DESC, _ts_meta_max_1 DESC, _ts_meta_min_2, _ts_meta_max_2);


--
-- Name: compress_hyper_573_18_chunk_symbol_timeframe__ts_meta_min_1_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX compress_hyper_573_18_chunk_symbol_timeframe__ts_meta_min_1_idx ON _timescaledb_internal.compress_hyper_573_18_chunk USING btree (symbol, timeframe, _ts_meta_min_1 DESC, _ts_meta_max_1 DESC, _ts_meta_min_2, _ts_meta_max_2);


--
-- Name: compress_hyper_573_21_chunk_symbol_timeframe__ts_meta_min_1_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX compress_hyper_573_21_chunk_symbol_timeframe__ts_meta_min_1_idx ON _timescaledb_internal.compress_hyper_573_21_chunk USING btree (symbol, timeframe, _ts_meta_min_1 DESC, _ts_meta_max_1 DESC, _ts_meta_min_2, _ts_meta_max_2);


--
-- Name: compress_hyper_573_23_chunk_symbol_timeframe__ts_meta_min_1_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX compress_hyper_573_23_chunk_symbol_timeframe__ts_meta_min_1_idx ON _timescaledb_internal.compress_hyper_573_23_chunk USING btree (symbol, timeframe, _ts_meta_min_1 DESC, _ts_meta_max_1 DESC, _ts_meta_min_2, _ts_meta_max_2);


--
-- Name: compress_hyper_573_25_chunk_symbol_timeframe__ts_meta_min_1_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX compress_hyper_573_25_chunk_symbol_timeframe__ts_meta_min_1_idx ON _timescaledb_internal.compress_hyper_573_25_chunk USING btree (symbol, timeframe, _ts_meta_min_1 DESC, _ts_meta_max_1 DESC, _ts_meta_min_2, _ts_meta_max_2);


--
-- Name: compress_hyper_573_27_chunk_symbol_timeframe__ts_meta_min_1_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX compress_hyper_573_27_chunk_symbol_timeframe__ts_meta_min_1_idx ON _timescaledb_internal.compress_hyper_573_27_chunk USING btree (symbol, timeframe, _ts_meta_min_1 DESC, _ts_meta_max_1 DESC, _ts_meta_min_2, _ts_meta_max_2);


--
-- Name: compress_hyper_573_29_chunk_symbol_timeframe__ts_meta_min_1_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX compress_hyper_573_29_chunk_symbol_timeframe__ts_meta_min_1_idx ON _timescaledb_internal.compress_hyper_573_29_chunk USING btree (symbol, timeframe, _ts_meta_min_1 DESC, _ts_meta_max_1 DESC, _ts_meta_min_2, _ts_meta_max_2);


--
-- Name: compress_hyper_573_30_chunk_symbol_timeframe__ts_meta_min_1_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX compress_hyper_573_30_chunk_symbol_timeframe__ts_meta_min_1_idx ON _timescaledb_internal.compress_hyper_573_30_chunk USING btree (symbol, timeframe, _ts_meta_min_1 DESC, _ts_meta_max_1 DESC, _ts_meta_min_2, _ts_meta_max_2);


--
-- Name: compress_hyper_573_33_chunk_symbol_timeframe__ts_meta_min_1_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX compress_hyper_573_33_chunk_symbol_timeframe__ts_meta_min_1_idx ON _timescaledb_internal.compress_hyper_573_33_chunk USING btree (symbol, timeframe, _ts_meta_min_1 DESC, _ts_meta_max_1 DESC, _ts_meta_min_2, _ts_meta_max_2);


--
-- Name: compress_hyper_573_34_chunk_symbol_timeframe__ts_meta_min_1_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX compress_hyper_573_34_chunk_symbol_timeframe__ts_meta_min_1_idx ON _timescaledb_internal.compress_hyper_573_34_chunk USING btree (symbol, timeframe, _ts_meta_min_1 DESC, _ts_meta_max_1 DESC, _ts_meta_min_2, _ts_meta_max_2);


--
-- Name: compress_hyper_573_37_chunk_symbol_timeframe__ts_meta_min_1_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX compress_hyper_573_37_chunk_symbol_timeframe__ts_meta_min_1_idx ON _timescaledb_internal.compress_hyper_573_37_chunk USING btree (symbol, timeframe, _ts_meta_min_1 DESC, _ts_meta_max_1 DESC, _ts_meta_min_2, _ts_meta_max_2);


--
-- Name: compress_hyper_573_46_chunk_symbol_timeframe__ts_meta_min_1_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX compress_hyper_573_46_chunk_symbol_timeframe__ts_meta_min_1_idx ON _timescaledb_internal.compress_hyper_573_46_chunk USING btree (symbol, timeframe, _ts_meta_min_1 DESC, _ts_meta_max_1 DESC, _ts_meta_min_2, _ts_meta_max_2);


--
-- Name: compress_hyper_573_49_chunk_symbol_timeframe__ts_meta_min_1_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX compress_hyper_573_49_chunk_symbol_timeframe__ts_meta_min_1_idx ON _timescaledb_internal.compress_hyper_573_49_chunk USING btree (symbol, timeframe, _ts_meta_min_1 DESC, _ts_meta_max_1 DESC, _ts_meta_min_2, _ts_meta_max_2);


--
-- Name: compress_hyper_573_52_chunk_symbol_timeframe__ts_meta_min_1_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX compress_hyper_573_52_chunk_symbol_timeframe__ts_meta_min_1_idx ON _timescaledb_internal.compress_hyper_573_52_chunk USING btree (symbol, timeframe, _ts_meta_min_1 DESC, _ts_meta_max_1 DESC, _ts_meta_min_2, _ts_meta_max_2);


--
-- Name: compress_hyper_573_54_chunk_symbol_timeframe__ts_meta_min_1_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX compress_hyper_573_54_chunk_symbol_timeframe__ts_meta_min_1_idx ON _timescaledb_internal.compress_hyper_573_54_chunk USING btree (symbol, timeframe, _ts_meta_min_1 DESC, _ts_meta_max_1 DESC, _ts_meta_min_2, _ts_meta_max_2);


--
-- Name: compress_hyper_573_56_chunk_symbol_timeframe__ts_meta_min_1_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX compress_hyper_573_56_chunk_symbol_timeframe__ts_meta_min_1_idx ON _timescaledb_internal.compress_hyper_573_56_chunk USING btree (symbol, timeframe, _ts_meta_min_1 DESC, _ts_meta_max_1 DESC, _ts_meta_min_2, _ts_meta_max_2);


--
-- Name: compress_hyper_573_60_chunk_symbol_timeframe__ts_meta_min_1_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX compress_hyper_573_60_chunk_symbol_timeframe__ts_meta_min_1_idx ON _timescaledb_internal.compress_hyper_573_60_chunk USING btree (symbol, timeframe, _ts_meta_min_1 DESC, _ts_meta_max_1 DESC, _ts_meta_min_2, _ts_meta_max_2);


--
-- Name: compress_hyper_573_62_chunk_symbol_timeframe__ts_meta_min_1_idx; Type: INDEX; Schema: _timescaledb_internal; Owner: trading
--

CREATE INDEX compress_hyper_573_62_chunk_symbol_timeframe__ts_meta_min_1_idx ON _timescaledb_internal.compress_hyper_573_62_chunk USING btree (symbol, timeframe, _ts_meta_min_1 DESC, _ts_meta_max_1 DESC, _ts_meta_min_2, _ts_meta_max_2);


--
-- Name: atr_archive_ts_idx; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX atr_archive_ts_idx ON public.atr_archive USING btree (ts DESC);


--
-- Name: brin_edge_gate_events_ts; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX brin_edge_gate_events_ts ON public.edge_gate_events USING brin (ts_ms);


--
-- Name: calendar_events_currency_ts_idx; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX calendar_events_currency_ts_idx ON public.calendar_events USING btree (currency, event_ts_ms DESC);


--
-- Name: calendar_events_event_ts_idx; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX calendar_events_event_ts_idx ON public.calendar_events USING btree (event_ts_ms DESC);


--
-- Name: calendar_features_scope_scope_ts_idx; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX calendar_features_scope_scope_ts_idx ON public.calendar_features_scope USING btree (scope, ts_ms DESC);


--
-- Name: calendar_features_scope_ts_idx; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX calendar_features_scope_ts_idx ON public.calendar_features_scope USING btree (ts_ms DESC);


--
-- Name: candles_archive_open_time_idx; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX candles_archive_open_time_idx ON public.candles_archive USING btree (open_time DESC);


--
-- Name: daily_metrics_uniq; Type: INDEX; Schema: public; Owner: trading
--

CREATE UNIQUE INDEX daily_metrics_uniq ON public.daily_metrics USING btree (date, COALESCE(source, ''::text), symbol);


--
-- Name: entry_policy_audit_arm_ts_idx; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX entry_policy_audit_arm_ts_idx ON public.entry_policy_audit USING btree (arm, ts DESC);


--
-- Name: entry_policy_audit_decision_ts_idx; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX entry_policy_audit_decision_ts_idx ON public.entry_policy_audit USING btree (decision, ts DESC);


--
-- Name: entry_policy_audit_payload_gin_idx; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX entry_policy_audit_payload_gin_idx ON public.entry_policy_audit USING gin (payload_json);


--
-- Name: entry_policy_audit_symbol_ts_idx; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX entry_policy_audit_symbol_ts_idx ON public.entry_policy_audit USING btree (symbol, ts DESC);


--
-- Name: entry_policy_audit_ts_idx; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX entry_policy_audit_ts_idx ON public.entry_policy_audit USING btree (ts DESC);


--
-- Name: entry_tag_metrics_uniq; Type: INDEX; Schema: public; Owner: trading
--

CREATE UNIQUE INDEX entry_tag_metrics_uniq ON public.entry_tag_metrics USING btree (date, COALESCE(source, ''::text), symbol, entry_tag);


--
-- Name: idx_atr_by_symbol; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX idx_atr_by_symbol ON public.atr_archive USING btree (symbol, ts DESC);


--
-- Name: idx_atr_symbol_tf_ts; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX idx_atr_symbol_tf_ts ON public.atr_archive USING btree (symbol, timeframe, ts DESC);


--
-- Name: idx_calibration_state_ts; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_calibration_state_ts ON public.calibration_state USING btree (ts_ms DESC);


--
-- Name: idx_candles_archived_at; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX idx_candles_archived_at ON public.candles_archive USING btree (archived_at) WHERE (archived_at IS NOT NULL);


--
-- Name: idx_candles_symbol_tf_time; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX idx_candles_symbol_tf_time ON public.candles_archive USING btree (symbol, timeframe, open_time DESC);


--
-- Name: idx_execution_plan_risk; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX idx_execution_plan_risk ON public.signal_execution_plan USING btree (risk_usd);


--
-- Name: idx_experiment_snapshot_experiment_as_of; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX idx_experiment_snapshot_experiment_as_of ON public.signal_experiment_snapshot USING btree (experiment_id, as_of DESC);


--
-- Name: idx_experiment_snapshot_variant; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX idx_experiment_snapshot_variant ON public.signal_experiment_snapshot USING btree (variant);


--
-- Name: idx_microbars_ts; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_microbars_ts ON public.microbars USING btree (ts_ms DESC);


--
-- Name: idx_performance_setup_ttd; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX idx_performance_setup_ttd ON public.signal_performance USING btree (setup_type, ttd_bars);


--
-- Name: idx_performance_symbol_outcome; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX idx_performance_symbol_outcome ON public.signal_performance USING btree (symbol, outcome);


--
-- Name: idx_performance_symbol_setup_ts; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX idx_performance_symbol_setup_ts ON public.signal_performance USING btree (symbol, setup_type, ts_signal DESC);


--
-- Name: idx_regime_quantiles_computed_at; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX idx_regime_quantiles_computed_at ON public.regime_quantiles USING btree (computed_at DESC);


--
-- Name: idx_regime_quantiles_lookup; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX idx_regime_quantiles_lookup ON public.regime_quantiles USING btree (symbol, timeframe);


--
-- Name: idx_regime_quantiles_symbol_timeframe; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX idx_regime_quantiles_symbol_timeframe ON public.regime_quantiles USING btree (symbol, timeframe);


--
-- Name: idx_regime_snapshot_lookup; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX idx_regime_snapshot_lookup ON public.regime_snapshot USING btree (symbol, timeframe, ts DESC);


--
-- Name: idx_regime_snapshot_symbol_timeframe_ts; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX idx_regime_snapshot_symbol_timeframe_ts ON public.regime_snapshot USING btree (symbol, timeframe, ts);


--
-- Name: idx_regime_snapshot_ts; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX idx_regime_snapshot_ts ON public.regime_snapshot USING btree (ts);


--
-- Name: idx_signal_exec_summary_lookup; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX idx_signal_exec_summary_lookup ON public.signal_exec_summary USING btree (symbol, family, opened_at);


--
-- Name: idx_signal_experiment_family; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX idx_signal_experiment_family ON public.signal_experiment USING btree (signal_family);


--
-- Name: idx_signal_experiment_filter_name; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX idx_signal_experiment_filter_name ON public.signal_experiment USING btree (filter_name);


--
-- Name: idx_signal_experiment_status_start; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX idx_signal_experiment_status_start ON public.signal_experiment USING btree (status, start_at);


--
-- Name: idx_signal_family_baseline_lookup; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX idx_signal_family_baseline_lookup ON public.signal_family_baseline USING btree (symbol, family, metric);


--
-- Name: idx_signal_family_regime_state_lookup; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX idx_signal_family_regime_state_lookup ON public.signal_family_regime_state USING btree (family, venue, symbol, timeframe, ts_state DESC);


--
-- Name: idx_signal_local_calibration_lookup; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX idx_signal_local_calibration_lookup ON public.signal_local_calibration USING btree (symbol, session, regime, metric);


--
-- Name: idx_signal_local_calibration_updated; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX idx_signal_local_calibration_updated ON public.signal_local_calibration USING btree (updated_at);


--
-- Name: idx_signal_quality_offline_lookup; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX idx_signal_quality_offline_lookup ON public.signal_quality_offline USING btree (symbol, signal_type, side, session, regime, feature_bucket, horizon);


--
-- Name: idx_signal_quality_offline_updated; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX idx_signal_quality_offline_updated ON public.signal_quality_offline USING btree (updated_at DESC);


--
-- Name: idx_signal_quality_online_lookup; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX idx_signal_quality_online_lookup ON public.signal_quality_online USING btree (symbol, signal_type, side, horizon);


--
-- Name: idx_signal_quality_online_updated; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX idx_signal_quality_online_updated ON public.signal_quality_online USING btree (updated_at DESC);


--
-- Name: idx_signals_experiment_family_ts; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX idx_signals_experiment_family_ts ON public.signals USING btree (experiment_id, signal_family, ts_signal DESC);


--
-- Name: idx_signals_experiment_id; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX idx_signals_experiment_id ON public.signals USING btree (experiment_id);


--
-- Name: idx_signals_experiment_variant; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX idx_signals_experiment_variant ON public.signals USING btree (experiment_variant);


--
-- Name: idx_signals_metrics; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX idx_signals_metrics ON public.signals USING btree (symbol, session, regime, ts_signal) WHERE ((delta_spike_z IS NOT NULL) OR (obi IS NOT NULL) OR (weak_progress IS NOT NULL) OR (atr_quantile IS NOT NULL));


--
-- Name: idx_signals_session_regime; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX idx_signals_session_regime ON public.signals USING btree (symbol, session, regime);


--
-- Name: idx_signals_setup_ts; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX idx_signals_setup_ts ON public.signals USING btree (setup_type, ts_signal DESC);


--
-- Name: idx_signals_symbol_setup_ts; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX idx_signals_symbol_setup_ts ON public.signals USING btree (symbol, setup_type, ts_signal DESC);


--
-- Name: idx_signals_symbol_ts; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX idx_signals_symbol_ts ON public.signals USING btree (symbol, ts_signal DESC);


--
-- Name: idx_signals_ts_session_regime; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX idx_signals_ts_session_regime ON public.signals USING btree (ts_signal, session, regime);


--
-- Name: idx_ticks_symbol_ts; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX idx_ticks_symbol_ts ON public.ticks USING btree (symbol, ts);


--
-- Name: idx_trades_closed_entry_tag_exit; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX idx_trades_closed_entry_tag_exit ON public.trades_closed USING btree (entry_tag, exit_ts);


--
-- Name: idx_trades_closed_ml_v2; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX idx_trades_closed_ml_v2 ON public.trades_closed USING btree (exit_ts DESC, symbol, entry_tag) INCLUDE (r_multiple, ind_delta_z, ind_obi, ind_weak_progress, ind_atr_th_bps) WHERE ((r_multiple IS NOT NULL) AND ((tp1_hit = true) OR (r_multiple > (0)::double precision)));


--
-- Name: idx_trades_closed_p0_exit; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX idx_trades_closed_p0_exit ON public.trades_closed_p0 USING btree (exit_ts DESC);


--
-- Name: idx_trades_closed_p0_order_exitms; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX idx_trades_closed_p0_order_exitms ON public.trades_closed_p0 USING btree (order_id, exit_ts_ms);


--
-- Name: idx_trades_closed_p0_order_id; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX idx_trades_closed_p0_order_id ON public.trades_closed_p0 USING btree (order_id);


--
-- Name: idx_trades_closed_p0_regime_exit; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX idx_trades_closed_p0_regime_exit ON public.trades_closed_p0 USING btree (regime, exit_ts DESC);


--
-- Name: idx_trades_closed_p0_scenario_exit; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX idx_trades_closed_p0_scenario_exit ON public.trades_closed_p0 USING btree (scenario, exit_ts DESC);


--
-- Name: idx_trades_closed_p0_session_exit; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX idx_trades_closed_p0_session_exit ON public.trades_closed_p0 USING btree (session, exit_ts DESC);


--
-- Name: idx_trades_closed_sid; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX idx_trades_closed_sid ON public.trades_closed USING btree (sid);


--
-- Name: idx_trades_closed_source_symbol_exit; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX idx_trades_closed_source_symbol_exit ON public.trades_closed USING btree (source, symbol, exit_ts);


--
-- Name: idx_trades_closed_symbol_exit; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX idx_trades_closed_symbol_exit ON public.trades_closed USING btree (symbol, exit_ts);


--
-- Name: ix_edge_gate_events_passed_ts; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX ix_edge_gate_events_passed_ts ON public.edge_gate_events USING btree (passed, ts_ms DESC);


--
-- Name: ix_edge_gate_events_symbol_ts; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX ix_edge_gate_events_symbol_ts ON public.edge_gate_events USING btree (symbol, ts_ms DESC);


--
-- Name: market_daily_ohlc_symbol_date_idx; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX market_daily_ohlc_symbol_date_idx ON public.market_daily_ohlc USING btree (symbol, date DESC);


--
-- Name: mv_exec_slippage_eval_1h_stats_bucket_t_idx; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX mv_exec_slippage_eval_1h_stats_bucket_t_idx ON public.mv_exec_slippage_eval_1h_stats USING btree (exec_regime_bucket, t DESC);


--
-- Name: mv_exec_slippage_eval_1h_stats_sym_t_idx; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX mv_exec_slippage_eval_1h_stats_sym_t_idx ON public.mv_exec_slippage_eval_1h_stats USING btree (sym, t DESC);


--
-- Name: mv_exec_slippage_eval_1h_stats_v2_ux; Type: INDEX; Schema: public; Owner: trading
--

CREATE UNIQUE INDEX mv_exec_slippage_eval_1h_stats_v2_ux ON public.mv_exec_slippage_eval_1h_stats_v2 USING btree (t, sym, exec_regime_bucket);


--
-- Name: news_analysis_symbol_ts_idx; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX news_analysis_symbol_ts_idx ON public.news_analysis USING btree (symbol, ts_ms DESC);


--
-- Name: news_analysis_ts_idx; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX news_analysis_ts_idx ON public.news_analysis USING btree (ts_ms DESC);


--
-- Name: news_features_symbol_ts_idx; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX news_features_symbol_ts_idx ON public.news_features_symbol USING btree (ts_ms DESC);


--
-- Name: of_gate_metrics_quarantine_dq_ts_idx; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX of_gate_metrics_quarantine_dq_ts_idx ON public.of_gate_metrics_quarantine USING btree (dq_code, ts DESC);


--
-- Name: of_gate_metrics_quarantine_ts_idx; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX of_gate_metrics_quarantine_ts_idx ON public.of_gate_metrics_quarantine USING btree (ts DESC);


--
-- Name: of_gate_metrics_reason_ts_idx; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX of_gate_metrics_reason_ts_idx ON public.of_gate_metrics USING btree (reason_code, ts DESC);


--
-- Name: of_gate_metrics_scenario_ts_idx; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX of_gate_metrics_scenario_ts_idx ON public.of_gate_metrics USING btree (scenario_v4, ts DESC);


--
-- Name: of_gate_metrics_symbol_ts_idx; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX of_gate_metrics_symbol_ts_idx ON public.of_gate_metrics USING btree (symbol, ts DESC);


--
-- Name: of_gate_metrics_ts_idx; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX of_gate_metrics_ts_idx ON public.of_gate_metrics USING btree (ts DESC);


--
-- Name: of_gate_q_dq_code_ts_idx; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX of_gate_q_dq_code_ts_idx ON public.of_gate_metrics_quarantine USING btree (dq_code, ts DESC);


--
-- Name: of_gate_q_symbol_ts_idx; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX of_gate_q_symbol_ts_idx ON public.of_gate_metrics_quarantine USING btree (symbol, ts DESC);


--
-- Name: position_events_meta_gin_idx; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX position_events_meta_gin_idx ON public.position_events USING gin (meta_json) WHERE (meta_json IS NOT NULL);


--
-- Name: position_events_payload_gin_idx; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX position_events_payload_gin_idx ON public.position_events USING gin (payload_json);


--
-- Name: position_events_position_ts_idx; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX position_events_position_ts_idx ON public.position_events USING btree (position_id, ts DESC) WHERE (position_id IS NOT NULL);


--
-- Name: position_events_symbol_ts_idx; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX position_events_symbol_ts_idx ON public.position_events USING btree (symbol, ts DESC) WHERE (symbol IS NOT NULL);


--
-- Name: position_events_type_ts_idx; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX position_events_type_ts_idx ON public.position_events USING btree (event_type, ts DESC);


--
-- Name: signal_confidence_scores_sid_ts_idx; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX signal_confidence_scores_sid_ts_idx ON public.signal_confidence_scores USING btree (sid, ts DESC);


--
-- Name: signal_confidence_scores_symbol_ts_idx; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX signal_confidence_scores_symbol_ts_idx ON public.signal_confidence_scores USING btree (symbol, ts DESC);


--
-- Name: signal_confidence_scores_ts_idx; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX signal_confidence_scores_ts_idx ON public.signal_confidence_scores USING btree (ts DESC);


--
-- Name: signal_experiment_snapshot_as_of_idx; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX signal_experiment_snapshot_as_of_idx ON public.signal_experiment_snapshot USING btree (as_of DESC);


--
-- Name: signal_family_regime_state_ts_state_idx; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX signal_family_regime_state_ts_state_idx ON public.signal_family_regime_state USING btree (ts_state DESC);


--
-- Name: trade_kpi_liqmap_v1_liqmap_kpi_gin; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX trade_kpi_liqmap_v1_liqmap_kpi_gin ON public.trade_kpi_liqmap_v1 USING gin (liqmap_kpi jsonb_path_ops);


--
-- Name: trade_kpi_liqmap_v1_symbol_ts_idx; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX trade_kpi_liqmap_v1_symbol_ts_idx ON public.trade_kpi_liqmap_v1 USING btree (symbol, ts DESC);


--
-- Name: trade_kpi_liqmap_v1_trade_id_ts_idx; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX trade_kpi_liqmap_v1_trade_id_ts_idx ON public.trade_kpi_liqmap_v1 USING btree (trade_id, ts DESC);


--
-- Name: trade_kpi_liqmap_v1_ts_idx; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX trade_kpi_liqmap_v1_ts_idx ON public.trade_kpi_liqmap_v1 USING btree (ts DESC);


--
-- Name: trades_closed_p0_exit_ts_idx; Type: INDEX; Schema: public; Owner: trading
--

CREATE INDEX trades_closed_p0_exit_ts_idx ON public.trades_closed_p0 USING btree (exit_ts DESC);


--
-- Name: ux_edge_gate_events_dedupe; Type: INDEX; Schema: public; Owner: trading
--

CREATE UNIQUE INDEX ux_edge_gate_events_dedupe ON public.edge_gate_events USING btree (signal_id, gate_name, stage, gate_version, ts_ms);


--
-- Name: _hyper_7_19_chunk trg_populate_exit_ts; Type: TRIGGER; Schema: _timescaledb_internal; Owner: trading
--

CREATE TRIGGER trg_populate_exit_ts BEFORE INSERT OR UPDATE ON _timescaledb_internal._hyper_7_19_chunk FOR EACH ROW EXECUTE FUNCTION public.populate_exit_ts();


--
-- Name: _hyper_7_1_chunk trg_populate_exit_ts; Type: TRIGGER; Schema: _timescaledb_internal; Owner: trading
--

CREATE TRIGGER trg_populate_exit_ts BEFORE INSERT OR UPDATE ON _timescaledb_internal._hyper_7_1_chunk FOR EACH ROW EXECUTE FUNCTION public.populate_exit_ts();


--
-- Name: _hyper_7_2_chunk trg_populate_exit_ts; Type: TRIGGER; Schema: _timescaledb_internal; Owner: trading
--

CREATE TRIGGER trg_populate_exit_ts BEFORE INSERT OR UPDATE ON _timescaledb_internal._hyper_7_2_chunk FOR EACH ROW EXECUTE FUNCTION public.populate_exit_ts();


--
-- Name: _hyper_7_35_chunk trg_populate_exit_ts; Type: TRIGGER; Schema: _timescaledb_internal; Owner: trading
--

CREATE TRIGGER trg_populate_exit_ts BEFORE INSERT OR UPDATE ON _timescaledb_internal._hyper_7_35_chunk FOR EACH ROW EXECUTE FUNCTION public.populate_exit_ts();


--
-- Name: _hyper_7_3_chunk trg_populate_exit_ts; Type: TRIGGER; Schema: _timescaledb_internal; Owner: trading
--

CREATE TRIGGER trg_populate_exit_ts BEFORE INSERT OR UPDATE ON _timescaledb_internal._hyper_7_3_chunk FOR EACH ROW EXECUTE FUNCTION public.populate_exit_ts();


--
-- Name: _hyper_7_59_chunk trg_populate_exit_ts; Type: TRIGGER; Schema: _timescaledb_internal; Owner: trading
--

CREATE TRIGGER trg_populate_exit_ts BEFORE INSERT OR UPDATE ON _timescaledb_internal._hyper_7_59_chunk FOR EACH ROW EXECUTE FUNCTION public.populate_exit_ts();


--
-- Name: _hyper_7_9_chunk trg_populate_exit_ts; Type: TRIGGER; Schema: _timescaledb_internal; Owner: trading
--

CREATE TRIGGER trg_populate_exit_ts BEFORE INSERT OR UPDATE ON _timescaledb_internal._hyper_7_9_chunk FOR EACH ROW EXECUTE FUNCTION public.populate_exit_ts();


--
-- Name: _hyper_7_19_chunk trg_populate_exit_ts_p0; Type: TRIGGER; Schema: _timescaledb_internal; Owner: trading
--

CREATE TRIGGER trg_populate_exit_ts_p0 BEFORE INSERT OR UPDATE ON _timescaledb_internal._hyper_7_19_chunk FOR EACH ROW EXECUTE FUNCTION public.populate_exit_ts_p0();


--
-- Name: _hyper_7_1_chunk trg_populate_exit_ts_p0; Type: TRIGGER; Schema: _timescaledb_internal; Owner: trading
--

CREATE TRIGGER trg_populate_exit_ts_p0 BEFORE INSERT OR UPDATE ON _timescaledb_internal._hyper_7_1_chunk FOR EACH ROW EXECUTE FUNCTION public.populate_exit_ts_p0();


--
-- Name: _hyper_7_2_chunk trg_populate_exit_ts_p0; Type: TRIGGER; Schema: _timescaledb_internal; Owner: trading
--

CREATE TRIGGER trg_populate_exit_ts_p0 BEFORE INSERT OR UPDATE ON _timescaledb_internal._hyper_7_2_chunk FOR EACH ROW EXECUTE FUNCTION public.populate_exit_ts_p0();


--
-- Name: _hyper_7_35_chunk trg_populate_exit_ts_p0; Type: TRIGGER; Schema: _timescaledb_internal; Owner: trading
--

CREATE TRIGGER trg_populate_exit_ts_p0 BEFORE INSERT OR UPDATE ON _timescaledb_internal._hyper_7_35_chunk FOR EACH ROW EXECUTE FUNCTION public.populate_exit_ts_p0();


--
-- Name: _hyper_7_3_chunk trg_populate_exit_ts_p0; Type: TRIGGER; Schema: _timescaledb_internal; Owner: trading
--

CREATE TRIGGER trg_populate_exit_ts_p0 BEFORE INSERT OR UPDATE ON _timescaledb_internal._hyper_7_3_chunk FOR EACH ROW EXECUTE FUNCTION public.populate_exit_ts_p0();


--
-- Name: _hyper_7_59_chunk trg_populate_exit_ts_p0; Type: TRIGGER; Schema: _timescaledb_internal; Owner: trading
--

CREATE TRIGGER trg_populate_exit_ts_p0 BEFORE INSERT OR UPDATE ON _timescaledb_internal._hyper_7_59_chunk FOR EACH ROW EXECUTE FUNCTION public.populate_exit_ts_p0();


--
-- Name: _hyper_7_9_chunk trg_populate_exit_ts_p0; Type: TRIGGER; Schema: _timescaledb_internal; Owner: trading
--

CREATE TRIGGER trg_populate_exit_ts_p0 BEFORE INSERT OR UPDATE ON _timescaledb_internal._hyper_7_9_chunk FOR EACH ROW EXECUTE FUNCTION public.populate_exit_ts_p0();


--
-- Name: trades_closed_p0 trg_populate_exit_ts; Type: TRIGGER; Schema: public; Owner: trading
--

CREATE TRIGGER trg_populate_exit_ts BEFORE INSERT OR UPDATE ON public.trades_closed_p0 FOR EACH ROW EXECUTE FUNCTION public.populate_exit_ts();


--
-- Name: trades_closed_p0 trg_populate_exit_ts_p0; Type: TRIGGER; Schema: public; Owner: trading
--

CREATE TRIGGER trg_populate_exit_ts_p0 BEFORE INSERT OR UPDATE ON public.trades_closed_p0 FOR EACH ROW EXECUTE FUNCTION public.populate_exit_ts_p0();


--
-- Name: ticks trg_populate_ticks_ts; Type: TRIGGER; Schema: public; Owner: trading
--

CREATE TRIGGER trg_populate_ticks_ts BEFORE INSERT OR UPDATE ON public.ticks FOR EACH ROW EXECUTE FUNCTION public.populate_ticks_ts();


--
-- Name: trades_closed trg_populate_trades_closed_ts; Type: TRIGGER; Schema: public; Owner: trading
--

CREATE TRIGGER trg_populate_trades_closed_ts BEFORE INSERT OR UPDATE ON public.trades_closed FOR EACH ROW EXECUTE FUNCTION public.populate_trades_closed_ts();


--
-- Name: signal_execution_plan signal_execution_plan_signal_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: trading
--

ALTER TABLE ONLY public.signal_execution_plan
    ADD CONSTRAINT signal_execution_plan_signal_id_fkey FOREIGN KEY (signal_id) REFERENCES public.signals(signal_id) ON DELETE CASCADE;


--
-- Name: signal_performance signal_performance_signal_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: trading
--

ALTER TABLE ONLY public.signal_performance
    ADD CONSTRAINT signal_performance_signal_id_fkey FOREIGN KEY (signal_id) REFERENCES public.signals(signal_id) ON DELETE CASCADE;


--
-- Name: SCHEMA public; Type: ACL; Schema: -; Owner: pg_database_owner
--

GRANT ALL ON SCHEMA public TO trading;
GRANT ALL ON SCHEMA public TO scanner;


--
-- Name: TABLE calibration_state; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.calibration_state TO trading;
GRANT ALL ON TABLE public.calibration_state TO scanner;


--
-- Name: TABLE microbars; Type: ACL; Schema: public; Owner: postgres
--

GRANT ALL ON TABLE public.microbars TO trading;
GRANT ALL ON TABLE public.microbars TO scanner;


--
-- Name: TABLE trades_closed; Type: ACL; Schema: public; Owner: trading
--

GRANT SELECT ON TABLE public.trades_closed TO scanner;


--
-- Name: TABLE regime_quantiles; Type: ACL; Schema: public; Owner: trading
--

GRANT ALL ON TABLE public.regime_quantiles TO scanner;


--
-- Name: TABLE regime_snapshot; Type: ACL; Schema: public; Owner: trading
--

GRANT ALL ON TABLE public.regime_snapshot TO scanner;


--
-- Name: SEQUENCE regime_snapshot_id_seq; Type: ACL; Schema: public; Owner: trading
--

GRANT ALL ON SEQUENCE public.regime_snapshot_id_seq TO scanner;


--
-- Name: DEFAULT PRIVILEGES FOR SEQUENCES; Type: DEFAULT ACL; Schema: public; Owner: postgres
--

ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public GRANT ALL ON SEQUENCES TO trading;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public GRANT ALL ON SEQUENCES TO scanner;


--
-- Name: DEFAULT PRIVILEGES FOR SEQUENCES; Type: DEFAULT ACL; Schema: public; Owner: trading
--

ALTER DEFAULT PRIVILEGES FOR ROLE trading IN SCHEMA public GRANT ALL ON SEQUENCES TO trading;


--
-- Name: DEFAULT PRIVILEGES FOR TABLES; Type: DEFAULT ACL; Schema: public; Owner: postgres
--

ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public GRANT ALL ON TABLES TO trading;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public GRANT ALL ON TABLES TO scanner;


--
-- Name: DEFAULT PRIVILEGES FOR TABLES; Type: DEFAULT ACL; Schema: public; Owner: trading
--

ALTER DEFAULT PRIVILEGES FOR ROLE trading IN SCHEMA public GRANT ALL ON TABLES TO trading;


--
-- PostgreSQL database dump complete
--

\unrestrict z7BD4H2cloFKfdb8aqXbsWFwJg6EtIX1JOrcZHT9hSmmBueWgz3IuUWj7daYi8Q

