--
-- PostgreSQL database dump
--

SET statement_timeout = 0;
SET client_encoding = 'SQL_ASCII';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

--
-- Name: postgres; Type: COMMENT; Schema: -; Owner: postgres
--

COMMENT ON DATABASE penguindb IS 'default administrative connection database';


--
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;


--
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';


SET search_path = public, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: bmsql_config; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE bmsql_config (
    cfg_name character varying(30) NOT NULL PRIMARY KEY,
    cfg_value character varying(50)
) with(storage_engine=rocksdb);


ALTER TABLE public.bmsql_config OWNER TO gpadmin;

--
-- Name: bmsql_customer; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE bmsql_customer (
    c_w_id integer NOT NULL,
    c_d_id integer NOT NULL,
    c_id integer NOT NULL,
    c_discount numeric(4,4),
    c_credit character(2),
    c_last character varying(16),
    c_first character varying(16),
    c_credit_lim numeric(12,2),
    c_balance numeric(12,2),
    c_ytd_payment numeric(12,2),
    c_payment_cnt integer,
    c_delivery_cnt integer,
    c_street_1 character varying(20),
    c_street_2 character varying(20),
    c_city character varying(20),
    c_state character(2),
    c_zip character(9),
    c_phone character(16),
    c_since timestamp without time zone,
    c_middle character(2),
    c_data character varying(500),
    PRIMARY KEY (c_w_id, c_d_id, c_id)
) with(storage_engine=rocksdb);


ALTER TABLE public.bmsql_customer OWNER TO gpadmin;

--
-- Name: bmsql_district; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE bmsql_district (
    d_w_id integer NOT NULL,
    d_id integer NOT NULL,
    d_ytd numeric(12,2),
    d_tax numeric(4,4),
    d_next_o_id integer,
    d_name character varying(10),
    d_street_1 character varying(20),
    d_street_2 character varying(20),
    d_city character varying(20),
    d_state character(2),
    d_zip character(9),
    PRIMARY KEY (d_w_id, d_id)
) with(storage_engine=rocksdb);


ALTER TABLE public.bmsql_district OWNER TO gpadmin;

--
-- Name: bmsql_hist_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE bmsql_hist_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.bmsql_hist_id_seq OWNER TO gpadmin;

--
-- Name: bmsql_history; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE bmsql_history (
    hist_id integer DEFAULT nextval('bmsql_hist_id_seq'::regclass) NOT NULL,
    h_c_id integer,
    h_c_d_id integer,
    h_c_w_id integer,
    h_d_id integer,
    h_w_id integer,
    h_date timestamp without time zone,
    h_amount numeric(6,2),
    h_data character varying(24),
    PRIMARY KEY (hist_id)
) with(storage_engine=rocksdb);


ALTER TABLE public.bmsql_history OWNER TO gpadmin;

--
-- Name: bmsql_item; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE bmsql_item (
    i_id integer NOT NULL,
    i_name character varying(24),
    i_price numeric(5,2),
    i_data character varying(50),
    i_im_id integer,
    PRIMARY KEY (i_id)
) with(storage_engine=rocksdb);


ALTER TABLE public.bmsql_item OWNER TO gpadmin;

--
-- Name: bmsql_new_order; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE bmsql_new_order (
    no_w_id integer NOT NULL,
    no_d_id integer NOT NULL,
    no_o_id integer NOT NULL,
    PRIMARY KEY (no_w_id, no_d_id, no_o_id)
) with(storage_engine=rocksdb);


ALTER TABLE public.bmsql_new_order OWNER TO gpadmin;

--
-- Name: bmsql_oorder; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE bmsql_oorder (
    o_w_id integer NOT NULL,
    o_d_id integer NOT NULL,
    o_id integer NOT NULL,
    o_c_id integer,
    o_carrier_id integer,
    o_ol_cnt integer,
    o_all_local integer,
    o_entry_d timestamp without time zone,
    PRIMARY KEY (o_w_id, o_d_id, o_id)
) with(storage_engine=rocksdb);


ALTER TABLE public.bmsql_oorder OWNER TO gpadmin;

--
-- Name: bmsql_order_line; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE bmsql_order_line (
    ol_w_id integer NOT NULL,
    ol_d_id integer NOT NULL,
    ol_o_id integer NOT NULL,
    ol_number integer NOT NULL,
    ol_i_id integer NOT NULL,
    ol_delivery_d timestamp without time zone,
    ol_amount numeric(6,2),
    ol_supply_w_id integer,
    ol_quantity integer,
    ol_dist_info character(24),
    PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number)
) with(storage_engine=rocksdb);


ALTER TABLE public.bmsql_order_line OWNER TO gpadmin;

--
-- Name: bmsql_stock; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE bmsql_stock (
    s_w_id integer NOT NULL,
    s_i_id integer NOT NULL,
    s_quantity integer,
    s_ytd integer,
    s_order_cnt integer,
    s_remote_cnt integer,
    s_data character varying(50),
    s_dist_01 character(24),
    s_dist_02 character(24),
    s_dist_03 character(24),
    s_dist_04 character(24),
    s_dist_05 character(24),
    s_dist_06 character(24),
    s_dist_07 character(24),
    s_dist_08 character(24),
    s_dist_09 character(24),
    s_dist_10 character(24),
    PRIMARY KEY (s_w_id, s_i_id)
) with(storage_engine=rocksdb);


ALTER TABLE public.bmsql_stock OWNER TO gpadmin;

--
-- Name: bmsql_warehouse; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE bmsql_warehouse (
    w_id integer NOT NULL,
    w_ytd numeric(12,2),
    w_tax numeric(4,4),
    w_name character varying(10),
    w_street_1 character varying(20),
    w_street_2 character varying(20),
    w_city character varying(20),
    w_state character(2),
    w_zip character(9),
    PRIMARY KEY (w_id)
) with(storage_engine=rocksdb);


ALTER TABLE public.bmsql_warehouse OWNER TO gpadmin;

--
-- Name: bmsql_config_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres; Tablespace: 
--

-- ALTER TABLE ONLY bmsql_config
--     ADD CONSTRAINT bmsql_config_pkey PRIMARY KEY (cfg_name);


--
-- Name: bmsql_customer_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres; Tablespace: 
--

-- ALTER TABLE ONLY bmsql_customer
--     ADD CONSTRAINT bmsql_customer_pkey PRIMARY KEY (c_w_id, c_d_id, c_id);


--
-- Name: bmsql_district_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres; Tablespace: 
--

-- ALTER TABLE ONLY bmsql_district
--     ADD CONSTRAINT bmsql_district_pkey PRIMARY KEY (d_w_id, d_id);


--
-- Name: bmsql_history_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres; Tablespace: 
--

-- ALTER TABLE ONLY bmsql_history
--     ADD CONSTRAINT bmsql_history_pkey PRIMARY KEY (hist_id);


--
-- Name: bmsql_item_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres; Tablespace: 
--

-- ALTER TABLE ONLY bmsql_item
--     ADD CONSTRAINT bmsql_item_pkey PRIMARY KEY (i_id);


--
-- Name: bmsql_new_order_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres; Tablespace: 
--

-- ALTER TABLE ONLY bmsql_new_order
--     ADD CONSTRAINT bmsql_new_order_pkey PRIMARY KEY (no_w_id, no_d_id, no_o_id);


--
-- Name: bmsql_oorder_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres; Tablespace: 
--

-- ALTER TABLE ONLY bmsql_oorder
--     ADD CONSTRAINT bmsql_oorder_pkey PRIMARY KEY (o_w_id, o_d_id, o_id);


--
-- Name: bmsql_order_line_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres; Tablespace: 
--

-- ALTER TABLE ONLY bmsql_order_line
--     ADD CONSTRAINT bmsql_order_line_pkey PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number);


--
-- Name: bmsql_stock_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres; Tablespace: 
--

-- ALTER TABLE ONLY bmsql_stock
--     ADD CONSTRAINT bmsql_stock_pkey PRIMARY KEY (s_w_id, s_i_id);


--
-- Name: bmsql_warehouse_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres; Tablespace: 
--

-- ALTER TABLE ONLY bmsql_warehouse
--     ADD CONSTRAINT bmsql_warehouse_pkey PRIMARY KEY (w_id);


--
-- Name: bmsql_customer_idx1; Type: INDEX; Schema: public; Owner: postgres; Tablespace: 
--

-- CREATE INDEX bmsql_customer_idx1 ON bmsql_customer USING btree (c_w_id, c_d_id, c_last, c_first);


--
-- Name: bmsql_oorder_idx1; Type: INDEX; Schema: public; Owner: postgres; Tablespace: 
--

-- CREATE UNIQUE INDEX bmsql_oorder_idx1 ON bmsql_oorder USING btree (o_w_id, o_d_id, o_carrier_id, o_id);


--
-- Name: c_district_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

-- ALTER TABLE ONLY bmsql_customer
--     ADD CONSTRAINT c_district_fkey FOREIGN KEY (c_w_id, c_d_id) REFERENCES bmsql_district(d_w_id, d_id);


--
-- Name: d_warehouse_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

-- ALTER TABLE ONLY bmsql_district
--     ADD CONSTRAINT d_warehouse_fkey FOREIGN KEY (d_w_id) REFERENCES bmsql_warehouse(w_id);


--
-- Name: h_customer_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

-- ALTER TABLE ONLY bmsql_history
--     ADD CONSTRAINT h_customer_fkey FOREIGN KEY (h_c_w_id, h_c_d_id, h_c_id) REFERENCES bmsql_customer(c_w_id, c_d_id, c_id);


--
-- Name: h_district_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

-- ALTER TABLE ONLY bmsql_history
--     ADD CONSTRAINT h_district_fkey FOREIGN KEY (h_w_id, h_d_id) REFERENCES bmsql_district(d_w_id, d_id);


--
-- Name: no_order_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

-- ALTER TABLE ONLY bmsql_new_order
--     ADD CONSTRAINT no_order_fkey FOREIGN KEY (no_w_id, no_d_id, no_o_id) REFERENCES bmsql_oorder(o_w_id, o_d_id, o_id);


--
-- Name: o_customer_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

-- ALTER TABLE ONLY bmsql_oorder
--     ADD CONSTRAINT o_customer_fkey FOREIGN KEY (o_w_id, o_d_id, o_c_id) REFERENCES bmsql_customer(c_w_id, c_d_id, c_id);


--
-- Name: ol_order_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

-- ALTER TABLE ONLY bmsql_order_line
--     ADD CONSTRAINT ol_order_fkey FOREIGN KEY (ol_w_id, ol_d_id, ol_o_id) REFERENCES bmsql_oorder(o_w_id, o_d_id, o_id);


--
-- Name: ol_stock_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

-- ALTER TABLE ONLY bmsql_order_line
--     ADD CONSTRAINT ol_stock_fkey FOREIGN KEY (ol_supply_w_id, ol_i_id) REFERENCES bmsql_stock(s_w_id, s_i_id);


--
-- Name: s_item_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

-- ALTER TABLE ONLY bmsql_stock
--     ADD CONSTRAINT s_item_fkey FOREIGN KEY (s_i_id) REFERENCES bmsql_item(i_id);


--
-- Name: s_warehouse_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

-- ALTER TABLE ONLY bmsql_stock
--     ADD CONSTRAINT s_warehouse_fkey FOREIGN KEY (s_w_id) REFERENCES bmsql_warehouse(w_id);


--
-- Name: public; Type: ACL; Schema: -; Owner: postgres
--

REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM gpadmin;
GRANT ALL ON SCHEMA public TO gpadmin;
GRANT ALL ON SCHEMA public TO PUBLIC;


--
-- PostgreSQL database dump complete
--
