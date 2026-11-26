-- Seed the pgcap capture PostgreSQL (compose.yml) with the schema Arneb's
-- client-smoke cluster exposes: the 8 TPC-H tables in schema "default", with
-- the column types Arneb reports in information_schema.columns. Rows: all of
-- nation, 2 orders and 1 customer -- enough for every probe question to
-- return data. The golden rows of the Metabase corpus test
-- (docs/superpowers/specs/2026-11-21-g6-stage-2b2/metabase-pg17-expected.json)
-- were dumped from an instance seeded like this, before the search_path
-- line below was added (it changes no metadata statement's result).
--
--   psql -h 127.0.0.1 -p 15499 -U smoke -d arneb -v ON_ERROR_STOP=1 -f seed_pgcap.sql
CREATE SCHEMA "default";
CREATE TABLE "default".nation (n_nationkey int4 NOT NULL, n_name varchar NOT NULL, n_regionkey int4 NOT NULL, n_comment varchar NOT NULL);
CREATE TABLE "default".region (r_regionkey int4 NOT NULL, r_name varchar NOT NULL, r_comment varchar NOT NULL);
CREATE TABLE "default".partsupp (ps_partkey int8 NOT NULL, ps_suppkey int8 NOT NULL, ps_availqty int8 NOT NULL, ps_supplycost numeric NOT NULL, ps_comment varchar NOT NULL);
CREATE TABLE "default".supplier (s_suppkey int8 NOT NULL, s_name varchar NOT NULL, s_address varchar NOT NULL, s_nationkey int4 NOT NULL, s_phone varchar NOT NULL, s_acctbal numeric NOT NULL, s_comment varchar NOT NULL);
CREATE TABLE "default".lineitem (l_orderkey int8 NOT NULL, l_partkey int8 NOT NULL, l_suppkey int8 NOT NULL, l_linenumber int8 NOT NULL, l_quantity numeric NOT NULL, l_extendedprice numeric NOT NULL, l_discount numeric NOT NULL, l_tax numeric NOT NULL, l_returnflag varchar NOT NULL, l_linestatus varchar NOT NULL, l_shipdate date NOT NULL, l_commitdate date NOT NULL, l_receiptdate date NOT NULL, l_shipinstruct varchar NOT NULL, l_shipmode varchar NOT NULL, l_comment varchar NOT NULL);
CREATE TABLE "default".part (p_partkey int8 NOT NULL, p_name varchar NOT NULL, p_mfgr varchar NOT NULL, p_brand varchar NOT NULL, p_type varchar NOT NULL, p_size int4 NOT NULL, p_container varchar NOT NULL, p_retailprice numeric NOT NULL, p_comment varchar NOT NULL);
CREATE TABLE "default".orders (o_orderkey int8 NOT NULL, o_custkey int8 NOT NULL, o_orderstatus varchar NOT NULL, o_totalprice numeric NOT NULL, o_orderdate date NOT NULL, o_orderpriority varchar NOT NULL, o_clerk varchar NOT NULL, o_shippriority int4 NOT NULL, o_comment varchar NOT NULL);
CREATE TABLE "default".customer (c_custkey int8 NOT NULL, c_name varchar NOT NULL, c_address varchar NOT NULL, c_nationkey int4 NOT NULL, c_phone varchar NOT NULL, c_acctbal numeric NOT NULL, c_mktsegment varchar NOT NULL, c_comment varchar NOT NULL);

-- Arneb resolves unqualified names in schema "default"; do the same here so
-- the probe's native SQL questions run unchanged against both.
ALTER DATABASE arneb SET search_path = "default", public;

INSERT INTO "default".nation VALUES
  (0, 'ALGERIA', 0, ' haggle. carefully final deposits detect slyly agai'),
  (1, 'ARGENTINA', 1, 'al foxes promise slyly according to the regular accounts. bold requests alon'),
  (2, 'BRAZIL', 1, 'y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special '),
  (3, 'CANADA', 1, 'eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold'),
  (4, 'EGYPT', 4, 'y above the carefully unusual theodolites. final dugouts are quickly across the furiously regular d'),
  (5, 'ETHIOPIA', 0, 'ven packages wake quickly. regu'),
  (6, 'FRANCE', 3, 'refully final requests. regular, ironi'),
  (7, 'GERMANY', 3, 'l platelets. regular accounts x-ray: unusual, regular acco'),
  (8, 'INDIA', 2, 'ss excuses cajole slyly across the packages. deposits print aroun'),
  (9, 'INDONESIA', 2, ' slyly express asymptotes. regular deposits haggle slyly. carefully ironic hockey players sleep blithely. carefull'),
  (10, 'IRAN', 4, 'efully alongside of the slyly final dependencies. '),
  (11, 'IRAQ', 4, 'nic deposits boost atop the quickly final requests? quickly regula'),
  (12, 'JAPAN', 2, 'ously. final, express gifts cajole a'),
  (13, 'JORDAN', 4, 'ic deposits are blithely about the carefully regular pa'),
  (14, 'KENYA', 0, ' pending excuses haggle furiously deposits. pending, express pinto beans wake fluffily past t'),
  (15, 'MOROCCO', 0, 'rns. blithely bold courts among the closely regular packages use furiously bold platelets?'),
  (16, 'MOZAMBIQUE', 0, 's. ironic, unusual asymptotes wake blithely r'),
  (17, 'PERU', 1, 'platelets. blithely pending dependencies use fluffily across the even pinto beans. carefully silent accoun'),
  (18, 'CHINA', 2, 'c dependencies. furiously express notornis sleep slyly regular accounts. ideas sleep. depos'),
  (19, 'ROMANIA', 3, 'ular asymptotes are about the furious multipliers. express dependencies nag above the ironically ironic account'),
  (20, 'SAUDI ARABIA', 4, 'ts. silent requests haggle. closely express packages sleep across the blithely'),
  (21, 'VIETNAM', 2, 'hely enticingly express accounts. even, final '),
  (22, 'RUSSIA', 3, ' requests against the platelets use never according to the quickly regular pint'),
  (23, 'UNITED KINGDOM', 3, 'eans boost carefully special requests. accounts are. carefull'),
  (24, 'UNITED STATES', 1, 'y final packages. slow foxes cajole quickly. quickly silent platelets breach ironic accounts. unusual pinto be');
INSERT INTO "default".orders VALUES
  (1, 1, 'O', 100.5, '1996-01-02', '1-URGENT', 'Clerk#1', 0, 'x'),
  (2, 2, 'F', 200000, '1995-03-04', '2-HIGH', 'Clerk#2', 0, 'y');
INSERT INTO "default".customer VALUES
  (1, 'Customer#000000001', 'a', 1, 'p', 1.0, 'BUILDING', 'c');
