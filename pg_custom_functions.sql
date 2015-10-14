CREATE TABLE column_order
(
  id serial NOT NULL,
  table_name character varying(64),
  column_name character varying(64),
  pos smallint,
  CONSTRAINT column_order_pkey PRIMARY KEY (id)
);

ALTER TABLE column_order ADD COLUMN original_column_name character varying(255) DEFAULT NULL;
ALTER table column_order RENAME TO column_meta;
ALTER SEQUENCE column_order_id_seq RENAME TO column_meta_id_seq;

CREATE TYPE dimension_or_metrics AS ENUM ('d', 'm');
ALTER TABLE column_meta ADD COLUMN d_or_m dimension_or_metrics DEFAULT 'd';

CREATE TYPE sub_data_types AS ENUM ('lat', 'lng');
ALTER TABLE column_meta ADD COLUMN sub_type sub_data_types DEFAULT NULL;
-- ALTER TYPE sub_data_types ADD VALUE 'IP' AFTER 'lng'; # for later use

CREATE OR REPLACE FUNCTION str_to_int(chartoconvert character varying)
  RETURNS integer AS
$BODY$
SELECT CASE WHEN trim($1) SIMILAR TO '[-+]?[0-9]*\.?[0-9]+' 
        THEN CAST(CAST(trim($1) AS double precision) as integer)
    ELSE NULL END;

$BODY$
  LANGUAGE sql IMMUTABLE STRICT;

-- _____________________________________________________________________________
-- [-+]?([0-9]*\.[0-9]+|[0-9]+)
-- [-+]?[0-9]*\.?[0-9]+
-- exactly 1 dot -- '[-+]?[0-9]*\.[0-9]+'
-- ______________________________________________________________________________

CREATE OR REPLACE FUNCTION str_to_float(chartoconvert character varying)
  RETURNS double precision AS
$BODY$
SELECT CASE WHEN trim($1) SIMILAR TO '[-+]?[0-9]*\.?[0-9]+' 
        THEN CAST(trim($1) AS double precision) 
    ELSE NULL END;

$BODY$
  LANGUAGE sql IMMUTABLE STRICT;

-- ___________________________________________________________________________


CREATE OR REPLACE FUNCTION str_to_boolean(chartoconvert character varying)
  RETURNS boolean AS
$BODY$
SELECT CASE WHEN lower(trim($1)) IN ('t', 'true', 'f', 'false', 'yes', 'no', 'y', 'n', '0', '1')
        THEN CAST(lower(trim($1)) AS boolean) 
    ELSE NULL END;

$BODY$
  LANGUAGE sql IMMUTABLE STRICT;

-- ___________________________________________________________________________

CREATE FUNCTION t1_inc(val integer) RETURNS integer AS $$
BEGIN
RETURN val + 1;
END; $$
LANGUAGE PLPGSQL;

-- _______________________________________________________________________

CREATE FUNCTION t1_dec(val integer) RETURNS integer AS $$
BEGIN
RETURN val - 1;
END; $$
LANGUAGE PLPGSQL;

-- _____________________________________________________
-- Median
-- http://wiki.postgresql.org/wiki/Aggregate_Median
-- ____________________________________________________

CREATE FUNCTION _final_median(anyarray) RETURNS float8 AS $$ 
  WITH q AS
  (
     SELECT val
     FROM unnest($1) val
     WHERE VAL IS NOT NULL
     ORDER BY 1
  ),
  cnt AS
  (
    SELECT COUNT(*) AS c FROM q
  )
  SELECT AVG(val)::float8
  FROM 
  (
    SELECT val FROM q
    LIMIT  2 - MOD((SELECT c FROM cnt), 2)
    OFFSET GREATEST(CEIL((SELECT c FROM cnt) / 2.0) - 1,0)  
  ) q2;
$$ LANGUAGE sql IMMUTABLE;
 
CREATE AGGREGATE median(anyelement) (
  SFUNC=array_append,
  STYPE=anyarray,
  FINALFUNC=_final_median,
  INITCOND='{}'
);
-- _______________________________________________________________________
