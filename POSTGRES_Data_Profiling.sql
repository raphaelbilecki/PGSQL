CREATE OR REPLACE FUNCTION public.data_profiling(
	table_value character varying,
	schema_value character varying)
 RETURNS TABLE(col_value character varying,col_type character varying, Total_value float8,count_null float8,
               distinct_count float8, min_value float8, max_value float8, avg_value float8, std_value float8)
 AS    $$
 
DECLARE 
    var_c record;    
BEGIN
   FOR var_c IN (SELECT c.table_schema, c.column_name,c.table_name
                 FROM information_schema.columns c
                 WHERE c.table_name = TABLE_VALUE
                   and c.table_schema = SCHEMA_VALUE)  
                 
LOOP
       RETURN QUERY EXECUTE 
        format(  'SELECT  c.column_name::varchar,
               			  pg_typeof(a.%I)::varchar, 
                	count(a.%I)::float,
               		sum(CASE WHEN a.%I is null THEN 1  ELSE 0 END)::float,               
          			count( distinct  a.%I)::float,               
                	min(CASE WHEN pg_typeof(a.%I)::varchar LIKE ANY (VALUES(''%%time%%''),(''%%date%%''))  THEN null 
        	                   WHEN pg_typeof(a.%I)::varchar LIKE ANY (VALUES(''%%char%%''),(''%%text%%''))  THEN null
               			       ELSE CAST( a.%I as TEXT)::float
                           END )::float,
                	max(CASE WHEN pg_typeof(a.%I)::varchar LIKE ANY (VALUES(''%%time%%''),(''%%date%%''))  THEN null
        	                   WHEN pg_typeof(a.%I)::varchar LIKE ANY (VALUES(''%%char%%''),(''%%text%%''))  THEN null
               			       ELSE CAST( a.%I as TEXT)::float
                           END )::float,               
                ROUND(AVG(CASE WHEN pg_typeof(a.%I)::varchar LIKE ANY (VALUES(''%%time%%''),(''%%date%%''))  THEN null
        	               WHEN pg_typeof(a.%I)::varchar LIKE ANY (VALUES(''%%char%%''),(''%%text%%''))  THEN null
               			   ELSE CAST( a.%I as TEXT)::float
                      END )::numeric,2)::float,               
                ROUND(stddev(CASE WHEN pg_typeof(a.%I)::varchar LIKE ANY (VALUES(''%%time%%''),(''%%date%%''))  THEN null 
        	               WHEN pg_typeof(a.%I)::varchar LIKE ANY (VALUES(''%%char%%''),(''%%text%%''))  THEN null 
               			   ELSE CAST( a.%I as TEXT)::float
                      END )::numeric,2)::float               
               
                 FROM information_schema.columns c , %I.%I a
                 WHERE c.table_name = $1
                   and c.table_schema = $2
                	and c.column_name = $3
                Group By 1,2',
               var_c.column_name
               ,var_c.column_name,var_c.column_name,var_c.column_name,var_c.column_name,var_c.column_name,
               var_c.column_name,var_c.column_name,var_c.column_name,var_c.column_name,var_c.column_name,
               var_c.column_name,var_c.column_name,var_c.column_name, var_c.column_name,var_c.column_name,
               var_c.table_schema, var_c.table_name, var_c.column_name)
        USING var_c.table_name,var_c.table_schema,var_c.column_name;

   END LOOP;
END;
$$
LANGUAGE 'plpgsql';

#Para testar chamar a Function
select * from public.data_profiling('valor da tabela','Valor do Schema');

#Autor: Raphael Bilecki Freitas

