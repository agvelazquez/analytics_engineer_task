/*
COLUMN WISE COMPARISSON
The purpose of this script is to compare column to column two tables that should have the exact same values for each column. 
It is useful to compare the result of a migration.
*/

with stg_comparisson as (
  --Compare value of each column at order level, the source table should be aggregated to the order
select 
  o.order_id,
  case when o.column1 = n.column1 then True else False end as column1, --text comparisson
  case when coalesce(o.column2,0) = coalesce(n.column2,0) then True else False end as column2, --id comparisson
  case when abs(o.column3)-abs(n.column3) < 1 then True else False end as column3 -- numeric column --decimal error buffer
from original_table o
inner join new_table n
  on o.order_id = n.order_id
)

, stg_checkpoint as (
  --generate a concatenated column with all comparissons and one column with the column names that are False.
select 
  order_id,
  case when concat(column1, column2, column3) like '%false%' then False else True end as column_checkpoint,
  case when column1=False then 'column1, ' else '' end ||
  case when column1=False then 'column2, ' else '' end ||
  case when column2=False then 'column3, ' else ''
end as false_columns
from stg_comparisson
)

--select only those orders where there are at least one difference
select 
  order_id,
  false_columns
from stg_checkpoint
where 
  1=1
  and column_checkpoint = false
