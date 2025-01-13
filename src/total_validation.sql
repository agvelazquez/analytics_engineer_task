/*
TOTALS VALIDATION
This script compares the most popular measures (if not all) at the most granular level. 
The script is useful when the result of the two measures is expected to be the same.
The level of detail can be adjusted.
*/

with stg_new_model as (
--Aggregate new models
select
  order_line_id,
  sum(unit_quantity) as quantity,
  sum(line_sale) as sale,
  sum(line_promo) as promo,
  ....
from new_model
group by 
    order_line_id
)

, stg_prod as (
--Aggregate production models
select
  order_line_id,
  sum(unit_quantity) as quantity,
  sum(line_sale) as sale,
  sum(line_promo) as promo,
  ...
from prod_model
group by 
    order_line_id
)

, final as (
-- compare both measures at the given aggregation level
select 
  p.order_line_id,
  coalesce(p.quantity,0) - coalesce(s.quantity,0) as qty_diff,
  coalesce(p.sale,0) - coalesce(s.sale,0) as sale_diff,
  coalesce(p.promo,0) - coalesce(s.promo,0) as promo_diff,
  ....
from stg_prod p
inner join stg_new_model s
on p.order_line_id = s.order_line_id
where 
    1=1
)

-- Filter those only with a difference greater than 1
select *
from final
where 
  1=1
  and (
      abs(qty_diff) > 0  
      or abs(sale_diff) > 1 
      or abs(promo_diff) > 1 
      )
order by 1 desc



