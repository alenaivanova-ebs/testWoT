with cte_hist as
         (select stg.battle_id, stg.name, stg.type, stg.sys_load_date,
                 lead(stg.sys_load_date) OVER (PARTITION BY stg.battle_id ORDER BY stg.sys_load_date) as to_date,
                 coalesce(lead(stg.name) OVER (PARTITION BY stg.battle_id ORDER BY stg.sys_load_date), lag(stg.name) OVER (PARTITION BY stg.battle_id ORDER BY stg.sys_load_date)) as new_name,
                 coalesce(lead(stg.type) OVER (PARTITION BY stg.battle_id ORDER BY stg.sys_load_date), lag(stg.type) OVER (PARTITION BY stg.battle_id ORDER BY stg.sys_load_date) ) as   new_type,
                 max(stg.sys_load_date) over () as max_sys_load_date,
                 dt.sys_load_date as next_sys_load_date
          from stg_battle stg
                   left join (select distinct sys_load_date from stg_battle) dt
                             on dt.sys_load_date>stg.sys_load_date
         )

SELECT count(*) as number_of_mismatches
FROM (
         select tst.battle_id, tst.name, tst.type, case when tst.eff_end_date = '2099-12-31 00:00:00' then 1 else 0 end as is_current,
                tst.eff_start_date, tst.eff_end_date
         from
             (select battle_id, name, type, sys_load_date as eff_start_date, next_sys_load_date as eff_end_date
              from cte_hist
              where sys_load_date <> max_sys_load_date and new_name is null and new_type is null
              union all
              select battle_id, name, type, sys_load_date as eff_start_date, '2099-12-31 00:00:00' as eff_end_date
              from cte_hist
              where sys_load_date = max_sys_load_date and new_name is null and new_type is null
              union all
              select g.battle_id, g.name, g.type, g.eff_start_date, g.eff_end_date
              from (
                       select battle_id, name, type, sys_load_date as eff_start_date, '2099-12-31 00:00:00' as eff_end_date,
                              rank() over (partition by battle_id ORDER BY sys_load_date) rnk
                       from cte_hist s
                       where s.name = s.new_name and s.type = s.new_type
                   ) g where g.rnk=1
              union all
              select battle_id, name, type, sys_load_date as eff_start_date, coalesce(to_date, '2099-12-31 00:00:00') as eff_end_date
              from cte_hist s
              where s.name <> s.new_name or s.type <> s.new_type
             ) tst
         union all
         select
             battle_id, name, type, is_current, eff_start_date, eff_end_date
         from battle)  tbl
GROUP BY battle_id, name, type, is_current, eff_start_date, eff_end_date
HAVING count(*) = 1
ORDER BY battle_id;