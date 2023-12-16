CREATE PROCEDURE game.sp_dim_battle(IN batch_date datetime)
BEGIN
DROP TABLE IF EXISTS tmp_battle;
CREATE TEMPORARY TABLE tmp_battle as
select
p.battle_id,
d.battle_sid,
p.name,
p.type,
( case when d.battle_sid is null then 1
when (d.battle_sid is not null and
(p.type <> d.type or p.name <> d.name )) then 2
else 0 end) as scd_row_type_id
from game.stg_battle p
left join  game.battle d
on p.battle_id = d.battle_id and d.is_current = 1
where p.sys_load_date = batch_date;

update battle d, tmp_battle t
set d.is_current = '0',
d.eff_end_date =  batch_date
where d.battle_sid = t.battle_sid
and t.scd_row_type_id = 2;

insert into battle(
battle_id ,
name,
type,
eff_start_date
)
select battle_id ,
name,
type,
batch_date as effective_start_date
from tmp_battle
where scd_row_type_id in (1,2);

DROP TABLE IF EXISTS  tmp_battle_inactive;
CREATE TEMPORARY TABLE tmp_battle_inactive as
select d.battle_id, d.battle_sid
from
		(select battle_id
		from game.stg_battle p
        where  p.sys_load_date = batch_date) p
		right join  game.battle d
		on p.battle_id = d.battle_id
        where  p.battle_id is null;

update battle d, tmp_battle_inactive t
set d.is_current = '0',
d.eff_end_date = batch_date
where d.battle_sid = t.battle_sid;

commit;
END