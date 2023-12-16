CREATE PROCEDURE game.sp_insert_data()
BEGIN
truncate table game.stg_battle;
insert into game.stg_battle (battle_id, name, type, sys_load_date)
values (1, 'random', 'random', '2023-12-16 02:33:04');
insert into game.stg_battle (battle_id, name, type, sys_load_date)
values (2, 'team-training', 'team-training', '2023-12-16 02:33:04');
insert into game.stg_battle (battle_id, name, type, sys_load_date)
values (3, 'tank-company', 'tank-company','2023-12-16 02:33:04');

insert into game.stg_battle (battle_id, name, type, sys_load_date)
values (1, 'random', 'random', '2023-12-16 14:00:43');
insert into game.stg_battle (battle_id, name, type, sys_load_date)
values (3, 'tank-company-new', 'tank-company-new', '2023-12-16 14:00:43');
insert into game.stg_battle (battle_id, name, type, sys_load_date)
values (4, 'stronghold', 'stronghold', '2023-12-16 14:00:43');
commit;
END;