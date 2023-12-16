CREATE TABLE game.stg_battle (
                                 `battle_id` int not null,
                                 `name` varchar(45) DEFAULT NULL,
                                 `type` varchar(45) DEFAULT NULL,
                                 `sys_load_date`  datetime NOT NULL
);