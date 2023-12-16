CREATE TABLE game.battle (
                             `battle_sid` INT UNSIGNED NOT NULL AUTO_INCREMENT,
                             `battle_id` INT NOT NULL,
                             `name` varchar(45) DEFAULT NULL,
                             `type` varchar(45) DEFAULT NULL,
                             `is_current` tinyint(1) DEFAULT '1',
                             `eff_start_date`  datetime NOT NULL,
                             `eff_end_date` datetime NOT NULL DEFAULT '2099-12-31 00:00:00',
                             PRIMARY KEY (`battle_sid`)
);