CREATE DEFINER=`root`@`localhost` PROCEDURE `sp_insertrows_data`()
BEGIN
DECLARE i INT DEFAULT 1;
DROP TABLE IF EXISTS game.task2;
CREATE TABLE game.task2 (
  `id` int DEFAULT NULL
);
WHILE (i <= 100) DO
    INSERT INTO game.task2 (`id`)
    select (IF(i DIV 7 = 0, NULL, FLOOR(1 + RAND() * 50))) as id;
    SET i = i+1;
END WHILE;
END