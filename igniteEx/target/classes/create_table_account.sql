CREATE TABLE if not exists `my_db`.`account` (
  `id` INT NOT NULL,
  `name` VARCHAR(45) NOT NULL,
  `balance` DOUBLE NOT NULL,
  `lastoperation` DOUBLE NOT NULL,
  `upd_date` BIGINT(19) NOT NULL,
  PRIMARY KEY (`id`));