CREATE DATABASE `realtime_db` DEFAULT CHARACTER SET utf8;

USE realtime_db;

CREATE TABLE `realtime_stat_result` (
  `indicator` INT(11) NOT NULL,
  `hour` CHAR(16) NOT NULL,
  `os_type` INT(11) NOT NULL,
  `channel` VARCHAR(64) NOT NULL,
  `version` VARCHAR(32) NOT NULL,
  `count` BIGINT(20) DEFAULT NULL,
  PRIMARY KEY (`indicator`,`hour`,`os_type`,`channel`,`version`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `realtime_user_result` (
  `indicator` INT(11) NOT NULL,
  `hour` CHAR(16) NOT NULL,
  `os_type` INT(11) NOT NULL,
  `channel` VARCHAR(64) NOT NULL,
  `version` VARCHAR(32) NOT NULL,
  `count` BIGINT(20) DEFAULT NULL,
  PRIMARY KEY (`indicator`,`hour`,`os_type`,`channel`,`version`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;