-- MySQL Script generated by MySQL Workbench
-- Mon Oct  1 12:58:59 2018
-- Model: New Model    Version: 1.0
-- MySQL Workbench Forward Engineering

SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='TRADITIONAL,ALLOW_INVALID_DATES';

-- -----------------------------------------------------
-- Schema 
-- -----------------------------------------------------

-- -----------------------------------------------------
-- Schema 
-- -----------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `` DEFAULT CHARACTER SET utf8 ;
USE `` ;

-- -----------------------------------------------------
-- Table ``.`mobility_summary`
-- -----------------------------------------------------
DROP TABLE IF EXISTS ``.`mobility_summary` ;

CREATE TABLE IF NOT EXISTS ``.`mobility_summary` (
  `service` VARCHAR(20) NOT NULL,
  `user` VARCHAR(45) NOT NULL,
  `date` DATE NOT NULL,
  PRIMARY KEY (`service`, `user`, `date`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table ``.`speed`
-- -----------------------------------------------------
DROP TABLE IF EXISTS ``.`speed` ;

CREATE TABLE IF NOT EXISTS ``.`speed` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `Slot1` FLOAT NULL,
  `Slot2` FLOAT NULL,
  `Slot3` FLOAT NULL,
  `Slot4` FLOAT NULL,
  `Slot5` FLOAT NULL,
  `Slot6` FLOAT NULL,
  `Slot7` FLOAT NULL,
  `Slot8` FLOAT NULL,
  `Slot9` FLOAT NULL,
  `Slot10` FLOAT NULL,
  `Slot11` FLOAT NULL,
  `Slot12` FLOAT NULL,
  `Slot13` FLOAT NULL,
  `Slot14` FLOAT NULL,
  `Slot15` FLOAT NULL,
  `Slot16` FLOAT NULL,
  `Slot17` FLOAT NULL,
  `Slot18` FLOAT NULL,
  `Slot19` FLOAT NULL,
  `Slot20` FLOAT NULL,
  `Slot21` FLOAT NULL,
  `Slot22` FLOAT NULL,
  `Slot23` FLOAT NULL,
  `Slot24` FLOAT NULL,
  `Slot25` FLOAT NULL,
  `Slot26` FLOAT NULL,
  `Slot27` FLOAT NULL,
  `Slot28` FLOAT NULL,
  `Slot29` FLOAT NULL,
  `Slot30` FLOAT NULL,
  `Slot31` FLOAT NULL,
  `Slot32` FLOAT NULL,
  `Slot33` FLOAT NULL,
  `Slot34` FLOAT NULL,
  `Slot35` FLOAT NULL,
  `Slot36` FLOAT NULL,
  `Slot37` FLOAT NULL,
  `Slot38` FLOAT NULL,
  `Slot39` FLOAT NULL,
  `Slot40` FLOAT NULL,
  `Slot41` FLOAT NULL,
  `Slot42` FLOAT NULL,
  `Slot43` FLOAT NULL,
  `Slot44` FLOAT NULL,
  `Slot45` FLOAT NULL,
  `Slot46` FLOAT NULL,
  `Slot47` FLOAT NULL,
  `Slot48` FLOAT NULL,
  `mobility_summary_service` VARCHAR(20) NOT NULL,
  `mobility_summary_user` VARCHAR(45) NOT NULL,
  `mobility_summary_date` DATE NOT NULL,
  `type` VARCHAR(5) NOT NULL,
  PRIMARY KEY (`id`),
  INDEX `fk_speed_mobility_summary1_idx` (`mobility_summary_service` ASC, `mobility_summary_user` ASC, `mobility_summary_date` ASC),
  CONSTRAINT `fk_speed_mobility_summary1`
    FOREIGN KEY (`mobility_summary_service` , `mobility_summary_user` , `mobility_summary_date`)
    REFERENCES ``.`mobility_summary` (`service` , `user` , `date`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table ``.`distance`
-- -----------------------------------------------------
DROP TABLE IF EXISTS ``.`distance` ;

CREATE TABLE IF NOT EXISTS ``.`distance` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `source` VARCHAR(45) NULL,
  `Slot1` FLOAT NULL,
  `Slot2` FLOAT NULL,
  `Slot3` FLOAT NULL,
  `Slot4` FLOAT NULL,
  `Slot5` FLOAT NULL,
  `Slot6` FLOAT NULL,
  `Slot7` FLOAT NULL,
  `Slot8` FLOAT NULL,
  `Slot9` FLOAT NULL,
  `Slot10` FLOAT NULL,
  `Slot11` FLOAT NULL,
  `Slot12` FLOAT NULL,
  `Slot13` FLOAT NULL,
  `Slot14` FLOAT NULL,
  `Slot15` FLOAT NULL,
  `Slot16` FLOAT NULL,
  `Slot17` FLOAT NULL,
  `Slot18` FLOAT NULL,
  `Slot19` FLOAT NULL,
  `Slot20` FLOAT NULL,
  `Slot21` FLOAT NULL,
  `Slot22` FLOAT NULL,
  `Slot23` FLOAT NULL,
  `Slot24` FLOAT NULL,
  `Slot25` FLOAT NULL,
  `Slot26` FLOAT NULL,
  `Slot27` FLOAT NULL,
  `Slot28` FLOAT NULL,
  `Slot29` FLOAT NULL,
  `Slot30` FLOAT NULL,
  `Slot31` FLOAT NULL,
  `Slot32` FLOAT NULL,
  `Slot33` FLOAT NULL,
  `Slot34` FLOAT NULL,
  `Slot35` FLOAT NULL,
  `Slot36` FLOAT NULL,
  `Slot37` FLOAT NULL,
  `Slot38` FLOAT NULL,
  `Slot39` FLOAT NULL,
  `Slot40` FLOAT NULL,
  `Slot41` FLOAT NULL,
  `Slot42` FLOAT NULL,
  `Slot43` FLOAT NULL,
  `Slot44` FLOAT NULL,
  `Slot45` FLOAT NULL,
  `Slot46` FLOAT NULL,
  `Slot47` FLOAT NULL,
  `Slot48` FLOAT NULL,
  `mobility_summary_service` VARCHAR(20) NOT NULL,
  `mobility_summary_user` VARCHAR(45) NOT NULL,
  `mobility_summary_date` DATE NOT NULL,
  `type` VARCHAR(5) NOT NULL,
  PRIMARY KEY (`id`),
  INDEX `fk_distance_mobility_summary1_idx` (`mobility_summary_service` ASC, `mobility_summary_user` ASC, `mobility_summary_date` ASC),
  CONSTRAINT `fk_distance_mobility_summary1`
    FOREIGN KEY (`mobility_summary_service` , `mobility_summary_user` , `mobility_summary_date`)
    REFERENCES ``.`mobility_summary` (`service` , `user` , `date`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table ``.`location`
-- -----------------------------------------------------
DROP TABLE IF EXISTS ``.`location` ;

CREATE TABLE IF NOT EXISTS ``.`location` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `time` TIME(0) NULL,
  `currentTimestamp` INT NULL,
  `light_sensor` INT NULL,
  `altitude` INT NULL,
  `mobility_summary_service` VARCHAR(20) NOT NULL,
  `mobility_summary_user` VARCHAR(45) NOT NULL,
  `mobility_summary_date` DATE NOT NULL,
  PRIMARY KEY (`id`),
  INDEX `fk_location_mobility_summary1_idx` (`mobility_summary_service` ASC, `mobility_summary_user` ASC, `mobility_summary_date` ASC),
  CONSTRAINT `fk_location_mobility_summary1`
    FOREIGN KEY (`mobility_summary_service` , `mobility_summary_user` , `mobility_summary_date`)
    REFERENCES ``.`mobility_summary` (`service` , `user` , `date`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table ``.`activity_summary`
-- -----------------------------------------------------
DROP TABLE IF EXISTS ``.`activity_summary` ;

CREATE TABLE IF NOT EXISTS ``.`activity_summary` (
  `service` VARCHAR(20) NOT NULL,
  `user` VARCHAR(45) NOT NULL,
  `date` DATE NOT NULL,
  PRIMARY KEY (`service`, `user`, `date`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table ``.`steps`
-- -----------------------------------------------------
DROP TABLE IF EXISTS ``.`steps` ;

CREATE TABLE IF NOT EXISTS ``.`steps` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `source` VARCHAR(45) NULL,
  `Slot1` FLOAT NULL,
  `Slot2` FLOAT NULL,
  `Slot3` FLOAT NULL,
  `Slot4` FLOAT NULL,
  `Slot5` FLOAT NULL,
  `Slot6` FLOAT NULL,
  `Slot7` FLOAT NULL,
  `Slot8` FLOAT NULL,
  `Slot9` FLOAT NULL,
  `Slot10` FLOAT NULL,
  `Slot11` FLOAT NULL,
  `Slot12` FLOAT NULL,
  `Slot13` FLOAT NULL,
  `Slot14` FLOAT NULL,
  `Slot15` FLOAT NULL,
  `Slot16` FLOAT NULL,
  `Slot17` FLOAT NULL,
  `Slot18` FLOAT NULL,
  `Slot19` FLOAT NULL,
  `Slot20` FLOAT NULL,
  `Slot21` FLOAT NULL,
  `Slot22` FLOAT NULL,
  `Slot23` FLOAT NULL,
  `Slot24` FLOAT NULL,
  `Slot25` FLOAT NULL,
  `Slot26` FLOAT NULL,
  `Slot27` FLOAT NULL,
  `Slot28` FLOAT NULL,
  `Slot29` FLOAT NULL,
  `Slot30` FLOAT NULL,
  `Slot31` FLOAT NULL,
  `Slot32` FLOAT NULL,
  `Slot33` FLOAT NULL,
  `Slot34` FLOAT NULL,
  `Slot35` FLOAT NULL,
  `Slot36` FLOAT NULL,
  `Slot37` FLOAT NULL,
  `Slot38` FLOAT NULL,
  `Slot39` FLOAT NULL,
  `Slot40` FLOAT NULL,
  `Slot41` FLOAT NULL,
  `Slot42` FLOAT NULL,
  `Slot43` FLOAT NULL,
  `Slot44` FLOAT NULL,
  `Slot45` FLOAT NULL,
  `Slot46` FLOAT NULL,
  `Slot47` FLOAT NULL,
  `Slot48` FLOAT NULL,
  `activity_summary_service` VARCHAR(20) NOT NULL,
  `activity_summary_user` VARCHAR(45) NOT NULL,
  `activity_summary_date` DATE NOT NULL,
  PRIMARY KEY (`id`),
  INDEX `fk_steps_activity_summary1_idx` (`activity_summary_service` ASC, `activity_summary_user` ASC, `activity_summary_date` ASC),
  CONSTRAINT `fk_steps_activity_summary1`
    FOREIGN KEY (`activity_summary_service` , `activity_summary_user` , `activity_summary_date`)
    REFERENCES ``.`activity_summary` (`service` , `user` , `date`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table ``.`activity_ts`
-- -----------------------------------------------------
DROP TABLE IF EXISTS ``.`activity_ts` ;

CREATE TABLE IF NOT EXISTS ``.`activity_ts` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `activity` VARCHAR(50) NULL,
  `TS` INT NULL,
  `activity_summary_service` VARCHAR(20) NOT NULL,
  `activity_summary_user` VARCHAR(45) NOT NULL,
  `activity_summary_date` DATE NOT NULL,
  PRIMARY KEY (`id`),
  INDEX `fk_activity_ts_activity_summary1_idx` (`activity_summary_service` ASC, `activity_summary_user` ASC, `activity_summary_date` ASC),
  CONSTRAINT `fk_activity_ts_activity_summary1`
    FOREIGN KEY (`activity_summary_service` , `activity_summary_user` , `activity_summary_date`)
    REFERENCES ``.`activity_summary` (`service` , `user` , `date`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table ``.`social_summary`
-- -----------------------------------------------------
DROP TABLE IF EXISTS ``.`social_summary` ;

CREATE TABLE IF NOT EXISTS ``.`social_summary` (
  `service` VARCHAR(20) NOT NULL,
  `user` VARCHAR(45) NOT NULL,
  `date` DATE NOT NULL,
  PRIMARY KEY (`service`, `user`, `date`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table ``.`call_register`
-- -----------------------------------------------------
DROP TABLE IF EXISTS ``.`call_register` ;

CREATE TABLE IF NOT EXISTS ``.`call_register` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `time` INT NULL,
  `duration` INT NULL,
  `number` VARCHAR(100) NULL,
  `type` VARCHAR(45) NULL,
  `social_summary_service` VARCHAR(20) NOT NULL,
  `social_summary_user` VARCHAR(45) NOT NULL,
  `social_summary_date` DATE NOT NULL,
  PRIMARY KEY (`id`),
  INDEX `fk_call_register_social_summary1_idx` (`social_summary_service` ASC, `social_summary_user` ASC, `social_summary_date` ASC),
  CONSTRAINT `fk_call_register_social_summary1`
    FOREIGN KEY (`social_summary_service` , `social_summary_user` , `social_summary_date`)
    REFERENCES ``.`social_summary` (`service` , `user` , `date`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table ``.`appusage`
-- -----------------------------------------------------
DROP TABLE IF EXISTS ``.`appusage` ;

CREATE TABLE IF NOT EXISTS ``.`appusage` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `slot` INT NULL,
  `app` VARCHAR(100) NULL,
  `time` INT NULL,
  `social_summary_service` VARCHAR(20) NOT NULL,
  `social_summary_user` VARCHAR(45) NOT NULL,
  `social_summary_date` DATE NOT NULL,
  PRIMARY KEY (`id`),
  INDEX `fk_appusage_social_summary1_idx` (`social_summary_service` ASC, `social_summary_user` ASC, `social_summary_date` ASC),
  CONSTRAINT `fk_appusage_social_summary1`
    FOREIGN KEY (`social_summary_service` , `social_summary_user` , `social_summary_date`)
    REFERENCES ``.`social_summary` (`service` , `user` , `date`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table ``.`clusters`
-- -----------------------------------------------------
DROP TABLE IF EXISTS ``.`clusters` ;

CREATE TABLE IF NOT EXISTS ``.`clusters` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `user` VARCHAR(45) NOT NULL,
  `longitude` FLOAT NULL,
  `latitude` FLOAT NULL,
  `radius` FLOAT NULL,
  `weight` FLOAT NULL,
  `LS` FLOAT NULL,
  `SS` FLOAT NULL,
  `creationTimeStamp` INT NULL,
  `altitude` FLOAT NULL,
  `wifi` VARCHAR(45) NULL,
  `bluetooth` VARCHAR(45) NULL,
  `activities` VARCHAR(45) NULL,
  `places` VARCHAR(45) NULL,
  `type_cluster` VARCHAR(45) NULL,
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table ``.`wifis`
-- -----------------------------------------------------
DROP TABLE IF EXISTS ``.`wifis` ;

CREATE TABLE IF NOT EXISTS ``.`wifis` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `hash` VARCHAR(100) NULL,
  `power` INT NULL,
  `location_id` INT NOT NULL,
  `clusters_id` INT NOT NULL,
  PRIMARY KEY (`id`),
  INDEX `fk_wifis_location1_idx` (`location_id` ASC),
  INDEX `fk_wifis_clusters1_idx` (`clusters_id` ASC),
  CONSTRAINT `fk_wifis_location1`
    FOREIGN KEY (`location_id`)
    REFERENCES ``.`location` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_wifis_clusters1`
    FOREIGN KEY (`clusters_id`)
    REFERENCES ``.`clusters` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table ``.`activity_slot`
-- -----------------------------------------------------
DROP TABLE IF EXISTS ``.`activity_slot` ;

CREATE TABLE IF NOT EXISTS ``.`activity_slot` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `slot` INT NULL,
  `activity` VARCHAR(45) NULL,
  `time` INT NULL,
  `activity_summary_service` VARCHAR(20) NOT NULL,
  `activity_summary_user` VARCHAR(45) NOT NULL,
  `activity_summary_date` DATE NOT NULL,
  PRIMARY KEY (`id`),
  INDEX `fk_activity_activity_summary1_idx` (`activity_summary_service` ASC, `activity_summary_user` ASC, `activity_summary_date` ASC),
  CONSTRAINT `fk_activity_activity_summary1`
    FOREIGN KEY (`activity_summary_service` , `activity_summary_user` , `activity_summary_date`)
    REFERENCES ``.`activity_summary` (`service` , `user` , `date`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table ``.`bluetooth`
-- -----------------------------------------------------
DROP TABLE IF EXISTS ``.`bluetooth` ;

CREATE TABLE IF NOT EXISTS ``.`bluetooth` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `hash` VARCHAR(100) NULL,
  `power` INT NULL,
  `location_id` INT NOT NULL,
  `clusters_id` INT NOT NULL,
  PRIMARY KEY (`id`),
  INDEX `fk_bluetooth_location1_idx` (`location_id` ASC),
  INDEX `fk_bluetooth_clusters1_idx` (`clusters_id` ASC),
  CONSTRAINT `fk_bluetooth_location1`
    FOREIGN KEY (`location_id`)
    REFERENCES ``.`location` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_bluetooth_clusters1`
    FOREIGN KEY (`clusters_id`)
    REFERENCES ``.`clusters` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table ``.`activities_cluster`
-- -----------------------------------------------------
DROP TABLE IF EXISTS ``.`activities_cluster` ;

CREATE TABLE IF NOT EXISTS ``.`activities_cluster` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `name` VARCHAR(100) NULL,
  `percentage` INT NULL,
  `location_id` INT NOT NULL,
  `clusters_id` INT NOT NULL,
  PRIMARY KEY (`id`),
  INDEX `fk_activities_cluster_location1_idx` (`location_id` ASC),
  INDEX `fk_activities_cluster_clusters1_idx` (`clusters_id` ASC),
  CONSTRAINT `fk_activities_cluster_location1`
    FOREIGN KEY (`location_id`)
    REFERENCES ``.`location` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_activities_cluster_clusters1`
    FOREIGN KEY (`clusters_id`)
    REFERENCES ``.`clusters` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table ``.`places`
-- -----------------------------------------------------
DROP TABLE IF EXISTS ``.`places` ;

CREATE TABLE IF NOT EXISTS ``.`places` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `name` VARCHAR(100) NULL,
  `percentage` INT NULL,
  `location_id` INT NOT NULL,
  `clusters_id` INT NOT NULL,
  PRIMARY KEY (`id`),
  INDEX `fk_places_location1_idx` (`location_id` ASC),
  INDEX `fk_places_clusters1_idx` (`clusters_id` ASC),
  CONSTRAINT `fk_places_location1`
    FOREIGN KEY (`location_id`)
    REFERENCES ``.`location` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_places_clusters1`
    FOREIGN KEY (`clusters_id`)
    REFERENCES ``.`clusters` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table ``.`last_modification`
-- -----------------------------------------------------
DROP TABLE IF EXISTS ``.`last_modification` ;

CREATE TABLE IF NOT EXISTS ``.`last_modification` (
  `service` VARCHAR(20) NOT NULL,
  `user` VARCHAR(45) NOT NULL,
  `date` DATE NOT NULL,
  `data_type` VARCHAR(20) NOT NULL,
  PRIMARY KEY (`service`, `user`, `data_type`))
ENGINE = InnoDB;


SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;
