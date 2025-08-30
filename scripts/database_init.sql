/* This Script is used to Setup a database with three schemas bronze, silver, gold */


-- Create Database.
CREATE DATABASE DataWarehouse;

-- Use Database.
USE DataWarehouse;

-- Creating Schemas for each of D/W Layers.
CREATE SCHEMA Bronze;
CREATE SCHEMA Silver;
CREATE SCHEMA Gold;
