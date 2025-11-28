-- Databricks notebook source
USE CATALOG lending_catalog;
USE SCHEMA sap_bronze;

CREATE VOLUME IF NOT EXISTS sap_bronze_vol;

SHOW VOLUMES IN sap_bronze;
