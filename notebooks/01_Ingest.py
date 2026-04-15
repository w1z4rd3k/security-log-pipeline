# Databricks notebook source
df = spark.read.option("header", True).csv("/Volumes/workspace/default/log_data/access.csv")
display(df)