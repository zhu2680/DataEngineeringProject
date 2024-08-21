# Databricks notebook source
raw_folder_path = "abfss://raw@formula1djulia.dfs.core.windows.net"
processed_folder_path = "abfss://processed@formula1djulia.dfs.core.windows.net"
presentation_folder_path = "abfss://presentation@formula1djulia.dfs.core.windows.net"
demo_path = "abfss://demo@formula1djulia.dfs.core.windows.net"
processed_database = "dbfs:/user/hive/warehouse/f1_processed.db"
presentation_database = "dbfs:/user/hive/warehouse/f1_presentation.db"