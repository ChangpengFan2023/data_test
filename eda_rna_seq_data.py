# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # EDA on RNA-Seq Data

# COMMAND ----------

import pandas as pd
import os

local_work_dir = "/local_disk0/rna_seq_data/"
if not os.path.exists(local_work_dir):
    os.makedirs(local_work_dir)

df = pd.read_csv(f"{local_work_dir}B73_full-tpm.tsv", delimiter="\t")

df

# COMMAND ----------


