# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC
# MAGIC %pip install quilt3==5.3.0

# COMMAND ----------

import os
import quilt3 as q3
import pandas as pd

local_work_dir = "/local_disk0/rna_seq_data/"
if not os.path.exists(local_work_dir):
    os.makedirs(local_work_dir)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Downlaod Data

# COMMAND ----------

import subprocess
file_name = 'pan-gene-expression-counts-tpm-rpkm_v2.csv'

print(f"Downloading \'{file_name}\'")

cmd = f"wget https://de.cyverse.org/anon-files//iplant/home/shared/NAM/NAM_genome_and_annotation_Jan2021_release/SUPPLEMENTAL_DATA/pangene-files/pan-gene-expression-counts-tpm-rpkm_v2.csv -O /local_disk0/rna_seq_data/{file_name}"

subprocess.run(cmd.split())

# COMMAND ----------

os.path.exists(local_work_dir)
file_names = os.listdir(local_work_dir)
import pandas as pd

file_path = local_work_dir+'pan-gene-expression-counts-tpm-rpkm_v2.csv'

# Read the CSV file into a pandas DataFrame
df_RNA = pd.read_csv(file_path)
df_RNA.dropna(inplace=True)
df_RNA

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Create Quilt Package

# COMMAND ----------

# initialize empty Quilt package
pkg = q3.Package()

# set Quilt package to track local directory
pkg.set_dir(lkey="/", path=local_work_dir)

# set Quilt package metedata
pkg.set_meta({
    "key": "value"
})

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Push Quilt Package

# COMMAND ----------

/dbfs/tmp
