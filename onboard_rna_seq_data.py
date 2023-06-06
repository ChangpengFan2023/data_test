# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Onboard RNA-Seq Data

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Setup

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC
# MAGIC %pip install quilt3==5.3.0

# COMMAND ----------

import os
import quilt3 as q3

local_work_dir = "/local_disk0/rna_seq_data/"
if not os.path.exists(local_work_dir):
    os.makedirs(local_work_dir)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Downlaod Data

# COMMAND ----------

import subprocess

file_names = ["B73_full-tpm.tsv", "B97_full-tpm.tsv"]

for file_name in file_names:
    print(f"Downloading \'{file_name}\'")
    
    cmd = f"wget https://de.cyverse.org/anon-files//iplant/home/shared/NAM/NAM_genome_and_annotation_Jan2021_release/SUPPLEMENTAL_DATA/pangene-files/NAM_pangene_expression_counts_per_tissue-TPM/{file_name} -O /local_disk0/rna_seq_data/{file_name}"

    subprocess.run(cmd.split())

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


