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

import subprocess


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Downlaod Data

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC wget -P /local_disk0/tmp https://de.cyverse.org/anon-files//iplant/home/shared/NAM/NAM_genome_and_annotation_Jan2021_release/GENOMIC_FASTA_FILES/Zm-B73-REFERENCE-NAM-5.0.fasta.gz

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /local_disk0/tmp
# MAGIC ls

# COMMAND ----------

dna_seq_namelist[i][:-9]+'_Zm000'+gff_name_spec[i]+'ab.1.gff3'

# COMMAND ----------

dna_seq_namelist=['Zm-B73-REFERENCE-NAM-5.0.fasta.gz','Zm-B97-REFERENCE-NAM-1.0.fasta.gz','Zm-CML103-REFERENCE-NAM-1.0.fasta.gz','Zm-CML228-REFERENCE-NAM-1.0.fasta.gz','Zm-CML247-REFERENCE-NAM-1.0.fasta.gz','Zm-CML277-REFERENCE-NAM-1.0.fasta.gz','Zm-CML322-REFERENCE-NAM-1.0.fasta.gz','Zm-CML333-REFERENCE-NAM-1.0.fasta.gz','Zm-CML52-REFERENCE-NAM-1.0.fasta.gz','Zm-CML69-REFERENCE-NAM-1.0.fasta.gz','Zm-HP301-REFERENCE-NAM-1.0.fasta.gz','Zm-Il14H-REFERENCE-NAM-1.0.fasta.gz','Zm-Ki11-REFERENCE-NAM-1.0.fasta.gz','Zm-Ki3-REFERENCE-NAM-1.0.fasta.gz','Zm-Ky21-REFERENCE-NAM-1.0.fasta.gz','Zm-M162W-REFERENCE-NAM-1.0.fasta.gz','Zm-M37W-REFERENCE-NAM-1.0.fasta.gz','Zm-Mo18W-REFERENCE-NAM-1.0.fasta.gz','Zm-Ms71-REFERENCE-NAM-1.0.fasta.gz','Zm-NC350-REFERENCE-NAM-1.0.fasta.gz','Zm-NC358-REFERENCE-NAM-1.0.fasta.gz','Zm-Oh43-REFERENCE-NAM-1.0.fasta.gz','Zm-Oh7B-REFERENCE-NAM-1.0.fasta.gz','Zm-P39-REFERENCE-NAM-1.0.fasta.gz',
'Zm-Tx303-REFERENCE-NAM-1.0.fasta.gz','Zm-Tzi8-REFERENCE-NAM-1.0.fasta.gz']
gff_name_spec=['01','18','21','22','23','24','25','26','19','20','27','28','30','29','31','33','32','34','35','36','37','39','38','40','41','42']
gff_namelist=list()
for i in range(26):
    gff_name=dna_seq_namelist[i][:-9]+'_Zm000'+gff_name_spec[i]+'ab.1.gff3'
    gff_namelist.append(gff_name)


#https://de.cyverse.org/anon-files//iplant/home/shared/NAM/NAM_genome_and_annotation_Jan2021_release/GENE_MODEL_ANNOTATIONS/Zm-Tzi8-REFERENCE-NAM-1.0/Zm-Tzi8-REFERENCE-NAM-1.0_Zm00042ab.1.gff3

# COMMAND ----------

i=0
for gff_name in gff_namelist:
    dna_seq_name=dna_seq_namelist[i][:-9]
    i+=1
    print(f"Downloading \'{gff_name}\'")
    
    cmd = f"wget https://de.cyverse.org/anon-files//iplant/home/shared/NAM/NAM_genome_and_annotation_Jan2021_release/GENE_MODEL_ANNOTATIONS/{dna_seq_name}/{gff_name} -O /local_disk0/rna_seq_data/{gff_name}"

    subprocess.run(cmd.split())

# COMMAND ----------

import subprocess
for dna_seq_name in dna_seq_namelist:
    print(f"Downloading \'{dna_seq_name}\'")
    
    cmd = f"wget https://de.cyverse.org/anon-files//iplant/home/shared/NAM/NAM_genome_and_annotation_Jan2021_release/GENOMIC_FASTA_FILES/{dna_seq_name} -O /local_disk0/rna_seq_data/{dna_seq_name}"

    subprocess.run(cmd.split())

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /local_disk0/rna_seq_data
# MAGIC ls -lh
