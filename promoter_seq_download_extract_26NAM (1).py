# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC
# MAGIC %pip install quilt3==5.3.0

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /local_disk0/rna_seq_data/

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
# MAGIC ### install or import packages

# COMMAND ----------

# MAGIC
# MAGIC %sh
# MAGIC apt-get update -y
# MAGIC apt-get install -y bedtools
# MAGIC apt-get install -y samtools

# COMMAND ----------

import subprocess
from sys import exit

# COMMAND ----------

# MAGIC %md
# MAGIC ###download data

# COMMAND ----------

dna_seq_namelist=['Zm-B73-REFERENCE-NAM-5.0.fasta.gz','Zm-B97-REFERENCE-NAM-1.0.fasta.gz','Zm-CML103-REFERENCE-NAM-1.0.fasta.gz','Zm-CML228-REFERENCE-NAM-1.0.fasta.gz','Zm-CML247-REFERENCE-NAM-1.0.fasta.gz','Zm-CML277-REFERENCE-NAM-1.0.fasta.gz','Zm-CML322-REFERENCE-NAM-1.0.fasta.gz','Zm-CML333-REFERENCE-NAM-1.0.fasta.gz','Zm-CML52-REFERENCE-NAM-1.0.fasta.gz','Zm-CML69-REFERENCE-NAM-1.0.fasta.gz','Zm-HP301-REFERENCE-NAM-1.0.fasta.gz','Zm-Il14H-REFERENCE-NAM-1.0.fasta.gz','Zm-Ki11-REFERENCE-NAM-1.0.fasta.gz','Zm-Ki3-REFERENCE-NAM-1.0.fasta.gz','Zm-Ky21-REFERENCE-NAM-1.0.fasta.gz','Zm-M162W-REFERENCE-NAM-1.0.fasta.gz','Zm-M37W-REFERENCE-NAM-1.0.fasta.gz','Zm-Mo18W-REFERENCE-NAM-1.0.fasta.gz','Zm-Ms71-REFERENCE-NAM-1.0.fasta.gz','Zm-NC350-REFERENCE-NAM-1.0.fasta.gz','Zm-NC358-REFERENCE-NAM-1.0.fasta.gz','Zm-Oh43-REFERENCE-NAM-1.0.fasta.gz','Zm-Oh7B-REFERENCE-NAM-1.0.fasta.gz','Zm-P39-REFERENCE-NAM-1.0.fasta.gz',
'Zm-Tx303-REFERENCE-NAM-1.0.fasta.gz','Zm-Tzi8-REFERENCE-NAM-1.0.fasta.gz']
gff_name_spec=['01','18','21','22','23','24','25','26','19','20','27','28','30','29','31','33','32','34','35','36','37','39','38','40','41','42']
gff_namelist=list()
for i in range(26):
    gff_name=dna_seq_namelist[i][:-9]+'_Zm000'+gff_name_spec[i]+'ab.1.gff3'
    if i == 0:
        gff_name=dna_seq_namelist[i][:-9]+'_Zm000'+gff_name_spec[i]+'eb.1.gff3'
        
    gff_namelist.append(gff_name)        


#https://de.cyverse.org/anon-files//iplant/home/shared/NAM/NAM_genome_and_annotation_Jan2021_release/GENE_MODEL_ANNOTATIONS/Zm-Tzi8-REFERENCE-NAM-1.0/Zm-Tzi8-REFERENCE-NAM-1.0_Zm00042ab.1.gff3

# COMMAND ----------

gff_namelist

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
# MAGIC ls -l

# COMMAND ----------

# MAGIC %md
# MAGIC ###extract promoter

# COMMAND ----------

os.environ['LIST']=' '.join(gff_namelist)
print(os.getenv('LIST'))

# COMMAND ----------

# MAGIC %sh
# MAGIC for gff_name in $LIST
# MAGIC do
# MAGIC   file /local_disk0/rna_seq_data/${gff_name}
# MAGIC   gz_name="${gff_name::${#gff_name}-17}.fasta.gz"
# MAGIC   file /local_disk0/rna_seq_data/${gz_name}
# MAGIC done

# COMMAND ----------



# COMMAND ----------

# MAGIC %sh
# MAGIC for gff_name in $LIST
# MAGIC do
# MAGIC   echo $gff_name
# MAGIC   gz_name="${gff_name::${#gff_name}-17}.fasta.gz"
# MAGIC   genome_name="${gff_name::${#gff_name}-17}.fasta"
# MAGIC   genome_fai_name="${gff_name::${#gff_name}-17}.fasta.fai"
# MAGIC   gene_gff_name="${gff_name::${#gff_name}-17}_gene_gff.gff"
# MAGIC   gene_1kbup_name="${gff_name::${#gff_name}-17}_gene_1kbup.gff"
# MAGIC   nov_name="${gff_name::${#gff_name}-17}_nov.gff"
# MAGIC   gunzip -c /local_disk0/rna_seq_data/${gz_name} > /local_disk0/rna_seq_data/${genome_name}
# MAGIC   samtools faidx /local_disk0/rna_seq_data/${genome_name}
# MAGIC   grep -P '\tgene.*' /local_disk0/rna_seq_data/${gff_name} > /local_disk0/rna_seq_data/${gene_gff_name}
# MAGIC   bedtools flank -i /local_disk0/rna_seq_data/${gene_gff_name} \
# MAGIC                 -g /local_disk0/rna_seq_data/${genome_fai_name} \
# MAGIC                 -l 1000 \
# MAGIC                 -r 0 \
# MAGIC                 -s \
# MAGIC                 > /local_disk0/rna_seq_data/${gene_1kbup_name}
# MAGIC   bedtools subtract -a /local_disk0/rna_seq_data/${gene_1kbup_name} \
# MAGIC                     -b /local_disk0/rna_seq_data/${gene_gff_name} \
# MAGIC                     >/local_disk0/rna_seq_data/${nov_name}
# MAGIC
# MAGIC
# MAGIC done

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC   gunzip -c /local_disk0/rna_seq_data/Zm-B73-REFERENCE-NAM-5.0.fasta.gz > /local_disk0/rna_seq_data/genome_file.fasta
# MAGIC
# MAGIC   samtools faidx /local_disk0/rna_seq_data/genome_file.fasta
# MAGIC   

# COMMAND ----------

# MAGIC %sh
# MAGIC for gff_name in $LIST
# MAGIC do
# MAGIC   echo $gff_name
# MAGIC   gz_name="${gff_name::${#gff_name}-17}.fasta.gz"
# MAGIC   gunzip -c /local_disk0/rna_seq_data/${gz_name} > /local_disk0/rna_seq_data/genome_file.fasta
# MAGIC   samtools faidx /local_disk0/rna_seq_data/genome_file.fasta
# MAGIC   grep -P '\tgene.*' /local_disk0/rna_seq_data/${gff_name} > /local_disk0/rna_seq_data/gene_gff.gff
# MAGIC   bedtools flank -i /local_disk0/rna_seq_data/gene_gff.gff \
# MAGIC                 -g /local_disk0/rna_seq_data/genome_file.fasta.fai \
# MAGIC                 -l 1000 \
# MAGIC                 -r 0 \
# MAGIC                 -s \
# MAGIC                 > /local_disk0/rna_seq_data/gene_1kbup.gff
# MAGIC
# MAGIC   nov_name="${gff_name::${#gff_name}-17}_nov.gff"
# MAGIC
# MAGIC   bedtools subtract -a /local_disk0/rna_seq_data/gene_1kbup.gff \
# MAGIC                     -b /local_disk0/rna_seq_data/gene_gff.gff \
# MAGIC                     >/local_disk0/rna_seq_data/${nov_name}
# MAGIC head /local_disk0/rna_seq_data/${nov_name}
# MAGIC done

# COMMAND ----------

# MAGIC %sh 
# MAGIC cd /local_disk0/tmp
# MAGIC ls -l
# MAGIC cd /local_disk0/rna_seq_data
# MAGIC ls -l

# COMMAND ----------

# MAGIC %sh
# MAGIC gunzip -c /local_disk0/tmp/Zm-B73-REFERENCE-NAM-5.0.fasta.gz > /local_disk0/rna_seq_data/genome_file.fasta
# MAGIC samtools faidx /local_disk0/rna_seq_data/genome_file.fasta
# MAGIC grep -P '\tgene.*' /local_disk0/rna_seq_data/dna_annotation_B73_gff.gff > /local_disk0/rna_seq_data/dna_annotation_B73_gff.gene.gff
# MAGIC

# COMMAND ----------

# MAGIC %sh
# MAGIC bedtools flank -i /local_disk0/rna_seq_data/dna_annotation_B73_gff.gene.gff \
# MAGIC               -g /local_disk0/rna_seq_data/genome_file.fasta.fai \
# MAGIC               -l 1000 \
# MAGIC               -r 0 \
# MAGIC               -s \
# MAGIC               > /local_disk0/rna_seq_data/gene_1kbup.gff
# MAGIC

# COMMAND ----------

# MAGIC %sh
# MAGIC bedtools subtract -a /local_disk0/rna_seq_data/gene_1kbup.gff \
# MAGIC                   -b /local_disk0/rna_seq_data/dna_annotation_B73_gff.gene.gff \
# MAGIC                   >/local_disk0/rna_seq_data/gene_1kbup_nov.gff
# MAGIC

# COMMAND ----------

"/local_disk0/rna_seq_data/"+dna_seq_namelist[i][:-9]+'_nov.gff'

# COMMAND ----------

from sys import exit

for i in range(26):

    # Read in all the extracted regions
    with open("/local_disk0/rna_seq_data/"+dna_seq_namelist[i][:-9]+'_nov.gff') as gtf_fh_in:
        fragment_dict = {}
        orientation_dict = {}
        for line in gtf_fh_in:
            line = line.rstrip()
            line_list = line.split('\t')
            gene_id = line_list[-1].split(';')[0]
            orientation = line_list[6]
            start_coord = line_list[3]
            line_list[2] = gene_id
            orientation_dict[gene_id] = orientation
            if gene_id not in fragment_dict:
                fragment_dict[gene_id] = []
            fragment_dict[gene_id].append("\t".join(line_list))


    # Write out only the retained ones
    with open("/local_disk0/rna_seq_data/"+dna_seq_namelist[i][:-9]+'_nov_final.gff', "w") as gtf_fh_out:
        for gene_id in fragment_dict:
            if orientation_dict[gene_id] == '+':
                # take fragment with highest coords, which is the latest one added
                gtf_fh_out.write("{}\n".format(fragment_dict[gene_id][-1]
                                            )
                                )
            else:
                # orientation == -
                # take fragment with lowest coords, which is the first one added
                gtf_fh_out.write("{}\n".format(fragment_dict[gene_id][0]
                                            )
                                )


# COMMAND ----------

# MAGIC %sh
# MAGIC head /local_disk0/rna_seq_data/gene_1kbup_nov_final.gff

# COMMAND ----------

# MAGIC %sh
# MAGIC for gff_name in $LIST
# MAGIC do
# MAGIC   echo $gff_name
# MAGIC   gz_name="${gff_name::${#gff_name}-17}.fasta.gz"
# MAGIC   genome_name="${gff_name::${#gff_name}-17}.fasta"
# MAGIC   genome_fai_name="${gff_name::${#gff_name}-17}.fasta.fai"
# MAGIC   gene_gff_name="${gff_name::${#gff_name}-17}_gene_gff.gff"
# MAGIC   gene_1kbup_name="${gff_name::${#gff_name}-17}_gene_1kbup.gff"
# MAGIC   nov_name="${gff_name::${#gff_name}-17}_nov.gff"
# MAGIC   nov_final_name="${gff_name::${#gff_name}-17}_nov_final.gff"
# MAGIC   output_name="${gff_name::${#gff_name}-17}_output.fa"
# MAGIC   bedtools getfasta -fi /local_disk0/rna_seq_data/${genome_name} \
# MAGIC                     -bed /local_disk0/rna_seq_data/${nov_final_name} \
# MAGIC                     -s \
# MAGIC                     -name+ \
# MAGIC                     > /local_disk0/rna_seq_data/${output_name}
# MAGIC   head /local_disk0/rna_seq_data/${output_name}
# MAGIC
# MAGIC
# MAGIC done

# COMMAND ----------

# MAGIC %sh
# MAGIC bedtools getfasta -fi /local_disk0/rna_seq_data/genome_file.fasta \
# MAGIC                   -bed /local_disk0/rna_seq_data/gene_1kbup_nov_final.gff \
# MAGIC                   -s \
# MAGIC                   -name+ \
# MAGIC                   > /local_disk0/rna_seq_data/output_try1.fa

# COMMAND ----------

# MAGIC %sh
# MAGIC head /local_disk0/rna_seq_data/output_try1.fa
