import os
import pandas as pd


#################

cyto_file = config["cyto_file"]
MAIN_PATH = config["main_path"]
PLINK_PATH = config["plink_path"]
RESULTS_PATH = config["results_path"]
WGS_PATH = config["WGS_path"]
MAC = config["MAC"]
KEEP_FILE = config["keep_file"]

#clean_up_script = "/data/Epic/subprojects/Gwas/work/Plink_datasets/protein_subset/snakemake/cleanup.py"


############################################
#Input functions

def get_files(wildcards):
    temp = cyto_file.loc[wildcards.gene_name, "files"]
    temp2 = [x[:-7] for x in temp.split(", ")]
    return temp2

def get_chromosome(wildcards): 
    return cyto_file.loc[wildcards.gene_name, "CHR"]


############################################


rule all:
    input:
        expand("{RESULT_PATH}/{chromosome}/{cyto}.done", cyto=cyto),


rule create_plink_chunks:
    input:
          "/mnt/project/{WGS_path}/chr{chromosome}/{WGS_FILE}.vcf.gz"
    resources:
        mem='32G',
        time='00:50:00',  
        cpus=8
    params:
        CHR= lambda wildcards: get_chromosome(wildcards)
        WGS_FILES= lambda wildcards: get_files(wildcards)
    output:
        "{RESULT_PATH}/{cyto}/{file}.done"
    shell:
        """
        {PLINK_PATH} --vcf {INPUT}  {wildcards.Protein} --keep {KEEP_FILE} --mac {MAC} --max-alleles 3 --no-input-missing-phenotype --make-pgen --out {RESULT_PATH}/{wildcards.cyto}/{WGS_FILE}
        metal /scratch/atkinsj/results/{wildcards.Protein}/{wildcards.sex}/chr{wildcards.chromosome}/{wildcards.Protein}.metal.sh > out.txt
        python /scratch/atkinsj/by_sex/cleanup.py -w /scratch/atkinsj/results/{wildcards.Protein}/{wildcards.sex}/chr{wildcards.chromosome}/ -p {wildcards.Protein} > out.txt
       ##touch /scratch/atkinsj/results/{wildcards.Protein}/chr{wildcards.chromosome}/{wildcards.cohort}_chr{wildcards.chromosome}.done 

       """

rule merge_chunks:
    input:
          "/scratch/atkinsj/results/{Protein}/{sex}/chr{chromosome}/"
    resources:
        mem='120G',
        time='05:00:00',
        partition="high_p",  
        cpus=8
    params:
        CHR= lambda wildcards: get_chromosome(wildcards)
    output:
        "/scratch/atkinsj/results/{Protein}/{sex}/chr{chromosome}/*tbl.gz"
    shell:
        """

       
       ##touch /scratch/atkinsj/results/{wildcards.Protein}/chr{wildcards.chromosome}/{wildcards.cohort}_chr{wildcards.chromosome}.done 

       """

