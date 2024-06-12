import os
import pandas as pd

configfile: "config.yaml"
#################

cyto_file= config["cyto_file"]
MAIN_PATH= config["main_path"]
PLINK_PATH= config["plink_path"]
RESULTS_PATH= config["results_path"]
WGS_PATH= config["WGS_path"]
CLEAN_UP= config["clean_up_script"]

## plink parameters
MAC= config["MAC"]
KEEP_FILE= config["keep_file"]
HWE= config["HWE"]
MIND= config["MISSING_per_person"]
GENO= config["MISSING_per_SNP"]
MAX_ALLELE= config["MAX_alleles"]



############################################
#Input functions

df = pd.read_csv(cyto_file, sep="\t")

def get_files(ID):
    temp = df[df['ID'] == ID]["Files"].str.split(",",expand=True).transpose()
    temp.columns = ["Files"]
    temp["Files"] = temp.Files.str.strip()
    temp["Files"] = temp["Files"].str.replace(r'.vcf.gz', '')
    temp = temp.Files
    temp = temp.str.strip()
    temp2 = temp.to_list()
    return temp2
############################################

def get_chromosome(ID):
    temp = df[df['ID'] == ID]["CHR"].unique()[0]
    return temp



file_list = get_files(config["ID"])
ID = config["ID"] 
CHROMOSOME = get_chromosome(ID)

rule all:
    input:
        f"results/{ID}/{ID}.done"


rule create_plink_chunks:
    input:
          config["cyto_file"]
    resources:
        mem='6G',
        time='00:50:00',  
        cpus=2
    params: 
 
    output:
         "results/{ID}/temp/{files}.done"
    shell:
        """
       {PLINK_PATH} --memory 6000 --threads 2  --vcf {WGS_path}{CHROMSOME}/{wildcards.files}.vcf.gz --keep {KEEP_FILE} --mac {MAC} --hwe {HWE} --mind {MIND} --geno {GENO} --max-allele {MAX_ALLELE} --make-pgen --out results/{wildcards.ID}/temp/{wildcards.files}
       echo "{wildcards.files}"  >> results/{wildcards.ID}/merge.list
       touch "results/{wildcards.ID}/{wildcards.files}.done"
       """

rule merge_chunks:
    input:
        expand("results/{ID}/temp/{files}.done", ID=config["ID"], files=file_list)
    resources:
        mem='1G',
        time='05:00:00',
        partition="high_p",  
        cpus=1
    params:
       # files = lambda wildcards: get_files()
    output:
        "results/{ID}/{ID}.done"
    shell:
        """ 
        {PLINK_PATH} --memory 12000 --threads 4  --pmerge-list results/{wildcards.ID}/merge.list --make-pgen --out results/{wildcards.ID}/{wildcards.ID}
       touch "results/{wildcards.ID}/{wildcards.ID}.done"
       rm -r results/{wildcards.ID}/temp/
      
       """



