import os
import pandas as pd

configfile: "config.yaml"
#################

cyto_file= config["cyto_file"]
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
        mem='4G',
        time='10:00:00',  
        cpus=2
    params: 
 
    output:
         "results/{ID}/temp/{files}.done"
    shell:
        """
        python3 make_plink_chunks.py -f {wildcards.files} -i {wildcards.ID} -c {CHROMOSOME} -k {KEEP_FILE} -m {MAC} -hwe {HWE} -mind {MIND} -g {GENO} -max {MAX_ALLELE}
       touch {output}
       """

rule merge_chunks:
    input:
        expand("results/{ID}/temp/{files}.done", ID=config["ID"], files=file_list)
    resources:
        mem='32G',
        time='10:00:00',  
        cpus=8
    params:
       # files = lambda wildcards: get_files()
    output:
        "results/{ID}/{ID}.done"
    shell:
        """ 
        plink2 --memory 32000 --threads 8 --pmerge-list results/{wildcards.ID}/merge.list --make-pgen --out results/{wildcards.ID}/{wildcards.ID}
       touch "results/{wildcards.ID}/{wildcards.ID}.done"
        dx upload {wildcards.ID}.pvar --destination project-Gf0f5B0JKX1pJ1p9KzJ176bQ:plink_WGS/chr{CHROMOSOME}/
        dx upload {wildcards.ID}.psam --destination project-Gf0f5B0JKX1pJ1p9KzJ176bQ:plink_WGS/chr{CHROMOSOME}/
        dx upload {wildcards.ID}.pgen --destination project-Gf0f5B0JKX1pJ1p9KzJ176bQ:plink_WGS/chr{CHROMOSOME}/
        dx upload {wildcards.ID}.log --destination project-Gf0f5B0JKX1pJ1p9KzJ176bQ:plink_WGS/chr{CHROMOSOME}/
       rm -r results/{wildcards.ID}/temp/
      
       """