import os
import pandas as pd

configfile: "config.yaml"
#################

cyto_file=config["cyto_file"]
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

def get_files(wildcards):
    #print (wildcards)
    ID = wildcards
    #PATH = wildcards[0]
    temp = df[df['ID'] == ID]["Files"].str.split(",",expand=True).transpose()
    temp.columns = ["Files"]
    temp["Files"] = temp.Files.str.strip()
    temp["Files"] = temp["Files"].str.replace(r'.vcf.gz', '')
    #temp["Files"] = PATH + "/" + ID + "/" + temp.Files + ".done"
    temp = temp.Files
    temp = temp.str.strip()
    temp2 = temp.to_list()
    print (temp2)
   # temp2 = [x[:-7] for x in temp.split(", ")]
    return temp2
    #id_files = [item for sublist in temp2 for item in sublist]
    #return id_files
    #send_back =  [f"{PATH}{ID}/{file}.done" for file in id_files]
    #print ("send_back")
    #return send_back
############################################


rule all:
    input:
        expand("{PATH}{ID}/{ID}.done", ID=df["ID"], PATH=RESULTS_PATH),
    
rule create_plink_chunks:
    input:
          "chr1.extract.txt"
    resources:
        mem='1G',
        time='00:50:00',  
        cpus=1
    params: 
        results = {RESULTS_PATH},
        #files = lambda wildcards: get_files()
    output:
        "{PATH}/{ID}/{file}.done"
    shell:
        """
       touch "{wildcards.PATH}/{wildcards.ID}/{wildcards.file}.done"

       """

rule merge_chunks:
    input:
          #lambda wildcards: expand('{PATH}/{ID}/{file}.done', ID=wildcards.ID, PATH=wildcards.RESULT_PATH, file=(get_files(wildcards.ID)))
         lambda wildcards: ["{wildcards.RESULT_PATH/{wildcards.ID}/{file}.done".format(file) for file in get_files(wildcards.ID) ]
            
    resources:
        mem='1G',
        time='05:00:00',
        partition="high_p",  
        cpus=1
    params:
       # files = lambda wildcards: get_files()
    output:
        "{RESULT_PATH}/{ID}/{ID}.done"
    shell:
        """ 
       touch "{RESULT_PATH}{wildcards.ID}/{wildcards.ID}.done"

       """

