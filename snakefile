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
        time='00:50:00',  
        cpus=2
    params: 
 
    output:
         "results/{ID}/temp/{files}.done"
    shell:
        """
        python3 make_plink_chunk.py -f {wildcards.files} -i {wildcards.ID} -c {CHROMOSOME} -k {KEEP_FILE} -m {MAC} -hwe {HWE} -mind {MIND} -g {GENO} -max {MAX_ALLELE}
       touch {output}
       """

rule merge_chunks:
    input:
        expand("results/{ID}/temp/{files}.done", ID=config["ID"], files=file_list)
    resources:
        mem='100G',
        time='05:00:00',  
        cpus=16
    params:
       # files = lambda wildcards: get_files()
    output:
        "results/{ID}/{ID}.done"
    shell:
        """ 
        {PLINK_PATH} --memory 100000 --threads 16 --pmerge-list results/{wildcards.ID}/merge.list --make-pgen --out results/{wildcards.ID}/{wildcards.ID}
       touch "results/{wildcards.ID}/{wildcards.ID}.done"
       rm -r results/{wildcards.ID}/temp/
      
       """