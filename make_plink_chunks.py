"""
makes plink chunks - if fail due to too many ALT, will run BCFTOOLS to remove them, if it fails again it drops that chunk!!!!!!
"""
import pandas as pd 
import numpy as np 
import argparse
import sys
import subprocess
import os
import warnings
warnings.filterwarnings('ignore')

def check_arg(args=None):
    parser = argparse.ArgumentParser(description='Clean UK biobank GWAS up for LDSC' ,formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-f', '--file', help='input file',required='True')
    parser.add_argument('-i', '--ID', help='region feature ID eg cytoband or gene ID', required='True')
    parser.add_argument('-c', '--chrom', help='CHROMOSOME', required='True')
    parser.add_argument('-k', '--keep', help='File of the ID that you want to keep in the VCFs', required='True')
    parser.add_argument('-m', '--mac', help='Minimum allele count', required='True')
    parser.add_argument('-hwe', '--hwe', help='Hardy-Weinberg equilibrium', required='True')
    parser.add_argument('-mind', '--mind', help='Missing rate per a person for each chunk', required='True')
    parser.add_argument('-g', '--geno', help='Missing rate per a SNP', required='True')
    parser.add_argument('-max', '--max', help='Max number of alleles', required='True')

    results = parser.parse_args(args)
    return (results.file , results.ID, results.chrom , results.keep , results.mac , results.hwe, results.mind , results.geno , results.max )


def main(FILE , ID, CHROM , KEEP , MAC , HWE, MIND , GENO , MAX_ALLELES ):
    #### make sure you update this to your correct path -- eg replace ../WGS_UKBB/.. to your project name on DNAnexus
    command1 = f"plink2 --memory 4000 --threads 2  --vcf /mnt/data/projects/WGS_UKBB/Bulk/DRAGEN\\ WGS/DRAGEN\\ population\\ level\\ WGS\\ variants\\,\\ pVCF\\ format\\ \\[500k\\ release\\]/chr{CHROM}/{FILE}.vcf.gz --keep {KEEP} --mac {MAC} --hwe {HWE} --mind {MIND} --geno {GENO} --max-alleles {MAX_ALLELES} --make-pgen --out results/{ID}/temp/{FILE}"
    command2 = f"bcftools view --threads 2 --max-alleles {MAX_ALLELES} /mnt/data/projects/WGS_UKBB/Bulk/DRAGEN\\ WGS/DRAGEN\\ population\\ level\\ WGS\\ variants\\,\\ pVCF\\ format\\ \\[500k\\ release\\]/chr{CHROM}/{FILE}.vcf.gz -O z -o results/{ID}/temp/{FILE}.vcf.gz"
    command3 = f"plink2 --memory 4000 --threads 2  --vcf results/{ID}/temp/{FILE}.vcf.gz --keep {KEEP} --mac {MAC} --hwe {HWE} --mind {MIND} --geno {GENO} --max-alleles {MAX_ALLELES} --make-pgen --out results/{ID}/temp/{FILE}"
    command4 = f"echo {FILE} >> results/{ID}/failed_ids.txt"
    merge_list_command = f"echo results/{ID}/temp/{FILE} >> results/{ID}/merge.list"

    try:
        # Try running the first command
        subprocess.check_call(command1, stdout=open(os.devnull, "w"), stderr=subprocess.STDOUT, shell=True)
        subprocess.check_call(merge_list_command, stdout=open(os.devnull, "w"), stderr=subprocess.STDOUT, shell=True)
    except subprocess.CalledProcessError as e:
        print(f"An error occurred while executing plink, now trying BCFTOOLS: {e}")
        try:
            # If the first command fails, try running the second command
            subprocess.check_call(command2, stdout=open(os.devnull, "w"), stderr=subprocess.STDOUT, shell=True)
            subprocess.check_call(command3, stdout=open(os.devnull, "w"), stderr=subprocess.STDOUT, shell=True)
            subprocess.check_call(merge_list_command, stdout=open(os.devnull, "w"), stderr=subprocess.STDOUT, shell=True)
        except subprocess.CalledProcessError as e:
            print(f"An error occurred while trying to make chunk with plink and BCFTOOLS, writing to failed_chunks.txt: {e}")
            try:
                # If the second command fails, try running the third command
                subprocess.check_call(command4, stdout=open(os.devnull, "w"), stderr=subprocess.STDOUT, shell=True)
            except subprocess.CalledProcessError as e:
                print(f"An error occurred while trying to write to failed_ids.txt {e}")
            except Exception as e:
                print(f"yeah somethings wrong: {e}")
        except Exception as e:
            print(f"An unexpected error occurred while using BCFTOOLS/PLINK: {e}")
    except Exception as e:
        print(f"An unexpected error occurred while executing plink {e}")
    

if __name__ == '__main__':
    FILE , ID, CHROM , KEEP , MAC , HWE, MIND , GENO , MAX_ALLELES  = check_arg(sys.argv[1:])
    main(FILE , ID, CHROM , KEEP , MAC , HWE, MIND , GENO , MAX_ALLELES )



