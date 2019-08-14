import subprocess
import sys
import threading


### Creation of lists & sample concatenation ### 
subprocess.call("ls *.fastq.gz > list", shell=True)
subprocess.call('''awk -F "_L" '{print$1}' list > R1_list''',shell=True)
subprocess.run('while read line; do cat "$line"_L001_R1_001.fastq.gz "$line"_L002_R1_001.fastq.gz "$line"_L003_R1_001.fastq.gz "$line"_L004_R1_001.fastq.gz > "$line"_R1.fastq.gz ; done < R1_list', shell = True)
subprocess.call("mkdir uncat", shell=True)
subprocess.call("mv *L00* uncat/.", shell=True)
subprocess.call("ls *.fastq.gz > list1", shell=True)
subprocess.call('''awk -F "_S" '{print$1}' list1 > R2_list''', shell=True)
subprocess.call('while read line; do mv "$line"_S*_R1.fastq.gz "$line"_R1.fastq.gz; done < R2_list', shell=True)
### Trim Galore ###
infile = open("R2_list") 

outfile = open("trimG_batch_qsub.sh", "w")

#outfile.write("#! /bin/bash\n#$ -N trimG_hg38_qsub\n#$ -q som,sam,bio,pub8i\n#$ -pe openmp 1\n\n")

for line in infile:

        name = line.strip().split('\n')[0]

        out = open(("t_" + name + "trimG_hg38.sh"), "w")

        out.write("#! /bin/bash\n#$ -N t_%strimG_hg38\n#$ -pe openmp 1\n#$ -q sam,som,sam128,bio,pub8i,pub64\n\n"%(name))
        out.write("module load fastx_toolkit/0.0.14\nmodule load fastqc/0.11.5\n")
        out.write("source activate py5\n")
        out.write("~/software/trimglore/trim_galore  \
                 --fastqc --stringency 5 --rrbs --length 30 --non_directional -o /dfs3/bio/hollinge/methylation_project/scripts/tests/%s/ \
                /dfs3/bio/hollinge/methylation_project/scripts/tests/%s_R1.fastq.gz"%(name,name))
        out.close()
        outfile.write("t_%strimG_hg38.sh\n"%(name))

outfile.close()

### Batch Trim Submission that waits until trimming is complete to run Bismark ###
def function_call():
    subprocess.run('while read line; do qsub "$line"; done < trimG_batch_qsub.sh', shell = True)
def main():
    thread = threading.Thread(target=function_call)
    thread.start()
    thread.join()

### Bismark ### 
### This program can generate the batch qsub file for Bismark to deal with methylation data.

infile = open("R2_list")   # the list of the trimmed methylation data files

outfile = open("bismark_batch_qsub.sh", "w")

for line in infile:

        name = line.strip().split('\n')[0]

        out = open(("t_" + name + "bismark_hg38.sh"), "w")

        out.write("#! /bin/bash\n#$ -N t_%sbismark_hg38\n#$ -pe openmp 8\n#$ -q sam128,sam,bio,pub64\n\n"%(name))
        out.write("module load bowtie2/2.2.7\nmodule load samtools/1.0\n")
        out.write("~/software/Bismark/bismark -p 8 --sam --non_directional /data/users/hollinge/hg38/ /dfs3/bio/hollinge/methylation_project/scripts/tests/%s/%s_R1_trimmed.fq.gz -o /dfs3/bio/hollinge/methylation_project/scripts/tests/%s/"%(name,name,name))
        out.close()
        outfile.write("t_%sbismark_hg38.sh\n"%(name))

outfile.close()

### Batch Bismark Mapping Submission ###
def function_call():
    subprocess.run('while read line; do qsub "$line"; done < bismark_batch_qsub.sh', shell = True)

def main():
    thread = threading.Thread(target=function_call)
    thread.start()
    thread.join()


### Methylation Extractor ### 

infile = open("R2_list")   # the list of the trimmed sam files 

outfile = open("bismark_extract_batch_qsub.sh", "w")

for line in infile:

        name = line.strip().split('\n')[0]

        out = open(("t_" + name + "extract_hg38.sh"), "w")

        out.write("#! /bin/bash\n#$ -N t_%sextract_hg38\n#$ -pe openmp 8\n#$ -q bio,sam,som,pub64,pub8i\n"%(name))
        out.write("module load bowtie2/2.2.7\nmodule load samtools/1.0\n")
        out.write("~/software/Bismark/bismark_methylation_extractor --multicore 8 -s --bedGraph --zero_based  /dfs3/bio/hollinge/methylation_project/scripts/tests/%s/%s_R1_trimmed_bismark_bt2.sam -o /dfs3/bio/hollinge/methylation_project/scripts/tests/%s/"%(name,name,name))
        out.close()
        outfile.write("t_%sextract_hg38.sh\n"%(name))

outfile.close()

subprocess.run('while read line; do qsub "$line"; done < bismark_extract_batch_qsub.sh', shell = True)
thread = threading.Thread(target=function_call)
thread.start()
thread.join()

### Coverage Filtering ### 

### Sort by Chromosome ###

### Symmetric CpG ### 

