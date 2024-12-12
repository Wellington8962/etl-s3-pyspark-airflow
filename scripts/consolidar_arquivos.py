#!/usr/bin/env python3
from pyspark.sql import SparkSession

# Inicializar Spark
spark = SparkSession.builder \
    .appName("Consolidar Arquivos") \
    .getOrCreate()

# Caminho dos arquivos no S3
path_senhas_processadas = "s3a://wb-s3-lab/second_step/processed_passwords/"

# Consolidar os dados processados
senhas_processadas = spark.read.json(path_senhas_processadas)
senhas_processadas.coalesce(1).write.csv(
    "s3a://wb-s3-lab/third_step/consolidated_files.csv", 
    mode="overwrite", 
    header=True
)

# Parar Spark
spark.stop()
