#!/usr/bin/env python3
from pyspark.sql import SparkSession

# Inicializar Spark
spark = SparkSession.builder \
    .appName("Consolidar Arquivos") \
    .getOrCreate()

# Caminho dos arquivos no S3
path_senhas_geradas = "s3a://wb-s3-lab/input/senhas_geradas.json/"
path_senhas_processadas = "s3a://wb-s3-lab/output/senhas_processadas/"

# Consolidar os dados gerados
senhas_geradas = spark.read.json(path_senhas_geradas)
senhas_geradas.coalesce(1).write.csv(
    "s3a://wb-s3-lab/output/senhas_geradas_consolidado.csv", 
    mode="overwrite", 
    header=True
)

# Consolidar os dados processados
senhas_processadas = spark.read.json(path_senhas_processadas)
senhas_processadas.coalesce(1).write.csv(
    "s3a://wb-s3-lab/output/senhas_processadas_consolidado.csv", 
    mode="overwrite", 
    header=True
)

# Parar Spark
spark.stop()
