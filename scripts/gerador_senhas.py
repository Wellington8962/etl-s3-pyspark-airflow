#!/usr/bin/env python
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import random
import string

# Criando a SparkSession
spark = SparkSession.builder \
    .appName("GeradorSenhasAleatorias") \
    .getOrCreate()

# Definindo a função UDF para gerar senhas com comprimento variável
def gerar_senha_udf(incluir_maiusculas=True, incluir_minusculas=True, incluir_numeros=True, incluir_simbolos=True):
    comprimento_senha = random.randint(8, 16)  # Comprimento variável entre 8 e 16
    caracteres = ""
    if incluir_maiusculas:
        caracteres += string.ascii_uppercase
    if incluir_minusculas:
        caracteres += string.ascii_lowercase
    if incluir_numeros:
        caracteres += string.digits
    if incluir_simbolos:
        caracteres += string.punctuation

    if not caracteres:
        return "Erro: Pelo menos um conjunto de caracteres deve ser selecionado."

    senha = ''.join(random.choice(caracteres) for _ in range(comprimento_senha))
    return senha

# Registrando a função UDF no Spark
gerar_senha_spark_udf = udf(gerar_senha_udf, StringType())

if __name__ == "__main__":
    # Criando dados de entrada para gerar múltiplas senhas
    input_data = [(True, True, True, True)] * 10  # Gera 10 senhas
    schema = ["incluir_maiusculas", "incluir_minusculas", "incluir_numeros", "incluir_simbolos"]
    input_df = spark.createDataFrame(input_data, schema)

    # Aplicando a UDF para gerar senhas
    senha_gerada_df = input_df.withColumn("senha_gerada", gerar_senha_spark_udf(
        "incluir_maiusculas", "incluir_minusculas", "incluir_numeros", "incluir_simbolos"
    ))

    # Caminho do bucket S3 de entrada
    s3_input_path = "s3a://wb-s3-lab/first_step/generated_passwords.json"

    # Salvando o DataFrame no S3 em formato JSON
    senha_gerada_df.write.mode("overwrite").json(s3_input_path)

    print(f"Senhas geradas salvas no S3 em: {s3_input_path}")

    # Exibindo as senhas geradas
    senha_gerada_df.select("senha_gerada").show(truncate=False)
