#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, udf
from pyspark.sql.types import StringType, BooleanType
import string

# Criando a SparkSession
spark = SparkSession.builder \
    .appName("ProcessadorSenhasGeradas") \
    .getOrCreate()

# Caminhos do S3
s3_input_path = "s3a://wb-s3-lab/input/senhas_geradas.json/"
s3_output_path = "s3a://wb-s3-lab/output/senhas_processadas/"

# Função para contar caracteres específicos (maiúsculas, números, símbolos)
def contar_caracteres(senha):
    maiusculas = sum(1 for c in senha if c.isupper())
    numeros = sum(1 for c in senha if c.isdigit())
    simbolos = sum(1 for c in senha if c in string.punctuation)
    return f"Maiúsculas: {maiusculas}, Números: {numeros}, Símbolos: {simbolos}"

# Função para verificar se a senha não possui sequência
def sem_sequencia(senha):
    if senha is None:
        return False
    for i in range(len(senha) - 2):
        if senha[i:i+3] in "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ":
            return False
    return True

# Registrando UDFs
contar_caracteres_udf = udf(contar_caracteres, StringType())
sem_sequencia_udf = udf(sem_sequencia, BooleanType())

if __name__ == "__main__":
    # 1. Carregar o JSON do S3
    df = spark.read.json(s3_input_path)

    # 2. Filtrar senhas que não possuem sequência
    df_filtrado = df.filter(sem_sequencia_udf(col("senha_gerada")))

    # 3. Adicionar uma nova coluna com o comprimento de cada senha
    df_com_comprimento = df_filtrado.withColumn("comprimento", length(col("senha_gerada")))

    # 4. Validar se todas as senhas têm comprimento maior ou igual a 12 caracteres
    df_validado = df_com_comprimento.withColumn(
        "valido",
        col("comprimento") >= 12
    )

    # 5. Contar a frequência de caracteres ou tipos de caracteres
    df_final = df_validado.withColumn("frequencia_caracteres", contar_caracteres_udf(col("senha_gerada")))

    # 6. Exportar o resultado para o bucket S3 de saída
    df_final.write.mode("overwrite").json(s3_output_path)

    print(f"Resultado salvo no S3 em: {s3_output_path}")

    # Mostrar o DataFrame final para verificar
    df_final.show(truncate=False)
