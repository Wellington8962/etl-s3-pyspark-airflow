Aqui está a versão atualizada do seu README.md, incluindo as novas etapas para configurar a conexão AWS no Airflow, configurar variáveis no Airflow e adicionar as configurações para o comando `spark-submit`:

---

# ETL S3 with PySpark and Airflow: Passwords Consolidation Project

This project demonstrates an ETL pipeline orchestrated with **Apache Airflow**, utilizing **PySpark** to process password data stored in an **AWS S3** bucket. The pipeline performs data generation, processing, and consolidation tasks, showcasing efficient orchestration and integration with cloud storage.

---

## Project Description

### ETL Pipeline Steps

1. **Generate Passwords**: A PySpark script generates random password data and writes it to an **AWS S3 bucket**.
2. **Process Passwords**: Another PySpark script processes the generated password data, performing necessary transformations or validations, and writes the processed output back to the S3 bucket.
3. **Consolidate Files**: The final PySpark script consolidates all processed files into a single output file stored in the S3 bucket.

---

## Project Structure

- `scripts/gerador_senhas.py`: Script to generate password data and save it to S3.
- `scripts/processar_senhas.py`: Script to process the generated password data.
- `scripts/consolidar_arquivos.py`: Script to consolidate the processed data into a single file.
- `dags/etl_s3_lab_dag.py`: Airflow DAG orchestrating the ETL process.
- `README.md`: Project documentation (this file).

---

## Prerequisites

- Python 3.10+
- Apache Airflow installed and configured
- Apache Spark with PySpark
- AWS CLI configured with valid credentials
- Access to an S3 bucket

---

## Execution Instructions

### 1. Clone the Repository

```bash
git clone https://github.com/Wellington8962/etl-s3-pyspark-airflow.git
cd etl-s3-pyspark-airflow
```

### 2. Set Up Virtual Environment

```bash
python3.10 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

---

### 3. Configure Airflow

1. **Initialize Airflow**:
   ```bash
   airflow db init
   ```

2. **Create an Airflow User** *(optional)*:
   If you prefer to create a custom user:
   ```bash
   airflow users create \
       --username admin \
       --firstname Admin \
       --lastname User \
       --role Admin \
       --email admin@example.com
   ```

3. **Start Airflow** *(alternative option)*:
   For a quicker setup without creating a user, you can use the standalone mode:
   ```bash
   airflow standalone
   ```
   > This command starts both the Airflow webserver and scheduler, and automatically creates an admin user. The username and password will be displayed in the terminal.

4. **Access the Airflow UI**:
   Open the browser and navigate to `http://localhost:8080` to access the Airflow UI.

---

## **Configure AWS Connection in Airflow**

1. In the Airflow UI, go to **Admin > Connections**.
2. Create or edit a connection with the following details:
   - **Connection Id**: `aws_default` (or a custom name).
   - **Connection Type**: `Amazon Web Services`.
   - **AWS Access Key ID**: Enter the **Access Key** in the *Login* field.
   - **AWS Secret Access Key**: Enter the **Secret Key** in the *Password* field.
   - **Extra**: Optionally, include the region in JSON format:
     ```json
     {
       "region_name": "us-east-1"
     }
     ```
3. Save the connection.

---

## **Configure Variables in Airflow**

1. In the Airflow UI, go to **Admin > Variables**.
2. Add or update the following variables:
   - **Key**: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` (if using temporary credentials).
   - **Value**: Enter the corresponding values.
3. Save the changes.

---

### 4. Update Spark Configuration

- Edit `spark-defaults.conf` to include AWS credentials and endpoints:
  ```
  spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain
  spark.hadoop.fs.s3a.endpoint=s3.amazonaws.com
  spark.hadoop.fs.s3a.region=us-east-1
  ```

---

### 5. Execute the Pipeline

1. Upload the `etl_s3_lab_dag.py` file to the Airflow DAGs directory.
2. Access the Airflow UI at `http://localhost:8080`.
3. Trigger the `etl_s3_lab` DAG manually to execute the ETL pipeline.

---

## Project Workflow

1. **Generate Passwords**:
   - Generates password data with random fields.
   - Stores data in `s3://<bucket-name>/input/`.

2. **Process Passwords**:
   - Reads the generated data from S3.
   - Applies transformations (e.g., cleaning or validation).
   - Writes the processed data to `s3://<bucket-name>/output/`.

3. **Consolidate Files**:
   - Reads all processed files from S3.
   - Consolidates them into a single output file.
   - Writes the consolidated file to `s3://<bucket-name>/output/`.

---

## Additional Notes

- **Spark Configuration**: Ensure `spark-submit` commands include necessary AWS and Hadoop libraries.
- **AWS Permissions**: The AWS user or role must have S3 read/write permissions.
- **Monitoring**: Use the Airflow UI to monitor DAG execution and logs.

---

## Repository

[ETL S3 PySpark Airflow Repository](https://github.com/Wellington8962/etl-s3-pyspark-airflow)

--- 