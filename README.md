# Dados_ANP

ETL project for National Petroleum Agency of Brazil data using Azure and Databricks.
Projeto de ETL dos dados da Agência Nacional do Petróleo utilizando Azure e Databricks.

# Data Lake

Preparation of layers in the Data Lake.
Organização das camadas no Data Lake.

![imagem_2023-06-29_120034417](https://github.com/Ronan-Alencar/Dados_ANP/assets/133599706/25a7d95c-1935-496b-9834-3c9b0e5e0ec9)

# Data Factory

Creation of the data ingestion pipeline in Data Factory, sending data from an Http address to the raw layer of the Data Lake, using parameterization for better organization.
Criação da pipeline de ingestão dos dados no Data Factory, enviando os dados de um endereço Http para a camada raw do Data Lake, utilizei a parametrização para uma melhor organização.

![imagem_2023-06-29_115654994](https://github.com/Ronan-Alencar/Dados_ANP/assets/133599706/d7b65b4c-9679-43ce-ab17-29d0cedead60)

![imagem_2023-06-29_122623956](https://github.com/Ronan-Alencar/Dados_ANP/assets/133599706/9e2c764b-5d05-452f-af6a-aec892fee29b)

# Databricks

After acquiring the data for the Data Lake, I used Databricks for data processing.
Após a ingestão dos dados para o Data Lake fiz o uso do Databricks para o processamento dos dados.

#  Generating Mounting point / Gerando Mount point

```python
dbutils.fs.mount  (
    source = "wasbs://container1@datalakeron1.blob.core.windows.net/",
    mount_point = "/mnt/engdados",
    extra_configs = {"fs.azure.account.key.datalakeron1.blob.core.windows.net":"PsXb5605fkCE7/CI4JDDgNsgCIvWC63Ksel1hgGiCanYwaK9CY2Sqp9GDrd+swjT06Ux/9eRlS8M+ASteXC0pQ=="}
)
```
# Listing the Directories / Listando os Diretórios
```python
dbutils.fs.ls ("/mnt/engdados")

display(dbutils.fs.ls ("/mnt/engdados/raw"))
```

# Reading the CSV / Leitura do CSV
```python
df = spark.read.format("csv")\
               .option("sep", ";")\
               .option("header", "true")\
               .option("inferschema", "true")\
               .load("/mnt/engdados/raw/1_sem_2020.csv")
```

# Renaming the Columns / Renomeando as Colunas
```python
df_rename = df.withColumnRenamed ('Regiao - Sigla','REGIAO')\
                   .withColumnRenamed ('Estado - Sigla','ESTADO')\
                   .withColumnRenamed ('Municipio','MUNICIPIO')\
                   .withColumnRenamed ('Revenda','REVENDA')\
                   .withColumnRenamed ('CNPJ da Revenda','CNPJ')\
                   .withColumnRenamed ('Nome da Rua','NOME_RUA')\
                   .withColumnRenamed ('Numero Rua','NUM_RUA')\
                   .withColumnRenamed ('Complemento','COMPLEMENTO')\
                   .withColumnRenamed ('Bairro','BAIRRO')\
                   .withColumnRenamed ('Cep','CEP')\
                   .withColumnRenamed ('Produto','PRODUTO')\
                   .withColumnRenamed ('Data da Coleta','DATA_COLETA')\
                   .withColumnRenamed ('Valor de Venda','VL_VENDA')\
                   .withColumnRenamed ('Valor de Compra','VL_COMPRA')\
                   .withColumnRenamed ('Unidade de Medida','UNID_MED')\
                   .withColumnRenamed ('Bandeira','BANDEIRA')
df_rename.createOrReplaceTempView("TABELA_COMPLETA")
```

# Update of Data Types / Troca dos Data Types
```python
df_data_types = spark.sql(""" SELECT REGIAO,
                                     ESTADO,
                                     MUNICIPIO,
                                     REVENDA, 
                                     CNPJ, 
                                     NOME_RUA, 
                                     NUM_RUA, 
                                     COMPLEMENTO, 
                                     BAIRRO, 
                                     CEP, 
                                     PRODUTO, 
                            TO_DATE (DATA_COLETA, 'dd/MM/yyyy')       AS DATA_COLETA,
                               CAST (REPLACE(VL_VENDA,',','.')      AS NUMERIC (4,3)) AS VL_VENDA,
                               CAST (REPLACE(VL_COMPRA,',','.')     AS NUMERIC (4,3)) AS VL_COMPRA,
                                     UNID_MED,
                                     BANDEIRA

                                FROM TABELA_COMPLETA """)
display(df_data_types)
df_data_types.createOrReplaceTempView("TABELA_CARGA")
```

# Creating a Query / Criando uma Consulta
```python
df_all = spark.sql("SELECT * FROM TABELA_CARGA WHERE VL_VENDA IS NOT NULL AND VL_COMPRA IS NOT NULL")
```
```python
df_all.count()
```
```python
df_all.createOrReplaceTempView("Consulta")
```
```sql
%sql
SELECT REGIAO, ESTADO, REVENDA, PRODUTO, DATA_COLETA, VL_VENDA, VL_COMPRA, UNID_MED, BANDEIRA FROM Consulta
```

# Saving Dataframe to Hive Table / Salvando Dataframe para Hive Table
```python
df_all.write.format("parquet").mode("overwrite").saveAsTable("TB_ANP_Parquet")
```

# Saving in the refined layer of the Data Lake / Salvando na camada refined do Data Lake

Fiz o uso de uma função para melhor visualização dos arquivos no container
```python
df_all.coalesce(1)\
      .write\
      .format("parquet")\
      .mode("overwrite")\
      .save("/mnt/engdados/refined/", header=True)
```
```python
def moverArquivos (extensao, origem, destino, novo_nome):
    files = dbutils.fs.ls(origem)
    for i in range(0, len(files)):
        file = files[i].name
        if extensao in file:
            dbutils.fs.cp(files[i].path, destino + novo_nome + extensao)
            print('Origem é: ' + file + 'Destino é: ' + novo_nome + extensao)
```
```python
moverArquivos ('.parquet', '/mnt/engdados/refined', '/mnt/engdados/refined/', 'tabela_anp')
```
![imagem_2023-06-29_122453451](https://github.com/Ronan-Alencar/Dados_ANP/assets/133599706/6b6b3f36-8b9f-4203-8e32-2858439e1800)





