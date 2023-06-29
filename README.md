# Dados_ANP

Projeto de ETL dos dados da Agência Nacional do Petróleo utilizando Azure e Databricks.

# Data Lake

Organização das camadas no Data Lake.

![imagem_2023-06-29_120034417](https://github.com/Ronan-Alencar/Dados_ANP/assets/133599706/25a7d95c-1935-496b-9834-3c9b0e5e0ec9)

# Data Factory

Criação da pipeline de ingestão dos dados no Data Factory, enviando os dados de um endereço Http para a camada raw do Data Lake, utilizei a parametrização para uma melhor organização.

![imagem_2023-06-29_115654994](https://github.com/Ronan-Alencar/Dados_ANP/assets/133599706/d7b65b4c-9679-43ce-ab17-29d0cedead60)

# Databricks

Após a ingestão dos dados para o Data Lake fiz o uso do Databricks para o processamento dos dados.

# Mount point

```python
dbutils.fs.mount  (
    source = "wasbs://container1@datalakeron1.blob.core.windows.net/",
    mount_point = "/mnt/engdados",
    extra_configs = {"fs.azure.account.key.datalakeron1.blob.core.windows.net":"PsXb5605fkCE7/CI4JDDgNsgCIvWC63Ksel1hgGiCanYwaK9CY2Sqp9GDrd+swjT06Ux/9eRlS8M+ASteXC0pQ=="}
)
```


