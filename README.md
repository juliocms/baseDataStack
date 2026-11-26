# baseDataStack

Stack local com tres imagens Docker para simular um pipeline medalhao no S3, sem conta AWS.

| Servico | Imagem | Funcao |
| --- | --- | --- |
| `emulator` | `basedatastack/aws-emulator` | [AWS Emulator](https://docs.aws.amazon.com/emulator/latest/userguide/getting-started-docker.html) na porta `9297` |
| `airflow` | `basedatastack/airflow` | Agenda o pipeline e cria o bucket |
| `spark` | `basedatastack/pyspark` | Executa o PySpark que le e grava no S3 |
| `spark-history` | `basedatastack/spark-history` | Exibe o historico das execucoes Spark |

O emulador exige o endpoint `https://127.0.0.1:9297` (IP, nao um nome de host) para o S3 em path-style, e entrega certificado e credenciais so quando sobe. Por isso o Airflow e o Spark compartilham a rede do emulador: dentro deles, `127.0.0.1:9297` e o emulador. As portas publicadas no host ficam presas a `127.0.0.1`.

Os dados do S3 ficam na memoria do emulador. Ao parar o container, o bucket some.

## Pre-requisitos

- Docker Desktop com Compose v2
- Cerca de 7 GB de RAM livres (emulador: 2 GB; Spark: 3 GB; History Server: ate 1 GB; alem do Airflow)

## Subir

```powershell
docker compose up --build
```

- Emulador: `https://127.0.0.1:9297`
- Airflow: `http://127.0.0.1:8080` (`admin` / `admin`)
- Spark History: `http://127.0.0.1:18080`

A DAG `medallion_s3` fica pronta para disparo manual.
As execucoes novas aparecem no History Server apos terminarem. Os eventos ficam no volume `spark-events`, separado do container do History Server; execucoes anteriores a esta configuracao nao sao recuperadas.

## Disparar o pipeline

Na interface do Airflow, abra `medallion_s3` e use Trigger DAG.

Ou, em outro terminal:

```powershell
docker compose exec airflow airflow dags trigger medallion_s3
```

Acompanhe com:

```powershell
docker compose logs -f airflow spark
```

## O que a DAG faz

1. `criar_bucket` cria `s3://lakehouse` com o boto3 apontando para o emulador.
2. `bronze` grava pedidos brutos, inclusive linhas invalidas, em `s3://lakehouse/bronze/pedidos/`.
3. `silver` le o bronze, remove nulos, valor negativo e status fora de `pago`, `pendente` e `cancelado`, e grava `s3://lakehouse/silver/pedidos/`.
4. `gold` agrega quantidade, valor e clientes por status e data em `s3://lakehouse/gold/pedidos_por_status/`.
5. `validar_s3` lista os tres prefixos e falha se algum parquet nao existir.

O Spark grava Parquet com um unico `PutObject` por arquivo (sem multipart) e commiter de staging local, porque o plano de dados do emulador e mais estreito que o da AWS.

## Trocar o job

Edite `spark/jobs/medallion.py` e `airflow/dags/medallion_s3.py`. Os dois diretorios estao montados nos containers; o servidor do Spark le o job a cada POST. Reinicie o Airflow se mudar codigo importado na carga da DAG:

```powershell
docker compose restart airflow
```

Se recriar o emulador, recrie tambem os outros dois. O certificado muda a cada subida.

```powershell
docker compose down
docker compose up --build
```

## Limites

- Uma regiao (`us-east-1`) e a conta `000000000000` do emulador.
- A chave gerada so autentica neste emulador. Nao e uma chave AWS.
- IAM e bucket policy nao reproduzem a AWS. Nao use esta stack para teste de seguranca.
- Operacoes de dados que o emulador ainda nao implementa respondem HTTP 501.
