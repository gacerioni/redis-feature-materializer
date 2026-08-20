# Runbook: hidratar o Redis a partir do SageMaker Feature Store (offline store)

Ambiente: a conta AWS do cliente. Ferramentas: AWS CLI, Athena e RIOT-X (ferramenta oficial Redis, binário único, open source). Nenhum código custom.

Placeholders: `<FEATURE_GROUP>`, `<RECORD_ID>` (coluna identificadora), `<EVENT_TIME>` (coluna de event time), `<BUCKET>`, `<REDIS_URI>` (ex.: `rediss://default:SENHA@host:porta`).

## Antes de começar: onde rodar

O RIOT-X precisa alcançar o S3 (fácil) e o Redis (se o endpoint for privado via PrivateLink ou peering, rode de dentro da VPC). Opções em ordem de menor atrito:

1. CloudShell com ambiente VPC (zero infra nova)
2. Uma EC2 ou bastion que já exista na VPC
3. Laptop, somente se o Redis tiver endpoint público liberado

Instalar o RIOT-X:

```bash
brew install redis/tap/riotx
```

Ou sem instalar nada, via Docker: `docker run riotx/riotx ...`

## 1. Descobrir onde o dado mora

```bash
aws sagemaker describe-feature-group \
  --feature-group-name <FEATURE_GROUP> \
  --query '{S3: OfflineStoreConfig.S3StorageConfig.ResolvedOutputS3Uri, Catalogo: OfflineStoreConfig.DataCatalogConfig}'
```

Saída: o URI do Parquet no S3 e o nome da tabela que o Feature Store registrou no Glue (database `sagemaker_featurestore`). Se `OfflineStoreConfig` vier vazio, o feature group é online-only: não existe S3 para ler e o caminho vira dual-write na ingestão. Pare aqui e realinhe.

## 2. Gerar o snapshot deduplicado (Athena)

O offline store é append-only com tombstones. O snapshot correto é a última versão por entidade, sem os deletados. Rode no console do Athena ou via CLI:

```sql
UNLOAD (
  SELECT <RECORD_ID>, <EVENT_TIME>, feature_1, feature_2, feature_3
  FROM (
    SELECT *, ROW_NUMBER() OVER (
      PARTITION BY <RECORD_ID>
      ORDER BY <EVENT_TIME> DESC, write_time DESC
    ) AS rn
    FROM "sagemaker_featurestore"."<TABELA_DO_FEATURE_GROUP>"
  )
  WHERE rn = 1 AND NOT is_deleted
) TO 's3://<BUCKET>/snapshot/<FEATURE_GROUP>/'
WITH (format = 'PARQUET')
```

Regras do UNLOAD: o prefixo de destino precisa estar vazio (apague ou versione a cada execução, ex.: sufixo com data).

## 3. Carregar no Redis (RIOT-X)

```bash
riotx file-import "s3://<BUCKET>/snapshot/<FEATURE_GROUP>/*" \
  -t parquet \
  --s3-region sa-east-1 \
  -u "<REDIS_URI>" \
  hset "fs:<FEATURE_GROUP>:#{<RECORD_ID>}"
```

Detalhes que evitam suporte:

| Detalhe | Motivo |
|---------|--------|
| `-t parquet` é obrigatório | O Athena UNLOAD grava arquivos sem extensão; sem a flag o RIOT-X não detecta o formato |
| Credenciais AWS | Vêm da cadeia padrão (role da EC2/CloudShell); `--s3-access`/`--s3-secret` só se precisar forçar |
| Chave `fs:<grupo>:#{id}` | Um hash por entidade; o app lê com `HGETALL`/`HMGET` |

## 4. Validar

Contagem na origem (Athena):

```sql
SELECT COUNT(*) FROM (
  SELECT <RECORD_ID>, ROW_NUMBER() OVER (
    PARTITION BY <RECORD_ID>
    ORDER BY <EVENT_TIME> DESC, write_time DESC
  ) AS rn, is_deleted
  FROM "sagemaker_featurestore"."<TABELA_DO_FEATURE_GROUP>"
) WHERE rn = 1 AND NOT is_deleted
```

Contagem e amostra no destino:

```bash
redis-cli -u "<REDIS_URI>" --scan --pattern 'fs:<FEATURE_GROUP>:*' | wc -l
```

```bash
redis-cli -u "<REDIS_URI>" HGETALL fs:<FEATURE_GROUP>:<UM_ID_CONHECIDO>
```

As duas contagens devem bater 1:1. A amostra deve conferir com um `GetRecord` do mesmo id no Feature Store.

## 5. Manter quente depois do bootstrap

1. Curto prazo (PoV): reexecutar passos 2 e 3 em janela agendada (EventBridge + Step Functions ou um cron simples). Lembrete: o `PutRecord` leva até 15 min para bufferizar no offline store, então o snapshot sempre atrasa esse tanto.
2. Definitivo: dual-write no mesmo ponto do código onde hoje chamam `PutRecord` (uma escrita Redis a mais), com TTL pelo orçamento de frescor de cada feature group.

## Critérios de sucesso sugeridos para a PoV

1. Hidratar [N] entidades do offline store para o Redis em até [X] minutos, com contagem Redis igual à contagem Athena (100%).
2. Ler o feature vector no Redis com p99 menor ou igual a [Y] ms sustentando [Z] QPS.
3. Reexecutar o refresh completo em janela de [W] minutos sem indisponibilidade de leitura.
