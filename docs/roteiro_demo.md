# Roteiro: esquentar o Redis a partir do S3 (offline store do SageMaker)

Duração alvo: 10 minutos. Ambiente 100% local e reproduzível (MinIO simula o S3).

## Mensagem central

O online store gerenciado do SageMaker não expõe o storage por baixo. O caminho suportado pra hidratar o Redis em massa é o offline store: o bucket S3 da própria conta de vocês, em Parquet, com histórico completo. A carga correta não é cópia de arquivo, é snapshot: última versão por entidade, descartando tombstones.

## Como o dado se parece no S3

Mostre no console do MinIO (http://localhost:9003, minioadmin/minioadmin):

```
s3://bucket/prefixo/111122223333/sagemaker/us-east-1/offline-store/
  customer-features-1755600000/data/year=2026/month=08/day=19/hour=13/
  20260819T130000Z_a1B2c3D4e5F6g7H8.parquet
```

Pontos pra falar em cima disso:

| Fato | Por que importa |
|------|-----------------|
| Sempre Parquet, particionado por event time (hora no formato Glue, dia no Iceberg) | Athena e Spark leem nativo; nada de JSON solto |
| Append-only: várias linhas por entidade | Carga ingênua deixa versão velha por cima da nova |
| Colunas extras: `write_time`, `api_invocation_time`, `is_deleted` | `DeleteRecord` vira tombstone; precisa filtrar |
| `PutRecord` leva até 15 min pra bufferizar no S3 | O offline store atrasa; não é fonte de tempo real |
| O caminho exato vem de `DescribeFeatureGroup.ResolvedOutputS3Uri` | Zero adivinhação de prefixo |

## Demo em 4 passos

```bash
./run_demo.sh
```

1. Sobe MinIO + Redis 8.
2. Gera o offline store fake: 1000 entidades, até 3 versões cada, 30 deletadas, layout idêntico ao da AWS.
3. Hidrata: dedupe (última versão por `customer_id`, ordena por `event_time` e `write_time`, filtra `is_deleted`), escreve HASH pipelined no Redis.
4. Prova: contagem esperada 970 (1000 menos 30 tombstones), validação de amostra campo a campo, `HGETALL` de um cliente ao vivo.

Narrativa do passo 4: "970, não 1000. O loader entendeu a semântica do offline store: quem foi deletado não ressuscita, e quem tem 3 versões entra só com a mais recente."

## Ato 2: o mesmo job rodando no AWS Glue de verdade

Pra público que vive de Glue (bancos e plataformas de ML em geral), rode na sua conta AWS e mostre pelo console:

```bash
REDIS_HOST=<host> REDIS_PORT=<porta> REDIS_PASSWORD=<senha> ./deploy_glue_aws.sh run
```

O que mostrar, nesta ordem:

1. Console do Glue: o job `gabs-fs-materializer-demo` rodando (a ferramenta que o time já usa, sem componente novo).
2. Console do Redis Cloud ou Redis Insight: os hashes `fs:customer-features:*` aparecendo.
3. O output do script: contagem 970/970, amostra validada, e o FT.AGGREGATE calculando média de fraud_score por segmento na hora.
4. Fechamento: "o job é parametrizado pelo mesmo contrato de metadados da plataforma de vocês; feature group novo = outro contrato, zero código novo".

## Bônus: a mesma carga com a ferramenta oficial (RIOT-X)

```bash
./try_riotx_s3.sh
```

O RIOT-X lê o snapshot Parquet direto do S3 e grava os 970 hashes no Redis (db 1). Validado local. Narrativa: "não precisa nem de código: Athena gera o snapshot, RIOT-X carrega. O Python entra quando vocês quiserem o dedupe e a validação embutidos num Glue job."

## Caminho de produção (AWS de verdade)

1. Descobrir onde o dado mora:

```bash
aws sagemaker describe-feature-group --feature-group-name customer-features --query OfflineStoreConfig.S3StorageConfig.ResolvedOutputS3Uri
```

2. Athena gera o snapshot deduplicado (o Feature Store já cria a tabela no catálogo `sagemaker_featurestore`):

```sql
UNLOAD (
  SELECT customer_id, event_time, fraud_score, credit_score, avg_ticket_30d, tx_count_24h, device_trust, segment, is_pep
  FROM (
    SELECT *, ROW_NUMBER() OVER (
      PARTITION BY customer_id
      ORDER BY event_time DESC, write_time DESC
    ) AS rn
    FROM "sagemaker_featurestore"."customer_features_1755600000"
  )
  WHERE rn = 1 AND NOT is_deleted
) TO 's3://meu-bucket/snapshot/customer-features/'
WITH (format = 'PARQUET')
```

3. Carregar o snapshot no Redis com a ferramenta oficial (RIOT-X lê S3 e Parquet nativamente):

```bash
riotx file-import "s3://meu-bucket/snapshot/customer-features/*.parquet" --s3-region us-east-1 -u "rediss://default:SENHA@host:porta" hset "fs:customer-features:#{customer_id}"
```

Alternativa sem RIOT-X: rodar o `hydrate.py` num Glue job, AWS Batch ou SageMaker Processing (mesmo dedupe, mesma validação).

4. Manter quente depois do bootstrap: dual-write no mesmo ponto onde hoje chamam `PutRecord`, e TTL por orçamento de frescor. O offline store vira o caminho de reidratação e reconciliação, não o de tempo real.

## Perguntas pra call

1. O feature group é online+offline ou só online? (sem offline store não existe S3 pra ler; aí o caminho é dual-write)
2. Formato de tabela: Glue ou Iceberg? (muda o particionamento e a leitura)
3. Quantas entidades e qual o tamanho do snapshot deduplicado?
4. Qual frescor o modelo tolera? Isso define TTL e a frequência do refresh via Athena.
5. Quem consome no serving: aceita HASH plano ou precisa de JSON aninhado?

## Gotchas pra não passar vergonha

1. O buffer de 15 min do `PutRecord`: snapshot do offline store nunca é "agora". Feche a janela com dual-write, não com re-scan.
2. Tombstone: `DeleteRecord` insere linha nova com `is_deleted = true`. Filtrar depois do dedupe, não antes.
3. Desempate por `write_time` além de `event_time`: duas escritas com o mesmo event time acontecem.
4. Tier InMemory do próprio SageMaker é ElastiCache (Redis OSS): a AWS valida a arquitetura; a conversa vira confiabilidade, ciclo de vida e requisitos como CMK (que o tier InMemory não suporta).
