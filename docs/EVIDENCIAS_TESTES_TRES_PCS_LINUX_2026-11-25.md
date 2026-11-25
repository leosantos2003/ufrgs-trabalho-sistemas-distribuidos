# Evidências dos testes em três PCs Linux

**Data dos registros:** 25/11/2026  
**Roteiro executado:** [ROTEIRO_TESTES_TRES_PCS_LINUX.md](ROTEIRO_TESTES_TRES_PCS_LINUX.md)  
**Porta do serviço:** UDP `4000`

Este documento consolida as evidências produzidas na execução distribuída com
três computadores. Os endereços observados nos registros foram:

| Nó | IPv4 | Papel inicial |
| --- | --- | --- |
| A | `143.54.55.14` | Front-End (FE) |
| B | `143.54.55.13` | Replica Manager (RM) |
| C | `143.54.55.12` | Replica Manager (RM) |

Foram usados os 14 arquivos de evidência abaixo, todos na raiz do projeto:

- clientes: [`cliente-a-100.log`](../cliente-a-100.log),
  [`cliente-b-100.log`](../cliente-b-100.log) e
  [`cliente-c-100.log`](../cliente-c-100.log);
- execução inicial e concorrência: [`servidor-a.log`](../servidor-a.log),
  [`servidor-b.log`](../servidor-b.log) e
  [`servidor-c.log`](../servidor-c.log);
- queda e retorno de RM: [`servidor-a-v2.log`](../servidor-a-v2.log),
  [`servidor-b-v2.log`](../servidor-b-v2.log),
  [`servidor-c-v2.log`](../servidor-c-v2.log) e
  [`servidor-c-v2-retorno.log`](../servidor-c-v2-retorno.log);
- queda e retorno de FE: [`servidor-a-v3.log`](../servidor-a-v3.log),
  [`servidor-b-v3.log`](../servidor-b-v3.log),
  [`servidor-c-v3.log`](../servidor-c-v3.log) e
  [`servidor-a-v3-retorno.log`](../servidor-a-v3-retorno.log).

Os arquivos `servidor-1m.log` e `servidor-concorrencia.log` foram
explicitamente excluídos desta consolidação.

## 1. Formação do cluster e requisição de referência

Na primeira execução, A se autoproclamou FE. B e C descobriram A, aplicaram
o snapshot inicial e receberam a topologia com os dois RMs. As linhas de
entrada de B e C constam em `servidor-b.log:1-5` e `servidor-c.log:1-4`; o FE
registra as duas entradas em `servidor-a.log:1-5`.

Em seguida, a operação de referência de valor `10` foi replicada nas duas
réplicas e consolidada com quórum `2/2`:

```text
2026-11-25 10:16:58 FE QUORUM seq_id 1 ... value 10 quorum 2/2
(expected_rms 2) num_reqs 1 total_sum 10
```

Essa confirmação está em `servidor-a.log:8`. B e C registraram a aplicação do
mesmo `seq_id` e o mesmo estado em `servidor-b.log:6` e `servidor-c.log:5`.

**Resultado:** bootstrap, descoberta, sincronização inicial e replicação com
quórum de dois RMs confirmados.

## 2. Concorrência com três clientes

Foram enviados 100 valores por cliente, nos intervalos `1..100`, `101..200`
e `201..300`. Cada um dos três logs de cliente contém exatamente 100 linhas
`SEND`, com identificadores locais de requisição de `1` a `100`. Não há linhas
`TIMEOUT` ou `RESEND` nesses três arquivos.

O FE registrou 301 despachos e 301 consolidações com `votes 2/2`; cada RM
registrou 301 aplicações. A última operação da execução foi:

```text
2026-11-25 10:20:26 FE QUORUM seq_id 301 ... value 200 quorum 2/2
(expected_rms 2) num_reqs 301 total_sum 45160
```

Ela consta em `servidor-a.log:906-908`; o mesmo estado está em
`servidor-b.log:306` e `servidor-c.log:305`. Os encerramentos dos três clientes
também são consistentes com esse total:

| Cliente | Último valor enviado | Última confirmação observada |
| --- | ---: | --- |
| A | `100` | `num_reqs 234`, `total_sum 33971` |
| B | `200` | `num_reqs 301`, `total_sum 45160` |
| C | `300` | `num_reqs 101`, `total_sum 25060` |

As posições finais diferem porque os datagramas dos três clientes foram
intercalados. O estado final é aritmeticamente consistente: a execução já
continha a requisição de referência (`1 / 10`) e recebeu os 300 valores cuja
soma é `45150`; portanto, `1 + 300 = 301` e `10 + 45150 = 45160`.

**Observação de rastreabilidade:** o roteiro prevê reiniciar o cluster antes
deste item e, nesse caso, o resultado isolado seria `300 / 45150`. Os logs
preservam a requisição de referência na mesma execução; por isso evidenciam
corretamente 300 novas operações sem duplicação, mas com estado final
`301 / 45160`.

## 3. Queda de C, continuidade com B e retorno por snapshot

Nesta execução, A iniciou como FE e B/C entraram como RMs ativos
(`servidor-a-v2.log:1-5`). A referência de valor `10` atingiu `1 / 10` com
quórum `2/2` (`servidor-a-v2.log:6-8`).

Após a interrupção de C (`143.54.55.12`), A registrou três perdas de
heartbeat e removeu a réplica:

```text
2026-11-25 10:41:11 FE RM_FAILURE detected rm 143.54.55.12
2026-11-25 10:41:11 FE RM_FAILURE rm 143.54.55.12 active_rms 1
```

As linhas estão em `servidor-a-v2.log:9-13`. B recebeu a topologia reduzida a
ele próprio em `servidor-b-v2.log:7`; não há evento de anel enquanto A
permanece FE.

Com apenas B ativo, a operação de valor `20` foi consolidada com quórum `1/1`,
chegando a `2 / 30` (`servidor-a-v2.log:14-16` e
`servidor-b-v2.log:8`). C retornou, descobriu A e aplicou o snapshot:

```text
2026-11-25 10:47:13 RM SNAPSHOT_APPLIED num_reqs 2 total_sum 30 expected_seq 3
```

A sincronizou e reincorporou C com `active_rms 2`
(`servidor-a-v2.log:17-18`). Por fim, o valor `30` foi confirmado com quórum
`2/2`, resultando em `3 / 60` (`servidor-a-v2.log:19-21`). B e o C retornado
registraram a aplicação do `seq_id 3` em `servidor-b-v2.log:10` e
`servidor-c-v2-retorno.log:5`.

**Resultado:** detecção da queda de RM, reconfiguração para um RM, continuidade
do serviço e recuperação por snapshot confirmadas.

## 4. Queda do FE e eleição de novo líder

Há duas evidências independentes da eleição após a queda de A. Ao término da
execução anterior, B e C detectaram a ausência de A às `10:49:57`. Os dois
processos participaram de rodadas de anel e convergiram para B (`143.54.55.13`)
como novo FE, o maior IP entre os RMs:

```text
2026-11-25 10:49:57 RING NEW_FRONTEND ... ip 143.54.55.13
```

Esse anúncio está em `servidor-b-v2.log:23`, e as mensagens correspondentes de
detecção, eleição e coordenador estão em `servidor-b-v2.log:11-22` e
`servidor-c-v2-retorno.log:6-19`.

O cenário foi repetido numa nova inicialização (`*-v3.log`). A iniciou como FE
e B/C como RMs (`servidor-a-v3.log:1-5`). Após a falha de A, B detectou a falha
às `10:57:15` e C às `10:57:16`. As rodadas iniciadas quase simultaneamente
convergiram novamente para B; a transição foi publicada por:

```text
2026-11-25 10:57:16 RING NEW_FRONTEND ... ip 143.54.55.13
```

As evidências completas estão em `servidor-b-v3.log:6-20` e
`servidor-c-v3.log:5-18`. Já como FE, B despachou o valor `2`, confirmou
quórum `1/1` e atingiu `1 / 2` (`servidor-b-v3.log:21-23`); C aplicou o mesmo
estado (`servidor-c-v3.log:19`).

**Resultado:** a detecção de falha do FE, o encaminhamento dos tokens de anel e
a promoção do RM de maior IP foram confirmados. Não foi preservado um log de
cliente específico desta etapa; assim, os logs disponíveis comprovam a
continuidade no servidor após a promoção, mas não documentam a redescoberta e
as tentativas do cliente.

## 5. Retorno do antigo FE como RM

Após a promoção de B, A retornou e descobriu corretamente o novo FE:

```text
2026-11-25 11:00:01 SERVER DISCOVERY: FE found ip 143.54.55.13; starting as RM
2026-11-25 11:00:01 RM SNAPSHOT_APPLIED num_reqs 1 total_sum 2 expected_seq 2
```

As linhas estão em `servidor-a-v3-retorno.log:1-4`. B registrou a
sincronização e a reinclusão de A como segundo RM em
`servidor-b-v3.log:24-25`.

Depois, o valor `3` foi confirmado por B com quórum `2/2`, produzindo `2 / 5`
(`servidor-b-v3.log:26-28`). Tanto A quanto C aplicaram o `seq_id 2`
(`servidor-a-v3-retorno.log:5` e `servidor-c-v3.log:21`).

**Observação de rastreabilidade:** nesta execução a operação registrada após o
failover foi o valor `2`, seguida do valor `3`; por isso o estado evidenciado é
`2 / 5`. O roteiro propõe ainda uma operação inicial de valor `1` antes da
falha e, nesse percurso exato, esperaria `3 / 6`. A evidência disponível valida
a recuperação e a manutenção do histórico efetivamente executado.

## Conclusão

Os 14 logs analisados demonstram, em máquinas Linux distintas e na mesma rede:

- descoberta e formação de um cluster com um FE e dois RMs;
- replicação ativa com confirmações de quórum `2/2`;
- processamento concorrente de 300 requisições de três clientes, sem registros
  de timeout ou reenvio nos logs preservados;
- remoção de RM por heartbeat, continuidade com quórum `1/1` e retorno por
  snapshot;
- eleição de B (`143.54.55.13`) como novo FE após a queda de A;
- retorno de A como RM e nova operação com quórum `2/2`.

Os valores finais de concorrência (`301 / 45160`) e de retorno do FE (`2 / 5`)
foram registrados tal como observados e têm explicação aritmética nos próprios
cenários. Eles diferem dos checkpoints numéricos isolados do roteiro porque
as respectivas execuções mantiveram ou alteraram a sequência de operações de
referência.
