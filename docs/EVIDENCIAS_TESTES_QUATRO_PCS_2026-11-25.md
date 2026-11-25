# Evidências dos testes em quatro PCs Linux

**Data dos testes:** 25/11/2026  
**Roteiro seguido:** [ROTEIRO_TESTES_TRES_PCS_LINUX.md](ROTEIRO_TESTES_TRES_PCS_LINUX.md), seção 8  
**Porta de comunicação:** UDP 4000

Este documento consolida as evidências dos testes executados com quatro
computadores. Os arquivos de log citados estão na raiz do repositório e foram
gerados durante as execuções descritas abaixo.

## Topologia usada

| Papel inicial | Computador | IP |
|---|---:|---|
| Front End (FE) | A | `143.54.55.14` |
| Replica Manager (RM) | B | `143.54.55.13` |
| Replica Manager (RM) | C | `143.54.55.12` |
| Replica Manager (RM) | D | `143.54.55.11` |

No ensaio de eleição em anel, os rótulos do roteiro correspondem a:

| Rótulo | Computador | IP |
|---|---:|---|
| `L_IP` | D | `143.54.55.11` |
| `M_IP` | C | `143.54.55.12` |
| `H_IP` | B | `143.54.55.13` |

Foram usados os conjuntos de logs `*-4pcs.log`, `*-4pcs-v2.log` e
`*-4pcs-v3.log`. A primeira execução demonstra a operação com três RMs e a
queda de um deles; a execução `v3` demonstra a eleição com sucessor
indisponível e a continuidade após a remoção desse RM.

## 1. Formação do grupo com três RMs e quórum de maioria

O computador A iniciou como FE e B, C e D entraram como RMs. O log de A
registra os três `RM_JOIN` e o conjunto ativo chegando a três réplicas:

```text
servidor-a-4pcs.log:3  FE RM_JOIN ... 143.54.55.13 active_rms 1
servidor-a-4pcs.log:5  FE RM_JOIN ... 143.54.55.12 active_rms 2
servidor-a-4pcs.log:7  FE RM_JOIN ... 143.54.55.11 active_rms 3
```

A primeira requisição, de valor `10`, foi despachada como sequência 1. O FE
confirmou a operação com dois votos de três, isto é, a maioria esperada para o
grupo inicial:

```text
servidor-a-4pcs.log:8   FE DISPATCH seq_id 1 client 143.54.55.13 id_req 1 value 10
servidor-a-4pcs.log:9   FE QUORUM_READY seq_id 1 votes 2/2
servidor-a-4pcs.log:10  FE QUORUM seq_id 1 ... quorum 2/2 (expected_rms 3) num_reqs 1 total_sum 10
```

As três réplicas registraram a aplicação da mesma sequência e do mesmo estado:

```text
servidor-b-4pcs.log:7  RM APPLIED seq_id 1 ... num_reqs 1 total_sum 10
servidor-c-4pcs.log:6  RM APPLIED seq_id 1 ... num_reqs 1 total_sum 10
servidor-d-4pcs.log:6  RM APPLIED seq_id 1 ... num_reqs 1 total_sum 10
```

**Resultado:** aprovado. Há três RMs ativos e a confirmação ocorre por maioria
(`2/3`), sem exigir que as três respostas cheguem.

## 2. Queda de um RM e continuidade com os dois restantes

Após interromper C (`143.54.55.12`), A detectou três perdas consecutivas de
heartbeat, declarou a falha e removeu o RM do conjunto ativo:

```text
servidor-a-4pcs.log:11-13  FE RM_HEARTBEAT_MISS ... 143.54.55.12 missed 1/2/3
servidor-a-4pcs.log:14     FE RM_FAILURE detected rm 143.54.55.12
servidor-a-4pcs.log:15     FE RM_FAILURE rm 143.54.55.12 active_rms 2
```

Em seguida, a requisição de valor `20` foi confirmada. Agora o FE aguardava os
dois RMs sobreviventes, e o estado acumulado passou de `1 / 10` para `2 / 30`:

```text
servidor-a-4pcs.log:17  FE DISPATCH seq_id 2 client 143.54.55.13 id_req 2 value 20
servidor-a-4pcs.log:18  FE QUORUM_READY seq_id 2 votes 2/2
servidor-a-4pcs.log:18  FE QUORUM seq_id 2 ... quorum 2/2 (expected_rms 2) num_reqs 2 total_sum 30
```

B e D também aplicaram a sequência 2 com o estado final esperado:

```text
servidor-b-4pcs.log:9  RM APPLIED seq_id 2 ... num_reqs 2 total_sum 30
servidor-d-4pcs.log:7  RM APPLIED seq_id 2 ... num_reqs 2 total_sum 30
```

**Resultado:** aprovado. A queda de um entre três RMs foi detectada e o serviço
continuou com os dois RMs remanescentes.

## 3. Eleição em anel com sucessor imediato indisponível

Na execução `v3`, o FE A foi interrompido e C (`M_IP`) também estava
indisponível. D (`L_IP`) detectou a falha do FE, iniciou a eleição e tentou
encaminhar o token para C, seu sucessor imediato. Sem confirmação, registrou a
indisponibilidade e encaminhou o token a B (`H_IP`):

```text
servidor-d-4pcs-v3.log:8   RM FE_FAILURE_DETECTED fe 143.54.55.14
servidor-d-4pcs-v3.log:9   RING ELECTION_START members 143.54.55.11,143.54.55.12,143.54.55.13
servidor-d-4pcs-v3.log:10  RING ELECTION_SEND ... to 143.54.55.12
servidor-d-4pcs-v3.log:11  RING SUCCESSOR_UNAVAILABLE ... successor 143.54.55.12
servidor-d-4pcs-v3.log:12  RING ELECTION_SEND ... to 143.54.55.13
servidor-d-4pcs-v3.log:13  RING ELECTION_FORWARDED ... to 143.54.55.13
```

B recebeu e concluiu a eleição, tornando-se o novo FE:

```text
servidor-b-4pcs-v3.log:17  RING ELECTION_RECEIVED candidate 143.54.55.13
servidor-b-4pcs-v3.log:18  RING ELECTION_COMPLETED leader 143.54.55.13
servidor-b-4pcs-v3.log:22  RING NEW_FRONTEND ... ip 143.54.55.13
```

**Resultado:** aprovado. O anel não ficou bloqueado pelo sucessor imediato
inativo; D pulou C e a eleição elegeu B (`H_IP`).

## 4. Continuidade da requisição pendente após a promoção

Logo após B assumir como FE, ele recebeu a requisição de valor `20`, mas ainda
considerava C parte do conjunto de RMs. As tentativas de replicação são
esperadas até que o detector de falhas remova C. O log mostra a remoção, o
recálculo do quórum pendente e a confirmação da requisição:

```text
servidor-b-4pcs-v3.log:23       FE DISPATCH seq_id 2 client 143.54.55.11 id_req 2 value 20
servidor-b-4pcs-v3.log:124-136  FE RM_HEARTBEAT_MISS rm 143.54.55.12 missed 1/2/3
servidor-b-4pcs-v3.log:137      FE RM_FAILURE detected rm 143.54.55.12
servidor-b-4pcs-v3.log:138      FE RM_FAILURE rm 143.54.55.12 active_rms 1
servidor-b-4pcs-v3.log:139      FE QUORUM_UPDATED seq_id 2 removed_rm 143.54.55.12 expected_rms 1 quorum 1
servidor-b-4pcs-v3.log:140      FE QUORUM_REACHED_AFTER_FAILURE seq_id 2
servidor-b-4pcs-v3.log:142      FE QUORUM seq_id 2 ... quorum 1/1 (expected_rms 1) num_reqs 2 total_sum 30
```

D, o único RM sobrevivente, recebeu a sequência 2 e a nova topologia sem C:

```text
servidor-d-4pcs-v3.log:24  RM APPLIED seq_id 2 ... num_reqs 2 total_sum 30
servidor-d-4pcs-v3.log:25  RM TOPOLOGY fe 143.54.55.13 rms 143.54.55.11
```

**Resultado:** aprovado. Mesmo com A e C indisponíveis, B assumiu como FE,
removeu C depois do timeout de heartbeat e confirmou a operação pendente com o
único RM remanescente. O estado permaneceu consistente em `num_reqs = 2` e
`total_sum = 30`.

## Observação sobre a execução `v2`

Os logs `servidor-*-4pcs-v2.log` são evidência suplementar de operação normal,
de aplicação das sequências 1 e 2 e de detecção da queda de C. Contudo, neles a
requisição de valor `20` foi confirmada **antes** da queda de C. Portanto, eles
não foram usados como prova da continuidade após a falha; essa comprovação é
feita pelos logs sem sufixo (queda de um RM) e pelos logs `v3` (eleição e
continuidade após a promoção).

## Conclusão

As evidências confirmam os critérios do roteiro para quatro PCs:

- formação de três RMs e confirmação por maioria (`2/3`);
- detecção da queda de um RM e continuidade com os dois restantes (`2/2`);
- eleição em anel que pula o sucessor indisponível;
- promoção de B a FE e conclusão da requisição pendente após a retirada de C
  do cálculo de quórum.

Não há, entre os arquivos disponibilizados, um log de cliente específico dessa
execução de quatro PCs. Ainda assim, os logs do FE e dos RMs comprovam o
despacho, a mudança de topologia e a aplicação final da requisição de valor
`20` com o estado `2 / 30`.
