# Relatório - Trabalho Prático 2

Preencha os campos de ambiente e a divisão final de responsabilidades antes da
entrega. O texto abaixo descreve a implementação existente e deve ser revisado
pela equipe depois dos testes no laboratório.

## Ambiente de testes

- Sistema operacional/distribuição: **preencher**.
- Processador, memória e versão do Python: **preencher**.
- Número de máquinas, IPs e porta UDP: **preencher**.

## Evolução arquitetural

A Parte 1 tinha um único servidor que recebia `REQUEST`, mantinha a soma e
respondia `ACK`. Na Parte 2, `servidor.py` foi separado conceitualmente em FE
e RM. O FE é o ponto de contato dos clientes e sequenciador global; ele envia
`REPLICATE(seq_id, valor, cliente, id_req)` por multicast. Cada RM mantém uma
fila de pendências por `seq_id`, executa a soma na mesma ordem e responde com
o estado local. O FE consolida somente uma maioria de respostas iguais.

O snapshot de ingresso passou a incluir, além de `num_reqs`, `total_sum` e
`next_seq`, o último ACK de cada cliente. Assim, quando um RM vence a eleição,
ele restaura a deduplicação por cliente e um cliente pode continuar no mesmo
`id_req` após redescobrir o novo FE.

## Eleição de líder

Os RMs formam um anel lógico ordenado pelo valor numérico do IPv4. Ao perder
três heartbeats consecutivos do FE, um RM inicia um token `ELECTION` contendo
o maior IP observado. Cada recebedor confirma a entrega, atualiza o candidato
e encaminha o token ao sucessor. Se não recebe a confirmação, registra
`SUCCESSOR_UNAVAILABLE` e tenta o próximo membro do anel.

Quando o token retorna ao iniciador, inicia-se o token `COORDINATOR`. O maior
IP é proclamado FE; o vencedor reaproveita seu socket UDP e o estado replicado,
encerra suas rotinas de RM e passa a enviar heartbeats/topologia aos RMs
restantes. Tokens tardios de eleições simultâneas recebem ACK do novo FE para
evitar uma eleição indevida durante a transição.

## Replicação ativa e quórum

O FE não integra o conjunto de RMs. Para cada operação, ele guarda os RMs que
estavam ativos no instante do envio e exige maioria simples. A contagem é por
IP de RM, e o resultado escolhido é o par `(num_reqs, total_sum)` com maior
concordância. Operações prontas são confirmadas aos clientes em ordem de
`seq_id`.

## Validação executada

`make check` executa a validação sintática. `make test` executa dois cenários
em loopback: (1) três servidores, queda do FE, eleição do maior IP e envio do
próximo `id_req` pelo cliente; (2) queda de um sucessor do anel e salto para o
próximo membro vivo. Registre também os resultados do teste em laboratório:
**preencher**.

## Uso de IA

Descreva aqui, de forma precisa, como ferramentas de IA foram usadas para
análise, implementação, testes e documentação: **preencher/revisar pela
equipe**.

## Responsabilidades da equipe

| Integrante | Concepção | Implementação | Testes/validação |
| --- | --- | --- | --- |
| Ana | **preencher** | **preencher** | **preencher** |
| Igor | **preencher** | **preencher** | **preencher** |
| Leonardo | **preencher** | **preencher** | **preencher** |
