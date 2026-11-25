# Tutorial de execução e testes - Parte 2

Este roteiro valida replicação ativa, quórum, eleição pelo anel e recuperação
de clientes. Todos os comandos usam somente Python padrão e UDP/IPv4.

## 1. Verificação local

```bash
make check
make test
```

`make test` usa quatro endereços de loopback e verifica que o maior IP vence
após a queda do FE e que um sucessor inativo é ignorado. Ele não passa IP como
argumento ao programa do servidor; os IPs são injetados apenas pelos testes de
integração para isolar processos na mesma máquina.

## 2. Preparação do laboratório

Use três ou mais computadores Linux na mesma rede, todos com os arquivos do
projeto e a mesma porta, por exemplo `4000`. Confirme a sintaxe em cada um:

```bash
make check
ip -4 addr
```

Não passe o IP ao servidor. Cada processo determina automaticamente sua
identidade pela interface de rede. Para teste manual em uma única máquina,
associe previamente interfaces/aliases de loopback distintos; o processo deve
continuar recebendo seu IP da interface, não de argumentos ou arquivos.

## 3. Formação do cluster

1. No primeiro computador, execute `make server PORT=4000`.
   O log deve indicar que ele se autoproclamou FE.
2. Nos outros computadores, execute o mesmo comando, um de cada vez.
   Cada um deve registrar `RM JOIN_REQUEST`, `RM SNAPSHOT_APPLIED` e
   `RM TOPOLOGY`.
3. No FE, confirme linhas `FE RM_JOIN` e o total de RMs ativos.

## 4. Processamento e quórum

Em uma ou mais máquinas cliente:

```bash
make client PORT=4000
```

Após `server_addr`, envie por exemplo:

```text
10
3
5
```

O FE deve mostrar `DISPATCH`, `QUORUM_READY` e `QUORUM`; todos os RMs devem
mostrar `RM APPLIED`. O cliente recebe `id_req`, `num_reqs` e `total_sum`.

## 5. Queda de RM

Encerre um RM com `Ctrl+C`. O FE deve registrar `RM_FAILURE`, publicar nova
`TOPOLOGY` e ajustar o quórum das operações pendentes. Não devem aparecer
mensagens `RING` enquanto o FE estiver vivo. Envie outro valor pelo cliente e
confirme que ele recebe ACK.

## 6. Queda de FE e anel

Com pelo menos dois RMs ativos, encerre abruptamente o FE. Nos RMs, registre
estes eventos para a demonstração:

```text
RM FE_FAILURE_DETECTED
RING ELECTION_START
RING ELECTION_RECEIVED / ELECTION_SEND / ELECTION_FORWARDED
RING COORDINATOR_ANNOUNCE / COORDINATOR_RECEIVED
RING NEW_FRONTEND
```

O IP numericamente maior entre os RMs deve aparecer em `NEW_FRONTEND`. O novo
FE envia uma `RM TOPOLOGY`; os RMs restantes passam a enviar heartbeats a ele.

Mantenha um cliente com uma requisição pendente ou envie outro valor após os
timeouts. Ele redescobre o novo FE e continua com o mesmo `id_req`; a soma não
deve duplicar nem reiniciar.

## 7. Sucessor indisponível durante a eleição

Com três RMs ativos, encerre primeiro um RM que seja sucessor de outro no anel
e, em seguida, o FE. O log deve conter:

```text
RING SUCCESSOR_UNAVAILABLE ...
```

O token deve ser encaminhado ao próximo RM e o maior IP entre os sobreviventes
deve assumir o papel de FE.
