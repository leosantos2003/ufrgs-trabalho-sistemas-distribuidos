# Registro de testes manuais: dois PCs, failover e cliente Windows

**Data:** 24 e 25/11/2026  
**Objetivo:** validar replicação ativa, eleição de líder, recuperação de cliente e queda abrupta de servidor em rede real.

## Ambiente

| Papel | Computador | Sistema | IP |
| --- | --- | --- | --- |
| A | `leonardo-Inspiron-15-3530` | Linux | `192.168.0.14` |
| B | notebook Windows | Windows / PowerShell | `192.168.0.11` |
| Cliente | Computador B | Windows / PowerShell | `192.168.0.11` |

- Porta UDP: `4000`.
- A tornou-se Front-End (FE) por iniciar primeiro; B entrou como Replica Manager (RM).
- Os relógios das máquinas tinham aproximadamente 11 segundos de diferença: A registrou os mesmos eventos antes de B. As conclusões usam a ordem causal e os estados, não a comparação direta entre horários.
- As quedas foram simuladas encerrando abruptamente somente o processo `servidor.py`, com o cliente mantido em execução.

## Resultados resumidos

| Etapa | Ação | Resultado verificado |
| --- | --- | --- |
| Inicialização | A inicia como FE; B entra como RM | Snapshot inicial, topologia e quorum `1/1` |
| Replicação normal | Cliente B envia `7`, `5`, `3` | Estado `num_reqs 3 total_sum 15` nos dois nós |
| Primeiro failover | A é encerrado; cliente mantém `id_req 4, value 4` pendente | B é promovido e confirma o estado `4 / 19` |
| Reintegração | A é reiniciado | A recebe snapshot `4 / 19` e entra como RM de B |
| Segundo failover | B é encerrado; cliente mantém `id_req 5, value 6` pendente | A é promovido e confirma o estado `5 / 25` |
| Regressão Windows | B encerra o servidor local enquanto o cliente segue ativo | Sem `WinError 10054`; cliente redescobre A e preserva a requisição |

A soma final está correta:

~~~text
7 + 5 + 3 + 4 + 6 = 25
num_reqs = 5
~~~

O valor `6` recebeu `id_req 5`, pois quatro requisições já tinham sido confirmadas. Não houve envio de um valor `5` depois da reintegração de A; portanto, esta rodada comprova o snapshot e os dois failovers, mas não uma nova operação replicada depois do retorno de A.

A linha `DUP!! id_req 3` é esperada: o cliente retransmitiu após atraso do ACK e o FE não alterou a soma uma segunda vez.

## Defeito encontrado no Windows e correção

Antes da correção, ao encerrar B durante o segundo failover, o Winsock entregou o ICMP de porta inalcançável como:

~~~text
ConnectionResetError: [WinError 10054] Foi forçado o cancelamento de uma conexão existente pelo host remoto
~~~

O cliente tratava apenas `socket.timeout` e encerrava. A correção passou a tratar `ConnectionResetError` e `ConnectionRefusedError` como ausência de ACK, tanto ao aguardar a resposta quanto na descoberta. Assim, o fluxo normal de timeout, retransmissão e redescoberta continua sem encerrar o cliente.

Foi incluído o teste unitário `test_udp_reset_is_retried_like_a_timeout`, que simula o erro 10054, verifica o reenvio e uma confirmação posterior. As verificações `make check` e `make test` passaram após a alteração.

### Log do defeito, antes da correção

~~~text
5
2026-11-24 23:49:41 server 192.168.0.11 SEND id_req 5 value 5
2026-11-24 23:49:41 server 192.168.0.11 TIMEOUT id_req 5 value 5
2026-11-24 23:49:41 server 192.168.0.11 RESEND id_req 5 value 5
2026-11-24 23:49:41 server 192.168.0.11 id_req 5 value 5 num_reqs 5 total_sum 24
6
2026-11-24 23:55:13 server 192.168.0.11 SEND id_req 6 value 6
Traceback (most recent call last):
  File "C:\Users\rafae\Desktop\ESTUDOS\CODING\trabalho-sist-distribuidos\cliente.py", line 270, in <module>
    main()
  File "C:\Users\rafae\Desktop\ESTUDOS\CODING\trabalho-sist-distribuidos\cliente.py", line 261, in main
    client.run()
  File "C:\Users\rafae\Desktop\ESTUDOS\CODING\trabalho-sist-distribuidos\cliente.py", line 229, in run
    while not self._send_until_acknowledged(request_id, value):
  File "C:\Users\rafae\Desktop\ESTUDOS\CODING\trabalho-sist-distribuidos\cliente.py", line 187, in _send_until_acknowledged
    data, address = self.socket.recvfrom(65535)
ConnectionResetError: [WinError 10054] Foi forçado o cancelamento de uma conexão existente pelo host remoto
~~~

## Logs completos da rodada bem-sucedida após a correção

### Computador A — servidor

~~~text
2026-11-25 00:27:24 SERVER DISCOVERY: no FE found, self-proclaim FE ip 192.168.0.14
2026-11-25 00:28:07 FE RM_JOIN_SYNC rm 192.168.0.11 num_reqs 0 total_sum 0 next_seq 1
2026-11-25 00:28:07 FE RM_JOIN rm 192.168.0.11 active_rms 1
2026-11-25 00:28:24 FE DISPATCH seq_id 1 client 192.168.0.11 id_req 1 value 7
2026-11-25 00:28:24 FE REPLICATION_RETRY seq_id 1 attempt 1
2026-11-25 00:28:24 FE QUORUM_READY seq_id 1 votes 1/1
2026-11-25 00:28:24 FE QUORUM seq_id 1 client 192.168.0.11 id_req 1 value 7 quorum 1/1 (expected_rms 1) num_reqs 1 total_sum 7
2026-11-25 00:28:28 FE DISPATCH seq_id 2 client 192.168.0.11 id_req 2 value 5
2026-11-25 00:28:28 FE REPLICATION_RETRY seq_id 2 attempt 1
2026-11-25 00:28:28 FE REPLICATION_RETRY seq_id 2 attempt 2
2026-11-25 00:28:28 FE QUORUM_READY seq_id 2 votes 1/1
2026-11-25 00:28:28 FE QUORUM seq_id 2 client 192.168.0.11 id_req 2 value 5 quorum 1/1 (expected_rms 1) num_reqs 2 total_sum 12
2026-11-25 00:28:31 FE DISPATCH seq_id 3 client 192.168.0.11 id_req 3 value 3
2026-11-25 00:28:31 FE REPLICATION_RETRY seq_id 3 attempt 2
2026-11-25 00:28:31 FE REPLICATION_RETRY seq_id 3 attempt 3
2026-11-25 00:28:31 FE QUORUM_READY seq_id 3 votes 1/1
2026-11-25 00:28:31 FE QUORUM seq_id 3 client 192.168.0.11 id_req 3 value 3 quorum 1/1 (expected_rms 1) num_reqs 3 total_sum 15
2026-11-25 00:28:31 client 192.168.0.11 DUP!! id_req 3 value 3 num_reqs 3 total_sum 15

# A foi encerrado e reiniciado como RM de B.
2026-11-25 00:29:50 SERVER DISCOVERY: FE found ip 192.168.0.11; starting as RM
2026-11-25 00:29:50 RM JOIN_REQUEST fe 192.168.0.11
2026-11-25 00:29:50 RM SNAPSHOT_APPLIED num_reqs 4 total_sum 19 expected_seq 5
2026-11-25 00:29:50 RM TOPOLOGY fe 192.168.0.11 rms 192.168.0.14

# B foi encerrado; A foi promovido a FE.
2026-11-25 00:31:24 RM FE_FAILURE_DETECTED fe 192.168.0.11
2026-11-25 00:31:24 RING ELECTION_START id 1795577484560495041 initiator 192.168.0.14 members 192.168.0.14
2026-11-25 00:31:24 RING COORDINATOR_ANNOUNCE id 1795577484560495041 leader 192.168.0.14
2026-11-25 00:31:24 RING NEW_FRONTEND id 1795577484560495041 ip 192.168.0.14
2026-11-25 00:31:25 FE LOCAL_COMMIT seq_id 5 client 192.168.0.11 id_req 5 value 6 num_reqs 5 total_sum 25
~~~

### Computador B — servidor

~~~text
2026-11-25 00:28:17 SERVER DISCOVERY: FE found ip 192.168.0.14; starting as RM
2026-11-25 00:28:17 RM JOIN_REQUEST fe 192.168.0.14
2026-11-25 00:28:18 RM JOIN_REQUEST fe 192.168.0.14
2026-11-25 00:28:18 RM SNAPSHOT_APPLIED num_reqs 0 total_sum 0 expected_seq 1
2026-11-25 00:28:18 RM TOPOLOGY fe 192.168.0.14 rms 192.168.0.11
2026-11-25 00:28:35 RM APPLIED seq_id 1 (+1 seqs) num_reqs 1 total_sum 7
2026-11-25 00:28:39 RM APPLIED seq_id 2 (+1 seqs) num_reqs 2 total_sum 12
2026-11-25 00:28:42 RM APPLIED seq_id 3 (+1 seqs) num_reqs 3 total_sum 15

# A foi encerrado; B foi promovido a FE.
2026-11-25 00:29:31 RM FE_FAILURE_DETECTED fe 192.168.0.14
2026-11-25 00:29:31 RING ELECTION_START id 1795577371217209400 initiator 192.168.0.11 members 192.168.0.11
2026-11-25 00:29:31 RING COORDINATOR_ANNOUNCE id 1795577371217209400 leader 192.168.0.11
2026-11-25 00:29:31 RING NEW_FRONTEND id 1795577371217209400 ip 192.168.0.11
2026-11-25 00:29:31 FE LOCAL_COMMIT seq_id 4 client 192.168.0.11 id_req 4 value 4 num_reqs 4 total_sum 19

# A retornou como RM.
2026-11-25 00:30:01 FE RM_JOIN_SYNC rm 192.168.0.14 num_reqs 4 total_sum 19 next_seq 5
2026-11-25 00:30:01 FE RM_JOIN rm 192.168.0.14 active_rms 1
~~~

### Computador B — cliente

~~~text
2026-11-25 00:28:27 server_addr 192.168.0.14
7
2026-11-25 00:28:35 server 192.168.0.14 SEND id_req 1 value 7
2026-11-25 00:28:35 server 192.168.0.14 TIMEOUT id_req 1 value 7
2026-11-25 00:28:35 server 192.168.0.14 RESEND id_req 1 value 7
2026-11-25 00:28:35 server 192.168.0.14 id_req 1 value 7 num_reqs 1 total_sum 7
5
2026-11-25 00:28:39 server 192.168.0.14 SEND id_req 2 value 5
2026-11-25 00:28:39 server 192.168.0.14 TIMEOUT id_req 2 value 5
2026-11-25 00:28:39 server 192.168.0.14 RESEND id_req 2 value 5
2026-11-25 00:28:39 server 192.168.0.14 id_req 2 value 5 num_reqs 2 total_sum 12
3
2026-11-25 00:28:42 server 192.168.0.14 SEND id_req 3 value 3
2026-11-25 00:28:42 server 192.168.0.14 TIMEOUT id_req 3 value 3
2026-11-25 00:28:42 server 192.168.0.14 RESEND id_req 3 value 3
2026-11-25 00:28:42 server 192.168.0.14 TIMEOUT id_req 3 value 3
2026-11-25 00:28:42 server 192.168.0.14 RESEND id_req 3 value 3
2026-11-25 00:28:42 server 192.168.0.14 id_req 3 value 3 num_reqs 3 total_sum 15
4
2026-11-25 00:29:25 server 192.168.0.14 SEND id_req 4 value 4
2026-11-25 00:29:25 server 192.168.0.14 TIMEOUT id_req 4 value 4
2026-11-25 00:29:25 server 192.168.0.14 RESEND id_req 4 value 4
2026-11-25 00:29:25 server 192.168.0.14 TIMEOUT id_req 4 value 4
2026-11-25 00:29:25 server 192.168.0.14 RESEND id_req 4 value 4
2026-11-25 00:29:25 server 192.168.0.14 TIMEOUT id_req 4 value 4
2026-11-25 00:29:25 server 192.168.0.14 RESEND id_req 4 value 4
2026-11-25 00:29:26 server 192.168.0.14 TIMEOUT id_req 4 value 4
2026-11-25 00:29:26 server 192.168.0.14 RESEND id_req 4 value 4
2026-11-25 00:29:26 server 192.168.0.14 TIMEOUT id_req 4 value 4
2026-11-25 00:29:31 server_addr 192.168.0.11
2026-11-25 00:29:31 server 192.168.0.11 SEND id_req 4 value 4
2026-11-25 00:29:31 server 192.168.0.11 id_req 4 value 4 num_reqs 4 total_sum 19
6
2026-11-25 00:31:30 server 192.168.0.11 SEND id_req 5 value 6
2026-11-25 00:31:30 server 192.168.0.11 TIMEOUT id_req 5 value 6
2026-11-25 00:31:30 server 192.168.0.11 RESEND id_req 5 value 6
2026-11-25 00:31:30 server 192.168.0.11 TIMEOUT id_req 5 value 6
2026-11-25 00:31:30 server 192.168.0.11 RESEND id_req 5 value 6
2026-11-25 00:31:30 server 192.168.0.11 TIMEOUT id_req 5 value 6
2026-11-25 00:31:30 server 192.168.0.11 RESEND id_req 5 value 6
2026-11-25 00:31:30 server 192.168.0.11 TIMEOUT id_req 5 value 6
2026-11-25 00:31:30 server 192.168.0.11 RESEND id_req 5 value 6
2026-11-25 00:31:30 server 192.168.0.11 TIMEOUT id_req 5 value 6
2026-11-25 00:31:35 server_addr 192.168.0.14
2026-11-25 00:31:35 server 192.168.0.14 SEND id_req 5 value 6
2026-11-25 00:31:35 server 192.168.0.14 id_req 5 value 6 num_reqs 5 total_sum 25
~~~

## Cobertura e próximo teste opcional

Esta rodada comprova descoberta, replicação com um RM, quorum `1/1`, deduplicação, dois failovers consecutivos, eleição com RM único, preservação de estado por snapshot e recuperação do cliente Windows após a queda abrupta de seu FE local.

Como próximo teste opcional com os mesmos dois PCs, iniciar B novamente como RM de A e enviar uma nova operação permite comprovar a replicação depois da reintegração. Para validar quorum entre múltiplos RMs e a eleição do maior IP entre concorrentes, são necessárias ao menos três identidades de servidor na mesma rede.

