# Roteiro de validação em três PCs Linux

**Objetivo:** executar uma bateria manual, reproduzível e observável antes da demonstração para o professor. O roteiro cobre descoberta, replicação ativa, quórum, concorrência, queda e retorno de RM, eleição do FE e continuidade dos clientes.

## Topologia planejada

Use três computadores Linux distintos, na mesma rede local IPv4 e sem isolamento entre clientes.

| Nome | Papel inicial | IPv4 de teste | Terminais necessários |
| --- | --- | --- | --- |
| A | Front-End (FE) inicial | A_IP | servidor, cliente opcional |
| B | Replica Manager (RM) | B_IP | servidor, cliente |
| C | Replica Manager (RM) | C_IP | servidor, cliente opcional |

Substitua A_IP, B_IP e C_IP pelos IPs observados na rede de teste. Inicie A primeiro, B em seguida e C por último; assim os papéis iniciais ficam previsíveis.

> Importante: cada cliente é identificado pelo IP. Execute no máximo um cliente por computador ao mesmo tempo. Um cliente e um servidor podem coexistir na mesma máquina, pois o cliente usa uma porta UDP efêmera.

## 1. Preparação e pré-requisitos

### 1.1 Mesma versão em todas as máquinas

Copie exatamente os mesmos arquivos do projeto para A, B e C, incluindo a correção do cliente para ConnectionResetError no Windows. Embora esta rodada seja em Linux, manter as versões idênticas evita resultados incompatíveis.

Em cada computador, dentro da raiz do projeto:

~~~bash
make check
make test
sha256sum cliente.py servidor.py protocol.py
~~~

Compare os três hashes entre as máquinas. Eles devem ser iguais.

### 1.2 Rede, relógio e firewall

Em cada computador:

~~~bash
ip -4 -br addr
timedatectl status
~~~

Ative a sincronização de relógio se necessário:

~~~bash
sudo timedatectl set-ntp true
~~~

Confirme conectividade nos dois sentidos entre todos os pares:

~~~bash
ping -c 3 IP_DO_OUTRO_PC
~~~

Se UFW estiver ativo, permita a porta unicast do serviço e a porta de multicast:

~~~bash
sudo ufw allow 4000/udp
sudo ufw allow 6571/udp
~~~

A porta 6571 é usada pelo canal multicast entre FE e RMs. Em redes de laboratório, confirme antes que multicast IPv4 não é bloqueado. Não use DISCOVERY_BROADCAST como modo normal de execução: descoberta por broadcast deve funcionar sem configuração manual.

### 1.3 Captura de evidências

Em cada PC, inicie os servidores com logs salvos. Use um terminal diferente para cada cliente.

~~~bash
# A
python3 servidor.py 4000 | tee servidor-a.log

# B
python3 servidor.py 4000 | tee servidor-b.log

# C
python3 servidor.py 4000 | tee servidor-c.log
~~~

Não envie requisições antes de os dois RMs terminarem a entrada no grupo.

## 2. Bootstrap do cluster e uma requisição de referência

1. Inicie o servidor de A e espere:

~~~text
SERVER DISCOVERY: no FE found, self-proclaim FE ip A_IP
~~~

2. Inicie B. Ele deve registrar:

~~~text
SERVER DISCOVERY: FE found ip A_IP; starting as RM
RM JOIN_REQUEST fe A_IP
RM SNAPSHOT_APPLIED num_reqs 0 total_sum 0 expected_seq 1
RM TOPOLOGY fe A_IP ...
~~~

A deve registrar FE RM_JOIN_SYNC e FE RM_JOIN para B.

3. Inicie C e espere as mesmas mensagens, agora para C.

4. Antes de iniciar clientes, confirme:

~~~text
# Em A
FE RM_JOIN rm B_IP active_rms 1
FE RM_JOIN rm C_IP active_rms 2

# Em B e C
RM TOPOLOGY fe A_IP rms B_IP,C_IP
~~~

5. Em um novo terminal de B, abra um cliente interativo:

~~~bash
python3 cliente.py 4000
~~~

Depois de server_addr A_IP, envie:

~~~text
10
~~~

**Critério de aprovação**

- A atribui seq_id 1 e registra FE DISPATCH.
- B e C registram RM APPLIED seq_id 1.
- A só responde após FE QUORUM_READY com votes 2/2 e FE QUORUM com quorum 2/2.
- O cliente mostra num_reqs 1 total_sum 10.

Se houver retry ou retransmissão, a operação ainda é válida desde que o estado final avance apenas uma vez.

## 3. Concorrência com três clientes

Este cenário demonstra que a soma global e a ordenação se mantêm corretas com clientes de IPs distintos.

1. Pare o cluster inteiro com Ctrl+C e reinicie a seção 2 do zero. Isso mantém os valores esperados simples.

2. Depois de A, B e C formarem o grupo, abra um terminal de cliente em cada máquina e execute os três comandos com o menor intervalo possível:

~~~bash
# Cliente em A
seq 1 100 | python3 cliente.py 4000 > cliente-a-100.log

# Cliente em B
seq 101 200 | python3 cliente.py 4000 > cliente-b-100.log

# Cliente em C
seq 201 300 | python3 cliente.py 4000 > cliente-c-100.log
~~~

3. Espere todos os clientes terminarem. O resultado esperado é:

~~~text
num_reqs 300 total_sum 45150
~~~

A soma é 1 + 2 + ... + 300 = 45150.

**Critério de aprovação**

- A última operação consolidada pelo FE deve chegar a num_reqs 300 e total_sum 45150.
- B e C devem aplicar sequências até o mesmo estado.
- A ordem de interleaving dos clientes pode variar; o resultado final não.
- Não pode haver incremento duplicado por retransmissões.

Se este teste passar, execute uma versão de carga maior apenas se houver tempo e estabilidade na rede, por exemplo 1.000 números por cliente. Nesse caso, os intervalos 1..1000, 1001..2000 e 2001..3000 devem produzir:

~~~text
num_reqs 3000 total_sum 4501500
~~~

## 4. Queda de um RM, continuidade e retorno por snapshot

Reinicie o cluster novamente do zero e faça a requisição de referência com valor 10. O estado inicial deste cenário será 1 / 10.

1. Em C, descubra o PID exato do servidor e encerre somente esse processo de forma abrupta:

~~~bash
pgrep -af 'servidor.py 4000'
kill -9 PID_DO_SERVIDOR_DE_C
~~~

2. Em A, aguarde aproximadamente 11 a 12 segundos. Os sinais esperados são:

~~~text
FE RM_FAILURE ...
FE QUORUM_UPDATED ... expected_rms 1 quorum 1
~~~

B deve receber uma topologia contendo somente B como RM ativo. Não deve haver mensagens RING: o FE A continua vivo.

3. Depois de A confirmar a remoção de C, use o cliente de B já aberto e envie:

~~~text
20
~~~

O resultado esperado é:

~~~text
num_reqs 2 total_sum 30
~~~

A deve confirmar com quorum 1/1 e B deve aplicar a operação.

4. Reinicie C:

~~~bash
python3 servidor.py 4000 | tee servidor-c-retorno.log
~~~

**Critério de aprovação do retorno**

~~~text
# Em C
SERVER DISCOVERY: FE found ip A_IP; starting as RM
RM SNAPSHOT_APPLIED num_reqs 2 total_sum 30 expected_seq 3

# Em A
FE RM_JOIN_SYNC rm C_IP num_reqs 2 total_sum 30 next_seq 3
FE RM_JOIN rm C_IP active_rms 2
~~~

5. Após C entrar no grupo, envie 30 pelo cliente em B. O estado esperado é:

~~~text
num_reqs 3 total_sum 60
~~~

Agora A deve esperar novamente quorum 2/2, e B e C devem registrar RM APPLIED para o novo seq_id.

## 5. Queda do FE e eleição entre dois RMs

Este é o cenário mais importante para a demonstração presencial, pois há dois candidatos reais para a liderança.

1. Reinicie A, B e C do zero; espere A como FE e B/C como RMs ativos.

2. Em um terminal de B, inicie um cliente interativo e envie:

~~~text
1
~~~

Confirme o estado 1 / 1 antes de continuar. Mantenha esse cliente aberto.

3. Descubra qual dos IPs B_IP e C_IP é numericamente maior. Esse deve ser o novo FE esperado depois da queda de A.

4. Em A, encerre abruptamente o servidor:

~~~bash
pgrep -af 'servidor.py 4000'
kill -9 PID_DO_SERVIDOR_DE_A
~~~

5. Imediatamente envie no cliente B:

~~~text
2
~~~

O cliente tentará o FE antigo por algumas tentativas, fará redescoberta e reenviará a mesma requisição.

**Critério de aprovação da eleição**

Nos RMs B e C, espere aproximadamente 11 a 12 segundos e procure:

~~~text
RM FE_FAILURE_DETECTED fe A_IP
RING ELECTION_START ...
RING ELECTION_SEND / RING ELECTION_RECEIVED ...
RING COORDINATOR_ANNOUNCE ...
RING NEW_FRONTEND ... ip IP_MAIOR_ENTRE_B_E_C
~~~

É normal haver mais de uma rodada de eleição iniciada quase ao mesmo tempo. O resultado relevante é haver somente um FE estável, no maior IP entre B e C.

**Critério de aprovação da continuidade**

- O cliente não pode encerrar; ele deve imprimir server_addr com o IP do novo FE.
- A operação pendente deve permanecer id_req 2, value 2.
- O novo FE atribui o próximo seq_id, dissemina a operação ao outro RM e recebe quorum 1/1.
- O resultado final deve ser num_reqs 2 total_sum 3.

## 6. Retorno do FE que havia caído

Este teste confirma que o antigo líder entra como RM e recebe o estado que foi consolidado após o failover.

1. Sem encerrar o novo FE nem o cliente, reinicie A:

~~~bash
python3 servidor.py 4000 | tee servidor-a-retorno.log
~~~

2. A deve descobrir o FE vencedor e aplicar:

~~~text
RM SNAPSHOT_APPLIED num_reqs 2 total_sum 3 expected_seq 3
~~~

3. O FE vencedor deve registrar:

~~~text
FE RM_JOIN_SYNC rm A_IP num_reqs 2 total_sum 3 next_seq 3
FE RM_JOIN rm A_IP active_rms 2
~~~

4. No cliente B que permaneceu aberto, envie:

~~~text
3
~~~

**Critério de aprovação**

O resultado deve ser:

~~~text
num_reqs 3 total_sum 6
~~~

O FE vencedor deve obter quorum 2/2 dos RMs restantes, e A deve registrar RM APPLIED para o novo seq_id. Isso demonstra a recuperação do antigo FE sem perda do histórico.

## 7. Evidências mínimas para guardar

Guarde os seis logs de servidor, quando aplicável, e os logs de cliente. Para cada cenário, anote:

- IP do FE inicial e do FE eleito;
- estado inicial e final: num_reqs e total_sum;
- sequência dos valores enviados;
- linhas FE DISPATCH, FE QUORUM_READY e FE QUORUM;
- linhas RM APPLIED de cada RM;
- linhas FE RM_FAILURE e FE RM_JOIN_SYNC / FE RM_JOIN;
- linhas RM FE_FAILURE_DETECTED, RING ELECTION_* e RING NEW_FRONTEND;
- linhas do cliente com TIMEOUT, RESEND, server_addr e ACK final;
- versão do sistema operacional, Python e configuração de rede.

## Limite do cenário com três PCs

Três PCs dão um FE e dois RMs, permitindo validar quorum real 2/2 e eleição entre dois candidatos. Isso é o mínimo recomendado para uma apresentação convincente.

O teste de pular um sucessor morto no anel com encaminhamento para outro RM vivo exige três RMs além do FE, ou seja, quatro identidades de servidor. Caso seja necessário demonstrá-lo fisicamente, use uma quarta máquina, VM ou namespace de rede com IP próprio na mesma LAN.

