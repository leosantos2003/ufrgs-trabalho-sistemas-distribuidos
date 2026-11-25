# Roteiro de validação em três e quatro PCs Linux

**Objetivo:** executar uma bateria manual, reproduzível e observável antes da demonstração para o professor. O roteiro cobre descoberta, replicação ativa, quórum, concorrência, queda e retorno de RM, eleição do FE e continuidade dos clientes. As seções 1 a 7 usam três PCs; a seção 8 aproveita um quarto PC para validar quórum com três RMs e o salto de um sucessor indisponível no anel.

## Topologia planejada

Use três computadores Linux distintos, na mesma rede local IPv4 e sem isolamento entre clientes.

| Nome | Papel inicial | IPv4 de teste | Terminais necessários |
| --- | --- | --- | --- |
| A | Front-End (FE) inicial | A_IP | servidor, cliente opcional |
| B | Replica Manager (RM) | B_IP | servidor, cliente |
| C | Replica Manager (RM) | C_IP | servidor, cliente opcional |
| D (opcional) | terceiro RM, usado na seção 8 | D_IP | servidor, cliente opcional |

Substitua A_IP, B_IP e C_IP pelos IPs observados na rede de teste. Para as seções 1 a 7, inicie A primeiro, B em seguida e C por último; assim os papéis iniciais ficam previsíveis. A seção 8 usa A como FE inicial e B, C e D como RMs.

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

Compare os hashes entre todas as máquinas usadas. Eles devem ser iguais.

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

# D (somente na seção 8)
python3 servidor.py 4000 | tee servidor-d.log
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

## 8. Extensão para quatro PCs: três RMs e sucessor indisponível

Com quatro PCs, mantenha A como FE inicial e use B, C e D como RMs. Além de aumentar o conjunto de réplicas, isso viabiliza o caso que não pode ser demonstrado com três identidades: um RM tenta encaminhar um token para seu sucessor imediato, descobre que ele caiu e o encaminha para o próximo RM ainda vivo.

> Execute cada subseção a seguir a partir de um cluster reiniciado do zero. Isso impede que uma remoção anterior altere a topologia que o próximo teste precisa observar.

### 8.1 Formação com três RMs e quórum de maioria

1. Inicie A e espere que ele se autoproclame FE. Em seguida, inicie B, C e D, um de cada vez:

~~~bash
# Em A, B, C e D, respectivamente
python3 servidor.py 4000 | tee servidor-a-4pcs.log
python3 servidor.py 4000 | tee servidor-b-4pcs.log
python3 servidor.py 4000 | tee servidor-c-4pcs.log
python3 servidor.py 4000 | tee servidor-d-4pcs.log
~~~

2. Antes de usar um cliente, confirme que A registrou as três entradas e ficou com `active_rms 3`. Em B, C e D, confirme uma topologia contendo os três IPs de RM.

3. Abra um cliente em qualquer um dos quatro PCs, envie `10` e aguarde a confirmação.

**Critério de aprovação**

- A registra `FE DISPATCH seq_id 1` e `FE QUORUM` com `expected_rms 3` e quórum de maioria `2/2`;
- B, C e D registram `RM APPLIED seq_id 1` e chegam a `num_reqs 1 total_sum 10`;
- o cliente recebe `num_reqs 1 total_sum 10`.

O FE pode confirmar assim que receber dois votos concordantes; o terceiro `RM APPLIED` ainda deve aparecer, pois a operação é disseminada para todos os RMs ativos.

### 8.2 Queda de um RM com dois RMs sobreviventes

1. Em uma nova execução com A como FE e B/C/D ativos, envie `10` e confirme o estado inicial `1 / 10`.

2. Escolha um RM, por exemplo C, e encerre apenas o processo de servidor dele:

~~~bash
# Em C
pgrep -af 'servidor.py 4000'
kill -9 PID_DO_SERVIDOR_DE_C
~~~

3. Aguarde aproximadamente 11 a 12 segundos. Em A, procure `FE RM_FAILURE ... active_rms 2`; nos outros dois RMs, confirme uma topologia sem C.

4. Pelo cliente ainda aberto, envie `20`.

**Critério de aprovação**

~~~text
FE QUORUM ... value 20 quorum 2/2 (expected_rms 2) num_reqs 2 total_sum 30
~~~

Os dois RMs sobreviventes devem registrar `RM APPLIED seq_id 2`. Diferentemente do cenário de três PCs, a queda de uma das três réplicas não reduz o quórum para `1/1`: ainda são necessários os dois votos dos RMs restantes.

### 8.3 Salto de sucessor morto durante a eleição

Este é o teste adicional mais importante habilitado pelo quarto PC. A ordem do anel é numérica pelo IPv4; portanto, os papéis abaixo são lógicos e não dependem de B, C e D terem endereços consecutivos.

1. Em uma execução nova, inicie A como FE e B/C/D como RMs. Entre os IPs de B, C e D, anote:

| Rótulo lógico | Escolha |
| --- | --- |
| `L_IP` | o menor IPv4 entre os três RMs |
| `M_IP` | o IPv4 intermediário |
| `H_IP` | o maior IPv4 entre os três RMs |

2. Aguarde a topologia com os três RMs e envie `10` por um cliente executado no computador de `L_IP`. Confirme `num_reqs 1 total_sum 10` antes de continuar. Mantenha esse cliente aberto.

3. Descubra os PIDs dos processos de servidor em A e no computador de `M_IP`. Encerre primeiro `M_IP` e, **em até poucos segundos e sempre antes de A detectar a queda de M**, encerre A. O intervalo precisa ser menor que os cerca de 10 segundos do timeout de heartbeat; assim L ainda terá M em sua topologia local quando a eleição começar.

~~~bash
# No computador de M_IP
pgrep -af 'servidor.py 4000'
kill -9 PID_DO_SERVIDOR_DE_M

# Logo em seguida, no computador A
pgrep -af 'servidor.py 4000'
kill -9 PID_DO_SERVIDOR_DE_A
~~~

4. Deixe `L_IP` e `H_IP` em execução e aguarde aproximadamente 11 a 12 segundos pela detecção da queda do FE. No log de `L_IP`, a sequência relevante é:

~~~text
RM FE_FAILURE_DETECTED fe A_IP
RING ELECTION_START ... members L_IP,M_IP,H_IP
RING ELECTION_SEND ... to M_IP
RING SUCCESSOR_UNAVAILABLE ... successor M_IP
RING ELECTION_SEND ... to H_IP
RING ELECTION_FORWARDED ... to H_IP
~~~

5. Confirme nos dois sobreviventes as mensagens `RING ELECTION_*` e `RING COORDINATOR_*`. O novo FE deve ser `H_IP`, o maior IP ainda vivo:

~~~text
RING NEW_FRONTEND ... ip H_IP
~~~

É normal que L e H iniciem rodadas quase simultâneas e que mais de uma linha `SUCCESSOR_UNAVAILABLE` apareça: os tokens `ELECTION` e `COORDINATOR` usam o mesmo mecanismo de encaminhamento confirmado. O critério essencial é existir ao menos um salto de `M_IP` para `H_IP` e apenas `H_IP` estabilizar como FE.

6. Para provar a continuidade, digite `20` no cliente de `L_IP` logo após encerrar A. Ele deve registrar `TIMEOUT` e `RESEND`, redescobrir `H_IP` como `server_addr` e manter `id_req 2`, `value 20`. Pode ser necessário aguardar mais um ciclo de detecção para H remover M de sua topologia; o resultado final esperado é:

~~~text
num_reqs 2 total_sum 30
~~~

No novo FE, procure a remoção de `M_IP`, seguida da consolidação da operação com o único RM sobrevivente:

~~~text
FE RM_FAILURE rm M_IP active_rms 1
FE QUORUM ... value 20 quorum 1/1 (expected_rms 1) num_reqs 2 total_sum 30
~~~

### 8.4 Evidências extras a guardar

Além das evidências da seção 7, guarde os quatro logs de servidor e o log do cliente em `L_IP`. Registre explicitamente:

- os três IPs de RM e sua ordenação `L_IP < M_IP < H_IP`;
- a entrada de B, C e D e a topologia inicial com três RMs;
- uma confirmação normal com `expected_rms 3` e quórum `2/2`;
- a confirmação com dois RMs sobreviventes, ainda com quórum `2/2`;
- `RING SUCCESSOR_UNAVAILABLE` para `M_IP` e o envio subsequente para `H_IP`;
- `RING NEW_FRONTEND` em `H_IP`, a redescoberta do cliente e o estado final `2 / 30`.

