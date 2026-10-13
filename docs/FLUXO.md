# Fluxo de funcionamento da implementação

Este documento descreve o caminho percorrido por uma entrada do usuário até a confirmação do resultado pelo servidor. A implementação é composta pelos arquivos `cliente.py`, `servidor.py` e `protocol.py`.

## Visão geral

```text
entrada padrão                         rede UDP                         estado do serviço
--------------                         --------                         -----------------
usuário -> cliente.py -> protocol.py -> datagrama R -> servidor.py -> num_reqs / total_sum
             ^                                            |
             |                                            v
             +--------- ACK A <- protocol.py <-------------+
```

Há dois fluxos de rede distintos:

1. **Descoberta:** o cliente encontra o servidor por broadcast antes de enviar valores.
2. **Requisição confiável:** o cliente envia um valor, espera uma confirmação e retransmite a mesma requisição se ela não chegar a tempo.

O UDP não garante entrega, ordem ou ausência de duplicatas. Por isso, o cliente usa IDs crescentes e o servidor registra o último ID confirmado para cada endereço IP de cliente.

## Responsabilidade de cada arquivo

| Arquivo | Responsabilidade | Não faz |
|---|---|---|
| `protocol.py` | Define os tipos de mensagem, a codificação binária e a validação dos campos. | Não abre sockets nem mantém estado do serviço. |
| `cliente.py` | Lê valores, descobre o servidor, envia requisições e trata timeouts. | Não calcula a soma global nem decide se uma requisição é duplicada. |
| `servidor.py` | Recebe datagramas, mantém o estado global e o estado de cada cliente, e devolve ACKs. | Não depende de conexão TCP nem de um processo por cliente. |

Essa separação é importante: o formato de uma mensagem está concentrado em `protocol.py`; assim, cliente e servidor sempre usam a mesma representação.

## Formato das mensagens: `protocol.py`

Todas as mensagens usam `struct` com ordem de bytes de rede (`!`), isto é, uma representação independente de arquitetura. Os números são inteiros sem sinal de 64 bits.

| Mensagem | Primeiro byte | Função que cria | Função que interpreta | Campos |
|---|---:|---|---|---|
| Descoberta | `D` | `pack_discover()` | — | somente o tipo |
| Resposta à descoberta | `S` | `pack_discover_ack()` | — | somente o tipo |
| Requisição | `R` | `pack_request()` | `unpack_request()` | `id_req`, `value` |
| Confirmação | `A` | `pack_ack()` | `unpack_ack()` | `id_req`, `num_reqs`, `total_sum` |
| Encerramento | `E` | `pack_exit()` | — | somente o tipo |

As estruturas de dados `Request` e `Acknowledgement` tornam os campos recebidos explícitos. As funções de desempacotamento também verificam o tamanho e o tipo da mensagem; um datagrama inválido não deve alterar o estado do serviço.

## Inicialização do servidor

O comando abaixo inicia o servidor na porta informada:

```bash
python3 servidor.py 4000
```

O fluxo interno é:

```text
main()
  -> parse_port()
  -> SumServer(port)
       -> cria socket UDP IPv4
       -> configura SO_REUSEADDR
       -> bind(("", port))
       -> inicia clients = {}
       -> inicia num_reqs = 0 e total_sum = 0
  -> serve_forever()
```

`serve_forever()` é o laço principal. Ele fica bloqueado em `recvfrom()`. A cada datagrama recebido, obtém também o endereço de origem `(IP, porta)` e decide o tratamento a partir do primeiro byte:

```text
recebe datagrama
  ├─ D -> _handle_discovery(...)
  ├─ R -> _handle_request(...)
  ├─ E -> remove o estado daquele IP
  └─ outro formato -> ignora
```

O servidor processa uma mensagem por vez nesse laço. Portanto, a atualização de `num_reqs`, `total_sum` e do estado do participante ocorre de modo sequencial, sem disputa entre threads no código do servidor.

## Descoberta do servidor

O cliente começa sem conhecer o IP do servidor. Em `SumClient.run()`, a primeira operação é `discover()`.

```text
cliente.py                                      servidor.py
----------                                      -----------
SumClient.run()
  -> discover()
       -> socket.sendto(D, broadcast:porta)  -> serve_forever()
                                                   -> _handle_discovery(D, endereço)
                                                       -> clients.setdefault(IP, ClientState())
                                                       -> sendto(S, endereço)
       <- recvfrom() recebe S
       -> salva self.server = (IP_do_servidor, porta)
       -> registra "server_addr ..."
```

O endereço de broadcast padrão é `255.255.255.255`; ele pode ser substituído pela variável de ambiente `DISCOVERY_BROADCAST`. Essa alternativa é útil caso a rede do laboratório exija o broadcast específico da sub-rede.

Ao receber `D`, `_handle_discovery()` cria, se necessário, uma entrada no dicionário `clients`. A chave é o IP do cliente e o valor é um objeto `ClientState`. A resposta `S` é unicast, enviada de volta ao endereço que originou a descoberta.

## Estado mantido pelo servidor

O estado do serviço é dividido em duas partes:

```text
SumServer
  num_reqs: número total de requisições aceitas de todos os clientes
  total_sum: soma global dos valores aceitos
  clients: dicionário indexado pelo IP do cliente
      IP A -> ClientState(last_req, last_num_reqs, last_total_sum)
      IP B -> ClientState(last_req, last_num_reqs, last_total_sum)
      ...
```

Para cada cliente, `ClientState` guarda:

| Campo | Significado |
|---|---|
| `last_req` | Maior ID de requisição aceito daquele cliente. |
| `last_num_reqs` | Valor global de `num_reqs` que constou no último ACK daquele cliente. |
| `last_total_sum` | Valor global de `total_sum` que constou no último ACK daquele cliente. |

Guardar os dois últimos campos é essencial: quando chega uma retransmissão da **última** requisição aceita daquele cliente, o servidor consegue reenviar a confirmação correspondente sem executar a soma novamente. O protocolo stop-and-wait garante que esse é o caso normal, pois o cliente só envia o próximo ID depois de confirmar o anterior.

## Leitura da entrada e threads do cliente

Depois da descoberta, `SumClient.run()` inicia a thread `self.reader`, cuja função é `_read_stdin()`.

```text
Thread de leitura: _read_stdin()              Thread principal: run()
--------------------------------              ---------------------
for cada linha da entrada                     while houver itens em values:
  converte para inteiro                           retira um valor da fila
  valida 1 <= valor <= MAX_UINT64                 envia e confirma esse valor
  values.put(valor)
ao fim da entrada:
  values.put(STOP)
```

A fila `values` impede que a thread que lê o teclado interfira diretamente no envio pela rede. A thread principal preserva a ordem dos valores e associa os IDs `1`, `2`, `3`, ... a eles.

Também há a classe `Console`. Ela usa uma fila própria e a thread `_write()` para serializar as mensagens exibidas. Isso evita que a saída da thread de leitura e a saída da thread principal se misturem visualmente.

## Envio normal de uma requisição

Para cada valor válido, `run()` chama `_send_until_acknowledged(request_id, value)`. Essa função cria o datagrama uma única vez e mantém o mesmo `request_id` em todas as retransmissões.

```text
cliente.py                                           servidor.py
----------                                           -----------
run()
  -> _send_until_acknowledged(7, 25)
       -> pack_request(7, 25)
       -> sendto(R, servidor)
                                                        -> _handle_request(R, endereço)
                                                           -> unpack_request()
                                                           -> consulta ClientState do IP
                                                           -> verifica que 7 == last_req + 1
                                                           -> num_reqs += 1
                                                           -> total_sum += 25
                                                           -> atualiza last_req, last_num_reqs,
                                                              last_total_sum
                                                           -> _send_ack(...)
                                                              -> pack_ack(7, num_reqs, total_sum)
       <- recvfrom() recebe A
       -> unpack_ack()
       -> verifica remetente, tipo e ID 7
       -> exibe num_reqs e total_sum
       -> retorna para run()
  -> incrementa request_id para 8
```

No caso de uma requisição nova e esperada, a regra no servidor é:

```text
id_req == state.last_req + 1
```

Somente nesse caso `_handle_request()` altera a soma e o contador. Depois de atualizar o estado, `_send_ack()` devolve um ACK com os valores salvos naquele `ClientState`.

## Timeout e retransmissão

Depois de cada `sendto()`, o cliente espera no máximo `REQUEST_TIMEOUT_SECONDS`, atualmente `0.100` segundo, por um ACK válido. Se não o recebe nesse intervalo, registra `TIMEOUT`, registra `RESEND` e repete o envio.

```text
cliente envia R(id=7, value=25)
  -> ACK não chega em 0,100 s
  -> cliente retransmite o mesmo R(id=7, value=25)
  -> repete até receber A(id=7, ...)
```

O cliente só aceita um ACK se todas estas condições forem verdadeiras:

1. O datagrama veio exatamente de `self.server`.
2. O primeiro byte é `A`.
3. `unpack_ack()` conseguiu validar o formato.
4. O `id_req` do ACK é igual ao ID que está pendente.

ACKs malformados, originados de outro endereço ou com outro ID são ignorados. Isso evita que um ACK antigo ou estranho conclua uma requisição errada.

## Como duplicatas são tratadas

Uma duplicata normalmente aparece quando a requisição original chegou ao servidor, mas o ACK se perdeu ou demorou além do timeout. A retransmissão chega com o mesmo ID.

```text
cliente envia R(id=7, value=25)       servidor aceita e atualiza soma
cliente não recebe o ACK
cliente reenvia R(id=7, value=25)     servidor detecta id_req <= last_req
                                      servidor não atualiza a soma
                                      servidor imprime DUP!!
                                      servidor reenvia o ACK salvo
cliente recebe A(id=7, ...)            cliente conclui a operação
```

Em `_handle_request()`, as decisões são:

| Condição do ID recebido | Significado | Efeito sobre a soma | Resposta |
|---|---|---|---|
| `id_req == last_req + 1` | Próxima requisição esperada. | Aceita e atualiza estado. | ACK novo. |
| `id_req <= last_req` | Requisição já processada. | Não altera nada. | Reenvia o último ACK salvo. |
| `id_req > last_req + 1` | Há uma lacuna; chegou fora de ordem. | Não altera nada. | Devolve o último ACK salvo. |

O rótulo `DUP!!` no log é esperado quando há retransmissão. Ele demonstra que a proteção contra execução repetida foi exercitada.

## Por que a operação é executada no máximo uma vez

A propriedade desejada vem da combinação de duas decisões:

1. O cliente não muda o ID enquanto a operação atual não for confirmada.
2. O servidor só altera seu estado quando recebe exatamente o próximo ID esperado para aquele cliente.

Assim, depois que `R(id=7)` é aceito, `last_req` passa a valer `7`. Qualquer nova cópia de `R(id=7)` satisfaz `id_req <= last_req`, portanto não volta a somar o valor. Ela apenas provoca o reenvio da confirmação já armazenada.

O identificador é local a cada cliente. Por isso, dois clientes podem enviar `id_req = 1` ao mesmo tempo: como possuem entradas diferentes em `clients`, ambos são corretamente reconhecidos como suas respectivas primeiras requisições. O servidor conserva somente o ACK mais recente de cada cliente; uma cópia muito antiga que apareça depois de requisições mais novas não muda o estado e recebe esse último ACK. Como o cliente compara o ID do ACK pendente, ele ignora essa resposta se ela não corresponder à operação atual.

## Encerramento

Quando a entrada padrão termina, `_read_stdin()` coloca o sentinela `STOP` na fila. Ao consumi-lo, `run()` encerra o laço de valores e chama `close()`.

```text
_read_stdin() termina
  -> values.put(STOP)
run() consome STOP
  -> close()
       -> envia E ao servidor, se ele foi descoberto
       -> fecha o socket UDP
```

Ao receber `E`, o servidor remove a entrada correspondente em `clients`. Os contadores globais `num_reqs` e `total_sum` permanecem: encerrar um cliente não desfaz operações que já foram aceitas.

## Fluxo completo em uma execução

```text
1. Inicia-se o servidor: python3 servidor.py PORTA.
2. O servidor faz bind UDP e zera o estado global.
3. Inicia-se o cliente: python3 cliente.py PORTA.
4. O cliente transmite D por broadcast até receber S.
5. O servidor cria o ClientState do IP e devolve S.
6. O cliente inicia a thread de leitura da entrada padrão.
7. Cada valor válido entra na fila values.
8. A thread principal transforma o valor em R(id_req, value).
9. O servidor aceita somente o próximo ID, atualiza a soma e envia A.
10. O cliente valida A; se o ACK não chegar, retransmite a mesma R.
11. Para uma retransmissão, o servidor não altera a soma e reenvia o ACK salvo.
12. Ao fim da entrada, o cliente envia E e fecha seu socket.
```

## Limites e hipóteses da implementação

- O protocolo usa IPv4 e UDP.
- Cada cliente é identificado pelo endereço IP. Dois processos atrás do mesmo IP compartilham a mesma entrada de estado; o cenário esperado é um cliente por máquina/IP.
- O estado reside em memória. Se o servidor reiniciar, os contadores e os IDs já vistos são perdidos.
- O servidor pressupõe que todos os clientes usam a mesma porta de serviço e o mesmo formato de protocolo.
- Os valores e contadores precisam caber em `uint64`; o código verifica esses limites antes de aceitar uma operação que causaria overflow.
- Em uma rede muito congestionada, `0.100` segundo pode gerar retransmissões legítimas. Elas são seguras, pois o servidor identifica duplicatas pelo ID.

Essas escolhas atendem ao objetivo de tolerar perda e duplicação de datagramas durante a execução do serviço, mantendo a soma correta sem usar conexão TCP.
