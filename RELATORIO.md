# Relatório - Trabalho Prático 1

**Disciplina:** INF01085 - Sistemas Distribuídos e Tolerantes a Falhas  
**Turma:** U - 2026/1  
**Integrantes:** preencher com os quatro integrantes do grupo.

## 1. Visão geral

Foi implementado um serviço distribuído de soma de inteiros positivos. Há um único processo servidor e um processo cliente por estação. A comunicação usa exclusivamente sockets UDP/IPv4 e um protocolo de aplicação próprio, definido em `protocol.py`.

O serviço preserva o processamento exatamente uma vez de cada requisição, mesmo que um datagrama de requisição ou confirmação seja perdido, duplicado ou recebido fora de ordem. A identificação do participante segue o enunciado: o endereço IPv4 de origem.

## 2. Implementação dos subserviços

### Descoberta

Ao iniciar, o cliente cria um socket UDP com `SO_BROADCAST` e transmite `DISCOVER` na porta informada. O endereço padrão é `255.255.255.255`, portanto a mensagem é difundida no segmento local. O servidor, associado a `0.0.0.0:porta`, registra o IP de origem quando ele ainda não existe na tabela e responde em unicast com `DISCOVER_ACK`. O cliente usa o IP de origem dessa resposta como endereço do servidor e exibe a linha `server_addr` solicitada.

Para teste local, há suporte a `DISCOVERY_BROADCAST=127.255.255.255`. Ela só é necessária quando o broadcast da rede não chega à interface loopback e não altera o comportamento normal de laboratório.

### Processamento confiável

Após a descoberta, o cliente aplica stop-and-wait: cria `REQUEST(id_req, value)`, envia-a e não busca o próximo valor até receber o ACK daquela mesma requisição. O identificador começa em 1 e só é incrementado após a confirmação. Se o ACK não chega em 100 ms, o cliente registra o timeout e retransmite exatamente o mesmo datagrama. O valor foi escolhido de forma conservadora para a rede local e está de acordo com a exigência de aguardar um limite de tempo antes do reenvio.

O servidor mantém uma entrada por IP com `last_req`, `last_num_reqs` e `last_total_sum`. Se `id_req` é o próximo esperado, o servidor soma `value`, atualiza as estruturas e responde com `ACK`. Se a requisição é duplicada ou adiantada, ele não soma novamente e responde com o ACK da última requisição efetivamente processada desse cliente. Assim, o cliente mantém a mensagem pendente até receber sua confirmação.

### Interface

O cliente possui uma thread dedicada à leitura da entrada padrão e outra à escrita de mensagens na tela. A thread principal coordena descoberta, envio e recepção dos ACKs. Não há prompt: cada linha da entrada deve conter somente um número. `Ctrl+D` encerra a leitura e `Ctrl+C` interrompe o processo; ambos enviam `EXIT` se o servidor já tiver sido descoberto.

O servidor imprime seu estado inicial e uma linha a cada requisição nova, duplicada ou fora de ordem. As impressões usam `flush=True`, evitando atrasos quando a saída é redirecionada.

## 3. Estruturas e funções principais

| Local | Estrutura/função | Responsabilidade |
| --- | --- | --- |
| `protocol.py` | `pack_request`, `unpack_request`, `pack_ack`, `unpack_ack` | Serializar e validar datagramas. |
| `protocol.py` | `Request`, `Acknowledgement` | Representar mensagens já decodificadas. |
| `servidor.py` | `ClientState` | Guardar a última resposta confirmável por IP. |
| `servidor.py` | `SumServer._handle_request` | Deduplicar, ordenar, agregar e confirmar requisições. |
| `cliente.py` | `SumClient.discover` | Difundir descoberta e identificar o servidor. |
| `cliente.py` | `SumClient._send_until_acknowledged` | Implementar stop-and-wait e retransmissão. |
| `cliente.py` | `Console` | Serializar a saída em uma thread própria. |

O protocolo binário usa ordem de bytes de rede e inteiros sem sinal de 64 bits. `REQUEST` contém tipo, identificador e valor; `ACK` contém tipo, último identificador processado, número global de requisições e soma global. As mensagens de descoberta e saída ocupam um byte. Limitar os valores a `uint64` evita que inteiros de precisão arbitrária do Python mascarem uma violação do limite exigido.

## 4. Sincronização e consistência

O servidor trata datagramas em um único loop de recepção. A leitura e a atualização de `clients`, `num_reqs` e `total_sum` ocorrem na mesma seção de execução, sem interleaving de outra requisição. Não é necessário lock no servidor porque não há múltiplas threads manipulando estado compartilhado.

No cliente, `queue.Queue` protege a passagem de valores da thread de entrada para a thread principal, e outra fila protege a passagem de linhas para a thread de saída. As operações de rede e o identificador da requisição ficam somente na thread principal, impedindo o envio concorrente de duas mensagens pendentes.

## 5. Primitivas de comunicação

Foram usados sockets `AF_INET` e `SOCK_DGRAM` da biblioteca padrão. `sendto` transmite cada datagrama e `recvfrom` recebe conteúdo e endereço de origem. `SO_BROADCAST` é configurado no cliente para permitir descoberta. `settimeout` implementa o limite de espera por respostas sem criar threads extras de rede.

UDP não fornece confiabilidade, ordem ou ausência de duplicação. Essas garantias são providas acima dele pelo número sequencial, tabela de estado no servidor, ACK com o último identificador aceito e retransmissão stop-and-wait no cliente.

## 6. Problemas encontrados e resolução

- **Perda de ACK:** o servidor poderia processar uma requisição e o cliente não saber disso. A tabela guarda o retrato do ACK original; uma duplicata recebe esse mesmo estado sem alterar a soma.
- **Datagrama adiantado:** aceitar diretamente uma mensagem com lacuna quebraria a ordenação por cliente. O servidor responde com o último ID aceito, e o cliente mantém a mensagem pendente.
- **Interleaving entre clientes:** as somas parciais dependem da ordem de chegada. O loop sequencial mantém cada atualização consistente, embora a ordem entre estações continue não determinística.
- **Volume grande:** JSON acrescentaria parsing e alocação a cada uma das milhões de requisições. Foi escolhido formato binário fixo e compacto no caminho crítico.

## 7. Validação executada

1. Compilação dos módulos com `python3 -m py_compile`.
2. Execução local entre Ubuntu e Windows, com descoberta UDP, enviando 10, 3 e 5. O servidor registrou corretamente `num_reqs 3 total_sum 18`. Uma retransmissão foi marcada como `DUP!!` sem alterar a soma.
3. Teste doméstico com os valores de 1 a 100. Considerando os valores anteriores, o servidor chegou a `num_reqs 102 total_sum 5063`, valor compatível com `13 + sum(1..100)`.
4. Teste de concorrência no laboratório com dois clientes Linux em IPs distintos. Cada cliente enviou 100 requisições; o servidor terminou com `num_reqs 200 total_sum 20100`.
5. Teste de carga no laboratório com dois clientes, cada um enviando 1.000 requisições. O log registrou 2.000 requisições aceitas, sem mensagens duplicadas ou fora de ordem, e o resultado final foi `num_reqs 2000 total_sum 2001000`.
6. Teste de carga com os arquivos `N1_1000000_NUM_ALEATORIOS.txt` e `N2_1000000_NUM_ALEATORIOS.txt`, processados concorrentemente em dois clientes Linux. As somas independentes foram 25486602 e 25513505; o servidor processou 2.000.000 requisições e terminou com `num_reqs 2000000 total_sum 51000107`, sem registros `DUP!!` nem `OUT_OF_ORDER`.

Os comandos, resultados e logs dos testes estão documentados em `TESTES_EM_CASA.md` e `TESTE_EM_LABORATORIO.md`.
