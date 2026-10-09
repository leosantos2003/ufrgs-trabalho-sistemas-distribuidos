# Serviço distribuído de soma confiável

**UFRGS - INF01085 - Sistemas Distribuídos e Tolerantes a Falhas - 2026/2**

Implementação da primeira parte do trabalho prático de Sistemas Distribuídos e
Tolerantes a Falhas. O sistema usa exclusivamente UDP/IP para a comunicação
entre processos e implementa descoberta por broadcast e processamento confiável
stop-and-wait.

## Requisitos

- Linux
- Python 3.10 ou superior (somente biblioteca padrao)

## Execução

Em uma máquina, inicie o servidor na porta escolhida:

```bash
python3 servidor.py 4000
```

Em cada máquina cliente no mesmo segmento de rede, execute:

```bash
python3 cliente.py 4000
```

Depois da linha `server_addr`, digite um inteiro positivo por linha (ou use
redirecionamento):

```bash
python3 cliente.py 4000 < N1_1000000_NUM_ALEATORIOS.txt
```

`Ctrl+C` ou `Ctrl+D` encerra o cliente. O servidor pode ser terminado com
`Ctrl+C`.

Para um teste local em loopback, em sistemas onde o broadcast global não é
roteado para a interface loopback, use somente no cliente:

```bash
DISCOVERY_BROADCAST=127.255.255.255 python3 cliente.py 4000
```

## Protocolo e confiabilidade

As mensagens de processamento têm formato binário com inteiros sem sinal de 64
bits em ordem de rede: `REQUEST(id_req, value)` e
`ACK(last_req, num_reqs, total_sum)`. As mensagens `DISCOVER`,
`DISCOVER_ACK` e `EXIT` ocupam um byte. A descoberta é enviada em broadcast;
as outras mensagens são unicast.

Cada cliente inicia em `id_req = 1` e envia apenas uma requisição pendente. Se
o ACK não chega em 100 ms, ele registra o timeout e retransmite a mesma
requisição. O servidor guarda, por IP de cliente, a última requisição aceita e
o retrato do acumulador devolvido naquela resposta. Assim, uma duplicata não é
somada novamente: recebe novamente o ACK previamente calculado. Uma mensagem
adiantada recebe ACK do último identificador processado e o cliente retransmite
a mensagem pendente.

O servidor atende datagramas em um único loop, o que torna atômicas as
alterações da tabela de clientes e do acumulador global. Valores, quantidade de
requisições e soma são validados como `uint64`; o servidor não aceita uma soma
que ultrapassaria esse limite.

## Arquivos

- `protocol.py`: codificação e validação das mensagens UDP.
- `servidor.py`: descoberta, tabela de participantes, deduplicação e soma.
- `cliente.py`: leitura em thread dedicada, saída em thread dedicada e envio
  stop-and-wait.

  ## License

  Distributed under the MIT License. See `LICENSE.txt` for more information.