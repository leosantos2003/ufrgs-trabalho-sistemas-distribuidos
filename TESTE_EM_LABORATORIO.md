# Testes realizados no laboratório

**Data:** 09/10/2026  
**Ambiente:** três computadores Linux da UFRGS na mesma rede local. Uma máquina executou o servidor; duas máquinas executaram clientes concorrentes.

## Configuração

- Porta UDP: `4000`.
- Clientes identificados nos logs pelos endereços `10.67.103.35` e `10.67.103.36`.
- A descoberta do servidor foi realizada normalmente pelo protocolo, sem configuração manual de endereço.
- O timeout dos clientes foi configurado como `REQUEST_TIMEOUT_SECONDS = 0.100`.

## 1. Concorrência com 200 requisições

Com o servidor iniciado sem requisições anteriores, dois clientes foram executados em máquinas distintas:

```bash
# Cliente A
seq 1 100 | python3 cliente.py 4000 > cliente-a.log

# Cliente B
seq 101 200 | python3 cliente.py 4000 > cliente-b.log
```

Cada cliente enviou 100 requisições. A soma esperada era:

```text
sum(1..100) + sum(101..200) = 5050 + 15050 = 20100
```

A última linha observada no servidor foi:

```text
2026-10-09 11:20:04 client 10.67.103.35 id_req 100 value 100 num_reqs 200 total_sum 20100
```

O resultado confirma que as 200 requisições foram processadas uma única vez e que o total global ficou correto. A identidade do cliente exibido na última linha é naturalmente não determinística, pois depende da ordem de chegada dos datagramas.

## 2. Carga concorrente com 2.000 requisições

O servidor foi reiniciado e sua saída foi salva em `servidor-concorrencia.log`. Foram usados dois clientes:

```bash
# Cliente A
seq 1 1000 | python3 cliente.py 4000 > /dev/null

# Cliente B
seq 1001 2000 | python3 cliente.py 4000 > /dev/null
```

O resultado esperado era:

```text
num_reqs 2000 total_sum 2001000
```

O log possui 2.001 linhas: uma linha inicial e 2.000 requisições aceitas. Sua última linha é:

```text
2026-10-09 11:25:25 client 10.67.103.35 id_req 1000 value 1000 num_reqs 2000 total_sum 2001000
```

Os dois clientes participaram efetivamente da execução: cada IP aparece em 1.000 linhas do log. As requisições dos dois IPs se alternam no registro do servidor, o que confirma o processamento concorrente. Não houve linhas `DUP!!` nem `OUT_OF_ORDER` nesse teste.

## 3. Carga com dois arquivos de um milhão de números

O servidor foi reiniciado e sua saída foi salva em `servidor-1m.log`. Dois clientes processaram, em paralelo, os arquivos fornecidos:

```bash
# Cliente A
python3 cliente.py 4000 < N1_1000000_NUM_ALEATORIOS.txt > /dev/null

# Cliente B
python3 cliente.py 4000 < N2_1000000_NUM_ALEATORIOS.txt > /dev/null
```

As somas dos arquivos foram verificadas previamente com `awk`:

```text
N1_1000000_NUM_ALEATORIOS.txt = 25486602
N2_1000000_NUM_ALEATORIOS.txt = 25513505
Soma esperada                    = 51000107
```

O log contém 2.000.001 linhas: o estado inicial e 2.000.000 requisições aceitas. A última linha foi:

```text
2026-10-09 11:37:24 client 10.67.103.36 id_req 1000000 value 46 num_reqs 2000000 total_sum 51000107
```

Cada cliente enviou exatamente 1.000.000 requisições. Não houve registros `DUP!!` ou `OUT_OF_ORDER`. As primeiras requisições aceitas foram registradas às 11:34:12 e a última às 11:37:24, correspondendo a aproximadamente 3 minutos e 12 segundos para processar dois milhões de requisições concorrentes.

## Conclusão

Os testes no laboratório validaram a descoberta e comunicação entre máquinas Linux distintas, o processamento concorrente de múltiplos clientes e a consistência do acumulador global sob carga. Em particular, foram processadas corretamente 2.000.000 requisições concorrentes, com o valor final da soma igual ao valor independente calculado a partir dos arquivos de entrada.

Os logs de evidência permanecem na raiz do projeto:

- `servidor-concorrencia.log`;
- `servidor-1m.log`.
