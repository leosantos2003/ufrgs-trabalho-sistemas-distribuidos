# Testes realizados em casa

**Data:** noite de 08/10/2026 e início de 09/10/2026  
**Ambiente:** servidor em Ubuntu e cliente em Windows, conectados à mesma rede local Wi-Fi.

## Configuração usada

- Servidor: notebook Ubuntu, endereço IPv4 `192.168.0.14`.
- Cliente: notebook Windows, endereço IPv4 `192.168.0.4`.
- Porta UDP: `4000`.
- Cliente executado com Python 3.14.8.
- Timeout final do cliente: `REQUEST_TIMEOUT_SECONDS = 0.100` (100 ms).

No Ubuntu, o servidor foi iniciado com:

```bash
python3 servidor.py 4000
```

No Windows, em PowerShell, o cliente foi iniciado com:

```powershell
py .\cliente.py 4000
```

## 1. Descoberta do servidor por broadcast

O cliente Windows exibiu:

```text
2026-10-09 00:08:19 server_addr 192.168.0.14
```

Isso confirma que a mensagem de descoberta enviada pelo cliente chegou ao servidor Ubuntu e que a resposta unicast do servidor retornou corretamente ao cliente. Não foi necessário definir a variável `DISCOVERY_BROADCAST`; portanto, a descoberta por broadcast funcionou na rede doméstica.

## 2. Envio manual de números

Foram enviados, nessa ordem, os valores `10`, `3` e `5` a partir do cliente Windows. O servidor registrou cada requisição somente uma vez e atingiu:

```text
num_reqs 3 total_sum 18
```

O valor é o esperado, pois `10 + 3 + 5 = 18`.

Durante os primeiros testes, usando timeout de 10 ms, ocorreram várias retransmissões. Após ajustar o timeout para 100 ms, o valor `10` foi confirmado sem retransmissão e o valor `3` precisou de apenas um reenvio.

Exemplo do comportamento observado para o valor `3`:

```text
# Cliente
... SEND id_req 2 value 3
... TIMEOUT id_req 2 value 3
... RESEND id_req 2 value 3
... id_req 2 value 3 num_reqs 2 total_sum 13

# Servidor
... client 192.168.0.4 id_req 2 value 3 num_reqs 2 total_sum 13
... client 192.168.0.4 DUP!! id_req 2 value 3 num_reqs 2 total_sum 13
```

Esse resultado valida o protocolo de confirmação: uma requisição retransmitida é identificada como duplicada e não altera o acumulador global.

## 3. Carga de 100 requisições

Foi executado no Windows:

```powershell
1..100 | py .\cliente.py 4000 > cliente.log
```

O servidor já continha as duas requisições da execução anterior, com `num_reqs 2` e `total_sum 13`. A nova execução enviou os valores de 1 a 100, cuja soma é 5050. Assim, o resultado final esperado era:

```text
num_reqs 102 total_sum 5063
```

A última linha registrada pelo servidor foi:

```text
2026-10-09 00:11:36 client 192.168.0.4 id_req 100 value 100 num_reqs 102 total_sum 5063
```

Logo, as 100 requisições foram processadas e a soma final foi correta. Houve uma duplicata da requisição `id_req 2`, que foi registrada como `DUP!!` e não afetou a contagem nem a soma.

## 4. Encerramento do cliente

O teste de carga foi alimentado por um *pipe*. Quando a sequência de entrada terminou, o cliente recebeu fim de arquivo e encerrou. Em uma execução seguinte, um novo cliente com o mesmo IP conseguiu iniciar novamente em `id_req 1`, o que é compatível com o envio e processamento da mensagem `EXIT`.

## Conclusão dos testes domésticos

Os testes confirmaram:

- descoberta por broadcast entre duas máquinas;
- comunicação UDP entre Ubuntu e Windows;
- confirmação stop-and-wait;
- retransmissão após timeout;
- deduplicação no servidor;
- soma correta de requisições consecutivas;
- processamento correto de uma sequência de 100 requisições;
- encerramento por fim de arquivo.

Ainda devem ser testados no laboratório: múltiplos clientes Linux executando simultaneamente em endereços IP distintos e os arquivos de entrada com um milhão de números cada.
