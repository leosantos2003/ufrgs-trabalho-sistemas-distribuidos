# Tutorial de execução e testes

Este guia permite executar e avaliar o serviço distribuído de soma nos computadores do laboratório. Ele foi escrito para ser seguido por uma pessoa que acabou de receber o diretório de entrega.

O sistema tem um processo servidor e um processo cliente por estação. A comunicação é feita exclusivamente por UDP/IPv4. O cliente descobre o servidor por broadcast; portanto, não é necessário informar manualmente o IP do servidor.

## 1. Pré-requisitos

- Computadores Linux no mesmo segmento de rede.
- Python 3.10 ou superior, disponível pelo comando `python3`.
- Uma porta UDP livre, por exemplo `4000`.
- Os arquivos `cliente.py`, `servidor.py` e `protocol.py` no mesmo diretório em cada máquina que participará do teste.

Não há bibliotecas externas nem instalação de dependências: somente a biblioteca padrão do Python é usada.

Para os testes concorrentes, use **três computadores com endereços IP diferentes**:

| Papel | Computador | Arquivos usados |
|---|---|---|
| `S` | servidor | `servidor.py` e `protocol.py` |
| `C1` | primeiro cliente | `cliente.py` e `protocol.py` |
| `C2` | segundo cliente | `cliente.py` e `protocol.py` |

Para simplificar, pode-se copiar todo o diretório de entrega para as três máquinas.

> **Importante:** execute clientes concorrentes em máquinas com IPs distintos. O protocolo identifica cada participante pelo endereço IP, conforme especificado no enunciado.

## 2. Preparação

Em cada computador, abra um terminal e entre no diretório que contém os arquivos Python:

```bash
cd /caminho/para/diretorio-da-entrega
```

Confirme a versão do interpretador e faça uma verificação de sintaxe:

```bash
python3 --version
python3 -m py_compile protocol.py servidor.py cliente.py
```

Os dois comandos devem terminar sem erro. A compilação pode criar a pasta `__pycache__`; isso é normal e não interfere na execução.

Em seguida, confirme que os computadores estão na mesma rede IPv4:

```bash
ip -4 addr
```

Não é preciso anotar o IP do servidor para a execução normal, mas esse comando ajuda no diagnóstico caso a descoberta por broadcast não seja concluída.

## 3. Teste básico: descoberta e processamento manual

Este teste valida a descoberta por broadcast, o envio de requisições e a atualização correta da soma.

### 3.1. Iniciar o servidor em `S`

No computador `S`, execute:

```bash
python3 servidor.py 4000 | tee servidor-basico.log
```

O processo permanece em execução aguardando datagramas. A primeira linha exibida deve ter este formato:

```text
AAAA-MM-DD HH:MM:SS num_reqs 0 total_sum 0
```

O uso de `tee` mostra a saída na tela e também salva uma cópia no arquivo `servidor-basico.log`.

### 3.2. Iniciar um cliente em `C1`

No computador `C1`, execute:

```bash
python3 cliente.py 4000
```

Após a descoberta, deve aparecer uma linha semelhante a:

```text
AAAA-MM-DD HH:MM:SS server_addr 10.x.y.z
```

O endereço exibido é o IP descoberto do servidor. Em seguida, digite os valores abaixo, um por linha, pressionando `Enter` após cada um:

```text
10
3
5
```

Para cada valor, o cliente exibe o envio e, quando recebe a confirmação, uma linha contendo `id_req`, `value`, `num_reqs` e `total_sum`. Não há prompt de entrada: isso é intencional, conforme o enunciado.

Após processar os três valores, finalize a entrada com `Ctrl+D`. O cliente envia `EXIT` e termina.

### 3.3. Resultado esperado

Como o servidor foi iniciado sem estado anterior, as três últimas requisições aceitas devem produzir, no servidor, valores equivalentes a:

```text
... id_req 1 value 10 num_reqs 1 total_sum 10
... id_req 2 value 3  num_reqs 2 total_sum 13
... id_req 3 value 5  num_reqs 3 total_sum 18
```

O horário e o IP variam conforme o ambiente. O resultado importante é:

```text
num_reqs 3 total_sum 18
```

Antes de iniciar um novo caso de teste independente, encerre o servidor com `Ctrl+C` e inicie-o novamente. Isso garante que os contadores comecem em zero e que cada resultado possa ser comparado diretamente com o esperado.

## 4. Teste de concorrência com dois clientes

Este teste verifica que requisições de dois IPs diferentes são agregadas corretamente no mesmo servidor.

### 4.1. Iniciar um servidor novo em `S`

No computador `S`, inicie uma execução nova:

```bash
python3 servidor.py 4000 | tee servidor-concorrencia.log
```

### 4.2. Iniciar os dois clientes quase ao mesmo tempo

No computador `C1`, execute:

```bash
seq 1 100 | python3 cliente.py 4000 > cliente-c1.log
```

No computador `C2`, execute:

```bash
seq 101 200 | python3 cliente.py 4000 > cliente-c2.log
```

Cada cliente envia 100 valores. Os IDs são locais a cada cliente e ambos começam em `id_req 1`; os valores enviados, contudo, são diferentes.

### 4.3. Verificar o resultado em `S`

Quando os dois comandos dos clientes terminarem, ainda no computador `S`, execute:

```bash
tail -n 1 servidor-concorrencia.log
grep -c ' id_req ' servidor-concorrencia.log
```

O resultado correto é:

```text
num_reqs 200 total_sum 20100
```

O segundo comando deve retornar `200`, pois há exatamente 200 requisições aceitas. A última linha pode conter qualquer um dos dois IPs, porque a ordem de chegada dos datagramas não é determinística.

A soma esperada é:

```text
sum(1..100) + sum(101..200) = 5050 + 15050 = 20100
```

Para verificar se foram registradas duplicatas ou mensagens fora de ordem, use:

```bash
if grep -nE 'DUP!!|OUT_OF_ORDER' servidor-concorrencia.log; then
    printf 'Foram registradas mensagens especiais; verifique as linhas acima.\n'
else
    printf 'Nenhuma duplicata ou mensagem fora de ordem foi registrada.\n'
fi
```

Não é obrigatório que uma rede local gere duplicatas. Se ocorrer uma perda de ACK e o cliente retransmitir, o servidor pode mostrar `DUP!!`; nesse caso, a soma e `num_reqs` **não** podem aumentar pela segunda cópia da requisição.

## 5. Teste de carga com 2.000 requisições

Este é um teste intermediário, útil para confirmar processamento concorrente sob volume maior. Primeiro, encerre o servidor anterior com `Ctrl+C` e inicie um novo processo em `S`:

```bash
python3 servidor.py 4000 | tee servidor-carga.log
```

Em `C1`, execute:

```bash
seq 1 1000 | python3 cliente.py 4000 > cliente-carga-c1.log
```

Em `C2`, execute simultaneamente:

```bash
seq 1001 2000 | python3 cliente.py 4000 > cliente-carga-c2.log
```

Depois que ambos os clientes terminarem, verifique em `S`:

```bash
tail -n 1 servidor-carga.log
grep -c ' id_req ' servidor-carga.log
```

O resultado esperado é:

```text
num_reqs 2000 total_sum 2001000
```

Isso corresponde a `sum(1..2000)`. A contagem de linhas de requisições aceitas deve ser `2000`.

## 6. Teste com arquivos grandes, se fornecidos

Caso os arquivos `N1_1000000_NUM_ALEATORIOS.txt` e `N2_1000000_NUM_ALEATORIOS.txt` estejam disponíveis, eles podem ser usados para um teste de carga maior. Coloque um arquivo em `C1` e o outro em `C2`.

Primeiro, calcule as somas independentes em cada máquina cliente:

```bash
awk '{s += $1} END {print s}' N1_1000000_NUM_ALEATORIOS.txt
awk '{s += $1} END {print s}' N2_1000000_NUM_ALEATORIOS.txt
```

Anote os dois resultados e some-os. Esse será o `total_sum` esperado.

Reinicie o servidor em `S` antes de começar:

```bash
python3 servidor.py 4000 | tee servidor-1m.log
```

Em `C1`, execute:

```bash
time python3 cliente.py 4000 < N1_1000000_NUM_ALEATORIOS.txt > cliente-n1.log
```

Em `C2`, execute em paralelo:

```bash
time python3 cliente.py 4000 < N2_1000000_NUM_ALEATORIOS.txt > cliente-n2.log
```

Ao final, no servidor, verifique:

```bash
tail -n 1 servidor-1m.log
grep -c ' id_req ' servidor-1m.log
```

Os critérios de sucesso são:

- `num_reqs 2000000`;
- `total_sum` igual à soma independente dos dois arquivos;
- `grep -c ' id_req ' servidor-1m.log` retornando `2000000`;
- nenhuma linha `OUT_OF_ORDER`;
- uma linha `DUP!!`, caso ocorra, não deve alterar a contagem ou a soma final.

## 7. Comportamentos esperados do protocolo

Os seguintes comportamentos não indicam defeito:

| Situação observada | Interpretação esperada |
|---|---|
| Cliente mostra `TIMEOUT` e depois `RESEND`. | O ACK não foi recebido antes de 100 ms; o cliente reenviou a mesma requisição. |
| Servidor mostra `DUP!!`. | Uma cópia de uma requisição já processada chegou novamente; ela não foi somada outra vez. |
| Linhas de `C1` e `C2` aparecem intercaladas no log do servidor. | A ordem entre clientes depende da chegada dos datagramas e é naturalmente não determinística. |
| O cliente demora antes de exibir `server_addr`. | Ele continua retransmitindo mensagens de descoberta até obter resposta do servidor. |

Os seguintes resultados indicam falha ou configuração incorreta:

- o cliente não exibe `server_addr` mesmo com o servidor iniciado;
- o total final diverge da soma conhecida das entradas;
- a contagem de requisições aceitas é maior que a quantidade de números enviados;
- uma duplicata aumenta `num_reqs` ou `total_sum`.

## 8. Diagnóstico de problemas comuns

### Cliente não encontra o servidor

1. Confirme que o servidor já está executando e que todos usam a mesma porta, por exemplo `4000`.
2. Confirme que as máquinas pertencem ao mesmo segmento de rede IPv4.
3. Em cada máquina, consulte `ip -4 addr` e localize o campo `brd` da interface de rede usada. Se necessário, informe explicitamente esse broadcast **somente no cliente**:

```bash
DISCOVERY_BROADCAST=ENDERECO_DE_BROADCAST python3 cliente.py 4000
```

Por exemplo, se a interface informar `brd 10.67.103.255`, use:

```bash
DISCOVERY_BROADCAST=10.67.103.255 python3 cliente.py 4000
```

Não use o IP unicast do servidor nessa variável: ela representa o endereço de broadcast da rede. A configuração padrão `255.255.255.255` deve ser tentada primeiro.

### A porta já está em uso

No computador servidor, verifique quem está usando a porta:

```bash
ss -lun | grep ':4000'
```

Encerre a execução anterior do servidor com `Ctrl+C` ou escolha outra porta livre e use a mesma nova porta em todos os comandos.

### O servidor parece não mostrar saída

Quando iniciado normalmente, o servidor fica bloqueado aguardando datagramas; isso é o comportamento correto. Ele exibe a linha inicial e depois só imprime novas linhas quando recebe mensagens. Se a saída foi redirecionada com `> arquivo.log`, acompanhe-a de outro terminal com:

```bash
tail -f arquivo.log
```

## 9. Encerramento

- Para encerrar um cliente interativo, use `Ctrl+D` para fim da entrada ou `Ctrl+C` para interrompê-lo.
- Para encerrar o servidor, use `Ctrl+C` no terminal em que ele está executando.
- Reinicie o servidor entre casos de teste independentes, pois a soma e a tabela de participantes residem somente em memória.

## 10. Arquivos que devem acompanhar a entrega

O pacote de entrega deve incluir ao menos:

```text
cliente.py
servidor.py
protocol.py
RELATORIO.md
TUTORIAL_EXECUCAO_E_TESTES.md
```

Os documentos `FLUXO.md`, `TESTES_EM_CASA.md` e `TESTE_EM_LABORATORIO.md` podem acompanhar a entrega como documentação complementar.
