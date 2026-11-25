# Serviço distribuído de soma - Parte 2

Implementação em Python 3 de um serviço UDP de soma com replicação ativa,
quórum e eleição de líder pelo algoritmo do anel.

## Arquitetura

- Clientes descobrem o front-end (FE) e usam stop-and-wait por `id_req`.
- O FE atribui um `seq_id`, dissemina a operação por multicast e só responde
  após obter o quórum de resultados concordantes dos replica managers (RMs).
- Cada RM reordena operações, descarta duplicatas, mantém seu acumulador e uma
  cópia do último ACK de cada cliente.
- Na queda do FE, os RMs executam um anel unidirecional. O maior IPv4 vence,
  assume o papel de FE e preserva o acumulador, `seq_id` e estado dos clientes.

## Requisitos

- Linux
- Python 3.10 ou posterior; não há dependências externas
- Uma porta UDP livre em todos os participantes, por exemplo `4000`

## Comandos

```bash
make check            # valida sintaxe
make test             # executa testes da Parte 2 em loopback
make server PORT=4000 # inicia um servidor
make client PORT=4000 # inicia um cliente
```

Também é possível executar diretamente:

```bash
python3 servidor.py 4000
python3 cliente.py 4000
```

Para um ensaio com dois computadores Linux, consulte
[o tutorial específico](docs/TUTORIAL_TESTE_DOIS_LINUX.md).

O servidor não aceita IP como argumento nem por arquivo de configuração: ele
obtém automaticamente seu endereço IPv4 da interface de rede. Em um cenário
local, os testes automatizados associam instâncias a `127.0.0.1`,
`127.0.0.2`, etc.; no laboratório, execute uma instância em cada estação ou
em interfaces locais distintas.

## Demonstração sugerida

1. Inicie três servidores na mesma porta. O primeiro vira FE; os demais
   descobrem o FE, recebem snapshot e entram como RMs.
2. Inicie um ou mais clientes e envie inteiros positivos.
3. Encerre um RM: o FE registra a falha e recalcula a topologia/quórum, sem
   eleição.
4. Encerre abruptamente o FE: os RMs registram `FE_FAILURE_DETECTED`, mensagens
   `RING`, a proclamação `NEW_FRONTEND` e o maior IP assume o serviço.
5. Mantenha o cliente ativo: depois dos timeouts ele redescobre o novo FE e
   reenvia a mesma requisição pendente, sem duplicar a soma.

Os logs `RING ELECTION_*`, `RING SUCCESSOR_UNAVAILABLE` e
`RING COORDINATOR_*` foram mantidos deliberadamente detalhados para a
apresentação e rastreabilidade da eleição.

## Arquivos

- `protocol.py`: mensagens binárias UDP, snapshots e tokens do anel.
- `servidor.py`: FE, RM, replicação ativa, quórum, heartbeat e eleição.
- `cliente.py`: descoberta e cliente confiável stop-and-wait.
- `tests/test_part2.py`: testes de failover e salto de sucessor indisponível.
- `docs/RELATORIO_PARTE_2.md`: roteiro factual do relatório da Parte 2.
