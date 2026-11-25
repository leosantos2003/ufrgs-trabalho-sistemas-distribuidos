# Teste manual da Parte 2 com Linux e Windows

Este roteiro testa o serviço em dois computadores: o computador A usa Linux e
inicia como Front-End (FE); o computador B usa Windows e entra como Replica
Manager (RM). Um terceiro terminal em qualquer um dos computadores executa o
cliente.

Ele valida descoberta, entrada de RM, multicast, quórum, remoção de RM e a
promoção do único RM sobrevivente. Para demonstrar uma eleição completa entre
vários RMs, inclusive a escolha do maior IP e o salto de um sucessor morto, é
necessário um terceiro servidor com outro IP.

## 1. Preparar a rede

Os dois computadores precisam estar na **mesma rede local que permita
broadcast UDP**. O programa tenta automaticamente tanto o broadcast limitado
quanto o broadcast dirigido calculado da interface. Ainda assim, uma rede de
laboratório pode separar os equipamentos em VLANs ou bloquear ambos; para este
teste, prefira um roteador doméstico ou um hotspot de celular sem isolamento
entre clientes.

No computador A (Linux), confira o endereço e a máscara IPv4:

```bash
ip -4 -br addr
```

No computador B (Windows), abra o **PowerShell** e execute:

```powershell
ipconfig
Get-NetIPConfiguration | Format-Table InterfaceAlias,IPv4Address,IPv4DefaultGateway
```

Os endereços IPv4 Wi-Fi devem pertencer à mesma rede. Teste a conectividade em
ambos os sentidos:

```bash
# No Linux (A)
ping -c 3 IP_DO_OUTRO_COMPUTADOR
```

```powershell
# No Windows (B)
ping -n 3 IP_DO_OUTRO_COMPUTADOR
```

### Máquinas com Ethernet e Wi-Fi ativos

O servidor identifica a própria estação pela interface usada na rota padrão.
Portanto, se o experimento será feito pelo hotspot Wi-Fi, mas uma interface
Ethernet de outra rede (por exemplo, rede da universidade) continuar ativa, o
processo pode escolher o IP Ethernet e responder por ele. Isso impede a
descoberta no hotspot.

Antes de iniciar os servidores, desconecte temporariamente a interface que
**não** faz parte do experimento — pela interface gráfica ou, por exemplo:

```bash
nmcli device disconnect enp1s0
```

Substitua `enp1s0` pelo nome mostrado em `ip -4 -br addr`. Confirme que o IP
selecionado pelo programa pertence à rede de teste:

```bash
python3 -c 'from servidor import SumServer; print(SumServer.get_local_ip())'
```

Em um hotspot `10.141.0.0/24`, esse comando deve exibir um endereço
`10.141.0.x`, e não um endereço da rede Ethernet. Ao terminar, a interface
pode ser reconectada com:

```bash
nmcli device connect enp1s0
```

No Windows, confira que o adaptador que possui o gateway padrão é o Wi-Fi ou a
rede usada no experimento. Desconecte temporariamente VPNs e adaptadores que
conduzam a outra rede, pela interface do Windows, se o comando abaixo não
mostrar o IPv4 esperado:

```powershell
py -c "from servidor import SumServer; print(SumServer.get_local_ip())"
```

O programa não aceita um IP manualmente: o endereço acima deve ser detectado
pela rota padrão do Windows.

Se houver firewall ativo, libere a porta UDP escolhida (neste roteiro, `4000`).
Em distribuições com UFW:

```bash
sudo ufw status
sudo ufw allow 4000/udp
```

Em distribuições com `firewalld`, use em vez disso:

```bash
sudo firewall-cmd --add-port=4000/udp --permanent
sudo firewall-cmd --reload
```

No Windows, permita o Python na rede **Privada** quando o Firewall do Windows
exibir a pergunta na primeira execução. Se a pergunta não aparecer ou o
firewall continuar bloqueando o RM, abra o PowerShell como administrador e
libere a porta UDP do trabalho:

```powershell
New-NetFirewallRule -DisplayName 'TP SD UDP 4000' -Direction Inbound -Action Allow -Protocol UDP -LocalPort 4000 -Profile Private
```

Após o experimento, a regra pode ser removida com:

```powershell
Remove-NetFirewallRule -DisplayName 'TP SD UDP 4000'
```

## 2. Preparar os dois computadores

Copie a mesma versão do projeto para ambos. No Linux (A), dentro da pasta do
projeto, execute:

```bash
python3 --version
make check
```

No Windows (B), dentro da pasta do projeto, execute no PowerShell:

```powershell
py --version
py -m py_compile protocol.py servidor.py cliente.py
```

Se o comando `py` não existir, instale o Python 3 e marque a opção **Add
Python to PATH** durante a instalação; depois use `python` no lugar de `py`.

Antes de um teste entre máquinas, confira os hashes. No Linux:

```bash
sha256sum protocol.py servidor.py cliente.py
```

No Windows:

```powershell
Get-FileHash .\protocol.py, .\servidor.py, .\cliente.py -Algorithm SHA256
```

Os valores da coluna `Hash` precisam coincidir com os valores produzidos pelo
Linux. Uma divergência, especialmente em `protocol.py`, faz um RM rejeitar
silenciosamente mensagens de snapshot por usar um formato incompatível.

O programa não recebe IP por argumento. Ele identifica automaticamente o IP da
interface de rede; informe somente a porta.

## 3. Iniciar o Front-End

No computador A (por exemplo, Ubuntu), inicie primeiro o servidor:

```bash
python3 servidor.py 4000
```

O log esperado inclui:

```text
SERVER DISCOVERY: no FE found, self-proclaim FE ip IP_DO_COMPUTADOR_A
```

Espere essa linha antes de iniciar qualquer outro servidor.

## 4. Iniciar o Replica Manager

No computador B, abra o PowerShell na pasta do projeto e execute:

```powershell
py .\servidor.py 4000
```

O computador B deve localizar o FE e iniciar como RM. Os sinais esperados são:

```text
SERVER DISCOVERY: FE found ip IP_DO_COMPUTADOR_A; starting as RM
RM JOIN_REQUEST ...
RM SNAPSHOT_APPLIED ...
RM TOPOLOGY ...
```

No computador A, devem aparecer `FE RM_JOIN_SYNC` e depois `FE RM_JOIN`.

> Se o computador B também registrar `no FE found, self-proclaim FE`, pare os
> dois processos: o broadcast não está chegando. Mude ambos para a mesma rede
> local sem isolamento e recomece pelo passo 3. Não passe o IP ao servidor para
> contornar isso, pois essa não é uma forma de execução permitida pelo trabalho.

## 5. Iniciar o cliente e validar processamento normal

Abra outro PowerShell no computador B (ou um terminal Linux no computador A)
e execute:

```powershell
py .\cliente.py 4000
```

Se o cliente for executado no computador A em vez de B, use:

```bash
python3 cliente.py 4000
```

Antes de digitar valores, espere a linha:

```text
server_addr IP_DO_FE
```

Então envie valores, mantendo o terminal aberto:

```text
7
5
3
```

Para cada valor, o cliente deve receber um ACK com `id_req`, `num_reqs` e
`total_sum`. No FE devem aparecer `FE DISPATCH`, `FE QUORUM_READY` e
`FE QUORUM`; no RM, `RM APPLIED`.

Não envie valores antes de o RM concluir a entrada no grupo: enquanto houver
RMs ativos, o FE só confirma a requisição após receber o quórum deles.

## 6. Diagnóstico quando o cliente não mostra `server_addr`

Sem a linha `server_addr`, o cliente ainda está esperando a resposta de
descoberta; valores digitados nesse momento não foram processados. Primeiro,
verifique a rede e o firewall pelos passos anteriores.

Como diagnóstico de conectividade unicast (não como modo normal do trabalho),
é possível apontar temporariamente a descoberta do **cliente** para o IP já
conhecido do FE:

```powershell
$env:DISCOVERY_BROADCAST = 'IP_DO_FE'
py .\cliente.py 4000
```

No Linux, o equivalente é:

```bash
DISCOVERY_BROADCAST=IP_DO_FE python3 cliente.py 4000
```

Se esse comando mostrar `server_addr`, mas o comando normal não, a comunicação
UDP unicast funciona e o problema é o broadcast da rede. Esse atalho não ajuda
o segundo servidor a descobrir o FE, portanto não substitui a rede local
adequada para a demonstração.

## 7. Testar queda de RM

No computador B com Windows, identifique o processo do servidor no PowerShell:

```powershell
Get-CimInstance Win32_Process -Filter "Name = 'python.exe'" |
  Where-Object CommandLine -match 'servidor\.py 4000' |
  Select-Object ProcessId, CommandLine
```

Mate apenas esse processo de forma abrupta:

```powershell
Stop-Process -Id PID_DO_SERVIDOR_RM -Force
```

No FE (computador A), espere os logs `FE RM_HEARTBEAT_MISS` e `FE RM_FAILURE`.
Não deve haver mensagens `RING`, pois o FE permanece vivo. Reinicie o RM pelo
passo 4 antes de testar a queda do FE.

## 8. Testar queda do FE e continuidade do cliente

Com o RM novamente ativo e o cliente ainda aberto, no computador A execute:

```bash
pgrep -af 'servidor.py 4000'
kill -9 PID_DO_SERVIDOR_FE
```

No computador B, os logs devem incluir:

```text
RM FE_FAILURE_DETECTED ...
RING ELECTION_START ...
RING COORDINATOR_ANNOUNCE ...
RING NEW_FRONTEND ...
```

Como há somente um RM sobrevivente, ele próprio será promovido a FE. Digite
outro valor no cliente. Depois dos timeouts (cerca de 12 segundos na versão
atual), ele deve redescobrir o novo FE, manter o próximo `id_req` e produzir
uma soma que inclua cada valor apenas uma vez. Como não restará outro RM, o
novo FE registrará `FE LOCAL_COMMIT`: é a confirmação local do único servidor
sobrevivente. Quando um novo RM entrar, ele receberá o snapshot atualizado.

## 9. Encerrar o experimento

Use `Ctrl+C` nos processos que ainda estiverem em execução. Para uma nova
rodada limpa, aguarde todos os processos terminar e recomece pelo passo 3.
