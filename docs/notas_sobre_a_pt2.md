## Notas do professor sobre o Trabalho Prático 2

- **Papéis das Entidades:** A implementação deve contemplar exclusivamente dois tipos de entidades: servidores e clientes. Um cliente manterá sempre o papel de cliente durante todo o ciclo de vida da aplicação. Já um servidor poderá assumir dinamicamente tanto o papel de front-end (FE) quanto o de replica manager (RM), conforme o resultado do processo de descoberta inicial ou da eleição de líder.

- **Algoritmo de Eleição e Rastreabilidade (Logs):** O único algoritmo de eleição aceito para este trabalho é o Algoritmo do Anel (conforme expressamente anunciado em aula; qualquer menção remanescente a outro algoritmo no enunciado será desconsiderada/corrigida). Para fins de avaliação, as mensagens de log geradas no terminal pelos servidores devem registrar de forma clara e sequencial cada etapa da execução do algoritmo (detecção de falha, início da eleição, recebimento/repasses da mensagem no anel e proclamação do novo coordenador).

- **Identificador de Eleição e Endereçamento:** Como critério de eleição no anel, deve-se utilizar o endereço IP da estação servidora, elegendo neste caso o nó detentor do maior endereço IP.

    - **Restrição de execução:** Não é permitido passar o endereço IP como argumento de linha de comando ou arquivo de configuração para o servidor; a aplicação deve recuperar automaticamente o endereço IP corrente diretamente da interface de rede.
    - **Testes em máquina única:** Para testes locais em um único hospedeiro, recomenda-se vincular cada instância de servidor a diferentes endereços de loopback disponíveis no sistema operacional (ex.: 127.0.0.1, 127.0.0.2, 127.0.0.3, etc.).

- **Roteiro Esperado de Execução e Demonstração:**

1. **Inicialização do 1º Servidor:** Executa o mecanismo de descoberta e identifica que não há outros nós ativos. Ele se autoproclama front-end (FE). Neste instante, ainda não há replica managers ativos.
2. **Inicialização do 2º Servidor:** Executa a descoberta, identifica a existência de um FE ativo e assume o papel de replica manager (RM). O FE atualiza a lista de servidores ativos com o novo integrante e propaga a topologia atualizada para o grupo.
3. **Inicialização do 3º Servidor (e subsequentes):** Repete o fluxo do passo anterior -- ingressa como RM, tem seu registro adicionado à lista de membros pelo FE e a nova topologia do anel é propagada para todos os servidores ativos.
4. **Inicialização do 1º Cliente:** Executa a descoberta para localizar o FE corrente, conecta-se a ele e inicia o envio das requisições de soma.
5. **Inicialização do 2º Cliente (e subsequentes):** Localiza o FE corrente via descoberta e inicia o envio concorrente de requisições.
6. **Queda do Front-End:** O processo que atua como FE é encerrado abruptamente. Os RMs remanescentes detectam a falha e disparam o Algoritmo do Anel para eleger o novo líder (maior IP), que assume o papel de FE e passa a responder aos clientes.
7. **Queda de um Replica Manager:** O processo de um RM é encerrado abruptamente. Não há disparo de eleição (pois o FE permanece ativo); o FE apenas detecta a saída da réplica, atualiza a lista de servidores ativos para ajuste de quórum/topologia do anel e o serviço segue operando normalmente sem interrupção para os clientes.
    - **Observação:** Após a inicialização do conjunto de servidores, o cenário de demonstração garantirá que o sistema sempre mantenha pelo menos um servidor ativo.
