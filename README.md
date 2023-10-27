# Desafio-Spark

## Visão Geral

Este projeto tem como objetivo a implementação de um código spark que lida com tabelas de saldo inicial e tabelas de movimentação armazenadas em um banco de dados para calculos de saldo e reembolso.

## Funcionamento

Na execução deste código, o processo pode ser dividido em duas situações principais:

### Sem Reembolso

1. Verificação da existência de reembolsos pendentes.

2. Se não houver reembolso pendente:
   - Inclusão de novas pessoas com base na tabela de movimentações.
   - Cálculo do saldo das pessoas da tabela de saldo inicial com base nos valores da tabela de movimentações.
   - Combinar as informações para gerar uma nova tabela de saldo como resultado final.

### Com Reembolso

1. Identificação dos saldos que necessitam de atualização e correção.

2. Realização das correções necessárias nos saldos afetados pelo reembolso.

3. Seguir os mesmos passos de inclusão de novas pessoas e cálculo de saldo como descrito na situação sem reembolso.

### Cenário Proposto

Para o cenário específico deste desafio, o processo é executado da seguinte maneira:

1. Execução da movimentação do dia 02, que resultará na tabela de saldo do dia 02.

2. Execução da função de cálculo de saldo novamente com base no retorno do dia 02 para calcular o saldo do dia 03.

O resultado final será uma tabela de saldos corrigida e que permite a rastreabilidade de um dia para o outro.

Este projeto é essencial para manter a integridade das informações contábeis e financeiras, permitindo o acompanhamento preciso das transações e garantindo a consistência dos saldos ao longo do tempo.

### Cenário Real

Em um cenário real, o processo se daria da seguinte maneira: 

O processo inclui filtragem de tabelas com base em parâmetros para obter o saldo inicial de um determinado dia (x) e a tabela de movimentação do dia seguinte (x+1). Além disso, o código aborda a correção de casos de reembolso através de operações de "insert overwrite" na tabela de saldo.

seria necessário pequenas adequaçõoes no codigo para funcionamento no cenario real