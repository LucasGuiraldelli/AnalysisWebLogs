# Análise de Logs Web com Apache Spark

Este projeto foi desenvolvido para processar e analisar logs de acesso ao servidor web de uma empresa de tecnologia, no padrão **Web Server Access Log**, com o objetivo de monitorar a performance do sistema, identificar padrões de uso e detectar possíveis problemas de segurança. Utilizando **Apache Spark** para processamento distribuído, o projeto extrai informações relevantes que auxiliam a equipe de operações na compreensão do comportamento dos usuários e da infraestrutura.

---

## Desafio

O objetivo é responder às seguintes perguntas com base nos dados do log:

1. **Identificar as 10 maiores origens de acesso (Client IP) por quantidade de acessos.**
2. **Listar os 6 endpoints mais acessados, desconsiderando aqueles que representam arquivos.**
3. **Quantificar o número de Client IPs distintos.**
4. **Calcular quantos dias de dados estão representados no arquivo.**
5. **Analisar o tamanho (em bytes) das respostas, incluindo:**
   - Volume total de dados retornados.
   - Maior volume de dados em uma única resposta.
   - Menor volume de dados em uma única resposta.
   - Volume médio de dados retornados.
6. **Determinar o dia da semana com o maior número de erros do tipo "HTTP Client Error".**

## Estrutura do Projeto

* [Deployment](#deployment)
* [Testes](#testes)
* [Justificativa](#justificativa)
* [Limitações](#limitações)

---

### Deployment

A opção de deployment escolhida foi o Databricks, então o projeto deve ser executado no **Databricks Community Edition**.

### Instruções de instalação/execução

Para executar o projeto no Databricks:

1. Crie uma conta no [Databricks Community Edition](https://community.cloud.databricks.com/).
2. Crie um cluster com a versão de runtime -> 12.2 LTS (inclui Apache Spark 3.3.2, Scala 2.12).
3. Importe o arquivo de log para o Databricks - [Limitação 1](#limitação-1). <br>
   3.1. **OBS:** Em uma etapa anterior, baixe os arquivos de log (arquivos: [link para os logs](https://github.com/seriallink/assignments/blob/main/)), utilize o 7zip para extrair os dados e resolver as partições.
4. Importe o notebook presente neste repositório ou acesse o link do notebook publicado no Databricks [AnalysisWebLogs](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/3503433957191274/2200104924572813/8694441731238967/latest.html).
5. Execute as células do notebook para obter as respostas aos desafios.

## Testes

Para este projeto, foram criados testes de validação de dados, separados por necessidade entre cada etapa da estrutura de processamento utilizada ([Medallion](https://www.databricks.com/br/glossary/medallion-architecture)).

Para que os dados fossem promovidos para a camada **Silver**, foram utilizadas as seguintes validações:
- Qualidade dos dados;
- Verificação de colunas obrigatórias;
- Verificação de null na coluna 'ip';
- Verificação da máscara do 'ip' utilizada;
- Validação do status de saída das requests;
- Validação do formato de data utilizada.

Para que os dados fossem promovidos para a camada **Gold**, foram utilizadas as seguintes validações:
- Verificação de colunas obrigatórias;
- Assegurar o tipo dos dados.

A camada **Silver** foi responsável por reter os dados tratados e assegurar sua qualidade de forma geral. <br>
Por sua vez, a camada **Gold** foi responsável por reter os dados preparados para que houvesse consultas diretas e facilitar processos inteligentes, assumindo algumas features. <br>

Todo arquivo dentro de cada camada foi salvo como **delta** a fim de facilitar o processamento futuro.

## Justificativa

A fim de facilitar o processo de consultas nos dados dentro do Databricks, optei por uma abordagem direta para salvar o arquivo de logs dentro da plataforma. Para isso, foi necessário criar uma tabela para que as [Limitações](#limitações) fosse superada. A criação da tabela foi feita da seguinte forma: ao importar o arquivo, passei todas as informações para uma só coluna, sendo ela string. Dentro dessa etapa, o único formato aceito de dados é o .csv, que não era o caso, mas para prosseguir, assumi o separador como um caractere inexistente nos dados ('^'). <br>
Criando assim a tabela dentro do Databricks, utilizando o DBFS (Databricks File System). <br>

## Limitações

### Limitação 1
O Databricks CE possui uma limitação de arquivo para importar diretamente em uma pasta, o que me fez surgir com a [solução acima](#justificativa).

### Limitação 2
Tempo de vida de um Cluster: Após uma hora de inatividade, o cluster é parado automaticamente, sendo impossível reiniciá-lo. Mesmo como administrador da conta, não é possível reativá-lo, obrigando o usuário a criar um novo a cada vez que isso acontece.
