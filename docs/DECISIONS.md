# Decisões de design

Registro das decisões que mais moldaram o projeto, no estilo ADR
(Architecture Decision Record) - contexto, decisão e consequências aceitas.
Todas foram tomadas (e várias revisadas) contra erros reais de execução em
ambiente de produção, não em ambiente de teste isolado.

---

## ADR-01: Nunca recriar uma tabela de destino já existente por padrão

**Contexto.** Rodar o ETL de novo sobre uma tabela que já foi carregada é o
caso mais comum (cargas agendadas, reprocessamento). Recriar a tabela do
zero a cada execução destruiria dados carregados por outros processos ou
ajustes manuais feitos no destino.

**Decisão.** Se a tabela já existe, **nunca** é recriada por padrão - só
recebe as colunas e constraints (PK/UNIQUE/FK) que a origem tem e o destino
ainda não tem, via `ALTER TABLE ADD` ("self-healing" incremental).
Recriação total só acontece com a flag explícita `--recreate`.

**Consequências.** `sync_missing_columns` só *adiciona* colunas - não
corrige o tipo de uma coluna que já existia no destino com estrutura
diferente da origem (ex: alguém alterou manualmente o tipo). Para corrigir
isso é preciso rodar uma vez com `--recreate` (carga completa) ou fazer o
ajuste manualmente.

---

## ADR-02: FK para tabela ainda não existente é um aviso, não um erro fatal

**Contexto.** Ao copiar um banco inteiro tabela por tabela, a ordem de
cópia nem sempre respeita o grafo de dependências entre FKs - e resolver
esse grafo automaticamente (ordenação topológica) adiciona complexidade
significativa para um ganho pequeno, já que o script é pensado para rodar
repetidamente.

**Decisão.** Quando uma FK referencia uma tabela que ainda não existe no
destino, a constraint é **pulada com um aviso** no log (o SQL Server não
permite criar uma FK apontando para tabela inexistente) - a(s) coluna(s)
continuam sendo copiadas normalmente, só sem a validação referencial. Numa
execução futura, depois que a tabela referenciada também tiver sido
copiada, `sync_missing_constraints` detecta e cria a FK pendente
automaticamente, sem recriar nada nem perder dados.

**Consequências.** Entre a primeira cópia e a cópia da tabela referenciada,
o destino fica temporariamente sem aquela garantia de integridade
referencial. Para um ETL de replicação (não o sistema transacional
original), esse trade-off foi considerado aceitável.

---

## ADR-03: Colunas virtuais/computadas do Oracle e HANA não são traduzidas

**Contexto.** Colunas virtuais no Oracle (`GENERATED ALWAYS AS`) e colunas
calculadas no HANA usam PL/SQL e SQLScript respectivamente - dialetos que
não têm um parser/tradutor simples e confiável para T-SQL. Traduzir errado
seria pior do que não traduzir: geraria uma fórmula silenciosamente
incorreta no destino.

**Decisão.** Essas colunas são criadas como coluna comum (nullable) no
destino, e a expressão original é registrada em log como aviso, para ajuste
manual caso o cálculo precise ser preservado. **Exceção**: quando a origem
já é SQL Server, a fórmula e a flag `PERSISTED` são lidas diretamente de
`sys.computed_columns` e recriadas automaticamente (mesmo dialeto,
tradução 1:1 segura).

**Consequências.** Um relatório/consulta no destino que dependa do valor
calculado de uma coluna virtual Oracle/HANA vai encontrar `NULL` até que
alguém preencha a coluna manualmente ou recrie a fórmula em T-SQL.

---

## ADR-04: Cópia literal de PK/FK/IDENTITY, não um "insert seguro"

**Contexto.** Para um ETL de *replicação* (não de integração/transformação
de dados), o objetivo é que o destino seja uma cópia fiel da origem - os
mesmos IDs, as mesmas chaves, para que outros sistemas que dependam desses
valores (joins, relatórios, chaves estrangeiras externas) continuem
funcionando.

**Decisão.** Por padrão, **todas** as colunas são inseridas literalmente,
inclusive PK, FK, SK e colunas IDENTITY (via `SET IDENTITY_INSERT ON`
durante a carga). As únicas exceções são colunas `computed` e
`rowversion`/`timestamp`, porque o próprio SQL Server nunca aceita valor
explícito nelas - não é uma escolha de design, é uma restrição do motor.

**Consequências.** Depende de `IDENTITY_INSERT` estar disponível e sem
conflito com outra sessão simultânea inserindo na mesma tabela.

---

## ADR-05: Watermark por `MAX()` como padrão, `--reconcile` como modo explícito

**Contexto.** A forma mais barata de fazer carga incremental é filtrar
`WHERE coluna > MAX(coluna já carregada)`. Isso é suficiente na imensa
maioria dos casos (ex: ID autoincremento, data de atualização crescente),
mas falha silenciosamente se existirem "furos": uma linha com ID menor que
o MAX atual que nunca chegou a ser inserida (ex: por uma falha de rede
pontual numa execução anterior que não derrubou o processo inteiro).

**Decisão.** O watermark por `MAX()` continua sendo o modo padrão (mais
rápido, menor custo em bancos de produção). `--reconcile` existe como um
modo **explícito e mais custoso**, para rodar periodicamente ou sob
suspeita de divergência: compara todas as chaves (ver `docs/ARCHITECTURE.md`
para como isso é feito com baixo consumo de memória) em vez de confiar no
`MAX()`.

**Consequências.** Quem usa só o watermark padrão não detecta furos
automaticamente - é preciso rodar `--reconcile` deliberadamente para
auditar/corrigir.

---

## ADR-06: `--delete-extra` é opt-in e nunca aborta o lote inteiro por causa de uma FK

**Contexto.** Remover do destino linhas que não existem mais na origem é
uma operação destrutiva por natureza. Além disso, numa exclusão em lote,
uma única linha protegida por FK de outra tabela derrubaria a transação
inteira se tratada de forma ingênua (`DELETE ... WHERE chave IN (...)`
falha por completo se qualquer linha do lote estiver protegida).

**Decisão.** `--delete-extra` só tem efeito junto com `--reconcile`
(nunca é o comportamento padrão) e, ao encontrar erro de FK, divide o lote
recursivamente até isolar exatamente quais chaves estão bloqueadas,
pulando-as com aviso e completando a exclusão das demais.

**Consequências.** Uma execução com `--delete-extra` pode ser mais lenta
que uma exclusão em lote única quando há muitas linhas protegidas por FK
(custo do isolamento recursivo), mas nunca deixa de excluir o que pode ser
excluído por causa de uma única linha bloqueada.

---

## ADR-07: Retry diferencia erro de conexão de erro de SQL/dados

**Contexto.** Um retry genérico (repetir qualquer exceção) é perigoso: se
o erro for uma violação de constraint ou um valor inválido, repetir a
mesma operação vai falhar da mesma forma indefinidamente (ou, pior,
mascarar um problema de dados real).

**Decisão.** `resilience._is_connection_error` mantém uma lista explícita
de marcadores de queda de conexão por banco (SQLSTATEs ODBC, códigos de
erro HANA, mensagens `ORA-031xx`/`ORA-125xx` do Oracle, mensagens de
texto como "Communication link failure"). Só esses erros disparam
reconexão + retry com backoff exponencial; qualquer outra exceção sobe
imediatamente.

**Consequências.** A lista de marcadores precisa ser mantida/estendida se
um novo tipo de erro de rede aparecer em produção que ainda não esteja
coberto (ver `docs/ARCHITECTURE.md`, item de roadmap sobre estender a
resiliência à fase de streaming inicial do `--reconcile`).

---

## ADR-08: Lotes grandes demais se dividem sozinhos em vez de exigir configuração manual

**Contexto.** O limite de itens numa cláusula `IN (...)` (ou de parâmetros
de uma chamada ODBC) varia por banco e por driver, e nem sempre é
documentado com clareza. Expor isso como um parâmetro que o usuário precisa
ajustar manualmente (por tentativa e erro, por origem) adiciona fricção
operacional.

**Decisão.** Quando uma consulta/exclusão em lote falha por um motivo que
não é queda de conexão, o lote é dividido pela metade e a operação é
repetida recursivamente, até um tamanho mínimo seguro (`MIN_RECONCILE_CHUNK`
/ `MIN_DELETE_CHUNK`). O script se adapta sozinho ao limite real de cada
ambiente em vez de quebrar a reconciliação inteira por causa de um único
lote grande demais.

**Consequências.** Em ambientes com limite de `IN` muito baixo, o custo de
descobrir o tamanho seguro (via divisões sucessivas) é pago a cada
execução que encontra um lote grande demais - não há cache do tamanho
"que funcionou da última vez".
