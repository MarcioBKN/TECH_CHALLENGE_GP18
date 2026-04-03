# Documentação do projeto — ETL Olist → SQLite → Power BI

Este documento descreve, passo a passo, como reproduzir o ambiente, executar o pipeline de dados e conectar as ferramentas de análise.

---

## 1. Visão geral

| Etapa | Ferramenta | Descrição |
|-------|------------|-----------|
| Fonte | CSVs (dataset Olist) | Arquivos na pasta `archive` |
| Tipagem e regras | `dicionario.xlsx` | Dicionário de dados por entidade |
| Transformação e carga | Python (`fiap18.ipynb` + `utils/etl_olist.py`) | ETL padronizado para SQLite |
| Destino | `database.db` | Banco SQLite na raiz do repositório |
| Indicadores e modelo | Power BI Desktop | Conexão ao `.db`, medidas e visualizações |

**Princípio de governança:** a conexão co' o banco fica centralizada em `utils/db.py` (`with_connection`, `DB_PATH`). O notebook não repete strings de conexão; o código reutiliza esses utilitários.

---

## 2. Pré-requisitos

- **Python** 3.10 ou superior (recomendado).
- **Power BI Desktop** (gratuito) para o dashboard.
- Opcional: **Git** para versionar o repositório.
- Opcional: **DBeaver** ou extensão de SQLite no VS Code para inspecionar dados e diagramas.

### 2.1 Dependências Python

Instale a partir da raiz do projeto:

```bash
pip install -r requirements-analise.txt
```

Conteúdo esperado do arquivo:

- `pandas` — leitura de CSV e carga no SQLite.
- `openpyxl` — leitura do `dicionario.xlsx`.

---

## 3. Estrutura de pastas (referência)

```
fiap/
├── database.db              # Gerado após o ETL (não versionar se for muito grande)
├── fiap18.ipynb             # Notebook principal do ETL
├── requirements-analise.txt
├── DOCUMENTACAO.md          # Este arquivo
└── utils/
    ├── __init__.py
    ├── db.py                # Conexão SQLite (DB_PATH, with_connection)
    └── etl_olist.py           # Pipeline ETL Olist
```

**Dados externos (fora do repositório, por padrão):**

- `C:\Users\lopes\Downloads\archive\` — CSVs do Olist (`olist_*.csv`, `product_category_name_translation.csv`).
- `C:\Users\lopes\Downloads\dicionario.xlsx` — dicionário de tipos.

Se você mover esses arquivos, atualize os caminhos na célula de configuração do `fiap18.ipynb` (variáveis `ARCHIVE` e `DICIONARIO_XLSX`).

---

## 4. Fontes de dados

### 4.1 Dataset Olist (e-commerce brasileiro)

Os CSVs seguem o modelo público **Brazilian E‑Commerce** (Olist). Cada arquivo corresponde a uma entidade (clientes, pedidos, itens, pagamentos, avaliações, produtos, vendedores, geolocalização, tradução de categorias).

### 4.2 Arquivo `dicionario.xlsx`

- Uma **aba por entidade** (nomes como *Clientes*, *Pedidos*, *Itens do Pedido*, etc.).
- Colunas esperadas: **`Campo`**, **`Tipo`**, **`Descrição`**.
- **`Tipo`** aceita valores como: `string`, `int`, `float`, `datetime` (o código normaliza variações).

O ETL associa cada CSV à aba correta por **palavras-chave** no nome da aba (evita problemas de encoding).

### 4.3 Ajuste de nomes (produtos)

O CSV oficial de produtos usa `product_name_lenght` e `product_description_lenght`. O código renomeia para `product_name_length` e `product_description_length` para alinhar ao dicionário.

---

## 5. Passo a passo — executar o ETL

### 5.1 Abrir o projeto no VS Code

1. **Arquivo → Abrir pasta** e selecione a pasta `fiap` (raiz do projeto).
2. Garanta que o interpretador Python correto está selecionado (barra de status ou `Ctrl+Shift+P` → *Python: Select Interpreter*).

### 5.2 Conferir caminhos no notebook

1. Abra `fiap18.ipynb`.
2. Na **Fase 1 — Configuração**, verifique:
   - `ROOT` aponta para a pasta que contém `utils/`.
   - `ARCHIVE` aponta para a pasta dos CSVs.
   - `DICIONARIO_XLSX` aponta para o `dicionario.xlsx`.
3. Execute a célula e confira no console se `Pasta CSV` e `Dicionário` aparecem como existentes (`True`).

### 5.3 Inspecionar o dicionário (opcional)

Execute a **Fase 2** para ver o resumo das abas e um exemplo de tipos (ex.: aba Pedidos).

### 5.4 Carga completa

1. Execute a célula da **Fase 3** que chama `run_olist_etl(archive=..., dicionario=...)`.
2. O script:
   - remove as tabelas Olist antigas (respeitando dependências);
   - recria o esquema com **chaves estrangeiras**;
   - insere os dados na ordem correta.

**Tempo:** depende do tamanho dos CSVs (milhões de linhas podem levar vários minutos).

### 5.5 Teste sem gravar no banco

Para validar apenas leitura e tipos:

```python
from pathlib import Path
from utils.etl_olist import run_olist_etl

run_olist_etl(
    archive=Path(r"C:\Users\lopes\Downloads\archive"),
    dicionario=Path(r"C:\Users\lopes\Downloads\dicionario.xlsx"),
    dry_run=True,
)
```

### 5.6 Regra de qualidade — avaliações

O arquivo de avaliações pode conter **`review_id` duplicado**. Na carga, mantém-se apenas a **primeira ocorrência** por `review_id`.

### 5.7 Conferência pós-carga

Execute a **Fase 4** do notebook para listar tabelas e contagens de linhas no `database.db`.

---

## 6. Ordem de carga e integridade referencial

A ordem segue as dependências do modelo Olist:

1. `olist_geolocation_dataset`
2. `product_category_name_translation`
3. `olist_customers_dataset`
4. `olist_sellers_dataset`
5. `olist_products_dataset`
6. `olist_orders_dataset` (referencia clientes)
7. `olist_order_items_dataset` (referencia pedidos, produtos, vendedores)
8. `olist_order_payments_dataset`
9. `olist_order_reviews_dataset`

Relacionamentos principais (conceituais): `order_id`, `customer_id`, `product_id`, `seller_id`; geolocalização e CEP conectam-se a clientes/vendedores no **Power BI** (nem sempre como FK rígida no SQLite para CEP com múltiplas linhas).

---

## 7. Onde fica o banco SQLite

- Caminho lógico no código: `utils/db.py` define `DB_PATH` como **`database.db` na raiz do projeto** (pasta pai de `utils/`).
- Exemplo: `C:\Users\lopes\OneDrive\Área de Trabalho\fiap\database.db`.

Se você copiar apenas o `.db` para outra máquina, use o **mesmo esquema** gerado pelo ETL.

---

## 8. Visualizar dados no VS Code

1. Instale uma extensão de SQLite (ex.: **SQLite Viewer** ou **SQLite** por alexcvzz).
2. Clique com o botão direito em `database.db` → abrir com a extensão.
3. Navegue pelas tabelas e execute consultas `SELECT` se a extensão permitir.

**Diagrama ER:** o SQLite não desenha diagramas sozinho. Use **DBeaver** (grátis): conecte ao arquivo → *View Diagram* / *ER Diagram* a partir das chaves estrangeiras.

---

## 9. Conectar o Power BI ao SQLite

1. Abra o **Power BI Desktop**.
2. **Página Inicial** → **Obter dados**.
3. Pesquise **SQLite** → **Banco de dados SQLite** → **Conectar**.
4. Em **caminho do arquivo**, informe o caminho **absoluto** do `database.db` (o mesmo de `DB_PATH` / pasta do projeto).
5. No navegador, marque as tabelas necessárias → **Carregar** (recomendado para começar) ou **Transformar dados**.
6. Na **visualização de modelo**, revise relacionamentos; crie medidas DAX conforme o escopo do trabalho.

**Observação:** se você mover a pasta do projeto, atualize o caminho da fonte em **Transformar dados** → **Configurações da fonte de dados**.

---

## 10. Checklist de entrega (faculdade)

- [ ] Repositório público no GitHub com `fiap18.ipynb`, `utils/`, `requirements-analise.txt` e esta documentação.
- [ ] Instrução clara de onde colocar `archive` e `dicionario.xlsx` (ou incluir amostra menor, se a política permitir).
- [ ] `database.db` opcional no repositório (pode ser grande; às vezes o professor prefere só o código + instrução para gerar o banco).
- [ ] Link da apresentação (storytelling).
- [ ] Link do vídeo executivo (até 5 minutos), em linguagem de negócio.

---

## 11. Problemas comuns

| Sintoma | O que verificar |
|--------|------------------|
| `ModuleNotFoundError: utils` | Execute o notebook com a pasta `fiap` como raiz ou ajuste `sys.path` na primeira célula. |
| `ModuleNotFoundError: pandas` / `openpyxl` | `pip install -r requirements-analise.txt` no ambiente correto. |
| `FileNotFoundError` no CSV ou no xlsx | Caminhos `ARCHIVE` e `DICIONARIO_XLSX`. |
| Falha de FK na carga | Ordens dos CSVs corrompidos ou dados inconsistentes; rode `dry_run=True` para isolar leitura/tipos. |
| Power BI não encontra o arquivo | Caminho absoluto atualizado; arquivo não foi movido após configurar a fonte. |

---

## 12. Referências úteis

- Documentação do dataset Olist (Kaggle): busque *Brazilian E‑Commerce* para o dicionário de negócio das colunas.
- [Power BI — Obter dados](https://learn.microsoft.com/pt-br/power-bi/connect-data/desktop-get-data): conectores oficiais.

---

*Documento gerado para apoiar reproducibilidade e entrega acadêmica. Atualize os caminhos absolutos conforme o computador do avaliador.*
