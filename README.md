# Dashboard Bayesiano de Liquidez

Arquitetura implementada:

- **Backend em Python** para coleta, transformacao, classificacao e modelo bayesiano.
- **SQLite** para guardar o historico das execucoes.
- **Frontend estatico** em HTML/CSS/JS puro, consumindo `frontend/data/latest.json` e `frontend/data/history.json`.
- **GitHub Actions** opcional para rodar semanalmente e atualizar os JSONs e o banco.
- **API do FRED** como fonte primaria para as series economicas.

## Estrutura

```text
backend/
  app/
  data/
frontend/
  assets/
  data/
.github/workflows/
```

## Requisitos

- Python 3.11+
- Dependencia Python: `requests`
- Variavel de ambiente `FRED_API_KEY`

## Configuracao da API do FRED

### Linux/macOS

```bash
export FRED_API_KEY="sua_chave_aqui"
```

### Windows PowerShell

```powershell
$env:FRED_API_KEY="sua_chave_aqui"
```

Sem essa variavel, o backend nao executa a coleta das series do FRED.

## Rodando localmente

### 1) Instalar dependencias

```bash
python3 -m pip install -r backend/requirements.txt
```

### 2) Configurar a chave da API do FRED

```bash
export FRED_API_KEY="sua_chave_aqui"
```

### 3) Executar o backend

Na raiz do projeto:

```bash
python3 -m backend.app.main
```

Isso ira:

- buscar dados publicos via **API do FRED**
- ler inputs manuais de `backend/data/raw/manual_inputs.csv`
- calcular sinais e probabilidade posterior
- gravar `backend/data/liquidez.db`
- exportar `frontend/data/latest.json`
- exportar `frontend/data/history.json`

### 4) Servir o frontend estatico

Uma forma simples:

```bash
python3 -m http.server 8000
```

Depois abra:

```text
http://localhost:8000/frontend/
```

## Inputs manuais

Edite `backend/data/raw/manual_inputs.csv` para complementar o que nao vem por endpoint publico estavel:

- `fra_ois_bp`
- `repo_stress_score`
- `tic_3m_usd_bn`
- `rrp_usd_bn` (fallback)
- `reserve_floor`

## SQLite

O banco fica em:

```text
backend/data/liquidez.db
```

Tabelas:

- `runs`
- `signal_results`
- `external_block_details`

## Publicacao estatica

Para GitHub Pages:

- publique a pasta `frontend/`
- mantenha o workflow semanal ativo para atualizar `frontend/data/latest.json` e `frontend/data/history.json`
- configure o segredo do repositório `FRED_API_KEY` no GitHub

## Observacoes

- O frontend **nao** consulta SQLite diretamente.
- O SQLite e usado somente pelo backend para historico e auditoria.
- O dashboard le apenas JSONs prontos.
- A coleta do FRED agora usa a **API oficial**, nao mais o endpoint CSV publico.
