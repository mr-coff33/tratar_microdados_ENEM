# 📊 ENEM Data Pipeline

Pipeline completo de engenharia de dados para processamento, limpeza, transformação e armazenamento dos microdados do ENEM ao longo de múltiplos anos.

---

## 🚀 Visão Geral

Este projeto implementa um pipeline ETL (Extract, Transform, Load) capaz de:

* 📥 Extrair dados brutos (arquivos compactados)
* 🧹 Limpar e organizar diretórios automaticamente
* 🔄 Tratar grandes volumes de dados (CSV)
* 🗄️ Carregar os dados em banco relacional
* 📈 Preparar os dados para análise e visualização

---

## 🧠 Arquitetura do Pipeline

```
        ┌────────────┐
        │   ZIPs     │
        │ (INEP)     │
        └─────┬──────┘
              │
              ▼
     ┌──────────────────┐
     │   Extração       │
     │ (extract)        │
     └─────┬────────────┘
           │
           ▼
     ┌──────────────────┐
     │ Limpeza Estrutural│
     │ (pastas inúteis) │
     └─────┬────────────┘
           │
           ▼
     ┌──────────────────┐
     │ Transformação    │
     │ (tratamento CSV) │
     └─────┬────────────┘
           │
           ▼
     ┌──────────────────┐
     │ Carga em Banco   │
     │ (SQL)            │
     └──────────────────┘
```

---

## 📂 Estrutura do Projeto

```
enem-data-pipeline/
├── data/
│   ├── raw/           # Dados brutos (ZIP)
│   ├── extracted/     # Dados extraídos
│   ├── processed/     # Dados tratados
│
├── src/
│   ├── extract/
│   │   └── extrator_de_arquivos.py
│   │
│   ├── transform/
│   │   ├── tratar_pastas.py
│   │   └── tratar_dados.py
│   │
│   ├── load/
│   │   └── envio_DB.py
│
├── notebooks/         # Análises futuras
├── requirements.txt
├── README.md
└── .gitignore
```

---

## ⚙️ Tecnologias Utilizadas

* Python 3
* Pandas
* SQLAlchemy
* PyMySQL
* Chardet

---

## 📦 Instalação

Clone o repositório:

```
git clone https://github.com/seu-usuario/enem-data-pipeline.git
cd enem-data-pipeline
```

Crie um ambiente virtual:

```
python -m venv .venv
source .venv/bin/activate
```

Instale as dependências:

```
pip install -r requirements.txt
```

---

## ▶️ Como Executar o Pipeline

### 1️⃣ Extração dos arquivos

```
python src/extract/extrator_de_arquivos.py
```

---

### 2️⃣ Limpeza de diretórios

Remove arquivos desnecessários como:

* LEIA-ME
* DOCUMENTOS TÉCNICOS
* INPUTS
* DICIONÁRIOS

```
python src/transform/tratar_pastas.py
```

---

### 3️⃣ Tratamento de dados

* Correção de encoding
* Padronização de colunas
* Limpeza de inconsistências

```
python src/transform/tratar_dados.py
```

---

### 4️⃣ Envio para banco de dados

```
python src/load/envio_DB.py
```

---

## 🗄️ Banco de Dados

O projeto utiliza banco relacional (MySQL ou compatível).

Exemplo de string de conexão:

```
mysql+pymysql://usuario:senha@localhost:3306/enem
```

---

## 📊 Possibilidades de Análise

Com os dados tratados, é possível analisar:

* 📍 Desempenho por estado
* 📅 Evolução das notas ao longo dos anos
* 🎓 Perfil socioeconômico dos participantes
* 📚 Comparação entre áreas do conhecimento

---

## 🔥 Melhorias Futuras

* [ ] Dashboard interativo com Streamlit
* [ ] Pipeline automatizado (cron ou Airflow)
* [ ] Data Warehouse (BigQuery / Redshift)
* [ ] API para consulta dos dados
* [ ] Machine Learning para previsão de notas

---

## 👨‍💻 Autor

Desenvolvido por **Danylo**

Projeto focado em:

* Engenharia de Dados
* Ciência de Dados
* Projetos reais para portfólio

---

## 📄 Licença

Este projeto está sob a licença MIT.
