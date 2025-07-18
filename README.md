# TCC - Análise de Performance de Consultas Espaciais em Diferentes Bancos de Dados

Este repositório contém o código, scripts e experimentos do Trabalho de Conclusão de Curso (TCC) que compara a performance de consultas espaciais em diferentes Sistemas de Gerenciamento de Banco de Dados (SGBDs): **PostgreSQL**, **SQL Server** e **Snowflake**.

## 📁 Estrutura do Projeto

```
sql/
├── config_loader.py
├── environment_keys.json.example
├── data_ingestion/
│   ├── data_ingestion.py
│   ├── treated_dataset/
│   ├── raw_dataset/
│   └── ...
├── discovery/
│   └── discovery_dataset.ipynb
├── experiments/
│   ├── postgresql/
│   │   ├── 1.sql ... 4.sql
│   ├── sqlserver/
│   │   ├── 1.sql ... 4.sql
│   ├── snowflake/
│   │   ├── 1.sql ... 4.sql
│   ├── results/
│   └── running_experiments.py
└── README.md
```

---

## 📚 Objetivo

Avaliar e comparar o desempenho de operações espaciais (consultas geográficas) em diferentes SGBDs, utilizando um dataset de municípios brasileiros e experimentos padronizados.

---

## ⚙️ Configuração do Ambiente

1. **Clone o repositório:**
   ```bash
   git clone <url-do-repositorio>
   cd sql
   ```

2. **Crie e configure o arquivo de variáveis de ambiente:**
   - Copie o arquivo de exemplo:
     ```bash
     cp environment_keys.json.example environment_keys.json
     ```
   - Preencha os campos com suas credenciais de acesso aos bancos de dados.

3. **Instale as dependências Python:**
   - Recomenda-se o uso de um ambiente virtual.
   - Instale as bibliotecas necessárias:
     ```bash
     pip install pandas sqlalchemy psycopg2-binary pyodbc snowflake-connector-python
     ```

4. **Configure os bancos de dados:**
   - Utilize os scripts em `setup_environments/` para criar as tabelas e schemas necessários em cada SGBD.

---

## 🚦 Como Executar

### 1. **Ingestão de Dados**

- Execute o script de ingestão para carregar os dados tratados nos bancos configurados:
  ```bash
  python data_ingestion/data_ingestion.py
  ```

### 2. **Execução dos Experimentos**

- Configure as variáveis no início do arquivo `experiments/running_experiments.py`:
  ```python
  SGBD_ESCOLHIDO = "PostgreSQL"      # "PostgreSQL", "SQL Server" ou "Snowflake"
  MODELO_ESCOLHIDO = "Desnormalizado" # "Desnormalizado" ou "Normalizado"
  EXPERIMENTO_ESCOLHIDO = "E1"        # "E1", "E2", "E3" ou "E4"
  ```
- Execute o script:
  ```bash
  python experiments/running_experiments.py
  ```
- Os resultados serão salvos em `experiments/results/`.

### 3. **Exploração e Descoberta**

- Utilize o notebook em `discovery/discovery_dataset.ipynb` para análise exploratória dos dados.

---

## 🧪 Experimentos

Os experimentos são padronizados e estão descritos nos arquivos SQL de cada SGBD:

- **E1:** Agregação Analítica Ampla (SUM, GROUP BY por estado)
- **E2:** Filtro Geográfico Simples (Point-in-Polygon)
- **E3:** Filtro Composto com JOIN (municípios da região Nordeste com área > 5.000 km²)
- **E4:** Self-Join Espacial (municípios vizinhos em Pernambuco)

---

## 📊 Resultados

- Os resultados de cada execução são salvos em arquivos CSV na pasta `experiments/results/`.
- Cada arquivo contém o tempo de execução, estatísticas de I/O e outras métricas relevantes para análise comparativa.

---

## 🔒 Segurança

- **NUNCA** commit suas credenciais!  
- O arquivo `environment_keys.json` está no `.gitignore` e não será enviado ao repositório.
- Use sempre o arquivo de exemplo `environment_keys.json.example` como base.

---

## 👨‍💻 Autor

- **Nome:** Otavio
- **Curso:**  Ciência da Computação
- **Instituição:** UFPE
- **Orientador:** Robson do Nascimento Fidalgo

---

## 📄 Licença

Este projeto é acadêmico e de uso livre para fins educacionais.

---

Se tiver dúvidas, sugestões ou encontrar algum problema, abra uma issue ou entre em contato!
