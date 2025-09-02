\# Crypto Analytics Dashboard



ETL em \*\*Python\*\* + \*\*Power BI\*\* para análise de preços, retornos e risco de criptomoedas utilizando a API pública \[CoinGecko](https://www.coingecko.com/).



Este projeto cobre o ciclo completo de dados: \*\*extração → transformação → carga (ETL) → modelagem → visualização\*\*.



---



\## Tecnologias Utilizadas

\- \*\*Python 3.13\*\*

&nbsp; - `requests` → consumo da API CoinGecko

&nbsp; - `pandas` e `numpy` → tratamento e cálculos (retorno, média móvel, drawdown, etc.)

\- \*\*Power BI\*\*

&nbsp; - Modelagem de dados

&nbsp; - Medidas em \*\*DAX\*\*

&nbsp; - Relatórios interativos e visuais

\- \*\*GitHub\*\* → versionamento e portfólio



---



\## Análises Incluídas

\- \*\*Evolução do Preço\*\*: Último Preço vs. Média Móvel (30 dias)  

\- \*\*Retornos\*\*: Heatmap do retorno médio mensal  

\- \*\*Volatilidade\*\*: desvio padrão dos retornos diários (%)  

\- \*\*Market Share\*\*: participação de cada moeda ao longo do tempo  



---



\## Estrutura do Projeto



crypto-analytics-dashboard/

│

├─ etl/

│ └─ crypto\_etl.py

│

├─ data

│ ├─ crypto\_prices.csv

│ └─ coin\_metadata.csv

│

├─ dashboard/

│ └─ CryptoDashboard.pbix

│

├─ requirements.txt

└─ README.md







---



\## Como Executar o Projeto



\### 1. Clone este repositório

```bash

git clone https://github.com/ViniciusDegelo/crypto-analytics-dashboard.git

cd crypto-analytics-dashboard





\### 2. Crie um ambiente virtual (opcional)

python -m venv .venv

\# Windows:

.venv\\Scripts\\activate

\# Linux/macOS:

source .venv/bin/activate





\### 3. Instale as dependências

pip install -r requirements.txt





**### 4. Execute o ETL em Python**

**python etl/crypto\_etl.py**





**### 5. Abra o Dashboard no Power BI**

**dashboard/CryptoDashboard.pbix**







\## Observações

\- O repositório inclui apenas amostras de um ano em data.

\- Para obter dados atualizados, execute o ETL

\- O modelo foi construído com foco em \*\*aprendizado\*\* e \*\*portfólio\*\*.



\## Roadmap (Futuros Incrementos)

\- Automatizar execução diária do ETL

\- Incluir métricas de volume de negociação

\- Criar versão web interativa (Flask ou Streamlit)

\- Publicar dashboard no Power BI Service



Desenvolvido por \*\*Vinicius Degelo\*\*

\[LinkedIn](https://linkedin.com/in/vinicius\_degelo)

\[GitHub](https://github.com/ViniciusDegelo)







