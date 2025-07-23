# BRFSS DataOps Pipeline

Este repositório contém a pipeline de dados do projeto BRFSS (Behavioral Risk Factor Surveillance System), parte de um trabalho acadêmico integrado entre as disciplinas de Engenharia de Sistemas Inteligentes e Aprendizagem de Máquina.

## 📁 Estrutura do Projeto

brfss_dataops/
│
├── data/
│ ├── raw/ # Dados brutos baixados automaticamente
│ ├── processed/ # Dados processados e prontos para uso
│ └── cleaned/ # Dados limpos e validados
│
├── src/
│ └── pipeline_brfss.py # Script principal da pipeline
│
├── .gitignore
└── README.md


## ⚙️ O que a pipeline faz?

- Baixa automaticamente os arquivos da pesquisa BRFSS a partir da internet
- Converte os arquivos de `.XPT` para `.CSV`
- Realiza limpeza básica dos dados
- Organiza os dados em pastas estruturadas

## ▶️ Como executar

1. Clone este repositório:
   ```bash
   git clone https://github.com/SEU_USUARIO/brfss-dataops.git
   cd brfss-dataops

2. Crie um ambiente virtual e ative:

bash
Copiar
Editar
python -m venv venv
venv\Scripts\activate   # Windows
source venv/bin/activate  # Linux/Mac
Instale as dependências:

bash
Copiar
Editar

3. pip install -r requirements.txt

4. Execute a pipeline:

bash
Copiar
Editar
python src/pipeline_brfss.py

🤝 Integração
A pipeline foi desenvolvida para se integrar com uma aplicação em Streamlit construída por outros membros da equipe. Os dados processados são utilizados diretamente pela aplicação.

📌 Observações
Não é necessário subir arquivos .csv manualmente.

Toda a coleta e pré-processamento dos dados é automatizada.

O projeto utiliza boas práticas de organização e versionamento.