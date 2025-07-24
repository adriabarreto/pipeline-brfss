import os
import requests
import zipfile
import pyreadstat
import datetime
import pandas as pd
import numpy as np
import logging

# Diretório base do projeto (pasta acima da pasta 'src')
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# Configuração do logging para registrar informações sobre o processo
data_dir = os.path.join(BASE_DIR, "data")
if not os.path.exists(data_dir):
    os.makedirs(data_dir)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),  # Exibe logs na tela
        logging.FileHandler(os.path.join(data_dir, "pipeline.log"))  # Salva logs em arquivo
    ]
)

# Lista de colunas que usaremos do dataset BRFSS (base CDC/Kaggle)
COLUNAS_KAGGLE = [
    'DIABETE3', 'DIABETE4', 'SEX', 'MARITAL', 'EDUCA', 'EMPLOY1', 'INCOME2',
    'GENHLTH', 'PHYSHLTH', 'MENTHLTH', 'POORHLTH', 'HLTHPLN1',
    'CHECKUP1', 'BPHIGH4', 'TOLDHI2', 'CVDSTRK3', 'CHCSCNCR', 'CHCOCNCR',
    'CHCCOPD1', 'HAVARTH3', 'ADDEPEV2', 'CHCKIDNY', 'DIFFWALK', 'CHCCOPD2', 'HAVARTH4',
    'ADDEPEV3', 'CHCKDNY2', 'BPHIGH6', 'TOLDHI3', 'CHCCOPD3', 'HAVARTH5', 'INCOME3', '_HLTHPLN',
    'CHCSCNC1', 'CHCOCNC1'
]

# Mapeamento para binarizar a variável alvo 'Diabetes'
MAPEAMENTO_DIABETES = {
    1: 1,  # Sim
    2: 1,  # Pré-diabetes
    3: 0,  # Gestacional
    4: 0,  # Não
    5: 0,  # Não sabe
    7: 0,  # Recusou
    9: 0   # Dados ausentes
}

# Renomeação das colunas para formato mais amigável (padronizado Kaggle)
RENOMEAR_COLUNAS = {
    'SEX': 'Sex',
    'MARITAL': 'Marital',
    'EDUCA': 'Educa',
    'EMPLOY1': 'Employ1',
    'INCOME2': 'Income2',
    'GENHLTH': 'GenHlth',
    'PHYSHLTH': 'PhysHlth',
    'MENTHLTH': 'MentHlth',
    'POORHLTH': 'PoorHlth',
    'HLTHPLN1': 'HlthPln1',
    'CHECKUP1': 'Checkup1',
    'BPHIGH4': 'BpHigh4',
    'TOLDHI2': 'ToldHi2',
    'CVDSTRK3': 'CvdStrk3',
    'CHCSCNCR': 'ChcScncr',
    'CHCOCNCR': 'ChcoCncr',
    'CHCCOPD1': 'ChcCopd1',
    'HAVARTH3': 'HavArth3',
    'ADDEPEV2': 'AddEpev2',
    'CHCKIDNY': 'ChkIdny',
    'DIFFWALK': 'DiffWalk',
    'CHCCOPD2': 'ChcCopd1',
    'HAVARTH4': 'HavArth3',
    'ADDEPEV3': 'AddEpev2',
    'CHCKDNY2': 'ChkIdny',
    'BPHIGH6' : 'BpHigh4',
    'TOLDHI3' : 'ToldHi2',
    'CHCCOPD3': 'ChcCopd1',
    'HAVARTH5': 'HavArth3',
    'INCOME3': 'Income2',
    '_HLTHPLN': 'HlthPln1',
    'CHCSCNC1': 'ChcScncr',
    'CHCOCNC1': 'CHCOCNC1'
}

# Valores inválidos que serão convertidos em NaN (dados ausentes, recusados, etc)
INVALIDOS = [
    7, 77, 777, 7777, 777777,  # Don't know / Not sure
    8, 88, 888, 8888, 888888,  # Refused
    9, 99, 999, 9999, 999999   # Missing / No answer
]

# Colunas categóricas para tratamento especial
COLUNAS_CATEGORICAS = [
    'Sex', 'Marital', 'Educa', 'Employ1', 'Income2', 'GenHlth', 'HlthPln1',
    'Checkup1', 'BpHigh4', 'ToldHi2', 'CvdStrk3', 'ChcScncr', 'ChcoCncr',
    'ChcCopd1', 'HavArth3', 'AddEpev2', 'ChkIdny', 'DiffWalk'
]

def anos_disponiveis(inicio=2015):
    """
    Retorna uma lista de anos desde o início informado até o ano atual.

    Args:
        inicio (int): Ano inicial (padrão 2015).

    Returns:
        list[int]: Lista de anos.
    """
    ano_atual = datetime.datetime.now().year
    return list(range(inicio, ano_atual + 1))

def baixar_arquivo(url, caminho_destino):
    """
    Baixa o arquivo de uma URL para o caminho local especificado, se não existir.

    Args:
        url (str): URL do arquivo para download.
        caminho_destino (str): Caminho local onde salvar o arquivo.

    Returns:
        bool: True se o download foi bem-sucedido ou o arquivo já existia, False se houve erro.
    """
    if not os.path.exists(caminho_destino):
        logging.info(f"Baixando {url} ...")
        try:
            response = requests.get(url)
            response.raise_for_status()
            with open(caminho_destino, "wb") as f:
                f.write(response.content)
            logging.info("Download concluído.")
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logging.warning(f"Arquivo não encontrado no servidor: {url}")
            else:
                logging.error(f"Erro ao baixar arquivo: {e}")
            return False
    else:
        logging.info(f"Arquivo {caminho_destino} já existe, pulando download.")
    return True

def extrair_zip(caminho_zip, pasta_destino):
    """
    Extrai o conteúdo de um arquivo ZIP para o diretório destino.

    Args:
        caminho_zip (str): Caminho do arquivo ZIP.
        pasta_destino (str): Diretório onde os arquivos serão extraídos.
    """
    if not os.path.exists(pasta_destino):
        os.makedirs(pasta_destino)
    with zipfile.ZipFile(caminho_zip, "r") as zip_ref:
        zip_ref.extractall(pasta_destino)
    logging.info(f"Arquivos extraídos em {pasta_destino}.")

def ler_xpt_com_fallback(caminho_arquivo):
    """
    Lê um arquivo XPT, tentando primeiro o encoding padrão (utf-8)
    e em caso de erro tenta com encoding 'latin1'.

    Args:
        caminho_arquivo (str): Caminho do arquivo XPT.

    Returns:
        tuple: (DataFrame com os dados, metadados)
    """
    try:
        df, meta = pyreadstat.read_xport(caminho_arquivo)
        logging.info("Arquivo lido com encoding padrão (utf-8).")
    except UnicodeDecodeError:
        logging.warning("Erro de encoding utf-8, tentando latin1...")
        df, meta = pyreadstat.read_xport(caminho_arquivo, encoding='latin1')
        logging.info("Arquivo lido com encoding latin1.")
    return df, meta

def limpar_dados(df, ano):
    """
    Limpa e prepara os dados, incluindo a criação da variável binária para diabetes
    e substituição de valores inválidos por NaN.

    Args:
        df (pd.DataFrame): DataFrame bruto.
        ano (int): Ano dos dados para saber qual coluna diabetes usar.

    Returns:
        pd.DataFrame: DataFrame limpo e preparado.
    """
    # Escolhe a coluna de diabetes correta conforme o ano
    coluna_diabetes = 'DIABETE3' if ano < 2019 else 'DIABETE4'

    # Seleciona apenas as colunas relevantes que existem no DataFrame
    colunas_para_selecionar = [col for col in COLUNAS_KAGGLE if col in df.columns]
    df = df[colunas_para_selecionar].copy()

    # Substitui valores inválidos por NaN, exceto na coluna diabetes
    for col in colunas_para_selecionar:
        if col != coluna_diabetes:
            df[col] = df[col].replace(INVALIDOS, np.nan)

    # Mapeia diabetes para variável binária
    df['Diabetes_binary'] = df[coluna_diabetes].map(MAPEAMENTO_DIABETES)

    # Remove a coluna original de diabetes
    df.drop(columns=[coluna_diabetes], inplace=True)

    # Renomeia colunas para formato padrão
    df.rename(columns=RENOMEAR_COLUNAS, inplace=True)

    # Cria colunas faltantes e preenche com a moda do ano anterior
    if 'BpHigh4' not in df.columns:
        df['BpHigh4'] = np.nan
    if 'ToldHi2' not in df.columns:
        df['ToldHi2'] = np.nan

    return df

def imputar_dados(df, moda_anteriores=None):
    """
    Imputa dados faltantes, usando a média para numéricas e moda para categóricas.
    Também imputa com a moda do ano anterior, se disponível.

    Args:
        df (pd.DataFrame): DataFrame limpo, porém com valores faltantes.
        moda_anteriores (dict): Moda das colunas do ano anterior.

    Returns:
        pd.DataFrame: DataFrame com valores faltantes imputados.
    """
    # Converte colunas categóricas para tipo category
    for col in COLUNAS_CATEGORICAS:
        if col in df.columns:
            df[col] = df[col].astype('category')

    # Imputa valores faltantes por média ou moda
    for col in df.columns:
        if df[col].isna().sum() > 0:
            if pd.api.types.is_numeric_dtype(df[col]):
                media = round(df[col].mean(), 1)
                df[col] = df[col].fillna(media)
                logging.info(f"Imputando média {media:.1f} na coluna numérica '{col}'")
            else:
                moda = df[col].mode()
                if not moda.empty:
                    moda_valor = moda[0]
                    # Adiciona a moda como uma nova categoria caso não exista
                    if moda_valor not in df[col].cat.categories:
                        df[col] = df[col].cat.add_categories(moda_valor)
                    df[col] = df[col].fillna(moda_valor)
                    logging.info(f"Imputando moda '{moda_valor}' na coluna categórica '{col}'")
                elif moda_anteriores is not None and col in moda_anteriores:
                    moda_valor = moda_anteriores[col]
                    # Adiciona a moda como uma nova categoria caso não exista
                    if moda_valor not in df[col].cat.categories:
                        df[col] = df[col].cat.add_categories(moda_valor)
                    df[col] = df[col].fillna(moda_valor)
                    logging.info(f"Imputando moda '{moda_valor}' do ano anterior na coluna '{col}'")
                else:
                    logging.warning(f"Não foi possível imputar moda na coluna '{col}', valor nulo permanece.")

    return df



def pipeline_brfss(ano, moda_anteriores=None):
    """
    Executa o pipeline completo para o ano: download, extração,
    limpeza, imputação e salvamento dos dados.

    Args:
        ano (int): Ano do dataset a processar.
        moda_anteriores (dict): Moda das colunas do ano anterior.
    """
    url = f"https://www.cdc.gov/brfss/annual_data/{ano}/files/LLCP{ano}XPT.zip"
    pasta_raw = os.path.join(BASE_DIR, "data", "raw")
    pasta_processed = os.path.join(BASE_DIR, "data", "processed")
    pasta_cleaned = os.path.join(BASE_DIR, "data", "cleaned")

    # Garante que as pastas existam
    for pasta in [pasta_raw, pasta_processed, pasta_cleaned]:
        if not os.path.exists(pasta):
            os.makedirs(pasta)

    caminho_zip = os.path.join(pasta_raw, f"LLCP{ano}.zip")
    pasta_extracao = os.path.join(pasta_raw, f"LLCP{ano}")
    caminho_xpt = os.path.join(pasta_extracao, f"LLCP{ano}.XPT")
    caminho_csv = os.path.join(pasta_processed, f"brfss_{ano}.csv")
    caminho_cleaned = os.path.join(pasta_cleaned, f"brfss_cleaned_{ano}.csv")

    # Se dados limpos já existem, pula processamento
    if os.path.exists(caminho_cleaned):
        logging.info(f"Dados limpos para o ano {ano} já existem, pulando.")
        return moda_anteriores

    # Se CSV bruto não existe, baixa e extrai
    if not os.path.exists(caminho_csv):
        sucesso = baixar_arquivo(url, caminho_zip)
        if not sucesso:
            logging.warning(f"Pulando o ano {ano} porque o arquivo não está disponível.")
            return moda_anteriores
        extrair_zip(caminho_zip, pasta_extracao)
        logging.info(f"Lendo arquivo XPT para o ano {ano} ...")
        df, meta = ler_xpt_com_fallback(caminho_xpt)
        logging.info(f"Salvando CSV bruto em {caminho_csv} ...")
        df.to_csv(caminho_csv, index=False)
    else:
        logging.info(f"CSV bruto para o ano {ano} já existe, carregando...") 
        df = pd.read_csv(caminho_csv)

    # Limpa e imputa dados
    logging.info(f"Limpando dados para o ano {ano} ...")
    df_clean = limpar_dados(df, ano)
    logging.info(f"Imputando valores faltantes para o ano {ano} ...")
    df_imputado = imputar_dados(df_clean, moda_anteriores)

    # Salva dados limpos e imputados
    df_imputado.to_csv(caminho_cleaned, index=False)
    logging.info(f"Dados limpos e imputados salvos em {caminho_cleaned}.")

    # Calcula e retorna as modas para o próximo ano
    moda_anteriores = {col: df_imputado[col].mode()[0] for col in COLUNAS_CATEGORICAS if col in df_imputado.columns}
    
    return moda_anteriores

if __name__ == "__main__":
    anos = anos_disponiveis()
    moda_anteriores = None
    for ano in anos:
        moda_anteriores = pipeline_brfss(ano, moda_anteriores)
