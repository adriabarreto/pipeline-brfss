import os
import pandas as pd
import difflib

# Lista de colunas desejadas, incluindo DIABETE3 e DIABETE4 para inspeção
COLUNAS_KAGGLE = [
    'DIABETE3', 'DIABETE4', 'SEX', 'MARITAL', 'EDUCA', 'EMPLOY1', 'INCOME2',
    'GENHLTH', 'PHYSHLTH', 'MENTHLTH', 'POORHLTH', 'HLTHPLN1',
    'CHECKUP1', 'BPHIGH4', 'TOLDHI2', 'CVDSTRK3', 'CHCSCNCR', 'CHCOCNCR',
    'CHCCOPD1', 'HAVARTH3', 'ADDEPEV2', 'CHCKIDNY', 'DIFFWALK'
]

def mapear_colunas_parecidas(colunas_reais, colunas_desejadas, limite=0.8):
    """
    Mapeia colunas desejadas para colunas reais do dataset com base em similaridade de nome.
    """
    mapeamento = {}
    for alvo in colunas_desejadas:
        mais_proxima = difflib.get_close_matches(alvo, colunas_reais, n=1, cutoff=limite)
        if mais_proxima:
            mapeamento[alvo] = mais_proxima[0]
        else:
            mapeamento[alvo] = None
    return mapeamento

def inspecionar_anos(anos, pasta_processed, arquivo_saida):
    """
    Gera um CSV com o mapeamento das colunas desejadas para as colunas reais em cada ano.
    """
    resultados = []
    for ano in anos:
        caminho_csv = os.path.join(pasta_processed, f"brfss_{ano}.csv")
        if not os.path.exists(caminho_csv):
            print(f"Aviso: arquivo {caminho_csv} não encontrado, pulando ano {ano}.")
            continue

        df = pd.read_csv(caminho_csv)
        colunas_reais = df.columns.tolist()
        mapeamento = mapear_colunas_parecidas(colunas_reais, COLUNAS_KAGGLE)

        for chave, valor in mapeamento.items():
            resultados.append({
                'Ano': ano,
                'Coluna_Kaggle': chave,
                'Coluna_Sugerida': valor
            })

    df_resultado = pd.DataFrame(resultados)

    # 🔥 Remove colunas da variável alvo (serão tratadas manualmente)
    df_resultado = df_resultado[~df_resultado['Coluna_Kaggle'].isin(['DIABETE3', 'DIABETE4'])]

    df_resultado.to_csv(arquivo_saida, index=False)
    print(f"Mapeamento salvo em {arquivo_saida}")

if __name__ == "__main__":
    pasta_processed = "./data/processed"
    anos = [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024]
    arquivo_saida = "./data/mapeamento_colunas_sugerido.csv"

    inspecionar_anos(anos, pasta_processed, arquivo_saida)
