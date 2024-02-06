## autor: Otavio Augusto 
## date: 2023-07-03

# Importando bibliotecas
import pandas as pd                                     # pandas para manipulação de dados
import pysus                                            # pysus para captar dados do DATASUS
import numpy as np                                      # numpy para manipulação de números
import os                                               # os para manipulação de arquivos
from pysus.online_data import cache_contents            # pysus para captar dados do DATASUS
from pysus.online_data import parquets_to_dataframe     # pysus para captar dados do DATASUS - parquets_to_dataframe
from pysus.online_data.SIA import download              # pysus para captar dados do DATASUS - download
import concurrent.futures                               # concurrent.futures para paralelizar o código

# Criando pasta para armazenar os arquivos
pasta = 'base_'
os.makedirs(pasta, exist_ok=True)
arquivos_csv = os.listdir(pasta)
dataframes = []

# Criando arquivo para armazenar o histórico de execução
historico = open("historico_SIA.txt", "w")

# Comando para exibir todas colunas do arquivo
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

# Comando para exibir todas linhas do arquivo
ano = int(input("Digite o ano com 4 dígitos: "))
meses = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
ufs = ["AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"]
colunas = ["PA_CODUNI","PA_CMP","PA_PROC_ID","PA_CATEND","PA_IDADE","PA_SEXO","PA_MUNPCN","PA_QTDAPR","PA_VALAPR","PA_VL_CF","PA_VL_CL","PA_VL_INC"]
grupo_procedimento = input("Digite o grupo de procedimento com 2 dígitos: ")
tipo = ['PA'] #('Produção Ambulatorial', 7, 1994),


# Função para captar os dados do SIH
def captar_dados_sia(uf, ano, mes, grupo_procedimento, colunas, tipo):
    try:
        df_geral_sia = parquets_to_dataframe(download(uf, ano, mes, group=tipo[0]))
        df_geral_sia = df_geral_sia.filter(items=colunas)
        df_geral_sia = df_geral_sia[df_geral_sia['PA_PROC_ID'].apply(lambda x: any(x.startswith(gp) for gp in grupo_procedimento))]
        print(f"{uf} {str(mes).zfill(2)}/{str(ano)} - Arquivo capturado com sucesso!.................. Tempo de execução: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{uf} {str(mes).zfill(2)}/{str(ano)} - Arquivo capturado com sucesso!.................. Tempo de execução: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}", file=historico)

        # Tratamento dos dados
        df_geral_sia = df_geral_sia.astype({"PA_VALAPR": float, "PA_VL_CF": float, "PA_VL_CL": float, "PA_VL_INC": float, "PA_IDADE": int, "PA_QTDAPR":int}, errors='ignore')

        print(f"{uf} {str(mes).zfill(2)}/{str(ano)} - Arquivo tratado com sucesso!.................... Tempo de execução: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{uf} {str(mes).zfill(2)}/{str(ano)} - Arquivo tratado com sucesso!.................... Tempo de execução: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}", file=historico)

        
        # Alterando o tipo de dado das colunas
        sexo = {1: "MASCULINO", 3: "FEMININO"}
        df_geral_sia['PA_SEXO'] = df_geral_sia['PA_SEXO'].map(sexo)


        # Balanciando os dados de CNES para 7 dígitos
        df_geral_sia['PA_CODUNI'] = df_geral_sia['PA_CODUNI'].apply(lambda x: str(x).zfill(7))

        return df_geral_sia

    except Exception as e:
        print(f"{uf} {str(mes).zfill(2)}/{str(ano)} - Arquivo não encontrado!......................... Tempo de execução: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{uf} {str(mes).zfill(2)}/{str(ano)} - Arquivo não encontrado!......................... Tempo de execução: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}", file=historico)
        print(f"Erro:{e}")

        return None


def processar_uf(uf):
    uf_dataframes = []  # Lista para armazenar os dataframes de cada UF
    for mes in meses:
        df_sia = captar_dados_sia(uf, ano, mes, grupo_procedimento, colunas, tipo)
        if df_sia is not None:
            uf_dataframes.append(df_sia)

    # Concatenando os dataframes da UF
    if uf_dataframes:
        df_uf = pd.concat(uf_dataframes)

        # Salvando o arquivo por UF
        arquivo_saida = os.path.join(pasta, f'{ano}_{uf}-base_sih.csv')
        df_uf.to_csv(arquivo_saida, index=False, sep=';', encoding='utf-8-sig', mode='w')

        print(f"Arquivo '{arquivo_saida}' salvo com sucesso!...... Tempo de execução: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Arquivo '{arquivo_saida}' salvo com sucesso!...... Tempo de execução: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}", file=historico)


# Utilizando ThreadPoolExecutor
with concurrent.futures.ThreadPoolExecutor() as executor:
    executor.map(processar_uf, ufs)

