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
colunas = ["ANO_CMPT", "N_AIH", "MUNIC_RES", "SEXO", "UTI_MES_TO", "QT_DIARIAS", "PROC_REA", "VAL_TOT", "VAL_UTI", "MUNIC_MOV", "IDADE", "DIAS_PERM", "MORTE", "CAR_INT", "GESTOR_COD", "CNES", "COMPLEX", "FINANC", "REGCT", "VAL_SH_FED", "VAL_SP_FED", "VAL_SH_GES", "VAL_SP_GES"]
grupo_procedimento = input("Digite o grupo de procedimento com 2 dígitos: ")


# Função para captar os dados do SIH
def captar_dados_sih(uf, ano, mes, grupo_procedimento, colunas):
    try:
        df_geral_sih = parquets_to_dataframe(download(uf, ano, mes))

        # if df_geral_sih is not None:
          #  df_geral_sih = df_geral_sih.filter(items=colunas)
          #  df_geral_sih = df_geral_sih[df_geral_sih['PROC_REA'].apply(lambda x: any(x.startswith(gp) for gp in grupo_procedimento))]
        df_geral_sih = df_geral_sih.filter(items=colunas)
        df_geral_sih = df_geral_sih[df_geral_sih['PROC_REA'].apply(lambda x: any(x.startswith(gp) for gp in grupo_procedimento))]
        print(f"{uf} {str(mes).zfill(2)}/{str(ano)} - Arquivo capturado com sucesso!.................. Tempo de execução: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{uf} {str(mes).zfill(2)}/{str(ano)} - Arquivo capturado com sucesso!.................. Tempo de execução: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}", file=historico)

        # Tratamento dos dados
        df_geral_sih = df_geral_sih.astype({"VAL_SH_FED": float, "VAL_SP_FED": float, "VAL_SH_GES": float, "VAL_SP_GES": float, "VAL_TOT": float, "VAL_UTI": float, "DIAS_PERM": int,
        "QT_DIARIAS": int, "IDADE": int, "DIAS_PERM": int, "MORTE": int}, errors='ignore')

        print(f"{uf} {str(mes).zfill(2)}/{str(ano)} - Arquivo tratado com sucesso!.................... Tempo de execução: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{uf} {str(mes).zfill(2)}/{str(ano)} - Arquivo tratado com sucesso!.................... Tempo de execução: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}", file=historico)

        # Criação de novas colunas
        df_geral_sih['VAL_SH_FED'] = pd.to_numeric(df_geral_sih['VAL_SH_FED'], errors='coerce')
        df_geral_sih['VAL_SP_FED'] = pd.to_numeric(df_geral_sih['VAL_SP_FED'], errors='coerce')

        df_geral_sih['VAL_SH_GES'] = pd.to_numeric(df_geral_sih['VAL_SH_GES'], errors='coerce')
        df_geral_sih['VAL_SP_GES'] = pd.to_numeric(df_geral_sih['VAL_SP_GES'], errors='coerce')

        # Criação de novas colunas
        df_geral_sih['VAL_FED'] = df_geral_sih['VAL_SH_FED'].astype(float) + df_geral_sih['VAL_SP_FED'].astype(float)
        df_geral_sih['VAL_GESTOR'] = df_geral_sih['VAL_SH_GES'].astype(float) + df_geral_sih['VAL_SP_GES'].astype(float)

        df_geral_sih = df_geral_sih.drop(['VAL_SH_GES','VAL_SP_GES','VAL_SH_FED','VAL_SP_FED'], axis=1)

        print(f"{uf} {str(mes).zfill(2)}/{str(ano)} - Novas colunas criadas com sucesso!.............. Tempo de execução: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{uf} {str(mes).zfill(2)}/{str(ano)} - Novas colunas criadas com sucesso!.............. Tempo de execução: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}", file=historico)

        # Agrupamento dos dados
        colunas_somar = ['QT_DIARIAS', 'DIAS_PERM', 'VAL_TOT', 'VAL_UTI', 'DIAS_PERM', 'VAL_FED', 'VAL_GESTOR']
        agregacoes = {coluna: sum for coluna in colunas_somar}
        agregacoes['N_AIH'] = 'size'

        df_agrupado = df_geral_sih.groupby(['ANO_CMPT', 'MUNIC_RES', 'SEXO', 'UTI_MES_TO', 'PROC_REA', 'IDADE', 'MUNIC_MOV', 'MORTE', 'CAR_INT', 'GESTOR_COD', 'CNES', 'COMPLEX', 'FINANC', 'REGCT'], as_index=False).agg(agregacoes)
        df_agrupado = df_agrupado.rename(columns={'N_AIH': 'QUANTIDADE'})

        # Alterando o tipo de dado das colunas
        sexo = {1: "MASCULINO", 3: "FEMININO"}
        df_agrupado['SEXO'] = df_agrupado['SEXO'].map(sexo)

        # Excluindo os registros em branco no campo sexo
        df_agrupado = df_agrupado[df_agrupado['SEXO'].notna()]

        # Balanciando os dados de CNES para 7 dígitos
        df_agrupado['CNES'] = df_agrupado['CNES'].apply(lambda x: str(x).zfill(7))

        # Alterando o tipo de dado da coluna financiamento
        financiamento = {'01': 'Atenção Básica (PAB)', '02': 'Assistência Farmacêutica', '04': 'Fundo de Ações Estratégicas e Compensações FAEC',
                        '05': 'Incentivo - MAC', '06': 'Média e Alta Complexidade (MAC)', '07': 'Vigilância em Saúde', '08': 'Gestão em Saúde'}
        df_agrupado['FINANC'] = df_agrupado['FINANC'].map(financiamento)
        print(f"{uf} {str(mes).zfill(2)}/{str(ano)} - Tipo de dado das colunas alterado com sucesso!.. Tempo de execução: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{uf} {str(mes).zfill(2)}/{str(ano)} - Tipo de dado das colunas alterado com sucesso!.. Tempo de execução: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}", file=historico)

        return df_agrupado

    except Exception as e:
        print(f"{uf} {str(mes).zfill(2)}/{str(ano)} - Arquivo não encontrado!......................... Tempo de execução: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{uf} {str(mes).zfill(2)}/{str(ano)} - Arquivo não encontrado!......................... Tempo de execução: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}", file=historico)
        print(f"Erro:{e}")

        return None


def processar_uf(uf):
    uf_dataframes = []  # Lista para armazenar os dataframes de cada UF
    for mes in meses:
        df_sih = captar_dados_sih(uf, ano, mes, grupo_procedimento, colunas)
        if df_sih is not None:
            uf_dataframes.append(df_sih)

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

