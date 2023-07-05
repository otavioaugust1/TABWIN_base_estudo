## autor: Otavio Augusto 
## date: 2023-07-03

# Importando bibliotecas
import pandas as pd                                         # pandas é uma biblioteca para manipulação de dados
import pysus                                                # pysus é uma biblioteca para conexão com o servidor FTP (PRODUÇÃO - SISTEMA DE INFORMAÇÃO HOSPITALAR)
import numpy as np                                          # numpy é uma biblioteca para manipulação de dados
import os                                                   # os é uma biblioteca para manipulação de arquivos e pastas 
from pysus.online_data import cache_contents                # cache_contents é uma biblioteca para conexão com o servidor FTP (PRODUÇÃO - SISTEMA DE INFORMAÇÃO HOSPITALAR)
from pysus.online_data import parquets_to_dataframe         # to_df é uma biblioteca para conexão com o servidor FTP (PRODUÇÃO - SISTEMA DE INFORMAÇÃO HOSPITALAR)
from pysus.online_data.SIH import download                  # download é uma biblioteca para conexão com o servidor FTP (PRODUÇÃO - SISTEMA DE INFORMAÇÃO HOSPITALAR)


# Criando pasta para armazenar os arquivos
pasta = 'base_'  # Caminho para a pasta que contém os arquivos CSV

# Criar a pasta de saída, se ainda não existir
os.makedirs(pasta, exist_ok=True)
arquivos_csv = os.listdir(pasta)  # Lista de arquivos CSV na pasta
dataframes = []

# Criando arquivo de histórico
historico = open("historico.txt", "w")

#Comando para exibir todas colunas do arquivo
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

#Comando para exibir todas linhas do arquivo
ano = int(input("Digite o ano com 4 digito: "))
meses = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
ufs = ["AC","AL","AM","AP","BA","CE","DF","ES","GO","MA","MG","MS","MT","PA","PB","PE","PI","PR","RJ","RN","RO","RR","RS","SC","SE","SP","TO"]
colunas = ["ANO_CMPT", "N_AIH", "MUNIC_RES", "SEXO", "UTI_MES_TO", "QT_DIARIAS", "PROC_REA", "VAL_TOT", "VAL_UTI", "MINIC_MOV", "IDADE", "DIAS_PERM", "MORTE", "CAR_INT", "GESTOR_COD", "CNES", "COMPLEX", "FINANC", "REGCT", "VAL_SH_FED", "VAL_SP_FED", "VAL_SH_GES", "VAL_SP_GES"]
grupo_procedimento = input("Digite o grupo de procedimento com 2 digitos: ")


# Função para captar os dados do SIH
def captar_dados_sih(uf, ano, mes, grupo_procedimento, colunas):
    try:
        df_geral_sih = parquets_to_dataframe(download(uf, ano, mes))
        df_geral_sih = df_geral_sih.filter(items=colunas)
        df_geral_sih = df_geral_sih[df_geral_sih['PROC_REA'].apply(lambda x: any(x.startswith(gp) for gp in grupo_procedimento))]
        print(f"Arquivo {uf} {str(mes)}/{str(ano)} capturado com sucesso!... Tempo de execução: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Arquivo {uf} {str(mes)}/{str(ano)} capturado com sucesso!... Tempo de execução: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}", file=historico)
        return df_geral_sih
    except:
        print(f"Arquivo {uf} {str(mes)}/{str(ano)} não encontrado!... Tempo de execução: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Arquivo {uf} {str(mes)}/{str(ano)} não encontrado!... Tempo de execução: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}", file=historico)
        return None

df = pd.DataFrame()

# Loop para captar os dados do SIH
for uf in ufs:        
     for mes in meses:
        for mes in meses:
            df_mes = captar_dados_sih(uf, ano, mes, grupo_procedimento, colunas)
            if df_mes is not None and not df_mes.empty:
                dataframes.append(df_mes)

# Concatenando os dataframes
df = pd.concat(dataframes)

# Tratamento dos dados
df = df.astype({"VAL_SH_FED": float, "VAL_SP_FED": float, "VAL_SH_GES": float, "VAL_SP_GES": float, "VAL_TOT": float, "VAL_UTI": float, 
                "QT_DIARIAS": 'Int64', "IDADE": 'Int64', "DIAS_PERM": 'Int64', "MORTE": 'Int64'}, errors='ignore')

print(f"Arquivo tratado com sucesso!... Tempo de execução: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Arquivo tratado com sucesso!... Tempo de execução: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}", file=historico)


# Criação de novas colunas
df['VAL_FED'] = df['VAL_SH_FED'] + df['VAL_SP_FED']
df = df.drop(['VAL_SH_FED', 'VAL_SP_FED'], axis=1)

df['VAL_GESTOR'] = df['VAL_SH_GES'] + df['VAL_SP_GES']
df = df.drop(['VAL_SH_GES', 'VAL_SP_GES'], axis=1)
print(f"Novas colunas criadas com sucesso!... Tempo de execução: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Novas colunas criadas com sucesso!... Tempo de execução: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}", file=historico)

# Agrupamento dos dados
colunas_somar = ['QT_DIARIAS', 'DIAS_PERM', 'VAL_TOT', 'VAL_UTI', 'DIAS_PERM', 'VAL_FED', 'VAL_GESTOR']
agregacoes = {coluna: sum for coluna in colunas_somar}
agregacoes['N_AIH'] = 'size'

df_agrupado = df.groupby(['ANO_CMPT', 'MUNIC_RES', 'SEXO', 'UTI_MES_TO', 'PROC_REA', 'IDADE', "MINIC_MOV", 'MORTE', 'CAR_INT', 'GESTOR_COD', 'CNES', 'COMPLEX', 'FINANC', 'REGCT'], as_index=False).agg(agregacoes)
df_agrupado = df_agrupado.rename(columns={'N_AIH': 'QUANTIDADE'})


# alterando o tipo de dado das colunas
sexo = {1: "MASCULINO", 3: "FEMININO"}
df_agrupado['SEXO'] = df_agrupado['SEXO'].map(sexo)

financiamento = {'01': 'Atenção Básica (PAB)', '02': 'Assistência Farmacêutica', '04': 'Fundo de Ações Estratégicas e Compensações FAEC', '05': 'Incentivo - MAC', '06': 'Média e Alta Complexidade (MAC)', '07': 'Vigilância em Saúde', '08': 'Gestão em Saúde'}
df_agrupado['FINANC'] = df_agrupado['FINANC'].map(financiamento)
print(f"Tipo de dado das colunas alterado com sucesso!... Tempo de execução: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Tipo de dado das colunas alterado com sucesso!... Tempo de execução: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}", file=historico)


# salvando o arquivo
arquivo_saida = f'{pasta}{ano}-base_sih.csv'
df_agrupado.to_csv(arquivo_saida, index=False, sep=';', encoding='utf-8-sig', mode='w')
print(f"Arquivo salvo com sucesso!... Tempo de execução: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Arquivo salvo com sucesso!... Tempo de execução: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}", file=historico)

# Fechando o arquivo de histórico
historico.close()
