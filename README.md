# Script de Extração de Dados do SIH

Este é um script em Python para extrair dados do Sistema de Informação Hospitalar (SIH) e Sistema de Informação Ambulatorial (SIA) 
realizando algumas transformações e agregações.


## Requisitos

- Python 3.6 ou superior
- Bibliotecas Python: pandas, numpy, pysus


## Instalação das Bibliotecas

```shell
pip install pandas numpy pysus
```

## Uso

1. Faça o download ou clone este repositório.
2. Certifique-se de ter os requisitos mencionados acima instalados.
3. Execute o script extração_dados_tabwin.py usando o Python.
4. Siga as instruções fornecidas pelo script:
    * Digite o ano desejado (com 4 dígitos).
    * Digite o grupo de procedimento desejado (com 2 dígitos).
5. Aguarde até que o script finalize a extração, tratamento e agregação dos dados.
6. O arquivo resultante base_sih.csv será salvo na pasta base_/ com os dados consolidados.


## Personalização

* Você pode modificar as variáveis pasta e arquivos_csv para especificar o diretório onde os arquivos CSV serão armazenados.
* Você pode modificar a lista ufs para selecionar apenas as Unidades Federativas desejadas.
* Você pode modificar a lista colunas para selecionar apenas as colunas desejadas.
* Você pode adicionar ou modificar as agregações no dicionário agregacoes para ajustar as estatísticas calculadas.
* Você pode adicionar ou modificar colunas com base no documento disponivel em <https://datasus.saude.gov.br/transferencia-de-arquivos/#>


## Contribuição
Contribuições são bem-vindas! Se você encontrar algum problema, tiver sugestões de melhorias ou quiser adicionar novos recursos, fique à vontade para abrir uma issue ou enviar um pull request.


## Licença
Este script é disponibilizado sob a licença MIT. Consulte o arquivo LICENSE para obter mais informações.

Certifique-se de substituir as seções relevantes com as informações específicas do seu script. Espero que isso ajude a criar o arquivo README.md para o seu projeto!
