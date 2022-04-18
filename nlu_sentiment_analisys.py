# saida do pip freeze durante os testes deste programa:
# 
# certifi==2021.10.8
# charset-normalizer==2.0.12
# ibm-cloud-sdk-core==3.15.1
# ibm-watson==6.0.0
# idna==3.3
# numpy==1.22.3
# pandas==1.4.2
# PyJWT==2.3.0
# python-dateutil==2.8.2
# pytz==2022.1
# requests==2.27.1
# six==1.16.0
# urllib3==1.26.9
# websocket-client==1.1.0
# 
import pandas as pd
import numpy as np
import json
import os
from ibm_watson import NaturalLanguageUnderstandingV1
from ibm_cloud_sdk_core.authenticators import IAMAuthenticator
from ibm_watson.natural_language_understanding_v1 import Features,CategoriesOptions,EmotionOptions,KeywordsOptions

# há formas mais seguras de proteger chaves de API e o uso de variáveis de ambiente 
# não é o ideal para aplicações baseadas em nuvem, mas para esta demostração é o suficiente
IAM_KEY = os.environ.get('IAM_KEY')
SERVICE_URL = os.environ.get('SERVICE_URL')

# Inicialisamos o objeto autenticador da API, o objeto de NLU e configuramos a URL do serviço
authenticator = IAMAuthenticator(IAM_KEY)
natural_language_understanding = NaturalLanguageUnderstandingV1(version='2020-08-01',authenticator=authenticator)
natural_language_understanding.set_service_url(SERVICE_URL)

# Nesta variável incluimos o nome do arquivo que vai ser tratado
file = 'tweets_1.csv'

def main():
  # criando o dataframe utilizando a coluna 11 do arquivo csv
  df = pd.read_csv(file, usecols=[10])
  print("O arquivo {}, contem {} tweets".format(file, len(df)))
  
  # inicializamos uma lista para as respostas da API
  responses = []
  # variavel para resposta sem keyword
  empty = 0
  # iterando o dataframe
  print("Iniciando a query na API...")
  for index, row in df.iterrows(): 
    resp = natural_language_understanding.analyze(
    text=row['tweet'],
    features=Features(keywords=KeywordsOptions(sentiment=True,limit=1)),language='pt').get_result()
    try:
      keyw = resp['keywords'][0]
    except IndexError:
      keyw = {}
      empty += 1
    responses.append(keyw)
    # colocamos um ponto na tela a cada 10 linhas processadas 
    # para indicar que o processo está funcionando
    if not index % 10: print('.', end='', flush=True)
  print()
  print("{} tweets processads pela API".format(index+1))
  print("{} respostas não retornaram keyword".format(empty))
  
  # criando o dataframe normalizado
  df_norm = pd.json_normalize(responses)

  # backup dos dados, caso tenham de ser reprocessados, sem a necessidade de 
  # efetuar a query na API novamente
  df_norm.to_csv('df.csv')
  
  # se houver a coluna 'sentiment.mixed' entao drop
  if 'sentiment.mixed' in df_norm.columns:
    df_norm.drop(['sentiment.mixed'], axis=1, inplace=True)
  
  # concatenando o df de tweets e as respostas
  df = pd.concat([df, df_norm], axis=1)

  # efetuando o drop das linhas com NaN
  df[df.isna().any(axis=1)]
  
  total = len(df)
  pos = np.sum(df['sentiment.label'].str.contains("positive"))
  neg = np.sum(df['sentiment.label'].str.contains("negative"))
  neu = np.sum(df['sentiment.label'].str.contains("neutral"))
  
  print("De um total de {} tweets analisados temos:".format(total))
  print("- {} tweets positivos".format(pos))
  print("- {} tweets negativos".format(neg))
  print("- {} tweets neutros".format(neu))  
  
if __name__ == "__main__":
  main()
