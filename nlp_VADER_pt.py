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
# adicionalmente precisamos da ferramenta LeIA:
# git clone https://github.com/rafjaa/LeIA.git
# 
import pandas as pd
import numpy as np
from LeIA.leia import SentimentIntensityAnalyzer

# instanciamos o objeto para Analise de Sentimento
sa = SentimentIntensityAnalyzer()

# Nesta variável incluimos o nome do arquivo que vai ser tratado
file = 'tweets_1.csv'


def main():
  # Do arquivo .csv utilizamos no DataFrame apenas a 11a. coluna:
  df = pd.read_csv(file, usecols=[10])
  total = len(df)
  print("O arquivo {}, contem {} tweets".format(file, total))

  # usando apply() processamos todo o dataframe com os dados:
  df['ratings'] = df['tweet'].apply(sa.polarity_scores)

  # a coluna ratings possui um dict com os dados, o processamento abaixo vai
  # separar os dados do dict em colunas
  df = pd.concat([df.drop(['ratings'], axis=1), df['ratings'].apply(pd.Series)], axis=1)

  # efetuando a contagem dos dados:

  pos = np.sum(df['compound'] >= 0.05)
  neg = np.sum(df['compound'] <= -0.05)
  neu = total - pos - neg

  print("De um total de {} tweets analisados temos:".format(total))
  print("- {} tweets positivos".format(pos))
  print("- {} tweets negativos".format(neg))
  print("- {} tweets neutros".format(neu)) 

  
if __name__ == "__main__":
  main()
