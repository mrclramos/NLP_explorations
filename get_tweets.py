#
# Aas seguintes variáveis devem ser ajustadas
# linha 113 keywords -> para os argumentos de busca 
# linha 116 start_list -> datas de inicio dos periodos de busca
# linha 124 end_list -> datas de final dos periodos de busca
# linha 133 max_results -> numero máximo de resultados por iteração
# linha 152 max_count -> limite máximo de resultados por período
#
#
import requests
import os
import json
import csv
import dateutil.parser
import time

def auth():
    return os.environ.get('BEARER_TOKEN')


def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers


def create_url(keyword, start_date, end_date, max_results=10):
    search_url = "https://api.twitter.com/2/tweets/search/recent"  # Change to the endpoint you want to collect data from

    # Estes parametros devem ser alterados a depender do endpoint utilizado
    query_params = {'query': keyword,
                    'start_time': start_date,
                    'end_time': end_date,
                    'max_results': max_results,
                    'expansions': 'author_id,in_reply_to_user_id,geo.place_id',
                    'tweet.fields': 'id,text,author_id,in_reply_to_user_id,geo,conversation_id,created_at,lang,public_metrics,referenced_tweets,reply_settings,source',
                    'user.fields': 'id,name,username,created_at,description,public_metrics,verified',
                    'place.fields': 'full_name,id,country,country_code,geo,name,place_type',
                    'next_token': {}}
    return (search_url, query_params)


def connect_to_endpoint(url, headers, params, next_token = None):
    params['next_token'] = next_token
    response = requests.request("GET", url, headers = headers, params = params)
    if response.status_code != 200:
      print("Endpoint RC -> " + str(response.status_code))
      raise Exception(response.status_code, response.text)
    return response.json()


def append_to_csv(json_response, fileName):
    # Variável de contador
    counter = 0

    # Abre ou cria o arquico .csv
    csvFile = open(fileName, "a", newline="", encoding='utf-8')
    csvWriter = csv.writer(csvFile)

    # Loop sobre cada tweet
    for tweet in json_response['data']:

        # Vamos criar variáveis para cada tweet pois alguns valores podem não existir

        # 1. Author ID
        author_id = tweet['author_id']

        # 2. Time created
        created_at = dateutil.parser.parse(tweet['created_at'])

        # 3. Geolocation
        if ('geo' in tweet):
            geo = tweet['geo']['place_id']
        else:
            geo = " "

        # 4. Tweet ID
        tweet_id = tweet['id']

        # 5. Language
        lang = tweet['lang']

        # 6. Tweet metrics
        retweet_count = tweet['public_metrics']['retweet_count']
        reply_count = tweet['public_metrics']['reply_count']
        like_count = tweet['public_metrics']['like_count']
        quote_count = tweet['public_metrics']['quote_count']

        # 7. source
        source = tweet['source']

        # 8. Tweet text
        text = tweet['text']

        # Adicionando todos os dados em uma lista
        res = [author_id, created_at, geo, tweet_id, lang, like_count, quote_count, reply_count, retweet_count, source,
               text]

        # gravando a lista no arquivo .csv
        csvWriter.writerow(res)
        counter += 1

    # Ao finalizar, fechamos o arquivo
    csvFile.close()

    # Número de tweets para esta iteração
    # print("Número de tweets desta resposta: ", counter)

def main():
    bearer_token = auth()
    headers = create_headers(bearer_token)
    # em uma versão futura podemos alterar o script para receber os argumentos da linha de comando,
    # no momento a varável abaixo precisa ser alterada com o argumento da busca
    keyword = '("jessilane") lang:pt -is:retweet'
    # da mesma forma que o argumento de busca, no momento a lista abaixo precisa ser alterada para
    # indicar as datas de busca
    start_list = ['2022-04-03T04:00:00.000Z',
                  '2022-04-04T00:00:00.000Z',
                  '2022-04-05T00:00:00.000Z',
                  '2022-04-06T00:00:00.000Z',
                  '2022-04-07T00:00:00.000Z',
                  '2022-04-08T00:00:00.000Z',
                  '2022-04-09T00:00:00.000Z']

    end_list = ['2022-04-03T23:59:59.999Z',
                '2022-04-04T23:59:59.999Z',
                '2022-04-05T23:59:59.999Z',
                '2022-04-06T23:59:59.999Z',
                '2022-04-07T23:59:59.999Z',
                '2022-04-08T23:59:59.999Z',
                '2022-04-09T23:59:59.999Z']
    
    # número máximo de tweets por iteração com a API
    max_results = 100

    # Contador para número total de tweets do loop
    total_tweets = 0

    # Aqui informamos o nome do arquivo a ser criado
    csvFile = open("data.csv", "a", newline="", encoding='utf-8')
    csvWriter = csv.writer(csvFile)

    # Gravando o header do arquivo .csv
    csvWriter.writerow(['author id', 'created_at', 'geo', 'id', 'lang', 'like_count',
                        'quote_count', 'reply_count', 'retweet_count', 'source', 'tweet'])
    csvFile.close()

    response_count = 0

    for i in range(0, len(start_list)): # Para cada data informada na lista:
        # cada data da lista vai ser tratada como um período
        count = 0  # contador de tweets por período
        max_count = 10000  # número máximo de tweets por período
        flag = True
        next_token = None

        print("Período sendo processado:", start_list[i])

        # enquanto a flag for verdadeira
        while flag:
            # Se o número máximo de tweets do período foi atingido, vamos para o próximo período do loop
            if count >= max_count:
                break
				
            url = create_url(keyword, start_list[i], end_list[i], max_results)
            json_response = connect_to_endpoint(url[0], headers, url[1], next_token)

            # # além de salvar os dados em um arquivo .csv
            # # salvamos o json de cada resposta em um arquivo separado para debug/teste
            # json_file = "data_{:03d}.json".format(response_count)
            # with open(json_file, 'w', encoding="utf-8") as f:
            #     json.dump(json_response, f)
            # response_count += 1

            result_count = json_response['meta']['result_count']

            if result_count is not None and result_count > 0:
                 append_to_csv(json_response, "data.csv")
                 count += result_count
                 total_tweets += result_count
                 print("Número de tweets processados no período: ", count)
                 time.sleep(5)

            if 'next_token' in json_response['meta']:
                # Salva o next_token para a proxima chamada
                next_token = json_response['meta']['next_token']
            else:
                # como não há mais next_token, alteramos a flag para processar o próximo período
                flag = False
                next_token = None
            time.sleep(5)

    print("Número total de tweets: ", total_tweets)

if __name__ == "__main__":
    main()
