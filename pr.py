import pandas as pd
import requests
from bs4 import BeautifulSoup
import os
from datetime import date

def getB3(base_url='http://bvmf.bmfbovespa.com.br/', exportar=False):
    '''
    Função que raspa os dados das empresas listadas no site da B3
    Retorna um DataFrame com as informações das empresas (Nome, Ticker, Setor, Segmento e Subsegmento)
    Se exportar=True a função exporta os dados em csv no diretório atual
    '''
    
    base_lista_empresas = f'{base_url}cias-listadas/empresas-listadas/'
    letras = [chr(i) for i in range(65, 91)]
    links_empresas = set()
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.61 Safari/537.36'}

    for letra in letras:
        
        listagem = f'{base_lista_empresas}BuscaEmpresaListada.aspx?Letra={letra}&idioma=pt-br'
        page = requests.get(listagem, headers=headers, timeout=10)
        soup = BeautifulSoup(page.content, 'html.parser')
        links = soup.find_all('a')
        urls = [link.get('href') for link in links]
        links_empresas.update(f'{base_lista_empresas}{url}' for url in urls[1:])
    
    def empresas_info(endereco):
        page = requests.get(endereco, headers=headers, timeout=10)
        conteudo = BeautifulSoup(page.content, 'html.parser')

        iframe = conteudo.find_all('iframe')
        link = iframe[1].get('src')

        url = f'{base_url}{link.replace("../../", "")}'

        ticker = [i.text for i in BeautifulSoup(requests.get(url).content, 'html.parser').select('a:contains("Mais Códigos") ~ a')]
        nome_pregao = [i.text for i in BeautifulSoup(requests.get(url).content, 'html.parser').select('td:contains("Nome de Pregão") ~ td') for _ in ticker]
        nome_empresa = [i.text for i in conteudo.find_all('h2') for _ in ticker]
        setor = [i.text for i in BeautifulSoup(requests.get(url).content, 'html.parser').select('td:contains("Classificação Setorial:") ~ td') for _ in ticker]

        dataframe = pd.DataFrame({
            'Nome Empresa': nome_empresa,
            'Nome Pregao': nome_pregao,
            'Ticker': ticker,
            'Setor': setor
        })

        return dataframe

    dados = pd.concat([empresas_info(i) for i in links_empresas], ignore_index=True)
    b3 = dados.reset_index(drop=True)
    b3[['Setor', 'Subsetor', 'Segmento']] = b3.Setor.str.split(' / ', expand=True)

    if exportar:
        arquivo = os.path.join(os.getcwd(), f'B3Empresas_{date.today()}.csv')
        b3.to_csv(arquivo, sep=',', index=False, header=True, encoding='latin1')
        print(f'B3Empresas.csv: dados raspados do dia {date.today()} exportados no diretório {os.getcwd()}')

    return b3

def getFundamentus(b3, exportar=False):
    '''
    Raspa os dados do site Fundamentus
    Recebe o DataFrame da função getB3 e retorna o Valor de Mercado, Volume Médio e o Número de Ações das empresas
    Se exportar=True a função exporta os dados em csv no diretório atual
    '''

    urls = []
    url_base = 'http://fundamentus.com.br/detalhes.php?papel='
    dados_fundamentus = pd.DataFrame(columns=["Ticker", "Cap", "Acoes", "VolMed2m"])
    
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.61 Safari/537.36'}
    
    for ticker in b3["Ticker"]:
        url_ticker = f'{url_base}{ticker}'
        response = requests.get(url_ticker, headers=headers, timeout=10)
        conteudo = BeautifulSoup(response.content, 'html.parser')

        if conteudo.find_all("h1"):
            continue

        table = conteudo.find_all("table")
        cap_column = table[1].find_all("tr")[0].find_all("td")[0].text
        cap_value = table[1].find_all("tr")[0].find_all("td")[1].text
        acoes_column = table[1].find_all("tr")[1].find_all("td")[2].text
        acoes_value = table[1].find_all("tr")[1].find_all("td")[3].text
        volmed2m_column = table[0].find_all("tr")[4].find_all("td")[2].text
        volmed2m_value = table[0].find_all("tr")[4].find_all("td")[3].text

        new_row = {
            "Ticker": ticker,
            cap_column: cap_value,
            acoes_column: acoes_value,
            volmed2m_column: volmed2m_value
        }

        dados_fundamentus = dados_fundamentus.append(new_row, ignore_index=True)

    cols = ['Cap', 'Acoes', 'VolMed2m']
    dados_fundamentus[cols] = dados_fundamentus[cols].replace('.', '', regex=True)
    dados_fundamentus[cols] = dados_fundamentus[cols].apply(pd.to_numeric, downcast="integer")

    if exportar:
        arquivo = os.path.join(os.getcwd(), f'FundamentusEmpresas_{date.today()}.csv')
        dados_fundamentus.to_csv(arquivo, sep=',', index=False, header=True, encoding='latin1')
        print(f'FundamentusEmpresas.csv: dados raspados do dia {date.today()} exportados no diretório {os.getcwd()}')

    return dados_fundamentus
    
def mergeB3Fund(b3, fundamentus, exportar=False):
    '''
    Une os dados da B3 aos dados da Fundamentus
    Se exportar=True a função exporta os dados em csv no diretório atual
    '''

    data = pd.merge(b3, fundamentus, on='Ticker', how='outer')
    data.columns = ["Empresa", "Pregao", "Ticker", "Setor", "Subsetor", "Segmento", "Cap", "Acoes", "VolMed2m"]
    data["Cap"] = data["Cap"].fillna(0)
    data["Acoes"] = data["Acoes"].fillna(0)
    data["VolMed2m"] = data["VolMed2m"].fillna(0)

    if exportar:
        arquivo = os.path.join(os.getcwd(), f'dadosEmpresasB3Fund_{date.today()}.csv')
        data.to_csv(arquivo, sep=",", index=False, header=True, encoding='latin1')
        print(f'dadosEmpresasB3Fund.csv: dados raspados do dia {date.today()} exportados no diretório {os.getcwd()}')

    return data