import requests
import pandas as pd
from unidecode import unidecode

# Função para obter dados de uma URL
def get_data_from_url(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return data.get('results', [])
    except requests.exceptions.RequestException as e:
        print(f"Erro na requisição para a URL {url}: {e}")
        return []

# Função principal para processar as URLs
def process_company_data(base_url, char_position, start_letter, increment):
    all_urls = []
    
    while True:
        modified_url = base_url[:char_position] + start_letter + 'sIn' + base_url[char_position + 4:]
        all_urls.append(modified_url)
        # Obter a próxima letra
        start_letter = chr(ord(start_letter) + increment)
        if (start_letter > 'Z' and increment > 0) or (start_letter > 'z' and increment < 0):
            break

    all_cnpjs = []

    for url in all_urls:
        print(f"Processando URL: {url}")
        results = get_data_from_url(url)
        if not results:
            print(f"Nenhum resultado encontrado para a URL: {url}.")
            continue  # Continue para processar a próxima URL
        for company in results:
            cnpj = company['cnpj']
            all_cnpjs.append(cnpj)

    return all_cnpjs

# URLs base e parâmetros
script_configs = [
    {
        'base_url': 'https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetInitialCompanies/eyJsYW5ndWFnZSI6InB0LWJyIiwicGFnZU51bWJlciI6MSwicGFnZVNpemUiOjEyMH0=',
        'char_position': 138,
        'start_letter': 'M',
        'increment': 1  # sequências de 'M' a 'Z'
    },
    {
        'base_url': 'https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetInitialCompanies/eyJsYW5ndWFnZSI6InB0LWJyIiwicGFnZU51bWJlciI6MTAsInBhZ2VTaXplIjoxMjB9',
        'char_position': 140,
        'start_letter': 'A',
        'increment': 4  # saltos de 4 letras
    },
    {
        'base_url': 'https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetInitialCompanies/eyJsYW5ndWFnZSI6InB0LWJyIiwicGFnZU51bWJlciI6MTcsInBhZ2VTaXplIjoxMjB9',
        'char_position': 140,
        'start_letter': 'c',
        'increment': 4  # saltos de 4 letras
    },
    {
        'base_url': 'https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetInitialCompanies/eyJsYW5ndWFnZSI6InB0LWJyIiwicGFnZU51bWJlciI6MjAsInBhZ2VTaXplIjoxMjB9',
        'char_position': 140,
        'start_letter': 'A',
        'increment': 4  # saltos de 4 letras
    },
]

# Armazenar todos os CNPJs coletados
all_cnpjs = []

# Processar cada configuração de script
for config in script_configs:
    cnpjs = process_company_data(
        config['base_url'], 
        config['char_position'], 
        config['start_letter'], 
        config['increment']
    )
    all_cnpjs.extend(cnpjs)

# Consultar a API da BrasilAPI para cada CNPJ coletado, limitando a 5 coletas válidas
empresas_b3 = []
success_count = 0

for cnpj in all_cnpjs:
    # Adiciona um '0' se o CNPJ tiver menos de 14 caracteres
    if len(cnpj) == 13:
        cnpj = '0' + cnpj

    if len(cnpj) == 12:
        cnpj = '00' + cnpj

    if len(cnpj) == 11:
        cnpj = '000' + cnpj        

    # Ignora o CNPJ se for apenas '0'
    if cnpj in ['0', '00']:
        continue

    # Tentativas de requisição
    for attempt in range(2):  # Duas tentativas
        try:
            response = requests.get(f"https://brasilapi.com.br/api/cnpj/v1/{cnpj}", timeout=10)  # Adiciona timeout de 10 segundos
            print(f"Status Code para o CNPJ {cnpj}: {response.status_code}")  # Imprime o código de status
            
            response.raise_for_status()  # Levanta um erro para status codes 4xx e 5xx
            if response.text:  # Verifica se a resposta não está vazia
                data = response.json()
                empresas_b3.append(data)
            else:
                print(f"Nenhum dado retornado para o CNPJ {cnpj}.")
            break  # Se a requisição for bem-sucedida, sai do loop

        except requests.exceptions.Timeout:
            if attempt == 0:  # Apenas tenta novamente se for a primeira tentativa
                print(f"Timeout ao consultar CNPJ {cnpj}. Tentando novamente.")
            else:
                print(f"Timeout ao consultar CNPJ {cnpj}. Ignorando essa requisição.")
                break  # Se for a segunda tentativa, vai para o próximo CNPJ

        except requests.exceptions.RequestException as e:
            print(f"Erro ao consultar CNPJ {cnpj}: {e}")
            break  # Em caso de erro, sair do loop

        except ValueError as ve:
            print(f"Erro ao decodificar a resposta JSON para o CNPJ {cnpj}: {ve}")
            print(f"Conteúdo da resposta: {response.text}")
            break  # Em caso de erro, sair do loop

# Criar um DataFrame com os dados coletados
df = pd.DataFrame(empresas_b3)

# Imprimir o DataFrame antes de finalizar o script
print("DataFrame coletado:")
print(df)

# Verifica se o DataFrame não está vazio
if not df.empty:
    # Transformar todas as letras em minúsculas e substituir espaços por underscores
    df.columns = [unidecode(col.lower().replace(' ', '_')) if isinstance(col, str) else col for col in df.columns]
    df = df.applymap(lambda x: unidecode(x.lower().replace(' ', '_')) if isinstance(x, str) else x)

    # Extrair ano e mês da coluna "data_inicio_atividade", se existir
    if 'data_inicio_atividade' in df.columns:
        df['ano'] = pd.to_datetime(df['data_inicio_atividade'], errors='coerce').dt.year
        df['mes'] = pd.to_datetime(df['data_inicio_atividade'], errors='coerce').dt.month

    # Criar um novo DataFrame para a coluna 'qsa'
    qsa_data = []
    for index, row in df.iterrows():
        cnpj = row['cnpj']  # Captura o CNPJ
        qsa_list = row['qsa']  # Captura a lista de sócios
        if isinstance(qsa_list, list):
            for socio in qsa_list:
                socio['cnpj'] = cnpj  # Associa o CNPJ a cada sócio
                qsa_data.append(socio)

    # Criar DataFrame de QSA
    df_qsa = pd.DataFrame(qsa_data)

    # Transformar todas as letras do DataFrame de QSA
    df_qsa.columns = [unidecode(col.lower().replace(' ', '_')) if isinstance(col, str) else col for col in df_qsa.columns]
    df_qsa = df_qsa.applymap(lambda x: unidecode(x.lower().replace(' ', '_')) if isinstance(x, str) else x)

    # Salvar o DataFrame de QSA
    df_qsa.to_csv('dim_socios_cnpj_b3.csv', index=False)  # Salvar o DataFrame de QSA

    # Remover a coluna 'cnaes_secundarios' do DataFrame principal
    df.drop(columns=['cnaes_secundarios'], inplace=True, errors='ignore')

    # Remover a coluna 'qsa' do DataFrame principal
    df.drop(columns=['qsa'], inplace=True, errors='ignore')

    # Salvar DataFrame em CSV (sem a coluna 'qsa')
    df.to_csv('f_dados_cnpj_b3.csv', index=False)

    print("Processo concluído e arquivos CSV gerados.")
else:
    print("Nenhum dado válido foi coletado.")
