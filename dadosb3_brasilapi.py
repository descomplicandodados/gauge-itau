import requests
import pandas as pd
from unidecode import unidecode

# Função para obter dados da URL
def get_data_from_url(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return data.get('results', [])
    except requests.exceptions.RequestException as e:
        print(f"Erro na requisição para a URL {url}: {e}")
        return []

# Função para gerar sequências personalizadas de letras
def generate_custom_sequences():
    sequences = []
    for first in ['M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z']:
        for second in ['C', 'S', 'i', 'y', 'C', 'S', 'i', 'y', 'C', 'S', 'i', 'y']:
            sequences.append(f"{first}{second}")
    return sequences

# Função para construir URLs com base nas posições e letras de alteração
def build_urls(base_url, char_position_start, char_position_end, sequences):
    all_urls = []
    
    for seq in sequences:
        # Construa a URL alterando o trecho entre char_position_start e char_position_end com a sequência atual
        modified_url = (base_url[:char_position_start] + seq + base_url[char_position_end:])
        all_urls.append(modified_url)
    
    return all_urls

# Função principal para processar os dados das URLs geradas
def process_company_data(base_url, char_position_start, char_position_end, sequences):
    all_urls = build_urls(base_url, char_position_start, char_position_end, sequences)
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

# Função para consultar dados para os CNPJs coletados
def fetch_cnpj_data(all_cnpjs):
    empresas_b3 = []
    success_count = 0
    processed_cnpjs = set()  # Conjunto para armazenar os CNPJs já consultados
    request_counter = 1  # Contador de requisições
    
    for cnpj in all_cnpjs:
        # Ignorar CNPJs que já foram processados
        if cnpj in processed_cnpjs:
            print(f"CNPJ {cnpj} já foi processado. Ignorando.")
            continue

        # Adiciona um '0' se o CNPJ tiver menos de 14 caracteres e ignora se for apenas 0 ou 00
        if len(cnpj) == 13:
            cnpj = '0' + cnpj
        if len(cnpj) == 12:
            cnpj = '00' + cnpj
        if len(cnpj) == 11:
            cnpj = '000' + cnpj        
        if cnpj in ['0', '00']:
            continue

        # Marcar CNPJ como processado
        processed_cnpjs.add(cnpj)

        # Tentativas de requisição
        for attempt in range(2):
            try:
                response = requests.get(f"https://brasilapi.com.br/api/cnpj/v1/{cnpj}", timeout=10)
                print(f"Status Code para o CNPJ {cnpj}: {response.status_code} - Requisição {request_counter}")
                request_counter += 1  # Incrementa o contador de requisições
                response.raise_for_status()
                if response.text:
                    data = response.json()
                    empresas_b3.append(data)
                else:
                    print(f"Nenhum dado retornado para o CNPJ {cnpj}.")
                break  # Se a requisição for bem-sucedida, sai do loop
            except requests.exceptions.Timeout:
                if attempt == 0:
                    print(f"Timeout ao consultar CNPJ {cnpj}. Tentando novamente.")
                else:
                    print(f"Timeout ao consultar CNPJ {cnpj}. Ignorando essa requisição.")
                    break
            except requests.exceptions.RequestException as e:
                print(f"Erro ao consultar CNPJ {cnpj}: {e}")
                break
            except ValueError as ve:
                print(f"Erro ao decodificar a resposta JSON para o CNPJ {cnpj}: {ve}")
                print(f"Conteúdo da resposta: {response.text}")
                break

    return empresas_b3

# Função para processar os dados em um DataFrame
def process_dataframe(empresas_b3):
    df = pd.DataFrame(empresas_b3)

    # Remover duplicatas
    df = df.drop_duplicates(subset=['cnpj'], keep='first')  # Mantém a primeira ocorrência do CNPJ

    if not df.empty:
        # Preencher coluna 'pais' com 'brasil' se estiver vazia
        df['pais'] = df['pais'].fillna('brasil').replace('', 'brasil')

        # Remover colunas indesejadas
        columns_to_remove = [
            'email', 
            'codigo_pais', 
            'opcao_pelo_mei', 
            'data_opcao_pelo_mei', 
            'data_exclusao_do_mei', 
            'data_exclusao_do_simples', 
            'ente_federativo_responsavel', 
            'data_opcao_pelo_simples'
        ]
        df.drop(columns=columns_to_remove, inplace=True, errors='ignore')

        # Transformar todas as letras em minúsculas e substituir espaços por '_'
        df.columns = [unidecode(col.lower().replace(' ', '_')) if isinstance(col, str) else col for col in df.columns]
        df = df.applymap(lambda x: unidecode(x.lower().replace(' ', '_')) if isinstance(x, str) else x)

        if 'data_inicio_atividade' in df.columns:
            df['ano'] = pd.to_datetime(df['data_inicio_atividade'], errors='coerce').dt.year
            df['mes'] = pd.to_datetime(df['data_inicio_atividade'], errors='coerce').dt.month

        # Criar um novo DataFrame para a coluna 'qsa', essa coluna continha uma lista dentro dela.
        qsa_data = []
        for index, row in df.iterrows():
            cnpj = row['cnpj']
            qsa_list = row['qsa']
            if isinstance(qsa_list, list):
                for socio in qsa_list:
                    socio['cnpj'] = cnpj
                    if 'pais' not in socio or socio['pais'] in [None, '']:
                        socio['pais'] = 'brasil'
                    qsa_data.append(socio)

        df_qsa = pd.DataFrame(qsa_data)

        # Transformar todas as letras do DataFrame de QSA
        df_qsa.columns = [unidecode(col.lower().replace(' ', '_')) if isinstance(col, str) else col for col in df_qsa.columns]
        df_qsa = df_qsa.applymap(lambda x: unidecode(x.lower().replace(' ', '_')) if isinstance(x, str) else x)

        df_qsa.to_csv('dim_socios_cnpj_b3.csv', index=False)

        # Remover colunas do df
        df.drop(columns=['cnaes_secundarios'], inplace=True, errors='ignore')
        df.drop(columns=['qsa'], inplace=True, errors='ignore')

        df.to_csv('f_dados_cnpj_b3.csv', index=False)

        print("Processo concluído e arquivos CSV gerados.")
    else:
        print("Nenhum dado válido foi coletado.")

# URLs base e parâmetros
script_configs = [
    {
        'base_url': 'https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetInitialCompanies/eyJsYW5ndWFnZSI6InB0LWJyIiwicGFnZU51bWJlciI6MSwicGFnZVNpemUiOjEyMH0=',
        'char_position_start': 138,
        'char_position_end': 140,
        'sequences': generate_custom_sequences()
    },
    {
        'base_url': 'https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetInitialCompanies/eyJsYW5ndWFnZSI6InB0LWJyIiwicGFnZU51bWJlciI6MTAsInBhZ2VTaXplIjoxMjB9',
        'char_position_start': 140,
        'char_position_end': 141,
        'sequences': ['A', 'E', 'I', 'M', 'Q', 'U', 'Y']  # Saltos de 4 letras
    },
    {
        'base_url': 'https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetInitialCompanies/eyJsYW5ndWFnZSI6InB0LWJyIiwicGFnZU51bWJlciI6MTcsInBhZ2VTaXplIjoxMjB9',
        'char_position_start': 140,
        'char_position_end': 141,
        'sequences': ['c', 'g', 'k', 'o', 's', 'u', 'w']  # Saltos de 4 letras
    },
    {
        'base_url': 'https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetInitialCompanies/eyJsYW5ndWFnZSI6InB0LWJyIiwicGFnZU51bWJlciI6MjAsInBhZ2VTaXplIjoxMjB9',
        'char_position_start': 140,
        'char_position_end': 141,
        'sequences': ['A', 'E', 'I', 'O', 'S', 'Y']  # Saltos de 4 letras
    }
]

# Processar cada configuração de script
all_cnpjs = []
for config in script_configs:
    cnpjs = process_company_data(
        config['base_url'], 
        config['char_position_start'], 
        config['char_position_end'], 
        config['sequences']
    )
    all_cnpjs.extend(cnpjs)

# Consultar dados para os CNPJs coletados
empresas_b3 = fetch_cnpj_data(all_cnpjs)

# Processar o DataFrame
process_dataframe(empresas_b3)

print("Processo concluído com sucesso.")
