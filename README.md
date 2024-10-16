# Coletor de CNPJs da B3
# Descrição Geral
Este projeto tem como objetivo coletar os CNPJs das empresas listadas na B3 (Bolsa de Valores do Brasil) utilizando web scraping. Após obter esses CNPJs, o script utiliza a API da BrasilAPI para recuperar informações detalhadas sobre cada empresa. Os dados coletados são processados e salvos em arquivos CSV para análise posterior.

A execução do script dadosb3_brasilapi.py podera levar entre 10 a 20 minutos a depender da velocidade de resposta da BrasilAPI

## Pré-requisitos

Antes de começar, você precisará ter o docker instalado na sua máquina:

- [Docker](https://docs.docker.com/get-docker/)

# Requisitos

- Certifi
- Charset-normalizer
- Idna
- Numpy
- Pandas
- Python 3.9+
- Pytz
- Requests
- Dix
- Tzdata
- Unidecode
- Urllib3



# Como executar

- Clone o repositório para sua máquina local
```
git clone git@github.com:descomplicandodados/gauge-itau.git
```
- Navegue até a pasta do projeto
```
cd gauge-itau
```
- Execute os comandos
```
docker build -t gauge-itau-api .
```

```
docker run --rm gauge-itau-api
```

# Funcionamento
- O script inicia acessando a B3 para coletar os CNPJs das empresas listadas.
- Em seguida, utiliza a API da BrasilAPI para buscar informações adicionais sobre cada CNPJ coletado.
- As informações obtidas são armazenadas em um DataFrame, onde são realizadas transformações, como normalização dos nomes das colunas.
- Os dados são salvos em arquivos CSV, que podem ser usados para análise ou relatórios futuros.

Após a execução, serão gerados dois arquivos CSV, f_dados_cnpj_b3 e dim_socios_cnpj_b3. Esses arquivos podem ser utilizados para várias análises, como por exemplo qual a média de anos que as empresas que estão listadas na B3 estão abertas, qual o tipo de cnae que mais existe entre as empresas listadas, qual a faixa etaria dos proprietarios e princiais diretores dessas empresas, nome completo dos mesmos para procurar meios de contato com fins comerciais, desde que não seja infringido a LGPD, em que local de estado e cidade existe maior concentração de empresas listadas, entre outros.