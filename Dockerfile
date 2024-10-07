# Usar uma imagem base com Python
FROM python:3.10-slim

# Definir o diretório de trabalho
WORKDIR /app

# Copiar o arquivo requirements.txt para o contêiner
COPY requirements.txt .

# Instalar as dependências
RUN pip install --no-cache-dir -r requirements.txt

# Copiar o restante dos arquivos da aplicação
COPY . .

# Comando para executar seu script Python
CMD ["python", "dadosb3_brasilapi.py"]
