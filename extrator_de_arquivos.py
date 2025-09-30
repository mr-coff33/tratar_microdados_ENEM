import zipfile
import os

def descompactar_zip(caminho_arquivo, pasta_destino):
    """
    Descompacta um arquivo .zip para uma pasta destino.

    :param caminho_arquivo: Caminho do arquivo .zip
    :param pasta_destino: Pasta onde os arquivos serão extraídos
    """
    # Cria a pasta destino se não existir
    if not os.path.exists(pasta_destino):
        os.makedirs(pasta_destino)

    # Abre e extrai o zip
    with zipfile.ZipFile(caminho_arquivo, 'r') as zip_ref:
        zip_ref.extractall(pasta_destino)
        print(f"Arquivos extraídos para: {pasta_destino}")

# Exemplo de uso
anos = range(2024, 2024)  # 2014 até 2024
for ano in anos:
    caminho_zip = rf"C:\Users\efg\Music\danylo\enem\microdados_enem_{ano}.zip"
    pasta_destino = rf"C:\Users\efg\Music\danylo\tratados\microdados_enem_{ano}_extraido"
    descompactar_zip(caminho_zip, pasta_destino)

