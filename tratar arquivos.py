import os
import shutil

def excluir_pastas(pasta_raiz, pastas_para_excluir):
    """
    Exclui pastas específicas dentro de uma pasta raiz, se existirem.

    :param pasta_raiz: Caminho da pasta onde procurar
    :param pastas_para_excluir: Lista de nomes de pastas a serem excluídas
    """
    for root, dirs, files in os.walk(pasta_raiz):
        for dir_name in dirs:
            if dir_name in pastas_para_excluir:
                caminho_completo = os.path.join(root, dir_name)
                shutil.rmtree(caminho_completo)
                print(f"Pasta excluída: {caminho_completo}")

# Exemplo de uso
anos = range(2014, 2025)
pastas_para_excluir = ["LEIA-ME e DOCUMENTOS TÉCNICOS", "PROVAS e GABARITOS", "INPUTS", "DICIONÁRIO"]

for ano in anos:
    pasta_extraida = rf"C:\Users\efg\Music\danylo\tratados\microdados_enem_{ano}_extraido"
    excluir_pastas(pasta_extraida, pastas_para_excluir)
