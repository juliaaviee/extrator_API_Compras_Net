import os
import json
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# --- FUNÇÕES AUXILIARES ---

# Salvar em NDJSON
def salvar_como_ndjson(dados, nome_arquivo):
    diretorio = os.path.dirname(nome_arquivo)
    if not os.path.exists(diretorio):
        os.makedirs(diretorio)

    with open(nome_arquivo, 'a') as f:
        for item in dados:
            f.write(json.dumps(item) + '\n')

# Função para achatar dados aninhados
def flatten_dados(item, prefix=''):
    item_flat = {}
    for key, value in item.items():
        new_key = f"{prefix}{key}" if prefix else key
        if isinstance(value, dict):
            item_flat.update(flatten_dados(value, prefix=f"{new_key}_"))
        elif isinstance(value, list):
            for i, sub_item in enumerate(value):
                if isinstance(sub_item, dict):
                    item_flat.update(flatten_dados(sub_item, prefix=f"{new_key}_{i}_"))
                else:
                    item_flat[f"{new_key}_{i}"] = sub_item
        else:
            item_flat[new_key] = value
    return item_flat

# --- FUNÇÃO PRINCIPAL PARA EXTRAIR DADOS ---

def baixar_pagina_fornecedor(url_base, pagina):
    # A URL usa 'pagina' e 'tamanhoPagina' como parâmetros
    params = {
        'pagina': pagina,
        'tamanhoPagina': 500, # Tamanho máximo permitido
        'ativo': 'true' # Filtra apenas fornecedores ativos
    }
    
    try:
        resposta = requests.get(url_base, params=params)
        resposta.raise_for_status() # Levanta um erro se a resposta não for 200
        
        if resposta.status_code == 200:
            dados_json = resposta.json()
            
            # Os dados estão na chave "resultado"
            dados = dados_json.get("resultado", [])
            
            # A paginação está na chave "paginasRestantes"
            paginas_restantes = dados_json.get("paginasRestantes", 0)

            # Se a lista de dados for vazia, indica que não há mais páginas
            if not dados:
                return [], 0
            
            # "Achata" os dados para remover hierarquias
            dados_limpos = [flatten_dados(item) for item in dados]
            
            return dados_limpos, paginas_restantes
            
    except requests.exceptions.RequestException as e:
        print(f"Erro ao baixar página {pagina}: {e}")
        return [], 0
    
def baixar_todas_paginas_fornecedor(url_base):
    dados_totais = []
    pagina_inicial = 1
    
    # Faz a primeira requisição para obter o total de páginas restantes
    primeiros_dados, paginas_restantes = baixar_pagina_fornecedor(url_base, pagina_inicial)
    dados_totais.extend(primeiros_dados)
    
    print(f"Total de páginas a serem baixadas: {paginas_restantes + 1}")
    
    # Baixa o restante das páginas em paralelo
    if paginas_restantes > 0:
        with ThreadPoolExecutor(max_workers=5) as executor:
            # Cria uma lista de tarefas para as páginas restantes
            tarefas = [executor.submit(baixar_pagina_fornecedor, url_base, pagina) for pagina in range(pagina_inicial + 1, pagina_inicial + paginas_restantes + 1)]
            
            # Acompanha o progresso das tarefas
            for i, tarefa in enumerate(as_completed(tarefas)):
                dados_baixados, _ = tarefa.result()
                dados_totais.extend(dados_baixados)
                print(f"Páginas concluídas: {i + 2}/{paginas_restantes + 1}", end='\r')
                
    return dados_totais

# --- CONFIGURAÇÃO E EXECUÇÃO ---

# URL base da API de fornecedores
url_base = 'https://dadosabertos.compras.gov.br/modulo-fornecedor/1_consultarFornecedor'

# Baixa todos os dados de fornecedores
print("Iniciando o download dos dados de fornecedores...")
dados_finais = baixar_todas_paginas_fornecedor(url_base)

# Salva em um único arquivo NDJSON
nome_arquivo = f'C:/Users/julia.vieira/Downloads/fornecedores_ativos.ndjson'
salvar_como_ndjson(dados_finais, nome_arquivo)

print(f"\nDownload concluído. Total de registros salvos: {len(dados_finais)}")
