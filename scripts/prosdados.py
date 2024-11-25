
from Dados import Dados
import json
import csv

path_json = 'data_raw/dados_empresaA.json'
path_csv = 'data_raw/dados_empresaB.csv'






# def leitura_json(path_json):
#     dados_json = []
#     with open(path_json, 'r') as file:
#         dados_json = json.load(file)
#     return dados_json

# def leitura_csv(path_csv):
#     dados_csv = []
#     with open(path_csv, 'r') as file:
#         spamreader = csv.DictReader(file, delimiter=',')
#         for row in spamreader:
#             dados_csv.append(row)
#     return dados_csv


# def leitura_dados(path, tipo_arquivo):
#     dados = []
#     if tipo_arquivo == 'csv':
#         dados = leitura_csv(path)
        
#     elif tipo_arquivo == 'json':
#         dados = leitura_json(path)
        
#     return dados

# def get_columns(dados):
#     return list(dados[-1].keys())

# def size_data(dados):
#   return len(dados)

# def rename_columns(dados, key_mapping):
#     new_dados_csv = []
    
#     for old_dict in dados:
#         dict_temp = {}
#         for old_key, value in old_dict.items():
#             dict_temp[key_mapping[old_key]] = value
#         new_dados_csv.append(dict_temp)
            
#     return new_dados_csv

def join(dadosA, dadosB):
    combined_list = []
    combined_list.extend(dadosA)
    combined_list.extend(dadosB)
    return combined_list


# def transformando_dados_tabela(dados, nomes_colunas):
    
#     dados_combinados_tabela = [nomes_colunas]

#     for row in dados:
#         linha = []
#         for coluna in nomes_colunas:
#             linha.append(row.get(coluna, 'Indisponivel'))
#         dados_combinados_tabela.append(linha)
    
#     return dados_combinados_tabela

def salvando_dados(dados, path):
    with open(path, 'w') as file:
        writer = csv.writer(file)
        writer.writerows(dados)


key_mapping = {'Nome do Item': 'Nome do Produto',
                            'Classificação do Produto': 'Categoria do Produto',
                            'Valor em Reais (R$)': 'Preço do Produto (R$)',
                            'Quantidade em Estoque': 'Quantidade em Estoque',
                            'Nome da Loja': 'Filial',
                            'Data da Venda': 'Data da Venda'}


#Extract
dados_empresaA = Dados(path_json, 'json')
dados_empresaB = Dados(path_csv, 'csv')


#Transform
dados_empresaB.rename_columns(key_mapping)
print(dados_empresaB.nome_colunas)

print(dados_empresaA.size)
print(dados_empresaB.size)


dados_fusao = Dados.join(dados_empresaA, dados_empresaB)
print (dados_fusao.nome_colunas)
print(dados_fusao.size)

#Load 

path_dados_combinados  = 'data_processed/dados_combinados.csv'
dados_fusao.salvando_dados (path_dados_combinados)
print(path_dados_combinados)




