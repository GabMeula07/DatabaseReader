import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os


# Gerando dados para dim_tempo
def gerar_dim_tempo(start_date='2023-01-01', end_date='2023-12-31'):
    dates = pd.date_range(start=start_date, end=end_date, freq='D')
    dim_tempo = pd.DataFrame({
        'id_tempo': range(1, len(dates) + 1),
        'data': dates,
        'dia': dates.day,
        'mes': dates.month,
        'ano': dates.year,
        'dia_semana': dates.dayofweek,
        'trimestre': dates.quarter
    })
    return dim_tempo

# Gerando dados para dim_produto
def gerar_dim_produto(num_produtos=100):
    categorias = ['Eletrônicos', 'Roupas', 'Alimentos', 'Móveis', 'Livros']
    subcategorias = {
        'Eletrônicos': ['Smartphones', 'Notebooks', 'Tablets', 'TVs'],
        'Roupas': ['Camisetas', 'Calças', 'Vestidos', 'Casacos'],
        'Alimentos': ['Bebidas', 'Snacks', 'Congelados', 'Cereais'],
        'Móveis': ['Sofás', 'Mesas', 'Cadeiras', 'Armários'],
        'Livros': ['Ficção', 'Não-Ficção', 'Infantil', 'Técnico']
    }
    
    produtos = []

    for i in range(1, num_produtos + 1):
        categoria = random.choice(categorias)
        produtos.append({
            'id_produto': i,
            'nome_produto': f'Produto_{i}',
            'categoria': categoria,
            'subcategoria': random.choice(subcategorias[categoria]),
            'preco_base': round(random.uniform(10, 1000), 2),
            'peso': round(random.uniform(0.1, 20), 2)
        })

    return pd.DataFrame(produtos)

# Gerando dados para dim_loja

def gerar_dim_loja(num_lojas=20):
    estados = ['SP', 'RJ', 'MG', 'RS', 'PR']
    regioes = ['Sudeste', 'Sul', 'Norte', 'Nordeste', 'Centro-Oeste']
    
    lojas = []
    for i in range(1, num_lojas + 1):
        estado = random.choice(estados)
        lojas.append({
            'id_loja': i,
            'nome_loja': f'Loja_{i}',
            'estado': estado,
            'cidade': f'Cidade_{i}',
            'regiao': random.choice(regioes),
            'tamanho_m2': random.randint(100, 1000)
        })
    return pd.DataFrame(lojas)

# Gerando dados para dim_cliente
def gerar_dim_cliente(num_clientes=1000):
    segmentos = ['Varejo', 'Atacado', 'Premium']
    
    clientes = []
    for i in range(1, num_clientes + 1):
        clientes.append({
            'id_cliente': i,
            'nome_cliente': f'Cliente_{i}',
            'segmento': random.choice(segmentos),
            'idade': random.randint(18, 80),
            'genero': random.choice(['M', 'F']),
            'cidade': f'Cidade_{random.randint(1, 50)}'
        })
    return pd.DataFrame(clientes)


# Gerando dados para fato_vendas
def gerar_fato_vendas(dim_tempo, dim_produto, dim_loja, dim_cliente, num_vendas=10000):
    vendas = []
    for _ in range(num_vendas):
        id_tempo = random.choice(dim_tempo['id_tempo'])
        id_produto = random.choice(dim_produto['id_produto'])
        preco_base = dim_produto[dim_produto['id_produto'] == id_produto]['preco_base'].iloc[0]
        
        vendas.append({
            'id_venda': _ + 1,
            'id_tempo': id_tempo,
            'id_produto': id_produto,
            'id_loja': random.choice(dim_loja['id_loja']),
            'id_cliente': random.choice(dim_cliente['id_cliente']),
            'quantidade': random.randint(1, 10),
            'valor_unitario': round(preco_base * random.uniform(0.9, 1.1), 2),  # Variação de preço
            'desconto': round(random.uniform(0, 0.2), 2),  # Desconto de 0% a 20%
            'custo_unitario': round(preco_base * 0.6, 2),  # Custo como 60% do preço base
        })
    
    df_vendas = pd.DataFrame(vendas)
    df_vendas['valor_total'] = df_vendas['quantidade'] * df_vendas['valor_unitario'] * (1 - df_vendas['desconto'])
    df_vendas['custo_total'] = df_vendas['quantidade'] * df_vendas['custo_unitario']
    df_vendas['margem'] = df_vendas['valor_total'] - df_vendas['custo_total']
    
    return df_vendas


# Gerando todas as dimensões
dim_tempo = gerar_dim_tempo()
dim_produto = gerar_dim_produto()
dim_loja = gerar_dim_loja()
dim_cliente = gerar_dim_cliente()


# Gerando fato vendas
fato_vendas = gerar_fato_vendas(dim_tempo, dim_produto, dim_loja, dim_cliente)

load_dotenv()
engine = create_engine(os.environ.get("database_url"))

fato_vendas.to_sql("fato_vendas", con=engine, index=False, if_exists="replace")
dim_cliente.to_sql("dim_cliente", con=engine, index=False, if_exists="replace")
dim_loja.to_sql("dim_loja", con=engine, index=False, if_exists="replace")
dim_produto.to_sql("dim_produto", con=engine, index=False, if_exists="replace")
dim_tempo.to_sql("dim_tempo", con=engine, index=False, if_exists="replace")






# Mostrando algumas informações sobre os dados gerados
print("Dimensões dos DataFrames gerados:")
print(f"dim_tempo: {dim_tempo.shape}")
print(f"dim_produto: {dim_produto.shape}")
print(f"dim_loja: {dim_loja.shape}")
print(f"dim_cliente: {dim_cliente.shape}")
print(f"fato_vendas: {fato_vendas.shape}")

