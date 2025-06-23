import re
import pandas as pd

def analisar_ceps_do_dataframe(df, zipcode):
    """
    Analisa uma coluna de CEPs em um DataFrame, removendo hífens,
    identificando CEPs inválidos e retornando dois DataFrames:
    um com CEPs inválidos e outro com CEPs válidos.
    """
    df['CEP_Invalido'] = False

    for index, row in df.iterrows():
        cep = row[zipcode]
        if not isinstance(cep, str):
            cep = str(cep)
        cep_sem_hifen = cep.replace("-", "").replace(".", "")
        if not re.match(r"^\d{8}$", cep_sem_hifen) or cep_sem_hifen == "00000000":
            df.loc[index, 'CEP_Invalido'] = True

    df_invalidos = df[df['CEP_Invalido']].copy()
    df_validos = df[~df['CEP_Invalido']].copy()

    df_invalidos.drop(columns=['CEP_Invalido'], inplace=True, errors='ignore')
    df_validos.drop(columns=['CEP_Invalido'], inplace=True, errors='ignore')
    
    return df_invalidos, df_validos



cidade_correta = {
    'SANASA':['Campinas'],
    
    'URBATECH':[
        'São Paulo',
        'Diadema',
        'Rio de Janeiro',
        'Mundo Novo'],
    
    'AEGEA RJ':[
        'Nova Iguaçu',
        'Rio de Janeiro',
        'Belford Roxo',
        'São João de Meriti',
        'São Gonçalo',
        'Duque de Caxias',
        'Rio Bonito',
        'Queimados',
        'Mesquita',
        'Miracema',
        'Maricá',
        'Itaboraí',
        'Nilópolis',
        'Japeri',
        'Cordeiro',
        'Cachoeiras de Macacu',
        'Cantagalo',
        'Paracambi',
        'Itaocara',
        'Aperibé'],
    
    'AEGEA MS':['Campo Grande'],
    
    'Acquax - Acquax do Brasil':[
        'Rio de Janeiro',
        'São Paulo',
        'Suzano',
        'Goiânia',
        'Taboão da Serra',
        'Campos dos Goytacazes',
        'Serra',
        'Niterói',
        'São Fidélis',
        'Viana',
        'São Gabriel da Palha',
        'Aracaju',
        'São João da Barra',
        'Piúma'],
    
    'Metragen':[
        'São Paulo',
        'São Bernardo do Campo',
        'Santo André',
        'Diadema',
        'São Caetano do Sul',
        'Barueri']
    }

# Verifica se as cidades estão corretas para cada empresa
def verificar_cidades_incorretas(df_validos, cidade_correta):
    """
    Verifica se cada empresa está registrada com a cidade correta.
    """
    empresas_com_cidade_errada = []

    for org_name, cidades_validas in cidade_correta.items():
        df_emp = df_validos[df_validos['org_name'] == org_name]
        for _, row in df_emp.iterrows():
            cidade = row['city']
            if cidade not in cidades_validas:
                empresas_com_cidade_errada.append({
                    'device_id': row['device_id'],
                    'org_name': org_name,
                    'cidade_atual': cidade,
                    'cidade_correta': ', '.join(cidades_validas),
                    'descricao': 'cidade incorreta'
                })

    df_erros = pd.DataFrame(empresas_com_cidade_errada)
    return df_erros





# Comparar CEPs válidos com faixas de CEP por região
def comparar_por_faixa_regiao(df_validos, cep_df, zipcode_col, inicio_col, fim_col, city_col, regiao_col):
    """
    Compara os CEPs válidos com as faixas de CEP por região
    e retorna um DataFrame com a coluna 'compatibilidade'.
    """
    df_validos = df_validos.copy()
    cep_df = cep_df.copy()

    df_validos['cep5'] = df_validos[zipcode_col].astype(str).str.zfill(8).str[:5]
    df_validos['cidade_lower'] = df_validos[city_col].astype(str).str.lower().str.strip()

    cep_df['inicio5'] = cep_df[inicio_col].astype(str).str.zfill(8).str[:5]
    cep_df['final5'] = cep_df[fim_col].astype(str).str.zfill(8).str[:5]
    cep_df['regiao'] = cep_df[regiao_col]

    regioes = []
    compatibilidades = []

    for idx, row in df_validos.iterrows():
        cep5 = row['cep5']
        faixas = cep_df[(cep_df['inicio5'] <= cep5) & (cep_df['final5'] >= cep5)]
        if not faixas.empty:
            regioes.append(faixas.iloc[0]['regiao'])
            compatibilidades.append('Compatível')
        else:
            regioes.append(None)
            compatibilidades.append('Incompatível')

    df_validos['regiao'] = regioes
    df_validos['compatibilidade'] = compatibilidades
    
    return df_validos            

def main():

    # === Carregar dados ===
    df = pd.read_csv('/home/Cida/Downloads/query_result_2025-06-16T16_47_29.979408795-03_00.csv')
    cep_df = pd.read_csv('/home/Cida/Desktop/Cep_estados.csv')

    # === Analisar CEPs ===
    df_invalidos, df_validos = analisar_ceps_do_dataframe(df.copy(), "zipcode")

    print(f"{len(df_invalidos)} CEPs inválidos encontrados.")
    print(f"{len(df_validos)} CEPs válidos encontrados.")

    # === Comparar por região ===
    df_resultado = comparar_por_faixa_regiao(
        df_validos, cep_df,
        zipcode_col="zipcode",
        inicio_col="Inicio",
        fim_col="Fim",
        city_col="city",
        regiao_col="Região"
    )

    # === Salvar incompatíveis ===
    df_incompativeis = df_resultado[df_resultado['compatibilidade'] == 'Incompatível']
    df_incompativeis.to_csv('/home/Cida/Documentos/utilities-water/data/ceps_incompativeis.csv', index=False)
    # === Verificar cidades incorretas ===
    df_invalidos.to_csv('/home/Cida/Documentos/utilities-water/data/cep_invalidos.csv', index=False)
    
    df_erros = verificar_cidades_incorretas(df_resultado, cidade_correta)
    df_erros.to_csv('/home/Cida/Documentos/utilities-water/data/empresas_com_cidade_errada.csv', index=False)

if __name__ == '__main__':
    main()
