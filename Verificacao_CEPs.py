import pandas as pd
import re
import os

def analisar_ceps_do_dataframe(df, zipcode):

    df['CEP_Invalido'] = False

    for index, row in df.iterrows():
        cep = row[zipcode]

        if not isinstance(cep, str):
            cep = str(cep)

        cep_sem_hifen = cep.replace("-", "").replace(".", "")

        if not re.match(r"^\d{8}$", cep_sem_hifen):
            df.loc[index, 'CEP_Invalido'] = True
        elif cep_sem_hifen == "00000000":
            df.loc[index, 'CEP_Invalido'] = True

    df_invalidos = df[df['CEP_Invalido']].copy()
    df_validos = df[~df['CEP_Invalido']].copy()

    df_invalidos.drop(columns=['CEP_Invalido'], inplace=True)
    df_validos.drop(columns=['CEP_Invalido'], inplace=True)

    return df_invalidos, df_validos

def comparar_por_org_e_cidade(df_validos, cep_df, zipcode, Inicio, Fim, city, Região):
    df_validos = df_validos.copy()
    cep_df = cep_df.copy()

    df_validos['cep5'] = df_validos[zipcode].astype(str).str.zfill(8).str[:5]
    df_validos['cidade_lower'] = df_validos[city].astype(str).str.lower().str.strip()

    cep_df['inicio5'] = cep_df[Inicio].astype(str).str.zfill(8).str[:5]
    cep_df['final5'] = cep_df[Fim].astype(str).str.zfill(8).str[:5]
    cep_df['regiao'] = cep_df[Região]

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
    # === Caminhos dos arquivos ===
    arquivo_dados = '/home/Cida/Documentos/utilities-water/data/guariroba/query_result_2025-01-09T17_21_18.465681Z.csv''
    arquivo_ceps = '/home/Cida/Desktop/Cep_estados.csv'

    
    df = pd.read_csv(arquivo_dados)
    cep_df = pd.read_csv(arquivo_ceps)
    
    df_invalidos, df_validos = analisar_ceps_do_dataframe(df, "zipcode")

    print(f" {len(df_invalidos)} CEPs inválidos encontrados.")
    print(f" {len(df_validos)} CEPs válidos encontrados.")
    
    df_resultado = comparar_por_org_e_cidade(
        df_validos,
        cep_df,
        'zipcode',     # Nome da coluna de CEPs
        'Inicio',      # Coluna de início de faixa
        'Fim',         # Coluna de fim de faixa
        'city',        # Nome da cidade no df
        'Região'       # Nome da região no cep_df
    )

    base_dir = "/home/Cida/Documentos/utilities-water/data"
    os.makedirs(base_dir, exist_ok=True)

    df_invalidos.to_csv(os.path.join(base_dir, "ceps_invalidos.csv"), index=False)
    df_resultado.to_csv(os.path.join(base_dir, "ceps_validos_compatibilidade.csv"), index=False)

if __name__ == "__main__":
    main()
