import pandas as pd
from statsbombpy import sb
import sqlite3  # Para armazenar os dados coletados


# Função para coletar dados de eventos da Bundesliga 2023/24
def extract_event_data(competition_id, season_id):
    try:
        # Extrair competições e buscar o ID da competição
        matches = sb.matches(competition_id=competition_id, season_id=season_id)
        
        # Coletar o primeiro match_id disponível na competição
        if not matches.empty:
            match_id = matches.iloc[0]['match_id']
            events_df = sb.events(match_id=match_id)  # Extrair dados de eventos pelo match_id
            print(f"Dados extraídos para a temporada {season_id} da competição {competition_id}")
            return events_df
        else:
            print(f"Nenhuma partida encontrada para a temporada {season_id} da competição {competition_id}")
            return None
        
    except Exception as e:
        print(f"Erro na extração de dados: {e}")
        return None

# Definindo os parâmetros da Bundesliga 2023/24
competition_id = 9  # Bundesliga
season_id = 281     # Temporada 2023/2024
events_df = extract_event_data(competition_id, season_id)

# Verificar se os dados foram extraídos com sucesso
if events_df is not None:
    # Conexão com banco SQLite para armazenar os dados
    def store_data_in_db(events_df, db_name='sports_data.db'):
        conn = sqlite3.connect(db_name)
        events_df.to_sql('leverkusen_events', conn, if_exists='replace', index=False)
        print(f"Dados armazenados no banco de dados {db_name}")
        conn.close()

    store_data_in_db(events_df)

    # Limpeza e transformação dos dados
    def clean_and_transform_data(events_df):
        # Remover valores ausentes
        events_df_cleaned = events_df.dropna()
        return events_df_cleaned

    events_df_cleaned = clean_and_transform_data(events_df)

else:
    print("Os dados não foram extraídos com sucesso, verifique os parâmetros da API.")
