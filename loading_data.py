import fastf1
import pandas as pd
import os

# Создаем папку кэша
if not os.path.exists('cache'):
    os.makedirs('cache')

fastf1.Cache.enable_cache('cache') 

def get_season_data(years):
    all_results = []
    
    for year in years:
        print(f"--- Скачивание расписания на {year} год ---")
        try:
            schedule = fastf1.get_event_schedule(year)
        except Exception as e:
            print(f"ОШИБКА: Не удалось получить расписание для {year}. Пропускаем. {e}")
            continue
        
        # Фильтруем только обычные гонки
        races = schedule[schedule['EventFormat'] == 'conventional']
        
        for _, event in races.iterrows():
            round_num = event['RoundNumber']
            location = event['Location']
            # if location == 'Zandvoort' or location == 'Mexico City':
            #     continue
            
            # --- ИСПРАВЛЕНИЕ ОШИБКИ ---
            # Сравниваем время сессии с текущим временем в UTC
            try:
                if event['Session5Date'] > pd.Timestamp.now(tz='UTC'):
                    continue
            except Exception:
                # Если вдруг сравнение не сработало (например, время NaT), пропускаем от греха подальше
                continue

            print(f"Загрузка: {year} Этап {round_num} - {location}")
            
            try:
                # --- ЗАГРУЗКА КВАЛИФИКАЦИИ ---
                session_q = fastf1.get_session(year, round_num, 'Q')
                session_q.load(telemetry=False, laps=False, weather=False)
                
                q_results = session_q.results.reset_index()
                if q_results.empty:
                    print(f"  Нет данных квалификации для {location}")
                    continue
                    
                q_subset = q_results[['Abbreviation', 'Position', 'Q3', 'Q2', 'Q1']].copy()
                q_subset.rename(columns={'Position': 'QualiPos'}, inplace=True)
                
                # --- ЗАГРУЗКА ГОНКИ ---
                session_r = fastf1.get_session(year, round_num, 'R')
                session_r.load(telemetry=False, laps=False, weather=False)
                r_results = session_r.results.reset_index()
                
                if r_results.empty:
                    print(f"  Нет данных гонки для {location}")
                    continue
                
                # Объединяем
                combined = pd.merge(r_results, q_subset, on='Abbreviation', how='left')
                
                combined['Year'] = year
                combined['Round'] = round_num
                combined['Circuit'] = location
                
                cols_to_keep = [
                    'Year', 'Round', 'Circuit', 'TeamName', 'Abbreviation', 
                    'QualiPos', 'GridPosition', 'ClassifiedPosition', 'Status', 'Time'
                ]
                
                # Проверка на наличие колонок
                available_cols = [c for c in cols_to_keep if c in combined.columns]
                all_results.append(combined[available_cols])
                
            except Exception as e:
                print(f"  Ошибка загрузки этапа {round_num}: {e}")
                continue

    if not all_results:
        print("Никаких данных не удалось собрать :(")
        return pd.DataFrame()

    final_df = pd.concat(all_results, ignore_index=True)
    return final_df

# Запускаем сбор
df = get_season_data([2025])

if not df.empty:
    df.to_csv('f1_2025_raw.csv', index=False)
    print("Готово! Данные сохранены в f1_teammates_raw.csv")