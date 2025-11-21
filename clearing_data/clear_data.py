import fastf1
import pandas as pd
import numpy as np
import os

# 1.0. НАСТРОЙКИ
current_path = os.path.abspath(__file__)

CACHE_DIR = os.path.join(current_path, 'f1_cache')  # Папка для кэша FastF1
OUTPUT_DIR = os.path.join(current_path,'data_output')
SESSION_YEAR = 2024
SESSION_GP = 'Bahrain'
SESSION_TYPE = 'R'  # R - Race, Q - Qualifying

if not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR)
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

# Включаем кэширование (обязательно для FastF1, чтобы не банили и работало быстро)
fastf1.Cache.enable_cache(CACHE_DIR)

def collect_data():
    print(f"--- 1.1. Сбор данных: {SESSION_YEAR} {SESSION_GP} ({SESSION_TYPE}) ---")
    
    # Загрузка сессии
    session = fastf1.get_session(SESSION_YEAR, SESSION_GP, SESSION_TYPE)
    session.load() 

    laps = session.laps
    weather = session.weather_data
    
    # --- ИСПРАВЛЕНИЕ ОШИБКИ ---
    # В погоде колонка называется 'Time', переименуем её в 'SessionTime', 
    # чтобы она совпадала с телеметрией для объединения
    if 'Time' in weather.columns:
        weather = weather.rename(columns={'Time': 'SessionTime'})
    
    # Создаем список для сбора
    all_telemetry = []

    print("Генерация телеметрии по пилотам...")
    for driver in session.drivers:
        try:
            driver_laps = laps.pick_drivers(driver)
            
            # Получаем телеметрию
            telemetry = driver_laps.get_telemetry()
            telemetry['Driver'] = driver
            
            all_telemetry.append(telemetry)
            
        except Exception as e:
            # Иногда у резервных пилотов нет данных
            print(f"Пропуск пилота {driver}: {e}")
            continue

    # Собираем всё в один DataFrame
    if not all_telemetry:
        raise ValueError("Не удалось собрать телеметрию ни для одного пилота!")
        
    raw_df = pd.concat(all_telemetry, ignore_index=True)
    
    # Теперь сортировка сработает, так как мы переименовали колонку выше
    raw_df = raw_df.sort_values(by=['SessionTime'])
    weather = weather.sort_values(by=['SessionTime'])
    
    # Мерджим погоду
    raw_weather_merged = pd.merge_asof(raw_df, weather, on='SessionTime', direction='backward')
    
    # Сохраняем
    save_parquet(raw_weather_merged, 'raw_data_fastf1.parquet')
    
    return raw_weather_merged, laps

def clean_data(telemetry_df, laps_df):
    print("\n--- 1.2. Лёгкая очистка данных ---")
    
    df = telemetry_df.copy()
    
    # 1. Приведение типов (Timedelta -> Seconds/Float)
    time_cols = ['SessionTime', 'Time'] 
    for col in time_cols:
        if col in df.columns:
            df[col] = df[col].dt.total_seconds()

    # 2. Удаление NaN
    initial_len = len(df)
    cols_subset = ['Speed', 'RPM', 'nGear', 'Driver']
    df.dropna(subset=cols_subset, inplace=True)
    print(f"Удалено строк с NaN: {initial_len - len(df)}")

    # 3. Проверка целостности кругов
    # --- ИСПРАВЛЕНИЕ: Используем 'Time' вместо 'SessionTime' для таблицы Laps ---
    # Берем 'Time' и сразу переименовываем в 'SessionTime', если хотим унификации,
    # но для сортировки достаточно просто взять 'Time'.
    
    try:
        # Обратите внимание: в списке колонок теперь 'Time'
        laps_subset = laps_df[['Driver', 'Time', 'LapNumber', 'Compound', 'TyreLife']].copy()
        
        # Для удобства переименуем Time -> SessionTime (время завершения круга)
        laps_subset = laps_subset.rename(columns={'Time': 'SessionTime'})
        
        # Преобразуем Timedelta в секунды, чтобы совпадало с форматом телеметрии
        laps_subset['SessionTime'] = laps_subset['SessionTime'].dt.total_seconds()
        
        laps_sorted = laps_subset.sort_values('SessionTime')
        
        # Примечание: В этом скрипте мы пока не объединяем laps_sorted с df, 
        # чтобы не усложнять "Человека 1". Это пригодится Человеку 2.
        # Но код теперь не будет падать.
        
    except KeyError as e:
        print(f"Внимание: Не удалось обработать данные кругов: {e}")
        print("Доступные колонки в laps:", laps_df.columns)

    # Доп. фильтрация физических аномалий
    df = df[df['Speed'] >= 0]
    
    # Оптимизация типов
    df['nGear'] = df['nGear'].astype('int8')
    df['RPM'] = df['RPM'].astype('int16')
    
    # Сохраняем результат
    save_parquet(df, 'raw_cleaned.parquet')
    
    # (Опционально) Можно сохранить и таблицу кругов отдельно, она пригодится
    # save_parquet(laps_sorted, 'laps_data.parquet')
    
    print("Очистка завершена.")

def save_parquet(df, filename):
    path = os.path.join(OUTPUT_DIR, filename)
    # Parquet не любит смешанные типы объектов, убедимся, что всё ок.
    # Для простоты конвертируем все object в string (кроме известных)
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].astype(str)
            
    df.to_parquet(path, index=False)
    print(f"Файл сохранен: {path} | Размер: {df.shape}")

# --- ЗАПУСК ---
if __name__ == "__main__":
    raw_data, laps_data = collect_data()
    clean_data(raw_data, laps_data)