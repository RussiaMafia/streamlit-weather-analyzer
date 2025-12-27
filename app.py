import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
import requests
import asyncio
import aiohttp
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---

def get_current_season():
    month = datetime.now().month
    if month in [12, 1, 2]:
        return "winter"
    elif month in [3, 4, 5]:
        return "spring"
    elif month in [6, 7, 8]:
        return "summer"
    return "autumn"

def analyze_city(df_city):
    df_city['moving_avg'] = df_city['temperature'].rolling(window=30).mean()
    season_stats = df_city.groupby('season')['temperature'].agg(['mean', 'std']).reset_index()
    df_city = pd.merge(df_city, season_stats, on='season', how='left')
    df_city['lower_bound'] = df_city['mean'] - 2 * df_city['std']
    df_city['upper_bound'] = df_city['mean'] + 2 * df_city['std']
    df_city['is_anomaly'] = (df_city['temperature'] < df_city['lower_bound']) | (df_city['temperature'] > df_city['upper_bound'])
    return df_city, season_stats

@st.cache_data
def process_data_with_timing(df, city):
    # --- Параллельный vs последовательный анализ ---
    start = time.time()
    with ThreadPoolExecutor() as executor:
        future = executor.submit(analyze_city, df[df['city'] == city].copy())
        parallel_result = future.result()
    parallel_time = time.time() - start

    start = time.time()
    sequential_result = analyze_city(df[df['city'] == city].copy())
    sequential_time = time.time() - start

    st.sidebar.info(f"⏱️ Анализ данных:\nПоследовательно: {sequential_time:.3f} с\nПараллельно: {parallel_time:.3f} с")

    return sequential_result[0], sequential_result[1]

# --- API: Синхронный и асинхронный ---

def get_weather_sync(city, api_key):
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {"q": city, "appid": api_key, "units": "metric"}
    try:
        return requests.get(url, params=params, timeout=10)
    except Exception:
        return None

async def get_weather_async(city, api_key):
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {"q": city, "appid": api_key, "units": "metric"}
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                return await resp.json(), resp.status
        except Exception:
            return None, None

@st.cache_data
def fetch_weather_with_timing(city, api_key):
    # Синхронный
    start = time.time()
    resp_sync = get_weather_sync(city, api_key)
    sync_time = time.time() - start

    # Асинхронный
    start = time.time()
    data_async, status_async = asyncio.run(get_weather_async(city, api_key))
    async_time = time.time() - start

    st.sidebar.info(f"⏱️ Запрос к API:\nСинхронно: {sync_time:.3f} с\nАсинхронно: {async_time:.3f} с")

    # Возвращаем синхронный ответ (он проще для обработки)
    return resp_sync

# --- Streamlit UI ---

st.set_page_config(page_title="🌤️ Погодный Аналитик", layout="wide")
st.title("🌤️ Погодный Аналитик")

uploaded_file = st.sidebar.file_uploader("Загрузите CSV с историческими данными", type="csv")

if uploaded_file:
    try:
        df = pd.read_csv(uploaded_file)
        required_cols = {'city', 'timestamp', 'temperature', 'season'}
        if not required_cols.issubset(df.columns):
            st.error(f"Файл должен содержать колонки: {', '.join(required_cols)}")
        else:
            city_list = df['city'].unique()
            selected_city = st.sidebar.selectbox("Выберите город", city_list)

            city_data, season_stats = process_data_with_timing(df, selected_city)

            st.header(f"Анализ данных: {selected_city}")

            # Описательная статистика
            st.subheader("📊 Описательная статистика")
            st.dataframe(city_data.describe()[['temperature']])

            # Долгосрочный тренд (365 дней)
            city_data['trend'] = city_data['temperature'].rolling(window=365, min_periods=1).mean()

            # График временного ряда
            st.subheader("📈 Временной ряд температур")
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=city_data['timestamp'], y=city_data['temperature'],
                                     mode='lines', name='Температура', line=dict(color='blue', width=0.8)))
            anomalies = city_data[city_data['is_anomaly']]
            fig.add_trace(go.Scatter(x=anomalies['timestamp'], y=anomalies['temperature'],
                                     mode='markers', name='Аномалии', marker=dict(color='red', size=4, symbol='x')))
            fig.add_trace(go.Scatter(x=city_data['timestamp'], y=city_data['moving_avg'],
                                     mode='lines', name='Скользящее среднее (30д)', line=dict(color='orange')))
            fig.add_trace(go.Scatter(x=city_data['timestamp'], y=city_data['trend'],
                                     mode='lines', name='Долгосрочный тренд (365д)', line=dict(color='green', dash='dot')))
            st.plotly_chart(fig, use_container_width=True)

            # Сезонные профили
            st.subheader("📅 Сезонные профили")
            st.table(season_stats.set_index('season').rename(
                columns={'mean': 'Средняя темп.', 'std': 'Станд. отклонение'}
            ))

            fig2 = px.bar(season_stats, x='season', y='mean', error_y='std',
                          title="Средняя температура по сезонам (±2σ)")
            st.plotly_chart(fig2, use_container_width=True)

            # Текущая погода
            st.divider()
            st.subheader("🌍 Текущая погода (Live)")
            api_key = st.sidebar.text_input("OpenWeatherMap API Key", type="password")

            if not api_key:
                st.info("Введите API Key в боковой панели для отображения текущей погоды.")
            else:
                response = fetch_weather_with_timing(selected_city, api_key)

                if response and response.status_code == 200:
                    data = response.json()
                    current_temp = data['main']['temp']
                    current_season = get_current_season()
                    st.metric(f"Текущая температура в {selected_city}", f"{current_temp:.1f} °C")
                    st.write(f"Текущий сезон: **{current_season}**")

                    season_row = season_stats[season_stats['season'] == current_season]
                    if not season_row.empty:
                        mean = season_row.iloc[0]['mean']
                        std = season_row.iloc[0]['std']
                        lower = mean - 2 * std
                        upper = mean + 2 * std
                        if lower <= current_temp <= upper:
                            st.success(f"✅ Температура в пределах нормы ({lower:.1f}...{upper:.1f}°C)")
                        else:
                            st.error(f"⚠️ АНОМАЛИЯ! Выход за пределы нормы ({lower:.1f}...{upper:.1f}°C)")
                    else:
                        st.warning("Недостаточно данных для этого сезона.")
                elif response.status_code == 401:
                    st.error(f"Ошибка авторизации: {response.text}")
                else:
                    st.error(f"Ошибка при получении данных: {response.status_code} - {response.reason}")
    except Exception as e:
        st.error(f"Ошибка при обработке файла: {e}")
else:
    st.info("📥 Загрузите CSV-файл с историческими данными для начала.")