import streamlit as st
import json
import pandas as pd
import plotly.express as px
from kafka import KafkaConsumer
import threading
import queue
import time

data_queue = queue.Queue()

def consume_kafka(bootstrap_servers, topic):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='latest',
        enable_auto_commit=True
    )
    for msg in consumer:
        data_queue.put(msg.value)

def main():
    st.set_page_config(layout='wide')
    st.title('Real-time Bitcoin Price Prediction Dashboard')

    # Запускаем поток-потребитель (если ещё не запущен)
    if 'consumer_thread' not in st.session_state:
        st.session_state.consumer_thread = threading.Thread(
            target=consume_kafka,
            args=('localhost:9092,localhost:9093', 'visualization-data'),
            daemon=True
        )
        st.session_state.consumer_thread.start()

    # Инициализируем хранилище данных, если его нет
    if 'df' not in st.session_state:
        st.session_state.df = pd.DataFrame(columns=['unix', 'close', 'prediction'])

    # Создаём пустые контейнеры для обновления
    placeholder_metrics = st.empty()
    placeholder_chart1 = st.empty()
    placeholder_chart2 = st.empty()

    # Бесконечный цикл обновления
    while True:
        # Забираем все новые сообщения из очереди
        while not data_queue.empty():
            data = data_queue.get()
            new_row = {
                'unix': data.get('unix', time.time()),
                'close': data.get('close', None),
                'prediction': data.get('prediction', None)
            }
            st.session_state.df = pd.concat([st.session_state.df, pd.DataFrame([new_row])], ignore_index=True)
            if len(st.session_state.df) > 1000:
                st.session_state.df = st.session_state.df.tail(1000)

        df = st.session_state.df

        # Отображаем метрики
        with placeholder_metrics.container():
            col1, col2, col3 = st.columns(3)
            if not df.empty:
                last_close = df['close'].iloc[-1]
                last_pred = df['prediction'].iloc[-1]
                col1.metric('Current Price (close)', f"{last_close:.2f}")
                col2.metric('Last Prediction', f"{last_pred:.2f}")
                col3.metric('Error (abs)', f"{abs(last_close - last_pred):.2f}")
            else:
                col1.write('Waiting for data...')

        # График цены и предсказания
        with placeholder_chart1.container():
            if len(df) > 1:
                fig = px.line(df, x='unix', y=['close', 'prediction'],
                              title='Price and Prediction over time',
                              labels={'value': 'Price', 'unix': 'Time'})
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info('Not enough data for chart (need at least 2 points)')

        # График ошибки
        with placeholder_chart2.container():
            if len(df) > 1:
                df['error'] = df['close'] - df['prediction']
                fig2 = px.line(df, x='unix', y='error', title='Prediction Error')
                st.plotly_chart(fig2, use_container_width=True)
            else:
                st.info('Not enough data for error chart')

        # Пауза перед следующим обновлением
        time.sleep(3)

if __name__ == '__main__':
    main()