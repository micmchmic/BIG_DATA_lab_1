import json
import pandas as pd
import numpy as np
from kafka import KafkaConsumer, KafkaProducer
import argparse
import time

class DataProcessorConsumer:
    def __init__(self, bootstrap_servers, input_topic, output_topic, group_id='processor-group'):
        self.input_topic = input_topic
        self.output_topic = output_topic
        self.consumer = KafkaConsumer(
            input_topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=False
        )
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all'
        )

        self.close_buffer = []
        

    def compute_rsi(self, prices, window=14):
        if len(prices) < window + 1:
            return 0.0
        deltas = np.diff(prices)
        seed = deltas[:window]
        up = seed[seed >= 0].sum() / window
        down = -seed[seed < 0].sum() / window
        if down == 0:
            return 100.0
        rs = up / down
        rsi = 100 - 100 / (1 + rs)
        return rsi

    def process(self, data):
        # Извлекаем close
        close = data.get('close')
        if close is None:
            return None

        # Добавляем в буфер
        self.close_buffer.append(close)
        if len(self.close_buffer) > 50:
            self.close_buffer.pop(0)

        # Базовые признаки
        open_price = data.get('open', 0.0)
        high = data.get('high', 0.0)
        low = data.get('low', 0.0)
        volume_btc = data.get('volume_btc', 0.0)
        volume_usd = data.get('volume_usd', 0.0)

        price_change = 0.0
        log_return = 0.0
        if len(self.close_buffer) >= 2:
            prev = self.close_buffer[-2]
            if prev != 0:
                price_change = (close - prev) / prev
                log_return = np.log(close / prev)

        volatility = high - low
        volume_btc_log = np.log1p(volume_btc)

        # Скользящие средние и стандартные отклонения
        ma_5 = 0.0
        ma_10 = 0.0
        ma_20 = 0.0
        ma_50 = 0.0
        std_5 = 0.0
        std_10 = 0.0
        std_20 = 0.0
        std_50 = 0.0
        if len(self.close_buffer) >= 5:
            ma_5 = np.mean(self.close_buffer[-5:])
            std_5 = np.std(self.close_buffer[-5:])
        if len(self.close_buffer) >= 10:
            ma_10 = np.mean(self.close_buffer[-10:])
            std_10 = np.std(self.close_buffer[-10:])
        if len(self.close_buffer) >= 20:
            ma_20 = np.mean(self.close_buffer[-20:])
            std_20 = np.std(self.close_buffer[-20:])
        if len(self.close_buffer) >= 50:
            ma_50 = np.mean(self.close_buffer[-50:])
            std_50 = np.std(self.close_buffer[-50:])

        # RSI (используем close)
        rsi_14 = self.compute_rsi(self.close_buffer, 14)

        processed = {
            'unix': data.get('unix'),
            'open': open_price,
            'high': high,
            'low': low,
            'close': close,
            'volume_btc': volume_btc,
            'volume_usd': volume_usd,
            'price_change': price_change,
            'log_return': log_return,
            'volatility': volatility,
            'volume_btc_log': volume_btc_log,
            'ma_5': ma_5,
            'ma_10': ma_10,
            'ma_20': ma_20,
            'ma_50': ma_50,
            'std_5': std_5,
            'std_10': std_10,
            'std_20': std_20,
            'std_50': std_50,
            'rsi_14': rsi_14
        }
        return processed

    def run(self):
        try:
            for msg in self.consumer:
                raw_data = msg.value
                processed_data = self.process(raw_data)
                if processed_data is not None:
                    self.producer.send(self.output_topic, value=processed_data)
                    self.consumer.commit()
                    print(f"Обработано и отправлено: unix={processed_data.get('unix')}")
                else:
                    print("Пропущено (нет close)")
        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()
            self.producer.close()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--bootstrap', default='localhost:9092,localhost:9093')
    parser.add_argument('--input-topic', default='raw-data')
    parser.add_argument('--output-topic', default='processed-data')
    args = parser.parse_args()

    processor = DataProcessorConsumer(args.bootstrap, args.input_topic, args.output_topic)
    processor.run()

if __name__ == '__main__':
    main()