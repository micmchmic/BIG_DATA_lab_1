import csv
import json
import time
import random
import argparse
from kafka import KafkaProducer
from kafka.errors import KafkaError

class DataProducer:
    def __init__(self, bootstrap_servers, topic):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3
        )

    def send_data(self, data_dict):
        try:
            future = self.producer.send(self.topic, value=data_dict)
            result = future.get(timeout=10)
            return result
        except KafkaError as e:
            print(f"Ошибка отправки: {e}")
            return None

    def close(self):
        self.producer.close()

def main():
    parser = argparse.ArgumentParser(description='Producer for Bitcoin data')
    parser.add_argument('--csv', default='data/BTC-2017min.csv', help='Путь к CSV')
    parser.add_argument('--bootstrap', default='localhost:9092,localhost:9093',
                        help='Kafka bootstrap servers')
    parser.add_argument('--topic', default='raw-data', help='Название топика')
    parser.add_argument('--delay', type=float, default=1.0, help='Задержка в секундах между отправками')
    parser.add_argument('--random-delay', action='store_true', help='Использовать случайную задержку 0.5-3 сек')
    args = parser.parse_args()

    producer = DataProducer(args.bootstrap, args.topic)

    with open(args.csv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Приводим все числовые значения к float
            data = {}
            for k, v in row.items():
                # Обрабатываем столбцы с пробелами
                clean_key = k.strip().replace(' ', '_')
                try:
                    data[clean_key] = float(v)
                except ValueError:
                    data[clean_key] = v
            if 'unix' not in data:
                data['unix'] = time.time()
            # Отправляем
            producer.send_data(data)
            print(f"Отправлено: unix={data.get('unix')}, close={data.get('close')}")
            # Задержка
            if args.random_delay:
                time.sleep(random.uniform(0.5, 3.0))
            else:
                time.sleep(args.delay)

    producer.close()

if __name__ == '__main__':
    main()