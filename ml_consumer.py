import json
import joblib
import numpy as np
from kafka import KafkaConsumer, KafkaProducer
import argparse

class MLConsumer:
    def __init__(self, bootstrap_servers, input_topic, output_topic, model_path, scaler_path, group_id='ml-group'):
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
        self.model = joblib.load(model_path)
        self.scaler = joblib.load(scaler_path)
        self.feature_cols = ['open', 'high', 'low', 'close', 'volume_btc', 'volume_usd',
                             'price_change', 'log_return', 'volatility', 'volume_btc_log',
                             'ma_5', 'ma_10', 'ma_20', 'ma_50', 'std_5', 'std_10', 'std_20', 'std_50',
                             'rsi_14']

    def predict(self, data):
        # Извлекаем признаки в правильном порядке
        features = np.array([[data.get(col, 0.0) for col in self.feature_cols]])
        # Масштабируем
        scaled = self.scaler.transform(features)
        pred = self.model.predict(scaled)[0]
        return pred

    def run(self):
        try:
            for msg in self.consumer:
                processed = msg.value
                pred = self.predict(processed)
                result = processed.copy()
                result['prediction'] = float(pred)
                self.producer.send(self.output_topic, value=result)
                self.consumer.commit()
                print(f"Предсказание: {pred:.2f} для unix={processed.get('unix')}")
        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()
            self.producer.close()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--bootstrap', default='localhost:9092,localhost:9093')
    parser.add_argument('--input-topic', default='processed-data')
    parser.add_argument('--output-topic', default='ml-results')
    parser.add_argument('--model', default='models/model.pkl')
    parser.add_argument('--scaler', default='models/scaler.pkl')
    args = parser.parse_args()

    ml = MLConsumer(args.bootstrap, args.input_topic, args.output_topic, args.model, args.scaler)
    ml.run()

if __name__ == '__main__':
    main()