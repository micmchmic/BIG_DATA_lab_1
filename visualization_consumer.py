import json
from kafka import KafkaConsumer, KafkaProducer
import argparse

class VisualizationConsumer:
    def __init__(self, bootstrap_servers, input_topic, output_topic, group_id='vis-group'):
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

    def run(self):
        try:
            for msg in self.consumer:
                data = msg.value
                self.producer.send(self.output_topic, value=data)
                self.consumer.commit()
                print(f"Переслано в визуализацию: unix={data.get('unix')}")
        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()
            self.producer.close()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--bootstrap', default='localhost:9092,localhost:9093')
    parser.add_argument('--input-topic', default='ml-results')
    parser.add_argument('--output-topic', default='visualization-data')
    args = parser.parse_args()

    vis = VisualizationConsumer(args.bootstrap, args.input_topic, args.output_topic)
    vis.run()

if __name__ == '__main__':
    main()