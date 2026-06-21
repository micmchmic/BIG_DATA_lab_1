import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
import joblib
import os
import time

def compute_rsi(data, window=14):
    delta = data.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def main():
    print("=== Обучение модели на полных данных ===")
    start_total = time.time()

    os.makedirs('models', exist_ok=True)
    print("1. Папка models создана.")

    df = pd.read_csv('data/BTC-2017min.csv')
    df.rename(columns={
        'Volume BTC': 'volume_btc',
        'Volume USD': 'volume_usd'
    }, inplace=True)
    
    print("2. Сортировка по unix...")
    df.sort_values('unix', inplace=True)

    print("3. Создание признаков...")
    # Базовые
    df['price_change'] = df['close'].pct_change()
    df['log_return'] = np.log(df['close'] / df['close'].shift(1))
    df['volatility'] = df['high'] - df['low']
    df['volume_btc_log'] = np.log1p(df['volume_btc'])

    # Скользящие средние разных периодов
    for window in [5, 10, 20, 50]:
        df[f'ma_{window}'] = df['close'].rolling(window).mean()
        df[f'std_{window}'] = df['close'].rolling(window).std()

    # RSI
    df['rsi_14'] = compute_rsi(df['close'], 14)

    # Заполняем NaN
    df.fillna(0, inplace=True)

    # Целевая переменная
    df['target'] = df['close'].shift(-1)
    df.dropna(inplace=True)

    print(f"   Итоговое количество строк: {len(df)}")

    # Признаки
    feature_cols = ['open', 'high', 'low', 'close', 'volume_btc', 'volume_usd',
                    'price_change', 'log_return', 'volatility', 'volume_btc_log',
                    'ma_5', 'ma_10', 'ma_20', 'ma_50', 'std_5', 'std_10', 'std_20', 'std_50',
                    'rsi_14']

    X = df[feature_cols]
    y = df['target']

    print("4. Масштабирование...")
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # Разделение без перемешивания (первые 80% – тренировка, последние 20% – тест)
    split_idx = int(0.8 * len(X_scaled))
    X_train, X_test = X_scaled[:split_idx], X_scaled[split_idx:]
    y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]
    print(f"   Train: {len(X_train)}, Test: {len(X_test)}")

    print("5. Обучение RandomForest (n_estimators=100, max_depth=10)...")
    start_train = time.time()
    model = RandomForestRegressor(
        n_estimators=100,
        max_depth=10,
        random_state=42,
        n_jobs=-1,
        verbose=1
    )
    model.fit(X_train, y_train)
    train_time = time.time() - start_train
    print(f"   Обучение завершено за {train_time:.2f} сек.")

    score = model.score(X_test, y_test)
    print(f"6. R^2 на тесте (последние 20% данных): {score:.4f}")

    print("7. Сохранение модели и скейлера...")
    joblib.dump(model, 'models/model.pkl')
    joblib.dump(scaler, 'models/scaler.pkl')
    print("   Сохранено.")

    total_time = time.time() - start_total
    print(f"=== Всё готово! Общее время: {total_time:.2f} сек. ===")

if __name__ == '__main__':
    main()