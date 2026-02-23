import pandas as pd
import numpy as np
import logging
import yaml
from pathlib import Path
import json
import time
import argparse

def setup_logging(log_file):
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

def load_config(config_path):
    if not Path(config_path).exists():
        raise FileNotFoundError('Config file not found')
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
    except Exception:
        raise ValueError('Invalid YAML format')
    
    required_fields = ['seed', 'window', 'version']

    for field in required_fields:
        if field not in config:
            raise ValueError(f'Missing required config field: {field}')
    
    logging.info(f"Config loaded successfully, seed={config['seed']}, window={config['window']}, version={config['version']}.")

    np.random.seed(config['seed'])

    return config

def load_data(data_path):
    if not Path(data_path).exists():
        raise FileNotFoundError('Input CSV file not found.')
    
    try:
        df = pd.read_csv(data_path)
    except Exception:
        raise ValueError('Invalid CSV format.')
    
    if df.empty:
        raise ValueError('Input CSV is empty.')
    if 'close' not in df.columns:
        raise ValueError('Missing required column: close')
    
    logging.info(f'Dataset loaded successfully. Rows loaded: {len(df)}')

    return df

def compute_rolling_mean(df, window):
    logging.info(f'Computing rolling mean with window={window}...')
    df['rolling_mean'] = df['close'].rolling(window=window).mean()
    return df

def generate_signal(df):
    logging.info('Generating trading signal...')
    df['signal'] = np.where(df['close'] > df['rolling_mean'], 1, 0)
    df = df.dropna()
    return df

def compute_metrics(df, latency_ms, config):
    rows_processed = len(df)
    signal_rate = float(df['signal'].mean())

    metrics = {
        'version': config['version'],
        'rows_processed': rows_processed,
        'metric': 'signal_rate',
        'value': round(signal_rate, 4),
        'latency_ms': latency_ms,
        'seed': config['seed'],
        'status': 'success'
    }

    logging.info(f'Metrics computed successfully: {metrics}')

    return metrics

def write_metrics(metrics, output_path):
    try:
        with open(output_path, 'w') as f:
            json.dump(metrics, f, indent=2)
        logging.info('Metrics saved successfully.')
    except Exception:
        raise logging.exception('Failed to write metrics file.')
    
if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True)
    parser.add_argument('--config', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--log-file', required=True)

    args = parser.parse_args()
    setup_logging(args.log_file)
    logging.info('Job started.')
    config = None

    try:
        start_time = time.time()
        config = load_config(args.config)
        df = load_data(args.input)

        df = compute_rolling_mean(df, config['window'])
        df = generate_signal(df)

        latency_ms = int((time.time() - start_time) * 1000)
        metrics = compute_metrics(df, latency_ms, config)
        write_metrics(metrics, args.output)
        print(json.dumps(metrics, indent=2))
        logging.info('Job completed successfully.')
        exit(0)


    except Exception as e:
        version = 'v1'
        if config and 'version' in config:
            version = config['version']

        error_metrics = {
            'version': version,
            'status': 'error',
            'error_message': str(e)
        }

        write_metrics(error_metrics, args.output)
        print(json.dumps(error_metrics, indent=2))
        logging.exception(f'Job failed: {e}')
        exit(1)