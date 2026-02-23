# MLOPS Batch Signal Generation Task

Batch job that computes rolling mean trading signals from OHLCV data.

## Local Run

### Install dependencies

```bash
pip install -r requirements.txt
```

### Run

```bash
python run.py --input data.csv --config config.yaml --output metrics.json --log-file run.log
```

## Docker Run

### Build

```bash
docker build -t mlops-task .
```

### Run

```bash
docker run --rm mlops-task
```

## Sample Output

**metrics.json:**

```json
{
  "version": "v1",
  "rows_processed": 9996,
  "metric": "signal_rate",
  "value": 0.4991,
  "latency_ms": 18,
  "seed": 42,
  "status": "success"
}
```

See `run.log` for detailed execution logs.
