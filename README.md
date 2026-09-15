# CM Collector

SNMP + Kafka telemetry collection tool for vCMTS and iCMTS lab testing. Collects per-poll modem metrics during LLD test sessions and stores them in SQLite for analysis.

GitHub: [birdyphillips/delta-docsis](https://github.com/birdyphillips/delta-docsis)

## Features

- **Web UI** — browser dashboard at `http://127.0.0.1:8000` to start/stop/delete sessions
- **SNMP collection** — per-poll modem US stats: flow octets/pkts, policed drop/delay, AQM drops, latency bins, congestion counters, QoS param set
- **Kafka collection** — real-time vCMTS `dp_flow_*` DS metrics: throughput, latency avg/max, AQM drops, CE marks, latency bins (vCMTS only)
- **Delta computation** — per-poll deltas computed for all SNMP counter fields, stored alongside raw cumulative values
- **SQLite persistence** — all rows written to `sessions.db` (`snmp_delta_rows`, `kafka_rows`) and CSV files
- **CMTS IPv6 auto-lookup** — SSH to CMTS to resolve modem IPv6 at session start
- **Debug mode** — single poll printed to stdout, no files written

## Output Structure

```
results/
└── <YYYYMMDD_HHMMSS>_<name>_<mac>_<type>/
    ├── kafka_<MAC>_<ts>.csv        (vCMTS only — DS Kafka metrics)
    ├── snmp_us_<MAC>_<ts>.csv      (modem US SNMP — raw + delta columns)
    ├── snmp_ds_<MAC>_<ts>.csv      (iCMTS only — DS SNMP)
    └── modem_info_<MAC>.txt
```

## API

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/` | Browser dashboard UI |
| `POST` | `/sessions/start` | Start a collection session |
| `POST` | `/sessions/{id}/stop` | Stop a running session |
| `GET` | `/sessions` | List all sessions |
| `DELETE` | `/sessions/{id}` | Delete a session |

## Usage

```bash
# Start the API server
python -m uvicorn cm_collector_api:app --reload --port 8000

# Interactive CLI collection
python cm_collector.py

# Debug mode — single poll, print to stdout, no files written
python cm_collector.py --debug
```

## Configuration

Copy `.env.example` to `.env` and fill in your values:

```bash
cp .env.example .env
```

| Variable | Description |
|---|---|
| `CMTS_HOST` | iCMTS host IP |
| `VCMTS_IP` | vCMTS IP |
| `KAFKA_BROKER` | Kafka broker `host:port` |
| `KAFKA_TOPIC` | Kafka topic name |
| `SNMP_JUMPSERVER` | Jump server hostname |
| `SNMP_USERNAME` | SSH username |
| `ICMTS_TARGET_IP` | iCMTS SNMP target IP |
| `SNMP_POLL_INTERVAL` | Poll interval in seconds (default: 15) |

## SNMP OIDs Collected (US — modem IPv6)

| Table | OID | Fields |
|---|---|---|
| `.2` Param Set (active) | `4491.2.1.21.1.2.1.{4,5,6,7,8,39,40,41,43}.2` | SCN, priority, max rate, burst, buffers, AQM target |
| `.4` Flow Stats | `4491.2.1.21.1.4` | pkts, octets, policed drop/delay, AQM drop |
| `.29.1` Latency Bin Edges | `4491.2.1.21.1.29.1` | bin edge config, AQM target |
| `.29.2` Latency Stats | `4491.2.1.21.1.29.2` | lat max usec, lat updates, bins 1–16 |
| `.30` Congestion | `4491.2.1.21.1.30` | sanctioned, ECT0, ECT1, CE marked, arrived CE |

## Requirements

```bash
pip install -r requirements.txt
```
