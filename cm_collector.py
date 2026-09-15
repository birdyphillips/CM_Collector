#!/usr/bin/env python3
"""
CM Collector — interactive Kafka + SNMP data collection tool.

  vCMTS  →  Kafka (latency/throughput) + SNMP (modem-side US stats)
             + CMTS CLI modem info (scm <mac> ip/service-flow/qos)
  iCMTS  →  SNMP only (modem + iCMTS DS/US stats, up to 2 iCMTS targets)
             + CMTS CLI modem info (show cable modem cm-mac <mac>)

Output structure:
    results/
    └── <MAC>_<cmts_type>/
        └── <YYYYMMDD_HHMMSS>/
            ├── kafka_<MAC>_<ts>.csv      (vCMTS only — modem info in header comments)
            ├── snmp_modem_<MAC>_<ts>.csv (modem info in header comments)
            └── snmp_icmts_<MAC>_<ts>.csv (iCMTS only)

Usage:
    python cm_collector.py
"""
import re
import csv
import os
import sys
import time
import argparse
import threading
import paramiko
import logging
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.getLogger("paramiko").setLevel(logging.WARNING)
log = logging.getLogger('cm_collector')

try:
    from kafka import KafkaConsumer
    import logging as _logging
    for _n in ["kafka", "kafka.conn", "kafka.client", "kafka.consumer",
               "kafka.coordinator", "kafka.cluster", "kafka.protocol"]:
        _logging.getLogger(_n).setLevel(_logging.CRITICAL)
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

# ---------------------------------------------------------------------------
# Defaults — overridden by .env if present
# ---------------------------------------------------------------------------

def _load_env():
    """Load key=value pairs from .env in the script directory into os.environ.
    Always overrides existing env vars so the .env file is the source of truth.
    """
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
    if not os.path.exists(env_path):
        return
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            k, _, v = line.partition('=')
            os.environ[k.strip()] = v.strip()

_load_env()

def _e(key, fallback=''):
    return os.environ.get(key, fallback)

DEFAULT_KAFKA_BROKER        = _e('KAFKA_BROKER',        '65.185.232.139:11203')
DEFAULT_KAFKA_TOPIC         = _e('KAFKA_TOPIC',         'cmts_metrics_apc01k1dccc')
DEFAULT_SNMP_JUMPSERVER     = _e('SNMP_JUMPSERVER')
DEFAULT_SNMP_USERNAME       = _e('SNMP_USERNAME')
DEFAULT_CMTS_HOST           = _e('CMTS_HOST')
DEFAULT_TACACS_PASSWORD     = _e('TACACS_PASSWORD')
DEFAULT_VCMTS_HOST          = _e('VCMTS_HOST')
DEFAULT_VCMTS_IP            = _e('VCMTS_IP')
DEFAULT_MODEM_COMMUNITY     = _e('MODEM_COMMUNITY',     'open')
DEFAULT_ICMTS_COMMUNITY     = _e('ICMTS_COMMUNITY',     'NMISread')
DEFAULT_ICMTS_TARGET_IP      = _e('ICMTS_TARGET_IP')
DEFAULT_SNMP_TIMEOUT        = 5
DEFAULT_SNMP_RETRIES        = 2
DEFAULT_SNMP_POLL_INTERVAL  = int(_e('SNMP_POLL_INTERVAL', '15'))
DEFAULT_RESULTS_DIR         = _e('RESULTS_DIR', 'results')

# ---------------------------------------------------------------------------
# Kafka metrics (vCMTS only)
# ---------------------------------------------------------------------------
KAFKA_METRICS = {
    # flow counters (mapped → CSV columns)
    'dp_flow_QueueLatencyMaxUsec',
    'dp_flow_QueueLatencyAvgUsec',
    'dp_flow_QueueLatencyBinPktCount',
    'dp_flow_AqmDroppedPackets',
    'dp_flow_AqmMarkedCongestedPackets',
    'dp_flow_SanctionedPackets',
    'K_Samis1_DeltaPacketsPassed',
    'K_Samis1_DeltaOctetsPassed',
    'K_Samis1_DeltaPacketsDropped',
    'snmp_docsQosServiceFlowPackets',
    'snmp_docsQosServiceFlowOctets',
    # sfid + params (handled explicitly, not via _KAFKA_METRIC_MAP)
    'K_DocsQos_Params',
    'K_Samis1_Sfid',
}

KAFKA_CSV_FIELDS = [
    'captured_utc', 'kafka_timestamp_ms', 'dir', 'sfIndex', 'sfid', 'scn',
    'mdName', 'node', 'pod', 'cluster',
    # flow counters
    'delta_octets', 'delta_pkts', 'delta_pkts_dropped',
    'total_octets', 'total_pkts',
    # latency
    'lat_avg_usec', 'lat_max_usec',
    # congestion
    'aqm_drop_pkts', 'aqm_marked_pkts', 'sanctioned_pkts',
    # latency bins (16)
    'lat_bin01', 'lat_bin02', 'lat_bin03', 'lat_bin04',
    'lat_bin05', 'lat_bin06', 'lat_bin07', 'lat_bin08',
    'lat_bin09', 'lat_bin10', 'lat_bin11', 'lat_bin12',
    'lat_bin13', 'lat_bin14', 'lat_bin15', 'lat_bin16',
    # bin edges (first/last)
    'bin01_lower_msec', 'bin16_upper_msec',
    # QoS params
    'max_rate_bps', 'aqm_target_msecs',
]

# Counter fields that accumulate — deltas are computed between polls
SNMP_DELTA_COUNTER_FIELDS = [
    'flow_pkts', 'flow_octets', 'flow_policed_drop', 'flow_policed_delay', 'flow_aqm_drop',
    'lat_updates',
    'lat_bin1', 'lat_bin2', 'lat_bin3', 'lat_bin4', 'lat_bin5', 'lat_bin6', 'lat_bin7', 'lat_bin8',
    'lat_bin9', 'lat_bin10', 'lat_bin11', 'lat_bin12', 'lat_bin13', 'lat_bin14', 'lat_bin15', 'lat_bin16',
    'cong_sanctioned', 'cong_ect0', 'cong_ect1', 'cong_ce_marked', 'cong_arrived_ce',
]

SNMP_CSV_FIELDS = [
    'captured_utc', 'poll_index', 'phase', 'target_ip', 'target_label', 'cmts_type', 'sfid',
    # .3 SF Table (buffer size only — direction/primary/sid/agg not supported on vCMTS modem)
    'sf_buffer_size',
    # .2 Param Set (active)
    'ps_scn', 'ps_priority', 'ps_max_rate', 'ps_max_burst',
    'ps_max_concat_burst', 'ps_aqm_latency_target',
    'ps_min_buffer', 'ps_target_buffer', 'ps_max_buffer',
    # .4 Flow Stats
    'flow_pkts', 'flow_octets', 'flow_policed_drop', 'flow_policed_delay', 'flow_aqm_drop',
    # .29.1 Latency Bin Edges
    'lat_bin_scn', 'lat_aqm_target',
    'lat_edge_bin1', 'lat_edge_bin2', 'lat_edge_bin3', 'lat_edge_bin4',
    'lat_edge_bin5', 'lat_edge_bin6', 'lat_edge_bin7', 'lat_edge_bin8',
    'lat_edge_bin9', 'lat_edge_bin10', 'lat_edge_bin11', 'lat_edge_bin12',
    'lat_edge_bin13', 'lat_edge_bin14', 'lat_edge_bin15',
    # .29.2 Latency Stats
    'lat_max_usec', 'lat_updates',
    'lat_bin1', 'lat_bin2', 'lat_bin3', 'lat_bin4', 'lat_bin5', 'lat_bin6', 'lat_bin7', 'lat_bin8',
    'lat_bin9', 'lat_bin10', 'lat_bin11', 'lat_bin12', 'lat_bin13', 'lat_bin14', 'lat_bin15', 'lat_bin16',
    # .30 Congestion  (order matches OID .30.1.1–.30.1.5)
    'cong_sanctioned', 'cong_ect0', 'cong_ect1', 'cong_ce_marked', 'cong_arrived_ce',
]

# Delta CSV adds delta_ columns after the raw counters
SNMP_DELTA_CSV_FIELDS = SNMP_CSV_FIELDS + [f'delta_{f}' for f in SNMP_DELTA_COUNTER_FIELDS]

# ---------------------------------------------------------------------------
# SNMP delta computation
# ---------------------------------------------------------------------------

# Module-level store: (session_id, target_label, sfid) → {field: int}
_snmp_prev: dict = {}


def _compute_snmp_deltas(rows, session_id=''):
    """Augment each row with delta_<field> columns for all counter fields.
    Skips negative deltas (counter reset / SFID reuse) per project convention.
    Mutates rows in-place and returns them.
    """
    for row in rows:
        key = (session_id, row.get('target_label', ''), row.get('sfid', ''))
        prev = _snmp_prev.get(key, {})
        for field in SNMP_DELTA_COUNTER_FIELDS:
            raw = row.get(field, '')
            try:
                cur = int(raw)
            except (TypeError, ValueError):
                row[f'delta_{field}'] = ''
                continue
            if field in prev:
                delta = cur - prev[field]
                row[f'delta_{field}'] = '' if delta < 0 else str(delta)
            else:
                row[f'delta_{field}'] = ''  # first poll — no prior value
            prev[field] = cur
        _snmp_prev[key] = prev
    return rows


# OID column index → CSV field name, keyed by col_path after stripping index suffix
# For most tables: col_path = strip last 2 (ifindex + sfid)
# For .2 param set: col_path = strip last 3 (ifindex + paramset_type + sfid), only type=2
_OID_COL_MAP = {
    # .3 SF Table
    '3.1.17': 'sf_buffer_size',  # docsQosServiceFlowBufferSize (still walked via .4 pivot)
    # .4 Flow Stats
    '4.1.1':  'flow_pkts',
    '4.1.2':  'flow_octets',
    '4.1.6':  'flow_policed_drop',
    '4.1.7':  'flow_policed_delay',
    '4.1.8':  'flow_aqm_drop',
    # .29.1 Latency Bin Edges
    '29.1.1.2':  'lat_bin_scn',
    '29.1.1.3':  'lat_edge_bin1',
    '29.1.1.4':  'lat_edge_bin2',
    '29.1.1.5':  'lat_edge_bin3',
    '29.1.1.6':  'lat_edge_bin4',
    '29.1.1.7':  'lat_edge_bin5',
    '29.1.1.8':  'lat_edge_bin6',
    '29.1.1.9':  'lat_edge_bin7',
    '29.1.1.10': 'lat_edge_bin8',
    '29.1.1.11': 'lat_edge_bin9',
    '29.1.1.12': 'lat_edge_bin10',
    '29.1.1.13': 'lat_edge_bin11',
    '29.1.1.14': 'lat_edge_bin12',
    '29.1.1.15': 'lat_edge_bin13',
    '29.1.1.16': 'lat_edge_bin14',
    '29.1.1.17': 'lat_edge_bin15',
    '29.1.1.18': 'lat_aqm_target',
    # .29.2 Latency Stats
    '29.2.1.1':  'lat_max_usec',
    '29.2.1.2':  'lat_updates',
    '29.2.1.3':  'lat_bin1',
    '29.2.1.4':  'lat_bin2',
    '29.2.1.5':  'lat_bin3',
    '29.2.1.6':  'lat_bin4',
    '29.2.1.7':  'lat_bin5',
    '29.2.1.8':  'lat_bin6',
    '29.2.1.9':  'lat_bin7',
    '29.2.1.10': 'lat_bin8',
    '29.2.1.11': 'lat_bin9',
    '29.2.1.12': 'lat_bin10',
    '29.2.1.13': 'lat_bin11',
    '29.2.1.14': 'lat_bin12',
    '29.2.1.15': 'lat_bin13',
    '29.2.1.16': 'lat_bin14',
    '29.2.1.17': 'lat_bin15',
    '29.2.1.18': 'lat_bin16',
    # .30 Congestion  (docsQosSfCongestion table — verified against OID JSON)
    '30.1.1': 'cong_sanctioned',   # docsQosSfCongestionSanctionedPkts
    '30.1.2': 'cong_ect0',         # docsQosSfCongestionTotalEct0Pkts
    '30.1.3': 'cong_ect1',         # docsQosSfCongestionTotalEct1Pkts
    '30.1.4': 'cong_ce_marked',    # docsQosSfCongestionCeMarkedEct1Pkts
    '30.1.5': 'cong_arrived_ce',   # docsQosSfCongestionArrivedCePkts
}

# .2 Param Set Table — strip last 3 (ifindex + paramset_type + sfid), only type=2 (active)
_OID_PARAM_MAP = {
    '2.1.4':  'ps_scn',
    '2.1.5':  'ps_priority',
    '2.1.6':  'ps_max_rate',
    '2.1.7':  'ps_max_burst',
    '2.1.8':  'ps_max_concat_burst',
    '2.1.39': 'ps_min_buffer',
    '2.1.40': 'ps_target_buffer',
    '2.1.41': 'ps_max_buffer',
    '2.1.43': 'ps_aqm_latency_target',
}

_RE_OID_SFID = re.compile(r'21\.1\.(\d+(?:\.\d+)*)\s*=\s*\S+:\s*(.*)')

# Prometheus exposition format: metric_name{labels} value timestamp_ms
RE_PROM = re.compile(r'^(\w+)\{([^}]*)\}\s+([\d.eE+\-]+)\s+(\d+)$')


def _pivot_results(results, ts, poll_idx, target_ip, target_label, cmts_type):
    """Parse SNMP results and pivot to one dict per SFID."""
    sfid_rows = {}

    def _get_or_create(sfid):
        if sfid not in sfid_rows:
            sfid_rows[sfid] = {
                'captured_utc': ts, 'poll_index': poll_idx,
                'target_ip': target_ip, 'target_label': target_label,
                'cmts_type': cmts_type, 'sfid': sfid,
            }
        return sfid_rows[sfid]

    for _label, output in results:
        for line in output.splitlines():
            m = _RE_OID_SFID.search(line)
            if not m:
                continue
            parts = m.group(1).split('.')
            val   = m.group(2).strip().strip('"')

            # .2 param set: targeted walks return col.2.ifindex.sfid
            # The .2 (active type) is already in the OID prefix walked,
            # so accept both the old full form and the new targeted form.
            if parts[0] == '2' and len(parts) >= 3:
                sfid = parts[-1]
                field = _OID_PARAM_MAP.get('.'.join(parts[:-3])) or \
                        _OID_PARAM_MAP.get('.'.join(parts[:-2]))
                if field:
                    _get_or_create(sfid)[field] = val
                continue

            # all other tables: index is col.ifindex.sfid (strip 2)
            if len(parts) < 3:
                continue
            sfid     = parts[-1]
            col_path = '.'.join(parts[:-2])
            field    = _OID_COL_MAP.get(col_path)
            if field:
                _get_or_create(sfid)[field] = val

    return list(sfid_rows.values())

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _norm_mac(mac):
    return mac.strip().replace(':', '').replace('.', '').replace('-', '').lower()

def _mac_colon(mac_norm):
    return ':'.join(mac_norm[i:i+2] for i in range(0, 12, 2))

def _mac_to_decimal(mac_norm):
    """Convert MAC to dotted-decimal OID suffix for iCMTS SFID grep.
    e.g. 606c63c469fc → 96.108.99.196.105.252
    """
    return '.'.join(str(int(mac_norm[i:i+2], 16)) for i in range(0, 12, 2))

def _prompt(label, default=None):
    suffix = f' [{default}]' if default not in (None, '') else ''
    val = input(f'  {label}{suffix}: ').strip()
    return val if val else (default or '')

def _prompt_choice(label, choices, default=None):
    opts = '/'.join(choices)
    while True:
        val = input(f'  {label} ({opts}): ').strip().lower()
        if not val and default:
            return default.lower()
        if val in [c.lower() for c in choices]:
            return val
        print(f'    Please enter one of: {opts}')

def _make_session_dir(results_root, mac_norm, cmts_type, ts_str):
    session_dir = os.path.join(results_root, f'{mac_norm}_{cmts_type}', ts_str)
    os.makedirs(session_dir, exist_ok=True)
    return session_dir

def _norm_mac_dotted(mac_norm):
    """Convert normalized MAC to Cisco dotted format e.g. 206a.9492.23b8"""
    return f'{mac_norm[0:4]}.{mac_norm[4:8]}.{mac_norm[8:12]}'


def _parse_ipv6(output):
    """Extract first IPv6 address found in output."""
    m = re.search(r'([0-9a-fA-F]{1,4}(?::[0-9a-fA-F]{0,4}){5,7})', output)
    return m.group(1) if m else None


def _ssh_connect(jumpserver, username):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    key_path = os.path.expanduser(_e('SSH_KEY_PATH', '~/.ssh/lld_key'))
    if os.path.exists(key_path):
        ssh.connect(jumpserver, username=username, key_filename=key_path, timeout=15)
    else:
        password = _e('TACACS_PASSWORD') or None
        ssh.connect(jumpserver, username=username, password=password, timeout=15)
    return ssh

def _get_ssh(jumpserver, username):
    ssh = _ssh_connect(jumpserver, username)
    return ssh

def _run_local(cmds, lbls):
    """Run SNMP commands locally in parallel (2 workers) via subprocess."""
    import subprocess
    from concurrent.futures import ThreadPoolExecutor, as_completed

    mid = (len(cmds) + 1) // 2
    batches = [(cmds[:mid], lbls[:mid]), (cmds[mid:], lbls[mid:])]

    def _run_batch(batch_cmds, batch_lbls):
        results = []
        for label, cmd in zip(batch_lbls, batch_cmds):
            print(f'  … {label}', flush=True)
            try:
                proc = subprocess.run(cmd, shell=True, capture_output=True, timeout=300)
                stdout = proc.stdout.decode(errors='replace')
                stderr = proc.stderr.decode(errors='replace').strip()
                if proc.returncode != 0 and not stdout:
                    print(f'  ✗ [{label}] exit={proc.returncode}  {stderr or "(no output)"}')
                elif stderr:
                    print(f'  ⚠ [{label}] stderr: {stderr}')
                results.append((label, stdout))
            except subprocess.TimeoutExpired:
                print(f'  ✗ [{label}] timed out after 300s')
                results.append((label, ''))
            except Exception as e:
                print(f'  ✗ [{label}] failed: {e}')
                results.append((label, ''))
        return results

    results_map = {}
    with ThreadPoolExecutor(max_workers=2) as ex:
        futures = [ex.submit(_run_batch, bc, bl) for bc, bl in batches if bc]
        for fut in as_completed(futures):
            for label, out in fut.result():
                results_map[label] = out
    return [(lbl, results_map.get(lbl, '')) for lbl in lbls]


def _run_via_ssh(jumpserver, username, cmds, lbls):
    """Run SNMP commands using 2 SSH connections in parallel — commands split evenly."""
    mid = (len(cmds) + 1) // 2
    batches = [(cmds[:mid], lbls[:mid]), (cmds[mid:], lbls[mid:])]

    def _run_batch(batch_cmds, batch_lbls):
        conn = _ssh_connect(jumpserver, username)
        results = []
        try:
            for label, cmd in zip(batch_lbls, batch_cmds):
                print(f'  … {label}', flush=True)
                try:
                    _, stdout, stderr = conn.exec_command(cmd)
                    out = stdout.read().decode(errors='replace')
                    err = stderr.read().decode(errors='replace').strip()
                    if not out and err:
                        print(f'  ✗ [{label}] {err}')
                    results.append((label, out))
                except Exception as e:
                    print(f'  ✗ [{label}] failed: {e}')
                    results.append((label, ''))
        finally:
            conn.close()
        return results

    results_map = {}
    with ThreadPoolExecutor(max_workers=2) as ex:
        futures = [ex.submit(_run_batch, bc, bl) for bc, bl in batches if bc]
        for fut in as_completed(futures):
            try:
                for label, out in fut.result():
                    results_map[label] = out
            except Exception as e:
                print(f'  ✗ batch failed: {e}')
    return [(lbl, results_map.get(lbl, '')) for lbl in lbls]

def _parse_snmp_output(output):
    rows = []
    for line in output.splitlines():
        line = line.strip()
        if not line:
            continue
        m = re.match(r'(?:SNMPv2-SMI::|iso\.|enterprises\.)(.+?)\s*=\s*(\w+):\s*(.*)', line)
        if m:
            rows.append((m.group(1).strip(), m.group(2).strip(), m.group(3).strip()))
        else:
            m2 = re.match(r'(.+?)\s*=\s*(.*)', line)
            if m2:
                rows.append((m2.group(1).strip(), 'RAW', m2.group(2).strip()))
    return rows

# ---------------------------------------------------------------------------
# SNMP command sets
# ---------------------------------------------------------------------------

def _icmts_snmp_commands(icmts_ip, modem_ip, icmts_community, modem_community, timeout, retries, mac_decimal):
    """Build DS and US SNMP command sets.

    iCMTS DS notes (verified against E6000 CER V14):
      .4   — flow stats: populated for all active SFIDs, reliable per-modem data
      .29  — latency bins: sparse, only AQM-enabled SCNs (e.g. dsHSI018-LL), not per-modem
      .30  — congestion: sparse, only AQM-enabled flows, not per-modem
      .27  — aggregate SF stats: not implemented on E6000 CER V14 (No Such Object)
      .2   — QoS params: CMTS-wide static config, walks entire table, hangs on large CMTS
      .8   — service class names: CMTS-wide static config, not useful for polling
      Cadant .4998 — channel-level aggregates, not per-modem

    US notes:
      All US OIDs target modem IPv6 directly (community: open).
      Modem IPv6 is only reachable from jump server, not locally.
      .3    — SF table: direction, primary, SID, time created, buffer size
      .3.1.19 — aggregate SFID map (SFID → ASF parent, 0 = no aggregate)
      .2    — param set: SCN, priority, rates, buffers, AQM latency target
      .8    — service class name table
      .4    — flow stats: pkts, octets, policed drop/delay, AQM drop
      .29.1 — latency bin edge config
      .29.2 — latency bins: per-bin pkt counts + max latency
      .30   — congestion: AQM drops, sanctioned, ECT, CE marked
      .27   — aggregate SF stats: not implemented on E6000 CER V14 (No Such Object)
    """
    t, r = timeout, retries
    ds_cmds = [
        f"snmpwalk -v 2c -c {icmts_community} -t {t} -r {r} {icmts_ip} 1.3.6.1.4.1.4491.2.1.21.1.11.1",
        f"snmpwalk -v 2c -c {icmts_community} -t {t} -r {r} {icmts_ip} 1.3.6.1.4.1.4491.2.1.21.1.4",
        f"snmpwalk -v 2c -c {icmts_community} -t {t} -r {r} {icmts_ip} 1.3.6.1.4.1.4491.2.1.21.1.29",
        f"snmpwalk -v 2c -c {icmts_community} -t {t} -r {r} {icmts_ip} 1.3.6.1.4.1.4491.2.1.21.1.30",
        f"snmpbulkget -v 2c -c {icmts_community} -t {t} -r {r} {icmts_ip} .1.3.6.1.4.1.4998.1.1.15.10.2",
        f"snmpbulkget -v 2c -c {icmts_community} -t {t} -r {r} {icmts_ip} .1.3.6.1.4.1.4998.1.1.15.10.8",
    ]
    ds_lbls = [
        'DS SF Index Table',
        'DS Flow Stats Table',
        'DS Latency Stats',       # sparse — AQM-enabled flows only, not per-modem
        'DS Congestion Stats',    # sparse — AQM-enabled flows only, not per-modem
        'DS Cadant Map Stats',    # channel-level, not per-modem
        'DS Map Stats Pages Flows',
    ]
    base = f'snmpwalk -v 2c -c {modem_community} -t {t} -r {r} {modem_ip}'
    # Walk only the specific active (type=2) param-set columns needed instead of
    # the entire .21.1.2 subtree (54+ columns × 3 param-set types × all SFIDs).
    param_oids = [
        '1.3.6.1.4.1.4491.2.1.21.1.2.1.4.2',   # ps_scn
        '1.3.6.1.4.1.4491.2.1.21.1.2.1.5.2',   # ps_priority
        '1.3.6.1.4.1.4491.2.1.21.1.2.1.6.2',   # ps_max_rate
        '1.3.6.1.4.1.4491.2.1.21.1.2.1.7.2',   # ps_max_burst
        '1.3.6.1.4.1.4491.2.1.21.1.2.1.8.2',   # ps_max_concat_burst
        '1.3.6.1.4.1.4491.2.1.21.1.2.1.39.2',  # ps_min_buffer
        '1.3.6.1.4.1.4491.2.1.21.1.2.1.40.2',  # ps_target_buffer
        '1.3.6.1.4.1.4491.2.1.21.1.2.1.41.2',  # ps_max_buffer
        '1.3.6.1.4.1.4491.2.1.21.1.2.1.43.2',  # ps_aqm_latency_target
    ]
    us_cmds = [
        ' && '.join(f"{base} {o}" for o in param_oids),
        f"{base} 1.3.6.1.4.1.4491.2.1.21.1.4",
        f"{base} 1.3.6.1.4.1.4491.2.1.21.1.29.1",
        f"{base} 1.3.6.1.4.1.4491.2.1.21.1.29.2",
        f"{base} 1.3.6.1.4.1.4491.2.1.21.1.30",
        f"snmpbulkget -v 2c -c {modem_community} -t {t} -r {r} {modem_ip} .1.3.6.1.4.1.4998.1.1.15.10.2",
        f"snmpbulkget -v 2c -c {modem_community} -t {t} -r {r} {modem_ip} .1.3.6.1.4.1.4998.1.1.15.10.8",
    ]
    us_lbls = [
        'US Param Set Table',       # SCN, priority, rates, buffers, AQM target (active only)
        'US Flow Stats Table',      # pkts, octets, policed drop/delay, AQM drop
        'US Latency Bin Edges',     # bin edge config (.29.1)
        'US Latency Stats Table',   # per-bin pkt counts + max latency (.29.2)
        'US Congestion Stats Table',# AQM drops, sanctioned, ECT, CE marked (.30)
        'Cadant Map Stats',
        'Map Stats Pages Flows',
    ]
    return (ds_cmds, ds_lbls), (us_cmds, us_lbls)


# Column index → human label for docsQosServiceFlowStats (.29.x)
# col index → (label, width) for .29.x latency summary
_LATENCY_COLS = [
    ('4',  'PktsEnq',     10),
    ('5',  'OctetsEnq',   12),
    ('6',  'AQMDrops',    10),
    ('11', 'SCNMarked',   10),
    ('14', 'PktsPassed',  12),
    ('15', 'OctetsPassed',14),
    ('18', 'LatMaxUsec',  12),
]

# col index → (label, width) for .30 congestion summary
_CONGESTION_COLS = [
    ('1', 'Sanctioned', 12),
    ('2', 'ECT0',       10),
    ('3', 'ECT1',       10),
    ('4', 'CEMarked',   10),
    ('5', 'ArrivedCE',  10),
]

# .4 flow stats: col 1=Pkts, 2=Octets, 6=PolicedDrop, 7=PolicedDelay, 8=AQMDrop
_FLOW_STATS_COLS = [
    ('1', 'Pkts',         12),
    ('2', 'Octets',       16),
    ('6', 'PolicedDrop',  12),
    ('7', 'PolicedDelay', 12),
    ('8', 'AQMDrop',      10),
]


def _print_snmp_section(label, rows):
    print(f'  ▶ {label}  ({len(rows)} rows)')
    for oid, dtype, val in rows:
        print(f'      {oid}  [{dtype}]  {val}')
    if not rows:
        print('      (no data)')
    print()


def _print_cadant_section(label, output):
    """Summarize Cadant .4998 channel stats — show channel index and value."""
    if not output.strip():
        print(f'  \u25b6 {label}  (no data)\n')
        return
    rows = []
    for line in output.splitlines():
        m = re.match(r'.*\.10\.\d+\.1\.1\.(\d+)\s*=\s*\w+:\s*(.*)', line)
        if m:
            rows.append((m.group(1), m.group(2).strip()))
    if not rows:
        print(f'  \u25b6 {label}  (no data)\n')
        return
    print(f'  \u25b6 {label}')
    print(f'  {"ChanIdx":>10}  {"Value":>20}')
    print(f'  {"-" * 34}')
    for idx, val in rows:
        print(f'  {idx:>10}  {val:>20}')
    print()


def _print_latency_section(label, output, sfids, col_defs):
    """Print .29.x or .30 table summarized per SFID — key metrics only."""
    if not output.strip():
        print(f'  ▶ {label}  (no data)\n')
        return

    data = {}  # sfid → {col_idx: val}
    for line in output.splitlines():
        m = re.match(r'.*\.1\.(\d+)\.(\d+)\.(\d+)\s*=\s*\w+:\s*(.*)', line)
        if m:
            col, _ifidx, sfid, val = m.group(1), m.group(2), m.group(3), m.group(4).strip()
            if sfids and sfid not in sfids:
                continue
            data.setdefault(sfid, {})[col] = val

    if not data:
        print(f'  ▶ {label}  (no data)\n')
        return

    active = [(c, lbl, w) for c, lbl, w in col_defs
              if any(data[s].get(c, '0') not in ('0', '-', '') for s in data)]
    if not active:
        active = col_defs

    hdr = '  {:>10}  '.format('SFID') + '  '.join(f'{lbl:>{w}}' for _, lbl, w in active)
    print(f'  ▶ {label}')
    print(hdr)
    print('  ' + '-' * (len(hdr) - 2))
    for sfid in sorted(data.keys(), key=int):
        vals = [data[sfid].get(c, '-') for c, _, _ in active]
        print('  {:>10}  '.format(sfid) + '  '.join(f'{v:>{w}}' for v, (_, _, w) in zip(vals, active)))
    print()


def _filter_by_mac(output, mac_decimal):
    """Filter output by MAC decimal OID suffix (for MAC-indexed tables like .11.1)."""
    if not mac_decimal or not output.strip():
        return output
    filtered = [line for line in output.splitlines() if mac_decimal in line]
    return '\n'.join(filtered) if filtered else output


def _extract_sfids(sf_index_output, mac_decimal):
    """Extract all SFIDs for this modem from SF Index Table output.
    OID format: ...11.1.3.<mac_decimal>.<sfid> = INTEGER: <ifIndex>
    """
    sfids = []
    for line in sf_index_output.splitlines():
        if mac_decimal and mac_decimal not in line:
            continue
        m = re.search(re.escape(mac_decimal) + r'\.(\d+)\s*=', line)
        if m:
            sfids.append(m.group(1))
    return sfids


def _filter_by_sfids(output, sfids):
    """Filter output keeping only lines whose last OID component is one of the SFIDs."""
    if not sfids or not output.strip():
        return output
    filtered = [line for line in output.splitlines()
                if re.search(r'\.(' + '|'.join(sfids) + r')\s*=', line)]
    return '\n'.join(filtered) if filtered else output


def _extract_sfid_ifindex(sf_index_output, mac_decimal):
    """Extract {sfid: ifindex} for this modem from SF Index Table output.
    OID format: ...11.1.3.<mac_decimal>.<sfid> = INTEGER: <ifIndex>
    """
    result = {}
    for line in sf_index_output.splitlines():
        if mac_decimal and mac_decimal not in line:
            continue
        m = re.search(re.escape(mac_decimal) + r'\.(\d+)\s*=\s*\S+:\s*(\d+)', line)
        if m:
            result[m.group(1)] = m.group(2)
    return result


def _pivot_results_ds(results, ts, poll_idx, target_ip, cmts_type, sfid_ifindex):
    """Parse DS SNMP results and return rows filtered to modem SFIDs via inner join.
    sfid_ifindex: {sfid: ifindex} from _extract_sfid_ifindex.
    Only rows whose (ifindex, sfid) match the modem's SF table are kept.
    """
    # Build reverse map: ifindex -> set of sfids for this modem
    ifindex_sfids = {}
    for sfid, ifindex in sfid_ifindex.items():
        ifindex_sfids.setdefault(ifindex, set()).add(sfid)

    sfid_rows = {}

    def _get_or_create(sfid):
        if sfid not in sfid_rows:
            sfid_rows[sfid] = {
                'captured_utc': ts, 'poll_index': poll_idx,
                'target_ip': target_ip, 'target_label': 'icmts_ds',
                'cmts_type': cmts_type, 'sfid': sfid,
            }
        return sfid_rows[sfid]

    for _label, output in results:
        if _label == 'DS SF Index Table':
            continue
        for line in output.splitlines():
            m = _RE_OID_SFID.search(line)
            if not m:
                continue
            parts = m.group(1).split('.')
            val   = m.group(2).strip().strip('"')
            if len(parts) < 3:
                continue
            sfid    = parts[-1]
            ifindex = parts[-2]
            # Inner join: only keep if (ifindex, sfid) matches this modem
            if sfid not in sfid_ifindex or sfid_ifindex[sfid] != ifindex:
                continue
            # Latency .29: filter to A==2 rows (stats, not bin edges)
            col_path = '.'.join(parts[:-2])
            if col_path.startswith('29.') and parts[0] != '2':
                continue
            field = _OID_COL_MAP.get(col_path)
            if field:
                _get_or_create(sfid)[field] = val

    return list(sfid_rows.values())

# ---------------------------------------------------------------------------
# CMTS modem info collector (SSH jump → CMTS CLI)
# ---------------------------------------------------------------------------

def _cmts_commands(cmts_type, mac_dotted):
    """Return (commands, labels) for the given CMTS type."""
    if cmts_type == 'icmts':
        cmds = [
            f'show cable modem cm-mac {mac_dotted}',
            f'show cable modem cm-mac {mac_dotted} verbose',
            f'show cable modem cm-mac {mac_dotted} service-flow',
        ]
        lbls = [
            'Cable Modem Summary',
            'Cable Modem Verbose',
            'Service Flow Information',
        ]
    else:  # vcmts
        cmds = [
            f'scm {mac_dotted} ip',
            f'scm {mac_dotted} service-flow aqm',
            f'scm {mac_dotted} cpe',
            f'scm {mac_dotted} qos bps',
        ]
        lbls = [
            'IP Address Information',
            'Service Flow AQM',
            'CPE Information',
            'QoS Bandwidth',
        ]
    return cmds, lbls


def _run_cmts_command(ssh, cmts_host, username, password, cmd):
    """Open interactive shell on jump server, SSH to CMTS, run one command."""
    shell = ssh.invoke_shell()
    shell.send(f'ssh -o StrictHostKeyChecking=no {username}@{cmts_host}\n')
    # Wait up to 8s for password prompt — vCMTS/iCMTS both use TACACS which may take a moment
    deadline = time.time() + 8
    buf = ''
    while time.time() < deadline:
        time.sleep(0.3)
        if shell.recv_ready():
            buf += shell.recv(8192).decode(errors='replace')
        if 'password' in buf.lower() or 'password:' in buf.lower():
            break
    if 'password' in buf.lower():
        shell.send(password + '\n')
        time.sleep(1.5)
        buf += shell.recv(8192).decode(errors='replace')
    shell.send(cmd + '\n')
    time.sleep(2)
    output = ''
    for _ in range(60):
        if shell.recv_ready():
            chunk = shell.recv(8192).decode(errors='replace')
            output += chunk
            if '--More--' in chunk:
                shell.send(' ')
                time.sleep(0.3)
        else:
            time.sleep(0.2)
            if not shell.recv_ready():
                break
    shell.send('exit\n')
    shell.close()
    return output


NOT_FOUND_PATTERNS = {
    'vcmts': ['not found'],
    'icmts': ['no cms were found', 'no cable modem', 'no entry', 'does not exist'],
}

def _is_modem_not_found(output, cmts_type):
    low = output.lower()
    return any(p in low for p in NOT_FOUND_PATTERNS.get(cmts_type, [])) or len(output.strip()) == 0


def modem_info_collector(cfg):
    """Collect CMTS CLI modem info once at session start.
    Returns dict with cm_ipv6 and raw output per section, or None on failure.
    """
    jumpserver = cfg['snmp_jumpserver']
    username   = cfg['snmp_username']
    cmts_host  = cfg.get('cmts_host', '')
    cmts_pass  = cfg.get('cmts_password', '')
    cmts_type  = cfg['cmts_type']
    mac_dotted = _norm_mac_dotted(cfg['mac_norm'])

    if not jumpserver or not username or not cmts_host:
        missing = [k for k, v in [('jumpserver', jumpserver), ('username', username), ('cmts_host', cmts_host)] if not v]
        print(f'[CMTS] Skipping modem info — missing config: {", ".join(missing)}')
        return None

    try:
        ssh = _get_ssh(jumpserver, username)
        print(f'  ✔ Connected to {jumpserver}')
    except Exception as e:
        print(f'  ✗ SSH failed: {e} — skipping modem info')
        return None

    cmds, lbls = _cmts_commands(cmts_type, mac_dotted)
    cm_ipv6    = None
    sections   = {}

    for i, (label, cmd) in enumerate(zip(lbls, cmds)):
        try:
            output = _run_cmts_command(ssh, cmts_host, username, cmts_pass, cmd)
        except Exception as e:
            print(f'  ✗ CMTS command failed: {e}')
            output = f'ERROR: {e}'
        sections[label] = output
        if i == 0:
            if _is_modem_not_found(output, cmts_type):
                ssh.close()
                print(f'  ✗ Modem not found on {cmts_host}')
                return {'not_found': True, 'cm_ipv6': None, 'cmts_host': cmts_host, 'sections': sections}
            print(f'  ✔ Connected to {cmts_host}')
        if cm_ipv6 is None:
            cm_ipv6 = _parse_ipv6(output)
            if cm_ipv6:
                print(f'  ✔ Modem IPv6: {cm_ipv6}')

    ssh.close()
    print('  ✔ Modem info collected')
    return {'cm_ipv6': cm_ipv6, 'cmts_host': cmts_host, 'sections': sections}


def _write_modem_info_comments(f, modem_info, cfg):
    """Write a single comment line at the top of a CSV file."""
    ts = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    f.write(f'# CM Collector — {cfg["mac_colon"]}  {cfg["cmts_type"].upper()}  collected {ts} UTC\n')


def _write_modem_info_txt(session_dir, modem_info, cfg):
    """Write modem info to a clean human-readable sidecar .txt file."""
    path = os.path.join(session_dir, f'modem_info_{cfg["mac_norm"]}.txt')
    ts   = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

    lines = []
    lines.append(f'CM Collector — {ts} UTC')
    lines.append(f'MAC       : {cfg["mac_colon"]}')
    lines.append(f'CMTS type : {cfg["cmts_type"].upper()}')

    if modem_info:
        lines.append(f'CMTS host : {modem_info["cmts_host"]}')
        lines.append(f'IPv6      : {modem_info["cm_ipv6"] or "unknown"}')

        # Parse verbose output for structured fields
        verbose = modem_info['sections'].get('Cable Modem Verbose', '')
        if verbose:
            lines.append('')
            lines.append('--- Modem Status ---')
            for line in verbose.splitlines():
                line = line.strip()
                if not line or line.startswith('show ') or line.startswith('Aug ') or line == 'cts01k1dccc#':
                    continue
                # Service flow table header
                if line.startswith('u/d') and 'SFID' in line:
                    lines.append('')
                    lines.append('--- Service Flows ---')
                    lines.append(line)
                    continue
                # Service flow rows
                if re.match(r'^[ud][BC]\s+\d+', line):
                    lines.append(line)
                    continue
                # Skip footer lines
                if line.startswith('L2VPN') or line.startswith('Current CPE') or line.startswith('Slot/Channels'):
                    continue
                # Key info lines
                if any(kw in line for kw in [
                    'State=', 'Uptime=', 'OFDM=', 'OFDMA=', 'MODEM CAPABILITY',
                    'Privacy=', 'Timing Offset', 'Cable-Mac=', 'LB Policy',
                ]):
                    lines.append(line)

    with open(path, 'w') as f:
        f.write('\n'.join(lines) + '\n')
    return path


# ---------------------------------------------------------------------------
# SNMP collector thread
# ---------------------------------------------------------------------------

def snmp_collector_thread(cfg, stop_event, csv_paths, poll_index_ref):
    if not csv_paths:
        return
    jumpserver = cfg['snmp_jumpserver']
    if not jumpserver:
        print('[SNMP] No jumpserver configured — check SNMP_JUMPSERVER in .env')
        return

    if not cfg.get('target_ip'):
        print(f'[SNMP] No target_ip for {cfg.get("mac_colon","?")} — modem IPv6 not resolved (CMTS_HOST={cfg.get("cmts_host") or "not set"})')
        return

    icmts_target = cfg.get('icmts_target', '')
    mac_decimal  = cfg.get('mac_decimal', '')
    modem_info   = cfg.get('modem_info')
    session_dir  = cfg.get('session_dir', '')

    if modem_info and session_dir:
        _write_modem_info_txt(session_dir, modem_info, cfg)

    session_id = cfg.get('session_id', '')

    # Open CSV writers — prepend modem info comment block
    file_handles = {}
    writers = {}
    for key, path in csv_paths.items():
        fh = open(path, 'w', newline='')
        _write_modem_info_comments(fh, modem_info, cfg)
        w  = csv.DictWriter(fh, fieldnames=SNMP_DELTA_CSV_FIELDS, extrasaction='ignore')
        w.writeheader()
        file_handles[key] = fh
        writers[key] = w

    _DS_SFID_FILTERED = {
        'DS Flow Stats Table',  # per-modem, filter to modem SFIDs
        # DS Latency/Congestion are sparse/AQM-only — not per-modem, store as-is
    }

    def _run_poll(poll_idx, ts):
        """Run one SNMP poll and return (us_rows, ds_rows)."""
        modem_ip = cfg.get('target_ip', '')
        if modem_ip and cfg['cmts_type'] == 'vcmts':
            _, (us_cmds, us_lbls) = _icmts_snmp_commands(
                '', modem_ip, cfg['icmts_community'], cfg['modem_community'],
                cfg['snmp_timeout'], cfg['snmp_retries'], mac_decimal,
            )
            try:
                us_results = _run_via_ssh(jumpserver, cfg['snmp_username'], us_cmds, us_lbls)
            except Exception as e:
                log.warning('[SNMP] SSH failed on poll %d: %s', poll_idx, e)
                us_results = [(lbl, '') for lbl in us_lbls]
            us_rows = _pivot_results(us_results, ts, poll_idx, modem_ip, 'modem_us', cfg['cmts_type'])
            return us_rows, []
        elif cfg.get('icmts_target') and modem_ip:
            (ds_cmds, ds_lbls), (us_cmds, us_lbls) = _icmts_snmp_commands(
                cfg['icmts_target'], modem_ip, cfg['icmts_community'], cfg['modem_community'],
                cfg['snmp_timeout'], cfg['snmp_retries'], mac_decimal,
            )
            try:
                with ThreadPoolExecutor(max_workers=2) as ex:
                    f_us = ex.submit(_run_via_ssh, jumpserver, cfg['snmp_username'], us_cmds, us_lbls)
                    f_ds = ex.submit(_run_via_ssh, jumpserver, cfg['snmp_username'], ds_cmds, ds_lbls)
                    us_res, ds_res = f_us.result(), f_ds.result()
            except Exception as e:
                log.warning('[SNMP] SSH failed on poll %d: %s', poll_idx, e)
                us_res = [(lbl, '') for lbl in us_lbls]
                ds_res = [(lbl, '') for lbl in ds_lbls]
            us_rows = _pivot_results(us_res, ts, poll_idx, modem_ip, 'modem_us', cfg['cmts_type'])
            sf_index_out = next((o for l, o in ds_res if l == 'DS SF Index Table'), '')
            sfid_ifindex = _extract_sfid_ifindex(sf_index_out, mac_decimal)
            ds_rows = _pivot_results_ds(ds_res, ts, poll_idx, cfg['icmts_target'], cfg['cmts_type'], sfid_ifindex)
            return us_rows, ds_rows
        return [], []

    def _write_rows(us_rows, ds_rows, phase='test'):
        for row in us_rows:
            row['phase'] = phase
        for row in ds_rows:
            row['phase'] = phase
        _compute_snmp_deltas(us_rows, session_id)
        for row in us_rows:
            writers['us'].writerow(row)
        file_handles['us'].flush()
        if cfg.get('db_insert_snmp_delta'):
            cfg['db_insert_snmp_delta'](us_rows)
        if ds_rows:
            _compute_snmp_deltas(ds_rows, session_id)
            for row in ds_rows:
                writers['ds'].writerow(row)
            file_handles['ds'].flush()
            if cfg.get('db_insert_snmp_ds_delta'):
                cfg['db_insert_snmp_ds_delta'](ds_rows)

    BASELINE_POLLS = cfg.get('baseline_polls', 3)
    COOLDOWN_POLLS = cfg.get('cooldown_polls', 3)
    phase_callback = cfg.get('phase_callback')

    # Seed poll — prime counters so poll 1 has valid deltas
    log.info('[SNMP] Seed poll — priming counters')
    try:
        seed_us, seed_ds = _run_poll(0, '')
        _compute_snmp_deltas(seed_us, session_id)
        _compute_snmp_deltas(seed_ds, session_id)
    except Exception as e:
        log.warning('[SNMP] Seed poll failed (non-fatal): %s', e)

    # Baseline polls
    log.info('[SNMP] Collecting %d baseline polls', BASELINE_POLLS)
    for bp in range(1, BASELINE_POLLS + 1):
        if stop_event.is_set():
            break
        ts = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        log.info('[SNMP] Baseline poll %d/%d', bp, BASELINE_POLLS)
        us_rows, ds_rows = _run_poll(poll_index_ref[0], ts)
        poll_index_ref[0] += 1
        _write_rows(us_rows, ds_rows, phase='baseline')
        if bp < BASELINE_POLLS:
            stop_event.wait(timeout=cfg['snmp_poll_interval'])

    if phase_callback:
        phase_callback('ready')
    log.info('[SNMP] Baseline complete — ready to start test')
    stop_event.wait(timeout=3)
    if phase_callback:
        phase_callback('running')
    log.info('[SNMP] Collecting test data')

    try:
        while not stop_event.is_set():
            poll_idx = poll_index_ref[0]
            poll_index_ref[0] += 1
            ts = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            log.info('[SNMP] Poll #%d  %s', poll_idx, ts)
            us_rows, ds_rows = _run_poll(poll_idx, ts)
            _write_rows(us_rows, ds_rows, phase='test')
            log.info('[SNMP] Poll #%d complete — US %d sfids  DS %d sfids', poll_idx, len(us_rows), len(ds_rows))
            stop_event.wait(timeout=cfg['snmp_poll_interval'])

        # Cooldown polls
        log.info('[SNMP] Stop received — collecting %d cooldown polls', COOLDOWN_POLLS)
        for cp in range(1, COOLDOWN_POLLS + 1):
            ts = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            log.info('[SNMP] Cooldown poll %d/%d', cp, COOLDOWN_POLLS)
            us_rows, ds_rows = _run_poll(poll_index_ref[0], ts)
            poll_index_ref[0] += 1
            _write_rows(us_rows, ds_rows, phase='cooldown')
            if cp < COOLDOWN_POLLS:
                stop_event.wait(timeout=cfg['snmp_poll_interval'])
    finally:
        for fh in file_handles.values():
            fh.close()

    print(f'[SNMP] Done')

# ---------------------------------------------------------------------------
# Kafka collector thread (vCMTS only)
# ---------------------------------------------------------------------------

# Metric name → KAFKA_CSV_FIELDS column
_KAFKA_METRIC_MAP = {
    'K_Samis1_DeltaOctetsPassed':          'delta_octets',
    'K_Samis1_DeltaPacketsPassed':         'delta_pkts',
    'K_Samis1_DeltaPacketsDropped':        'delta_pkts_dropped',
    'snmp_docsQosServiceFlowOctets':       'total_octets',
    'snmp_docsQosServiceFlowPackets':      'total_pkts',
    'dp_flow_QueueLatencyAvgUsec':         'lat_avg_usec',
    'dp_flow_QueueLatencyMaxUsec':         'lat_max_usec',
    'dp_flow_AqmDroppedPackets':           'aqm_drop_pkts',
    'dp_flow_AqmMarkedCongestedPackets':   'aqm_marked_pkts',
    'dp_flow_SanctionedPackets':           'sanctioned_pkts',
}


def kafka_collector_thread(cfg, stop_event, csv_path):
    if not KAFKA_AVAILABLE:
        print('[Kafka] kafka-python not installed — pip install kafka-python')
        return

    # vCMTS Kafka messages use colon-formatted MAC (e.g. 60:6c:63:f1:98:88)
    # mac_b_norm (no separators) never appears in raw messages — colon format is the only match
    mac_b_colon = cfg['mac_colon'].encode('ascii')

    try:
        consumer = KafkaConsumer(
            cfg['kafka_topic'],
            bootstrap_servers=cfg['kafka_broker'],
            group_id=f'cm_collector_{int(time.time())}',
            auto_offset_reset='latest',
            enable_auto_commit=True,
        )
    except Exception as e:
        print(f'[Kafka] Connect failed: {e}')
        return

    print(f'[Kafka] Connected  broker={cfg["kafka_broker"]}  '
          f'topic={cfg["kafka_topic"]}  mac={cfg["mac_colon"]}')

    # Startup check — wait up to 2 poll intervals for at least one message for this MAC
    log.info('[Kafka] Waiting for first message for MAC %s...', cfg['mac_colon'])
    deadline = time.time() + cfg.get('snmp_poll_interval', 15) * 2
    mac_seen = False
    while time.time() < deadline and not stop_event.is_set():
        batch = consumer.poll(timeout_ms=2000)
        for tp, messages in batch.items():
            for message in messages:
                if mac_b_colon in message.value:
                    mac_seen = True
                    break
            if mac_seen:
                break
        if mac_seen:
            break
    if not mac_seen:
        err = f'Kafka: no messages received for {cfg["mac_colon"]} within {cfg.get("snmp_poll_interval", 15) * 2}s — vCMTS not publishing this modem'
        log.error('[Kafka] %s', err)
        if cfg.get('error_callback'):
            cfg['error_callback'](err)
        consumer.close()
        return
    log.info('[Kafka] MAC confirmed on Kafka stream — starting collection')

    # sfid lookup: (kafka_ts, dir, sfIndex) → sfid from K_Samis1_Sfid
    # params lookup: (dir, sfIndex) → {scn, max_rate_bps, aqm_target_msecs}
    # pending rows: (kafka_ts, dir, sfIndex) → row dict
    sfid_map   = {}   # (kafka_ts, dir, sfIndex) → sfid
    params_map = {}   # (dir, sfIndex) → {scn, max_rate_bps, aqm_target_msecs}
    pending    = {}   # (kafka_ts, dir, sfIndex) → row dict
    written    = set()
    count      = 0

    with open(csv_path, 'w', newline='') as f:
        _write_modem_info_comments(f, cfg.get('modem_info'), cfg)
        writer = csv.DictWriter(f, fieldnames=KAFKA_CSV_FIELDS, extrasaction='ignore')
        writer.writeheader()

        def _flush_pending(current_ts=None):
            """Write completed rows — those from a previous kafka_ts batch."""
            nonlocal count
            done = [k for k in pending
                    if current_ts is None or k[0] != current_ts]
            flushed = []
            for key in done:
                if key in written:
                    del pending[key]
                    continue
                row = pending.pop(key)
                # fill sfid + params
                row['sfid'] = sfid_map.get(key, '')
                p = params_map.get((key[1], key[2]), {})
                row.setdefault('scn',            p.get('scn', ''))
                row.setdefault('max_rate_bps',   p.get('max_rate_bps', ''))
                row.setdefault('aqm_target_msecs', p.get('aqm_target_msecs', ''))
                writer.writerow(row)
                flushed.append(row)
                written.add(key)
                count += 1
            if flushed:
                f.flush()
                if cfg.get('db_insert_kafka'):
                    cfg['db_insert_kafka'](flushed)

        poll_interval = cfg.get('snmp_poll_interval', 15)

        cooldown_polls = cfg.get('cooldown_polls', 3)
        cooldown_secs = poll_interval * cooldown_polls
        cooldown_deadline = None

        while True:
            if stop_event.is_set():
                if cooldown_deadline is None:
                    cooldown_deadline = time.time() + cooldown_secs
                    log.info('[Kafka] Stop received — continuing %ds for cooldown', cooldown_secs)
                if time.time() >= cooldown_deadline:
                    break
            batch = consumer.poll(timeout_ms=2000)
            if not batch:
                continue
            current_kafka_ts = None
            for tp, messages in batch.items():
                for message in messages:
                    if stop_event.is_set() and cooldown_deadline and time.time() >= cooldown_deadline:
                        break
                    raw = message.value
                    if mac_b_colon not in raw:
                        continue
                    line = raw.decode('utf-8', errors='replace').strip()
                    m = RE_PROM.match(line)
                    if not m:
                        print(f'[Kafka] unmatched line: {line[:120]}')
                        continue
                    metric, labels_str, value, kafka_ts = m.groups()
                    if metric not in KAFKA_METRICS:
                        continue
                    labels = dict(re.findall(r'(\w+)="([^"]*)"', labels_str))
                    dir_    = labels.get('dir', '') or labels.get('direction', '')
                    sfidx   = labels.get('sfIndex', '') or labels.get('sfindex', '')
                    key     = (kafka_ts, dir_, sfidx)  # dir_ ensures DS/US sfIndex collisions don't merge
                    current_kafka_ts = kafka_ts

                    # Skip upstream flows — Kafka is DS only; US comes from SNMP
                    if dir_.lower() in ('us', 'upstream'):
                        continue

                    # K_DocsQos_Params — store rate/aqm/scn params, no row
                    if metric == 'K_DocsQos_Params':
                        if not dir_ or not sfidx:
                            continue
                        p = params_map.setdefault((dir_, sfidx), {})
                        if labels.get('scn'):            p['scn']              = labels['scn']
                        if labels.get('maxRateBps'):     p['max_rate_bps']     = labels['maxRateBps']
                        if labels.get('aqmTargetMsecs'): p['aqm_target_msecs'] = labels['aqmTargetMsecs']
                        continue

                    # Skip non-flow metrics
                    if not dir_ or not sfidx:
                        continue

                    if key not in pending:
                        ts = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                        pending[key] = {
                            'captured_utc':       ts,
                            'kafka_timestamp_ms': kafka_ts,
                            'dir':                dir_,
                            'sfIndex':            sfidx,
                            'mdName':             labels.get('mdName', ''),
                            'node':               labels.get('node', ''),
                            'pod':                labels.get('pod', ''),
                            'cluster':            labels.get('cluster', ''),
                        }

                    row = pending[key]

                    # K_Samis1_Sfid — write sfid + scn directly into row
                    if metric == 'K_Samis1_Sfid':
                        sfid = str(int(float(value)))
                        row['sfid'] = sfid
                        sfid_map[key] = sfid
                        scn = labels.get('scn', '')
                        if scn:
                            row['scn'] = scn
                            params_map.setdefault((dir_, sfidx), {})['scn'] = scn
                        continue

                    # Latency bins
                    if metric == 'dp_flow_QueueLatencyBinPktCount':
                        bin_n = labels.get('bin', '')
                        if bin_n:
                            row[f'lat_bin{bin_n}'] = value
                            if bin_n == '01':
                                row['bin01_lower_msec'] = labels.get('edgeLowerMsec', '')
                            if bin_n == '16':
                                row['bin16_upper_msec'] = labels.get('edgeUpperMsec', '')
                        continue

                    col = _KAFKA_METRIC_MAP.get(metric)
                    if col:
                        row[col] = value

            _flush_pending(current_kafka_ts)

        _flush_pending()  # flush remainder on stop

    consumer.close()
    print(f'[Kafka] Done — {count} rows → {os.path.basename(csv_path)}')

# ---------------------------------------------------------------------------
# Interactive prompts
# ---------------------------------------------------------------------------

def prompt_config():
    print()
    print('╔══════════════════════════════════════════════════════════╗')
    print('║      CM Collector — Kafka + SNMP Data Collection        ║')
    print('╚══════════════════════════════════════════════════════════╝')
    print()

    # --- 1. CMTS environment ---
    cmts_type = _prompt_choice('CMTS type', ['vcmts', 'icmts'], 'icmts')
    snmp_jumpserver = DEFAULT_SNMP_JUMPSERVER
    snmp_username   = DEFAULT_SNMP_USERNAME
    if cmts_type == 'icmts':
        cmts_host     = DEFAULT_CMTS_HOST
        cmts_password = DEFAULT_TACACS_PASSWORD
    else:
        cmts_host     = DEFAULT_VCMTS_IP or DEFAULT_VCMTS_HOST
        cmts_password = DEFAULT_TACACS_PASSWORD
    print()

    # --- 2. Cable modem MAC + auto-resolve IPv6 ---
    _early_modem_info = None
    resolved_ip = ''
    while True:
        mac_raw = input('  Cable modem MAC address: ').strip()
        if len(_norm_mac(mac_raw)) != 12:
            print('    Invalid MAC — enter 12 hex digits (any separator)')
            continue
        mac_norm    = _norm_mac(mac_raw)
        mac_colon   = _mac_colon(mac_norm)
        mac_decimal = _mac_to_decimal(mac_norm)
        print(f'    → {mac_colon}  (OID decimal: {mac_decimal})')

        if cmts_host and snmp_jumpserver and snmp_username:
            print('  [auto] Looking up modem on CMTS...')
            _early_cfg = {
                'mac_norm': mac_norm, 'mac_colon': mac_colon,
                'cmts_type': cmts_type, 'cmts_host': cmts_host,
                'cmts_password': cmts_password,
                'snmp_jumpserver': snmp_jumpserver, 'snmp_username': snmp_username,
            }
            _early_modem_info = modem_info_collector(_early_cfg)
            if _early_modem_info is None:
                print('  [error] Could not connect to CMTS — continuing without modem info')
                break
            if _early_modem_info.get('not_found'):
                print(f'  [error] Modem {mac_colon} not found on {cmts_host} — re-enter MAC or Ctrl+C to abort')
                _early_modem_info = None
                continue
            resolved_ip = _early_modem_info.get('cm_ipv6', '')
            if resolved_ip:
                print(f'  [auto] Modem IPv6: {resolved_ip}')
            else:
                print('  [auto] Modem found but IPv6 not resolved')
        break
    print()

    # --- 3. Modem IP ---
    target_ip       = resolved_ip
    modem_community = DEFAULT_MODEM_COMMUNITY

    # --- 4. iCMTS target / Kafka ---
    icmts_target    = DEFAULT_ICMTS_TARGET_IP if cmts_type == 'icmts' else ''
    icmts_community = DEFAULT_ICMTS_COMMUNITY
    kafka_broker    = DEFAULT_KAFKA_BROKER if cmts_type == 'vcmts' else ''
    kafka_topic     = DEFAULT_KAFKA_TOPIC  if cmts_type == 'vcmts' else ''

    # --- 5. Start/stop time + output ---
    start_time   = None
    stop_time    = None
    duration     = None  # run until Ctrl+C
    results_root = DEFAULT_RESULTS_DIR

    ts_str      = datetime.now().strftime('%Y%m%d_%H%M%S')
    session_dir = _make_session_dir(results_root, mac_norm, cmts_type, ts_str)
    csv_paths   = {
        'us': os.path.join(session_dir, f'snmp_us_{mac_norm}_{ts_str}.csv'),
    } if cmts_type == 'vcmts' else {
        'us': os.path.join(session_dir, f'snmp_us_{mac_norm}_{ts_str}.csv'),
        'ds': os.path.join(session_dir, f'snmp_ds_{mac_norm}_{ts_str}.csv'),
    }
    kafka_csv = os.path.join(session_dir, f'kafka_{mac_norm}_{ts_str}.csv') if cmts_type == 'vcmts' else None

    print()
    print('  ┌─ Session summary ──────────────────────────────────────────')
    print(f'  │  MAC          : {mac_colon}')
    print(f'  │  MAC decimal  : {mac_decimal}')
    print(f'  │  CMTS type    : {cmts_type.upper()}')
    print(f'  │  CMTS host    : {cmts_host or "(none)"}')
    print(f'  │  SNMP jump    : {snmp_jumpserver or "(none)"}')
    if cmts_type == 'vcmts':
        print(f'  │  Kafka broker : {kafka_broker}')
        print(f'  │  Kafka topic  : {kafka_topic}')
    else:
        if icmts_target:
            print(f'  │  iCMTS target : {icmts_target}')
    print(f'  │  Modem IP     : {target_ip or "(none — modem SNMP skipped)"}')
    print(f'  │  Duration     : {f"{duration}s" if duration else "until Ctrl+C"}  SNMP poll every {DEFAULT_SNMP_POLL_INTERVAL}s')
    if cmts_type == 'vcmts':
        print(f'  │  Kafka        : continuous listen (rate set by vCMTS)')
    print(f'  │  Output dir   : {session_dir}')
    for key, path in csv_paths.items():
        print(f'  │  snmp_{key:<9}: {os.path.basename(path)}')
    if cmts_type == 'vcmts':
        print(f'  │  snmp_ds        : (from Kafka)')
    if kafka_csv:
        print(f'  │  kafka        : {os.path.basename(kafka_csv)}')
    print('  └────────────────────────────────────────────────────────────')
    print()

    return {
        'mac_norm':           mac_norm,
        'mac_colon':          mac_colon,
        'mac_decimal':        mac_decimal,
        'cmts_type':          cmts_type,
        'target_ip':          target_ip,
        'modem_community':    modem_community,
        'icmts_community':    icmts_community,
        'icmts_target':       icmts_target,
        'kafka_broker':       kafka_broker,
        'kafka_topic':        kafka_topic,
        'cmts_host':          cmts_host,
        'cmts_password':      cmts_password,
        'snmp_jumpserver':    snmp_jumpserver,
        'snmp_username':      snmp_username,
        'snmp_timeout':       DEFAULT_SNMP_TIMEOUT,
        'snmp_retries':       DEFAULT_SNMP_RETRIES,
        'snmp_poll_interval': DEFAULT_SNMP_POLL_INTERVAL,
        'duration':           duration,
        'stop_time':          stop_time,
        'start_time':         start_time,
        'session_dir':        session_dir,
        'csv_paths':          csv_paths,
        'kafka_csv':          kafka_csv,
        'modem_info':         _early_modem_info,
    }

# ---------------------------------------------------------------------------
# Test mode — single poll, print raw SNMP output to stdout
# ---------------------------------------------------------------------------

def run_test_mode(cfg):
    jumpserver   = cfg['snmp_jumpserver']
    icmts_target = cfg.get('icmts_target', '')
    modem_ip     = cfg.get('target_ip', '')
    mac_decimal  = cfg.get('mac_decimal', '')
    poll_interval = cfg['snmp_poll_interval']

    print(f'  ┌─ Debug Session ─────────────────────────────────────────────')
    print(f'  │  MAC        : {cfg["mac_colon"]}')
    print(f'  │  CMTS type  : {cfg["cmts_type"].upper()}')
    print(f'  │  CMTS host  : {cfg.get("cmts_host") or "(none)"}')
    print(f'  │  Jump server: {jumpserver or "(local)"}')
    if icmts_target:
        print(f'  │  iCMTS IP   : {icmts_target}')
    if modem_ip:
        print(f'  │  Modem IP   : {modem_ip}')
    print(f'  │  Poll every : {poll_interval}s  (Ctrl+C to stop)')
    print(f'  └───────────────────────────────────────────────────────────')
    print()

    poll_idx = 1
    try:
        while True:
            ts = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            print(f'  ──── Poll #{poll_idx}  {ts} UTC ───────────────────────────────────────────')
            print()

            if icmts_target and modem_ip:
                (ds_cmds, ds_lbls), (us_cmds, us_lbls) = _icmts_snmp_commands(
                    icmts_target, modem_ip,
                    cfg['icmts_community'], cfg['modem_community'],
                    cfg['snmp_timeout'], cfg['snmp_retries'], mac_decimal,
                )
                t0 = time.time()
                try:
                    with ThreadPoolExecutor(max_workers=2) as ex:
                        f_us = ex.submit(_run_via_ssh, jumpserver, cfg['snmp_username'], us_cmds, us_lbls)
                        f_ds = ex.submit(_run_via_ssh, jumpserver, cfg['snmp_username'], ds_cmds, ds_lbls)
                        us_results = f_us.result()
                        ds_results = f_ds.result()
                except Exception as e:
                    print(f'  ✗ SSH failed: {e}')
                    us_results = [(lbl, '') for lbl in us_lbls]
                ds_results = _run_local(ds_cmds, ds_lbls)

                print(f'  ── US  {modem_ip} ──────────────────────────────────────────────────────')
                print()
                for label, output in us_results:
                    if label == 'US Flow Stats Table':
                        _print_latency_section(label, output, None, _FLOW_STATS_COLS)
                    elif label == 'US Latency Stats Table':
                        _print_latency_section(label, output, None, _LATENCY_COLS)
                    elif label == 'US Congestion Stats Table':
                        _print_latency_section(label, output, None, _CONGESTION_COLS)
                    elif label in ('Cadant Map Stats', 'Map Stats Pages Flows'):
                        _print_cadant_section(label, output)
                    else:
                        _print_snmp_section(label, _parse_snmp_output(output))

                sf_index_output = next((o for l, o in ds_results if l == 'DS SF Index Table'), '')
                sfids = _extract_sfids(sf_index_output, mac_decimal)
                print(f'  ── DS  {icmts_target}  (mac: {mac_decimal or "none"}  sfids: {sfids or "none"}) ──────────────')
                print()
                for label, output in ds_results:
                    if label == 'DS Flow Stats Table':
                        filtered = _filter_by_sfids(output, sfids)
                        _print_latency_section(label, filtered, None, _FLOW_STATS_COLS)
                    elif label == 'DS Latency Stats':
                        _print_latency_section(label, output, None, _LATENCY_COLS)
                    elif label == 'DS Congestion Stats':
                        _print_latency_section(label, output, None, _CONGESTION_COLS)
                    elif label in ('DS Cadant Map Stats', 'DS Map Stats Pages Flows'):
                        _print_cadant_section(label, output)
                    else:
                        _print_snmp_section(label, _parse_snmp_output(output))

                elapsed = time.time() - t0
                sleep_for = max(0, poll_interval - elapsed)
                if sleep_for > 0:
                    print(f'  (next poll in {sleep_for:.0f}s)')
                    time.sleep(sleep_for)
                poll_idx += 1
                continue

            if cfg['cmts_type'] == 'vcmts' and cfg.get('kafka_broker') and KAFKA_AVAILABLE:
                print(f'  ── Kafka  {cfg["kafka_broker"]}  topic={cfg["kafka_topic"]} ──────────────────')
                print()
                mac_b_colon = cfg['mac_colon'].encode('ascii')
                try:
                    consumer = KafkaConsumer(
                        cfg['kafka_topic'],
                        bootstrap_servers=cfg['kafka_broker'],
                        group_id=f'cm_collector_debug_{int(time.time())}',
                        auto_offset_reset='latest',
                        enable_auto_commit=False,
                    )
                    deadline = time.time() + poll_interval
                    count = 0
                    while time.time() < deadline:
                        for tp, messages in consumer.poll(timeout_ms=1000).items():
                            for msg in messages:
                                raw = msg.value
                                if mac_b_colon not in raw:
                                    continue
                                line = raw.decode('utf-8', errors='replace').strip()
                                if not any(m in line for m in KAFKA_METRICS):
                                    continue
                                m = RE_PROM.match(line)
                                if not m:
                                    continue
                                metric, labels_str, value, kafka_ts = m.groups()
                                labels = dict(re.findall(r'(\w+)="([^"]*)"', labels_str))
                                bin_info = (f"  bin={labels['bin']} [{labels.get('edgeLowerMsec','?')}–"
                                            f"{labels.get('edgeUpperMsec','?')}ms]") if labels.get('bin') else ''
                                print(f'  ▶ {metric}  sf={labels.get("sfIndex","?")}  dir={labels.get("dir","?")}  val={value}{bin_info}')
                                count += 1
                    consumer.close()
                    print(f'  {count} Kafka messages  poll #{poll_idx}')
                    print()
                except Exception as e:
                    print(f'  ✗ Kafka failed: {e}')
                    print()
                poll_idx += 1
                continue

            poll_idx += 1
    except KeyboardInterrupt:
        pass

    print()
    print('  ✔ Done')


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _cfg_from_env(mac_raw):
    """Build cfg dict from .env defaults — no prompts. Used by --test."""
    mac_norm    = _norm_mac(mac_raw)
    mac_colon   = _mac_colon(mac_norm)
    mac_decimal = _mac_to_decimal(mac_norm)
    return {
        'mac_norm':           mac_norm,
        'mac_colon':          mac_colon,
        'mac_decimal':        mac_decimal,
        'cmts_type':          'icmts',
        'target_ip':          DEFAULT_ICMTS_TARGET_IP,
        'modem_community':    DEFAULT_MODEM_COMMUNITY,
        'icmts_community':    DEFAULT_ICMTS_COMMUNITY,
        'icmts_target':       DEFAULT_ICMTS_TARGET_IP,
        'kafka_broker':       DEFAULT_KAFKA_BROKER,
        'kafka_topic':        DEFAULT_KAFKA_TOPIC,
        'cmts_host':          DEFAULT_CMTS_HOST,
        'cmts_password':      DEFAULT_TACACS_PASSWORD,
        'snmp_jumpserver':    DEFAULT_SNMP_JUMPSERVER,
        'snmp_username':      DEFAULT_SNMP_USERNAME,
        'snmp_timeout':       DEFAULT_SNMP_TIMEOUT,
        'snmp_retries':       DEFAULT_SNMP_RETRIES,
        'snmp_poll_interval': DEFAULT_SNMP_POLL_INTERVAL,
        'duration':           None,
        'modem_info':         None,
    }


def main():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--debug', action='store_true',
                        help='Run one SNMP poll and print output to stdout — no CSV written')
    args, _ = parser.parse_known_args()

    if args.debug:
        print()
        print('╔══════════════════════════════════════════════════════════╗')
        print('║      CM Collector — Kafka + SNMP Data Collection        ║')
        print('╚══════════════════════════════════════════════════════════╝')
        print()
        cmts_type = _prompt_choice('  CMTS type', ['vcmts', 'icmts'], 'icmts')
        while True:
            mac_raw = input('  Cable modem MAC address: ').strip()
            if len(_norm_mac(mac_raw)) == 12:
                break
            print('    Invalid MAC — enter 12 hex digits (any separator)')
        cfg = _cfg_from_env(mac_raw)
        cfg['cmts_type'] = cmts_type
        if cmts_type == 'vcmts':
            cfg['cmts_host']   = DEFAULT_VCMTS_IP or DEFAULT_VCMTS_HOST
            cfg['icmts_target'] = ''
            cfg['kafka_broker'] = DEFAULT_KAFKA_BROKER
            cfg['kafka_topic']  = DEFAULT_KAFKA_TOPIC
        print('  [auto] Resolving modem IPv6 from CMTS...')
        modem_info = modem_info_collector(cfg)
        if modem_info and not modem_info.get('not_found') and modem_info.get('cm_ipv6'):
            cfg['target_ip'] = modem_info['cm_ipv6']
            cfg['modem_info'] = modem_info
            print(f'  ✔ Modem IPv6: {cfg["target_ip"]}')
        else:
            modem_ip = input('  Modem IPv6 not resolved — enter IP manually (Enter to skip): ').strip()
            cfg['target_ip'] = modem_ip
        print()
        print('[DEBUG] Polling every 15s — output to stdout, no files written')
        print()
        run_test_mode(cfg)
        return

    cfg = prompt_config()

    if _prompt_choice('Start collection?', ['y', 'n'], 'y') != 'y':
        print('Aborted.')
        return

    stop_event  = threading.Event()
    poll_index  = [1]
    threads     = []

    # Wait for start time if specified
    if cfg.get('start_time'):
        wait_secs = max(0, int((cfg['start_time'] - datetime.now()).total_seconds()))
        if wait_secs > 0:
            print(f'  Waiting until {cfg["start_time"].strftime("%H:%M")} ({wait_secs}s)...')
            try:
                time.sleep(wait_secs)
            except KeyboardInterrupt:
                print('Aborted.')
                return

    collection_start = datetime.now()

    # Collect modem info once upfront (blocking) — skip if already resolved during prompts
    modem_info = cfg.get('modem_info') or modem_info_collector(cfg)
    cfg['modem_info'] = modem_info
    if modem_info and modem_info.get('cm_ipv6'):
        cfg['cm_ipv6'] = modem_info['cm_ipv6']
        print(f'  CM IPv6: {modem_info["cm_ipv6"]}')
    print()

    # SNMP thread (always)
    snmp_t = threading.Thread(
        target=snmp_collector_thread,
        args=(cfg, stop_event, cfg['csv_paths'], poll_index),
        daemon=True,
    )
    threads.append(snmp_t)

    # Kafka thread (vCMTS only)
    if cfg['cmts_type'] == 'vcmts':
        kafka_t = threading.Thread(
            target=kafka_collector_thread,
            args=(cfg, stop_event, cfg['kafka_csv']),
            daemon=True,
        )
        threads.append(kafka_t)

    print(f'\nCollecting {f"until {cfg["stop_time"].strftime("%H:%M")}" if cfg.get("stop_time") else "until Ctrl+C"}  (Ctrl+C to stop early)\n')
    for t in threads:
        t.start()

    try:
        if cfg['duration']:
            wait = max(0, cfg['duration'] - int((datetime.now() - collection_start).total_seconds()))
            time.sleep(wait)
        else:
            while True:
                time.sleep(1)
    except KeyboardInterrupt:
        pass

    print('\nStopping...')
    stop_event.set()
    for t in threads:
        t.join(timeout=15)

    print()
    print('Done.')
    print(f'  Output : {cfg["session_dir"]}')
    for key, path in cfg['csv_paths'].items():
        print(f'  snmp_{key:<4}: {os.path.basename(path)}')
    if not cfg['csv_paths']:
        print(f'  snmp      : (none — Kafka only)')
    if cfg['kafka_csv']:
        print(f'  kafka      : {os.path.basename(cfg["kafka_csv"])}')
    if cfg.get('cmts_host'):
        print(f'  modem info : modem_info_{cfg["mac_norm"]}.txt')


if __name__ == '__main__':
    main()
