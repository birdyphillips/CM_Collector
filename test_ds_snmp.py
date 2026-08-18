#!/usr/bin/env python3
"""
test_ds_snmp.py — run downstream SNMP commands against iCMTS, print raw output.

Usage:
    python test_ds_snmp.py
"""
import os
import subprocess

def _load_env():
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
    if not os.path.exists(env_path):
        return
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            k, _, v = line.partition('=')
            os.environ.setdefault(k.strip(), v.strip())

_load_env()

IP  = os.environ.get('ICMTS_TARGET_IP', '172.31.17.66')
COM = os.environ.get('ICMTS_COMMUNITY', 'NMISread')
T   = 10
R   = 2
B   = '1.3.6.1.4.1.4491.2.1.21.1'
CAD = '1.3.6.1.4.1.4998.1.1.15'

CMDS = [
    ('SF Direction (1=DS 2=US)',  f'snmpwalk    -v 2c -c {COM} -t {T} -r {R} {IP} {B}.11.1.3'),
    ('SF Flow Stats',             f'snmpwalk    -v 2c -c {COM} -t {T} -r {R} {IP} {B}.4'),
    ('SF Latency Stats',          f'snmpwalk    -v 2c -c {COM} -t {T} -r {R} {IP} {B}.29'),
    ('SF Congestion Stats',       f'snmpwalk    -v 2c -c {COM} -t {T} -r {R} {IP} {B}.30'),
    ('Cadant Map Stats',          f'snmpbulkget -v 2c -c {COM} -t {T} -r {R} {IP} .{CAD}.10.2'),
    ('Map Stats Pages Flows',     f'snmpbulkget -v 2c -c {COM} -t {T} -r {R} {IP} .{CAD}.10.8'),
]


def run(label, cmd):
    print(f'\n{"="*70}')
    print(f'  {label}')
    print(f'  {cmd}')
    print(f'{"="*70}')
    try:
        proc = subprocess.run(cmd, shell=True, capture_output=True, timeout=300)
        out = proc.stdout.decode(errors='replace')
        err = proc.stderr.decode(errors='replace').strip()
        if err:
            print(f'[stderr] {err}')
        print(out if out.strip() else '(no output)')
    except subprocess.TimeoutExpired:
        print('TIMEOUT after 300s')
    except Exception as e:
        print(f'ERROR: {e}')


if __name__ == '__main__':
    print(f'Target: {IP}  community: {COM}')
    for label, cmd in CMDS:
        run(label, cmd)
    print(f'\n{"="*70}')
    print('Done.')
