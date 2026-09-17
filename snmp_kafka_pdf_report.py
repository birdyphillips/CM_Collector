"""
snmp_kafka_pdf_report.py
========================
Generates a PDF report from a CM Collector session stored in sessions.db.
Loads SNMP delta rows (upstream) and Kafka rows (downstream + upstream) by session ID.

Usage:
    python snmp_kafka_pdf_report.py                  # lists available sessions, prompts for ID
    python snmp_kafka_pdf_report.py 708aaba9         # specific session ID

Requirements:
    pip install pandas matplotlib
"""
import os
import sys
import re
import json
import sqlite3
import matplotlib
matplotlib.use('Agg')  # non-interactive backend — required when called from threads
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.backends.backend_pdf import PdfPages
from datetime import datetime

HERE   = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(HERE, 'sessions.db')

# ---------------------------------------------------------------------------
# Theme
# ---------------------------------------------------------------------------
CHART_COLORS = ['#1a73e8', '#34a853', '#fa7b17', '#ea4335', '#a142f4', '#24c1e0', '#f538a0']
BG_DARK      = '#0d1b2a'
BG_PANEL     = '#112240'
GRID_COLOR   = '#1e3a5f'
TEXT_COLOR   = '#e8eaed'
SUBTEXT      = '#e8eaed'
ACCENT       = '#1a73e8'

MODEM_NAMES = {
    '0c:b9:37:64:3a:b0': 'Lab CM (vCMTS)',
    '0c:b9:37:9c:64:b4': 'Lab CM (iCMTS)',
}

# ---------------------------------------------------------------------------
# DB loaders
# ---------------------------------------------------------------------------
def _list_sessions(conn):
    return pd.read_sql(
        "SELECT id, mac, cmts_type, started_at FROM sessions ORDER BY started_at DESC", conn)


def _load_snmp(conn, session_id):
    """Load snmp_upstream_delta_rows."""
    df = pd.read_sql(
        "SELECT * FROM snmp_upstream_delta_rows WHERE session_id=? ORDER BY captured_utc, sfid",
        conn, params=(session_id,))
    df['captured_utc'] = pd.to_datetime(df['captured_utc'])
    df['sfid'] = df['sfid'].astype(str)
    skip = {'captured_utc', 'session_id', 'target_ip', 'target_label',
            'cmts_type', 'sfid', 'ps_scn', 'lat_bin_scn', 'sf_direction'}
    for c in df.columns:
        if c not in skip and isinstance(df[c], pd.Series):
            df[c] = pd.to_numeric(df[c], errors='coerce')
    # Filter out SFIDs with no upstream traffic (DS SFIDs and empty flows)
    if 'flow_octets' in df.columns:
        active_sfids = df.groupby('sfid')['flow_octets'].max()
        active_sfids = active_sfids[active_sfids.fillna(0) > 0].index
        df = df[df['sfid'].isin(active_sfids)].reset_index(drop=True)
    return df


def _load_kafka(conn, session_id):
    """Load kafka rows — checks kafka_downstream_rows first, falls back to kafka_rows."""
    for table in ('kafka_downstream_rows', 'kafka_rows'):
        try:
            df = pd.read_sql(
                f'SELECT * FROM {table} WHERE session_id=? ORDER BY captured_utc, sfid',
                conn, params=(session_id,))
            if not df.empty:
                break
        except Exception:
            df = pd.DataFrame()
    if df.empty:
        return pd.DataFrame(), pd.DataFrame()
    df['captured_utc'] = pd.to_datetime(df['captured_utc'])
    df['sfid'] = df['sfid'].astype(str)
    df['sfid_label'] = df.apply(
        lambda r: r['scn'] if pd.notna(r.get('scn')) and str(r.get('scn', '')).strip()
                  else r['sfid'], axis=1)
    renames = {
        'aqm_drop_pkts':   'cong_aqm_drop',
        'aqm_marked_pkts': 'cong_ce_marked',
        'sanctioned_pkts': 'cong_sanctioned',
        'total_octets':    'flow_octets',
        'total_pkts':      'flow_pkts',
    }
    for i in range(1, 17):
        renames[f'lat_bin{str(i).zfill(2)}'] = f'lat_bin{i}'
    df = df.rename(columns=renames)
    num_skip = {'captured_utc', 'session_id', 'dir', 'sfIndex', 'sfid',
                'scn', 'sfid_label', 'mdName', 'node', 'pod', 'cluster'}
    for c in df.columns:
        if c not in num_skip and isinstance(df[c], pd.Series):
            df[c] = pd.to_numeric(df[c], errors='coerce')
    df = df.sort_values('captured_utc').reset_index(drop=True)
    if 'dir' in df.columns:
        us = df[df['dir'] == 'upstream'].copy().reset_index(drop=True)
        ds = df[df['dir'] == 'downstream'].copy().reset_index(drop=True)
    else:
        us = pd.DataFrame()
        ds = df.reset_index(drop=True)
    return us, ds


def pick_session(arg=None):
    conn = sqlite3.connect(DB_PATH)
    sessions = _list_sessions(conn)
    if sessions.empty:
        print('No sessions found in sessions.db')
        sys.exit(1)

    if arg:
        row = sessions[sessions['id'].str.startswith(arg)]
        if row.empty:
            print(f'No session matching: {arg}')
            print(sessions.to_string(index=False))
            sys.exit(1)
        sid = row.iloc[0]['id']
    else:
        print('\nAvailable sessions:')
        print(sessions.to_string(index=False))
        sid = input('\nEnter session ID (or prefix): ').strip()
        row = sessions[sessions['id'].str.startswith(sid)]
        if row.empty:
            print(f'No session matching: {sid}')
            sys.exit(1)
        sid = row.iloc[0]['id']

    meta = sessions[sessions['id'] == sid].iloc[0]
    us   = _load_snmp(conn, sid)
    k_us, k_ds = _load_kafka(conn, sid)
    test_start, cooldown_start = _get_phase_times(conn, sid)
    conn.close()
    return sid, meta, us, k_us, k_ds, test_start, cooldown_start


# ---------------------------------------------------------------------------
# Chart helpers
# ---------------------------------------------------------------------------
def fmt_ax(ax):
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right', color=TEXT_COLOR)
    ax.grid(True, color=GRID_COLOR, linewidth=0.7, linestyle='--', alpha=0.8)
    ax.set_axisbelow(True)

def style_ax(ax):
    ax.set_facecolor(BG_PANEL)
    ax.tick_params(colors=TEXT_COLOR, labelsize=9)
    ax.xaxis.label.set_color(SUBTEXT)
    ax.yaxis.label.set_color(SUBTEXT)
    for spine in ax.spines.values():
        spine.set_edgecolor(GRID_COLOR)

def add_header(fig, title, subtitle=None):
    hax = fig.add_axes([0, 0.91, 1, 0.09])
    hax.set_facecolor(ACCENT)
    hax.axis('off')
    hax.text(0.5, 0.62, title, transform=hax.transAxes, fontsize=16, fontweight='bold',
             color='white', ha='center', va='center')
    if subtitle:
        hax.text(0.5, 0.15, subtitle, transform=hax.transAxes, fontsize=8,
                 color=TEXT_COLOR, ha='center', va='center', fontstyle='italic')

def add_footer(fig, mac_fmt, modem_name, session_start, session_end, cmts_type='vCMTS'):
    fax = fig.add_axes([0, 0, 1, 0.04])
    fax.set_facecolor('#0a1628')
    fax.axis('off')
    fax.text(0.5, 0.5,
             f'{cmts_type.upper()} SNMP+Kafka Report  |  {modem_name} ({mac_fmt})  |  {session_start} — {session_end}  |  aphillips — Spectrum Access Engineering',
             transform=fax.transAxes, fontsize=7, color='#445566', ha='center', va='center')

def make_fig():
    fig, ax = plt.subplots(figsize=(11, 5.8), subplot_kw={'facecolor': BG_PANEL})
    fig.patch.set_facecolor(BG_DARK)
    fig.subplots_adjust(top=0.88, bottom=0.18, left=0.09, right=0.97)
    style_ax(ax)
    return fig, ax

def save_page(pdf, fig, ax, header_title, subtitle, mac_fmt, modem_name,
              session_start, session_end, cmts_type='vcmts', **kwargs):
    add_header(fig, header_title, subtitle)
    add_footer(fig, mac_fmt, modem_name, session_start, session_end, cmts_type)
    pdf.savefig(fig, facecolor=fig.get_facecolor())
    plt.close(fig)

def plot_line(ax, df, y_col, group_col, ylabel, x_col='captured_utc'):
    for i, (name, grp) in enumerate(df.groupby(group_col)):
        grp = grp.sort_values(x_col)
        color = CHART_COLORS[i % len(CHART_COLORS)]
        ax.plot(grp[x_col], grp[y_col], marker='o', markersize=4, linewidth=2,
                color=color, label=str(name),
                markerfacecolor='white', markeredgecolor=color, markeredgewidth=1.5)
        ax.fill_between(grp[x_col], grp[y_col], alpha=0.08, color=color)
    ax.set_ylabel(ylabel, color=SUBTEXT, fontsize=10, fontweight='bold')
    ax.set_xlabel('Time (UTC)', color=SUBTEXT, fontsize=10)
    ax.legend(fontsize=8, facecolor=BG_DARK, edgecolor=GRID_COLOR,
              labelcolor=TEXT_COLOR, framealpha=0.9)
    fmt_ax(ax)

def plot_dual(ax, df, col_a, col_b, label_a, label_b, group_col, ylabel):
    for i, (name, grp) in enumerate(df.groupby(group_col)):
        grp = grp.sort_values('captured_utc')
        c = CHART_COLORS[i % len(CHART_COLORS)]
        ax.plot(grp['captured_utc'], grp[col_a], marker='o', markersize=4, linewidth=2,
                color=c, label=f'{name} {label_a}',
                markerfacecolor='white', markeredgecolor=c, markeredgewidth=1.5)
        ax.plot(grp['captured_utc'], grp[col_b], marker='x', markersize=4, linewidth=1.5,
                linestyle='--', color=c, alpha=0.7, label=f'{name} {label_b}')
    ax.set_ylabel(ylabel, color=SUBTEXT, fontsize=10, fontweight='bold')
    ax.set_xlabel('Time (UTC)', color=SUBTEXT, fontsize=10)
    ax.legend(fontsize=7, facecolor=BG_DARK, edgecolor=GRID_COLOR,
              labelcolor=TEXT_COLOR, framealpha=0.9, ncol=2)
    fmt_ax(ax)


# ---------------------------------------------------------------------------
# Phase boundary helpers
# ---------------------------------------------------------------------------

def _get_phase_times(conn, session_id):
    """
    Returns (test_start_utc, cooldown_start_utc) as pandas Timestamps or None.
    Derived from snmp_upstream_delta_rows poll phases stored in the DB.
    We use the poll_index boundaries written by the collector:
      - baseline_polls stored in cfg_json
      - test starts at poll baseline_polls + 1
      - cooldown = last N polls (cooldown_polls from cfg_json)
    """
    try:
        cfg_row = conn.execute(
            "SELECT cfg_json FROM sessions WHERE id=?", (session_id,)
        ).fetchone()
        cfg = json.loads(cfg_row[0] or '{}') if cfg_row else {}
        baseline_polls = int(cfg.get('baseline_polls', 3))
        cooldown_polls = int(cfg.get('cooldown_polls', 3))

        polls = pd.read_sql(
            "SELECT DISTINCT poll_index, captured_utc FROM snmp_upstream_delta_rows "
            "WHERE session_id=? ORDER BY poll_index",
            conn, params=(session_id,)
        )
        if polls.empty:
            return None, None
        polls['captured_utc'] = pd.to_datetime(polls['captured_utc'])
        polls = polls.sort_values('poll_index').reset_index(drop=True)

        # test starts after baseline polls
        test_rows = polls[polls['poll_index'] > baseline_polls]
        test_start = test_rows['captured_utc'].iloc[0] if not test_rows.empty else None

        # cooldown starts at the (N - cooldown_polls + 1)th poll from the end
        total = len(polls)
        if cooldown_polls > 0 and total > cooldown_polls:
            cooldown_start = polls['captured_utc'].iloc[total - cooldown_polls]
        else:
            cooldown_start = None

        return test_start, cooldown_start
    except Exception as e:
        log.warning('_get_phase_times failed: %s', e) if 'log' in dir() else None
        return None, None


def _annotate_phases(ax, test_start, cooldown_start):
    """Draw vertical phase boundary lines on a time-series axes."""
    for ts, label, color in [
        (test_start,     'Test Start', '#34a853'),
        (cooldown_start, 'Cooldown',   '#fa7b17'),
    ]:
        if ts is None:
            continue
        ax.axvline(x=ts, color=color, linewidth=1.5, linestyle='--', alpha=0.9, zorder=5)
        ax.text(ts, ax.get_ylim()[1], f' {label}', color=color,
                fontsize=7, va='top', ha='left', rotation=90,
                transform=ax.get_xaxis_transform())


def page_cover(pdf, mac_fmt, modem_name, session_start, session_end,
               duration_str, total_polls, us_sfids, ds_sfids, cmts_type, session_name, session_id):
    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor(BG_PANEL)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_facecolor(BG_PANEL)
    ax.axis('off')

    ax.text(0.5, 0.935, f'{cmts_type.upper()} SNMP + KAFKA SESSION REPORT',
            transform=ax.transAxes, fontsize=26, fontweight='bold',
            color='white', ha='center', va='center')
    ax.text(0.5, 0.865, 'Spectrum  •  Access Engineering',
            transform=ax.transAxes, fontsize=12, color=TEXT_COLOR,
            ha='center', va='center', fontstyle='italic')
    ax.text(0.5, 0.815, session_name,
            transform=ax.transAxes, fontsize=14, fontweight='bold',
            color=ACCENT, ha='center', va='center')
    ax.axhline(y=0.79, xmin=0.05, xmax=0.95, color=ACCENT, linewidth=1.5)

    ds_label = ', '.join(str(s) for s in sorted(ds_sfids)) if ds_sfids else 'Kafka (DS)'
    labels = [
        ('Session ID',    session_id),
        ('Modem',         modem_name),
        ('Modem MAC',     mac_fmt),
        ('CMTS Type',     cmts_type.upper()),
        ('Session Start', session_start),
        ('Session End',   session_end),
        ('Duration',      duration_str),
        ('Total Polls',   str(total_polls)),
        ('US SFIDs',      ', '.join(str(s) for s in sorted(us_sfids))),
        ('DS SFIDs',      ds_label),
    ]
    y = 0.72
    for label, value in labels:
        ax.text(0.12, y, f'{label}:', transform=ax.transAxes,
                fontsize=11, color=SUBTEXT, fontweight='bold', va='center')
        ax.text(0.38, y, value, transform=ax.transAxes,
                fontsize=11, color='white', va='center', fontfamily='monospace')
        y -= 0.057

    ax.text(0.5, 0.02,
            f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M")}   |   aphillips — Spectrum Access Engineering',
            transform=ax.transAxes, fontsize=8, color='#445566', ha='center', va='center')
    pdf.savefig(fig, facecolor=fig.get_facecolor())
    plt.close(fig)


def page_toc(pdf, mac_fmt, modem_name, session_start, session_end, contents, cmts_type):
    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor(BG_PANEL)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_facecolor(BG_PANEL)
    ax.axis('off')

    ax.text(0.5, 0.935, 'TABLE OF CONTENTS',
            transform=ax.transAxes, fontsize=22, fontweight='bold',
            color='white', ha='center', va='center')
    ax.axhline(y=0.88, xmin=0.05, xmax=0.95, color=ACCENT, linewidth=1)

    n   = len(contents)
    gap = min(0.075, 0.78 / max(n, 1))
    y   = 0.80
    for i, (page, title, desc) in enumerate(contents):
        row_color = '#112240' if i % 2 == 0 else '#0d1b2a'
        ax.axhspan(y - gap * 0.4, y + gap * 0.55, facecolor=row_color, alpha=1.0)
        ax.text(0.07, y + 0.005, f'pg {page}', transform=ax.transAxes,
                fontsize=9, fontweight='bold', color=ACCENT, va='center', ha='center',
                bbox=dict(boxstyle='round,pad=0.3', facecolor='#0d2a4a',
                          edgecolor=ACCENT, linewidth=1))
        ax.text(0.15, y + 0.005, title, transform=ax.transAxes,
                fontsize=10, fontweight='bold', color=TEXT_COLOR, va='center')
        ax.text(0.15, y - gap * 0.35, desc, transform=ax.transAxes,
                fontsize=8, color=SUBTEXT, va='center', fontstyle='italic')
        ax.axhline(y=y - gap * 0.4, xmin=0.05, xmax=0.95,
                   color=GRID_COLOR, linewidth=0.5, linestyle=':')
        y -= gap

    ax.text(0.5, 0.02,
            f'{cmts_type.upper()} SNMP+Kafka Report  |  {modem_name} ({mac_fmt})  |  {session_start} — {session_end}',
            transform=ax.transAxes, fontsize=8, color='#445566', ha='center', va='center')
    pdf.savefig(fig, facecolor=fig.get_facecolor())
    plt.close(fig)


# ---------------------------------------------------------------------------
# Summary page
# ---------------------------------------------------------------------------
def _toint(val):
    v = pd.to_numeric(val, errors='coerce')
    return int(v) if pd.notna(v) else 0


def _calc_percentile(deltas, pct):
    total = sum(deltas)
    if total == 0:
        return 0
    target, cumulative = total * pct, 0
    for i, count in enumerate(deltas):
        cumulative += count
        if cumulative >= target:
            return i + 1
    return len(deltas)

def _calc_weighted_avg(deltas):
    total = sum(deltas)
    if total == 0:
        return 0.0
    return sum((i + 1) * v for i, v in enumerate(deltas)) / total


# vCMTS fixed DS latency bin upper edges in ms
VCMTS_BIN_EDGES_MS = [0.5, 1, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 25, 30, 40, 999]

def _bin_to_ms(bin_num, edges=VCMTS_BIN_EDGES_MS):
    """Convert 1-based bin number to upper edge ms string."""
    if bin_num <= 0:
        return '0'
    idx = bin_num - 1
    if idx < len(edges):
        v = edges[idx]
        return f'{v:.0f}ms' if v >= 1 else f'{v*1000:.0f}\u00b5s'
    return f'{edges[-1]:.0f}ms+'


def _test_only(df):
    """Filter to test-phase rows only if phase column is populated, else return all."""
    if 'phase' in df.columns and df['phase'].notna().any():
        test = df[df['phase'] == 'test']
        return test if not test.empty else df
    return df


    """Filter to test-phase rows only if phase column is populated, else return all."""
    if 'phase' in df.columns and df['phase'].notna().any():
        test = df[df['phase'] == 'test']
        return test if not test.empty else df
    return df


def page_summary(pdf, us, k_us, k_ds, **m):
    rows = []

    # SNMP US
    if us is not None and not us.empty:
        for sfid, grp in _test_only(us).groupby('sfid'):
            grp = grp.sort_values('captured_utc')
            scn = grp['ps_scn'].dropna().iloc[-1] if 'ps_scn' in grp.columns and grp['ps_scn'].notna().any() else ''
            if 'delta_flow_octets' in grp.columns:
                grp['interval_s'] = grp['captured_utc'].diff().dt.total_seconds().clip(lower=1)
                grp['mbps'] = pd.to_numeric(grp['delta_flow_octets'], errors='coerce').clip(lower=0) * 8 / grp['interval_s'] / 1_000_000
                tp = grp['mbps'].max()
            else:
                tp = 0
            lat_max = pd.to_numeric(grp.get('lat_max_usec', pd.Series()), errors='coerce').max()
            lat_max_ms = round(lat_max / 1000, 3) if pd.notna(lat_max) else 0
            bin_cols   = [f'lat_bin{i}' for i in range(1, 17)]
            delta_cols = [f'delta_lat_bin{i}' for i in range(1, 17)]
            if all(c in grp.columns for c in delta_cols):
                grp_d  = grp[grp['poll_index'] > grp['poll_index'].min()] if 'poll_index' in grp.columns else grp
                deltas = [_toint(pd.to_numeric(grp_d[c], errors='coerce').clip(lower=0).sum()) for c in delta_cols]
            elif [c for c in bin_cols if c in grp.columns]:
                present = [c for c in bin_cols if c in grp.columns]
                cum = grp[present].apply(pd.to_numeric, errors='coerce')
                deltas = [_toint(cum[c].diff().clip(lower=0).sum()) for c in present]
            else:
                deltas = []
            p50  = _calc_percentile(deltas, 0.50)  if deltas else 0
            p99  = _calc_percentile(deltas, 0.99)  if deltas else 0
            p999 = _calc_percentile(deltas, 0.999) if deltas else 0
            wavg = round(_calc_weighted_avg(deltas), 3) if deltas else 0
            aqm    = int(pd.to_numeric(grp['delta_cong_aqm_drop'],   errors='coerce').sum()) if 'delta_cong_aqm_drop'   in grp.columns else 0
            ce     = int(pd.to_numeric(grp['delta_cong_ce_marked'],  errors='coerce').sum()) if 'delta_cong_ce_marked'  in grp.columns else 0
            ect0   = int(pd.to_numeric(grp['delta_cong_ect0'],       errors='coerce').sum()) if 'delta_cong_ect0'       in grp.columns else 0
            ect1   = int(pd.to_numeric(grp['delta_cong_ect1'],       errors='coerce').sum()) if 'delta_cong_ect1'       in grp.columns else 0
            policed= int(pd.to_numeric(grp['delta_flow_policed_drop'],errors='coerce').sum()) if 'delta_flow_policed_drop' in grp.columns else 0
            rows.append((str(sfid), str(scn), 'US', 'SNMP',
                         round(tp, 3), wavg, lat_max_ms, p50, p99, p999,
                         aqm, ce, ect0, ect1, policed, 0.0))

    # Kafka DS + US
    for kdf, direction, source in [(_test_only(k_ds), 'DS', 'Kafka'), (_test_only(k_us), 'US', 'Kafka')]:
        if kdf is None or kdf.empty:
            continue
        grp_col = 'sfid_label' if 'sfid_label' in kdf.columns else 'sfid'
        for name, grp in kdf.groupby(grp_col):
            grp  = grp.sort_values('captured_utc')
            sfid = str(grp['sfid'].dropna().iloc[-1]) if 'sfid' in grp.columns and grp['sfid'].notna().any() else str(name)
            scn  = str(grp['scn'].dropna().iloc[-1])  if 'scn'  in grp.columns and grp['scn'].notna().any()  else ''
            if 'delta_octets' in grp.columns:
                grp['interval_s'] = grp['captured_utc'].diff().dt.total_seconds().clip(lower=1)
                grp['mbps'] = pd.to_numeric(grp['delta_octets'], errors='coerce').clip(lower=0) * 8 / grp['interval_s'] / 1_000_000
                tp = grp['mbps'].max()
            else:
                tp = 0
            lat_max = pd.to_numeric(grp.get('lat_max_usec', pd.Series()), errors='coerce').max()
            lat_max_ms = round(float(lat_max) / 1000, 3) if pd.notna(lat_max) else 0
            lat_avg = pd.to_numeric(grp.get('lat_avg_usec', pd.Series()), errors='coerce')
            active_lat = lat_avg[lat_avg > 1000.0] / 1000  # usec → ms, exclude idle polls (<1ms)
            wavg = round(active_lat.mean(), 3) if not active_lat.empty else 0
            bin_cols = [f'lat_bin{i}' for i in range(1, 17)]
            present  = [c for c in bin_cols if c in grp.columns]
            if present:
                cum    = grp[present].apply(pd.to_numeric, errors='coerce')
                deltas = [_toint(cum[c].diff().clip(lower=0).sum()) for c in present]
                p50  = _bin_to_ms(_calc_percentile(deltas, 0.50))
                p99  = _bin_to_ms(_calc_percentile(deltas, 0.99))
                p999 = _bin_to_ms(_calc_percentile(deltas, 0.999))
            else:
                p50 = p99 = p999 = '0'
            aqm  = int(pd.to_numeric(grp['cong_aqm_drop'],  errors='coerce').diff().clip(lower=0).sum()) if 'cong_aqm_drop'  in grp.columns else 0
            ce   = int(pd.to_numeric(grp['cong_ce_marked'], errors='coerce').diff().clip(lower=0).sum()) if 'cong_ce_marked' in grp.columns else 0
            pkts_pass = pd.to_numeric(grp['delta_pkts'],         errors='coerce').sum() if 'delta_pkts'         in grp.columns else 0
            pkts_drop = pd.to_numeric(grp['delta_pkts_dropped'], errors='coerce').sum() if 'delta_pkts_dropped' in grp.columns else 0
            total_pkts = pkts_pass + pkts_drop
            loss_pct = round(pkts_drop / total_pkts * 100, 3) if total_pkts > 0 else 0.0
            rows.append((sfid, scn, direction, source,
                         round(tp, 3), wavg, lat_max_ms, p50, p99, p999,
                         aqm, ce, 0, 0, 0, loss_pct))

    if not rows:
        return

    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor(BG_DARK)
    ax = fig.add_axes([0.01, 0.06, 0.98, 0.80])
    ax.set_facecolor(BG_PANEL)
    ax.axis('off')

    col_labels = ['SFID', 'SCN', 'Dir', 'Src',
                  'Peak\nMbps', 'WAvg\nms', 'Max\nms',
                  'P50\nms', 'P99\nms', 'P99.9\nms',
                  'AQM\nDrop', 'CE\nMark', 'ECT0', 'ECT1',
                  'Policed\nDrop', 'Loss%']
    col_widths  = [0.07, 0.10, 0.04, 0.05,
                   0.06, 0.06, 0.06,
                   0.05, 0.05, 0.06,
                   0.06, 0.06, 0.05, 0.05,
                   0.07, 0.05]
    tbl = ax.table(cellText=[[str(v) for v in r] for r in rows],
                   colLabels=col_labels, colWidths=col_widths,
                   loc='center', cellLoc='center')
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(7.5)
    tbl.scale(1, 1.8)
    for col in range(len(col_labels)):
        cell = tbl[0, col]
        cell.set_facecolor(ACCENT)
        cell.set_text_props(color='white', fontweight='bold')
    for row_i in range(len(rows)):
        bg = BG_PANEL if row_i % 2 == 0 else BG_DARK
        for col in range(len(col_labels)):
            cell = tbl[row_i + 1, col]
            cell.set_facecolor(bg)
            cell.set_text_props(color=TEXT_COLOR)
            cell.set_edgecolor(GRID_COLOR)

    _sp2 = {k: m[k] for k in ('mac_fmt', 'modem_name', 'session_start', 'session_end')}
    _sp2['cmts_type'] = m.get('cmts_type', 'vcmts')
    save_page(pdf, fig, ax,
              'SESSION SUMMARY — THROUGHPUT & LATENCY',
              f'{m["modem_name"]} ({m["mac_fmt"]})  |  Peak Mbps, WAvg/Max latency, P50/P99/P99.9, AQM/CE/ECT/Policed drops',
              **_sp2)


# ---------------------------------------------------------------------------
# US SNMP chart pages
# ---------------------------------------------------------------------------
def _sp(m):
    d = {k: m[k] for k in ('mac_fmt', 'modem_name', 'session_start', 'session_end')}
    d['cmts_type']       = m.get('cmts_type', 'vcmts')
    d['test_start']      = m.get('test_start')
    d['cooldown_start']  = m.get('cooldown_start')
    return d


def _annotate(ax, m):
    _annotate_phases(ax, m.get('test_start'), m.get('cooldown_start'))


def page_us_flow_stats(pdf, us, **m):
    if 'delta_flow_octets' not in us.columns:
        return
    df = us.sort_values(['sfid', 'captured_utc']).copy()
    # Compute Mbps using actual interval between consecutive polls per SFID
    # Poll 1 has no prior baseline — NaN so it is omitted from the line,
    # x-axis still anchors at the first timestamp
    df['interval_s'] = df.groupby('sfid')['captured_utc'].diff().dt.total_seconds().clip(lower=1)
    df['mbps'] = (df['delta_flow_octets'].clip(lower=0) * 8 / df['interval_s'] / 1_000_000)
    # Fill poll-1 NaN with 0 so the line anchors at session start instead of poll 2
    df['mbps'] = df['mbps'].fillna(0)
    if not df['mbps'].gt(0).any():
        return
    fig, ax = make_fig()
    plot_line(ax, df, 'mbps', 'sfid', 'Throughput (Mbps)')
    _annotate(ax, m)
    save_page(pdf, fig, ax, 'US FLOW THROUGHPUT (Mbps)',
              f'{m["modem_name"]} ({m["mac_fmt"]})  |  SNMP delta_flow_octets / poll interval', **_sp(m))

    if all(c in us.columns for c in ('delta_flow_policed_drop', 'delta_flow_policed_delay')):
        fig, ax = make_fig()
        for i, (sfid, grp) in enumerate(us.groupby('sfid')):
            grp = grp.sort_values('captured_utc')
            color = CHART_COLORS[i % len(CHART_COLORS)]
            intervals = grp['captured_utc'].diff().dt.total_seconds().dropna()
            bar_width = (intervals.median() if not intervals.empty else 15) * 0.4 / 86400
            drop = pd.to_numeric(grp['delta_flow_policed_drop'], errors='coerce').fillna(0)
            delay = pd.to_numeric(grp['delta_flow_policed_delay'], errors='coerce').fillna(0)
            ax.bar(grp['captured_utc'], drop, width=bar_width, color=color,
                   alpha=0.85, label=f'{sfid} drop')
            ax.bar(grp['captured_utc'], delay, width=bar_width, color=color,
                   alpha=0.4, linestyle='--', label=f'{sfid} delay', edgecolor=color, linewidth=1)
        ax.set_ylabel('Packets / poll', color=SUBTEXT, fontsize=10, fontweight='bold')
        ax.set_xlabel('Time (UTC)', color=SUBTEXT, fontsize=10)
        ax.legend(fontsize=8, facecolor=BG_DARK, edgecolor=GRID_COLOR,
                  labelcolor=TEXT_COLOR, framealpha=0.9)
        fmt_ax(ax)
        _annotate(ax, m)
        save_page(pdf, fig, ax, 'US POLICED DROP & DELAY',
                  f'{m["modem_name"]} ({m["mac_fmt"]})  |  delta per poll', **_sp(m))

    if 'delta_flow_aqm_drop' in us.columns:
        df = us.copy()
        grp_col = 'sfid'
        fig, ax = make_fig()
        for i, (sfid, grp) in enumerate(df.groupby(grp_col)):
            grp = grp.sort_values('captured_utc')
            color = CHART_COLORS[i % len(CHART_COLORS)]
            vals = pd.to_numeric(grp['delta_flow_aqm_drop'], errors='coerce').fillna(0)
            # Width = 40% of median poll interval in matplotlib date units (days)
            intervals = grp['captured_utc'].diff().dt.total_seconds().dropna()
            bar_width = (intervals.median() if not intervals.empty else 15) * 0.4 / 86400
            ax.bar(grp['captured_utc'], vals, width=bar_width, color=color,
                   alpha=0.85, label=str(sfid))
        ax.set_ylabel('Packets / poll', color=SUBTEXT, fontsize=10, fontweight='bold')
        ax.set_xlabel('Time (UTC)', color=SUBTEXT, fontsize=10)
        ax.legend(fontsize=8, facecolor=BG_DARK, edgecolor=GRID_COLOR,
                  labelcolor=TEXT_COLOR, framealpha=0.9)
        fmt_ax(ax)
        _annotate(ax, m)
        save_page(pdf, fig, ax, 'US AQM DROPPED PACKETS',
                  f'{m["modem_name"]} ({m["mac_fmt"]})  |  delta per poll', **_sp(m))


def page_us_latency_max(pdf, us, **m):
    col = 'lat_max_usec'
    if col not in us.columns or pd.to_numeric(us[col], errors='coerce').dropna().empty:
        return
    df = us.copy()
    df[col] = pd.to_numeric(df[col], errors='coerce') / 1000
    fig, ax = make_fig()
    plot_line(ax, df, col, 'sfid', 'Latency (ms)')
    _annotate(ax, m)
    save_page(pdf, fig, ax, 'US LATENCY MAX (ms)',
              f'{m["modem_name"]} ({m["mac_fmt"]})  |  AQM-enabled SFIDs only', **_sp(m))


def page_us_latency_histogram(pdf, us, **m):
    us = _test_only(us)
    bin_cols = [f'lat_bin{i}' for i in range(1, 17)]
    present  = [c for c in bin_cols if c in us.columns]
    if not present:
        return
    has_bins = us[present].apply(pd.to_numeric, errors='coerce').notna().any(axis=1)
    for sfid in us.loc[has_bins, 'sfid'].unique():
        grp = us[(us['sfid'] == sfid) & has_bins].sort_values('captured_utc')
        if grp.empty:
            continue
        last = grp.iloc[-1]
        scn  = str(last.get('ps_scn') or sfid)

        # Sum delta_lat_bin cols across all polls — skip poll 1 (no prior baseline)
        delta_cols = [f'delta_lat_bin{i}' for i in range(1, 17)]
        if all(c in grp.columns for c in delta_cols):
            # poll_index 1 has empty deltas — drop it before summing
            grp_delta = grp[grp['poll_index'] > grp['poll_index'].min()] if 'poll_index' in grp.columns else grp
            bins = [int(pd.to_numeric(grp_delta[c], errors='coerce').clip(lower=0).sum()) for c in delta_cols]
        else:
            cum = grp[present].apply(pd.to_numeric, errors='coerce')
            bins = [int(cum[c].diff().clip(lower=0).sum()) for c in present]

        # Build x-axis labels from lat_edge_bin values (in µs → ms) — bins 1-16
        edge_cols = [f'lat_edge_bin{i}' for i in range(1, 17)]
        edges_us  = [pd.to_numeric(last.get(c), errors='coerce') for c in edge_cols]
        x_labels = [f'Bin {i+1}' for i in range(len(present))]

        fig, ax = make_fig()
        x = range(len(present))
        bars = ax.bar(x, bins, color=ACCENT, edgecolor=BG_DARK, linewidth=0.5, width=0.7)
        max_val = max(bins) if any(b > 0 for b in bins) else 1
        for bar, val in zip(bars, bins):
            if val > 0:
                ax.text(bar.get_x() + bar.get_width() / 2,
                        bar.get_height() + max_val * 0.01,
                        f'{val:,}', ha='center', va='bottom', fontsize=6, color=TEXT_COLOR)
        ax.set_xticks(list(x))
        ax.set_xticklabels(x_labels, rotation=45, ha='right', fontsize=7, color=TEXT_COLOR)
        ax.set_xlabel('Latency Range', color=SUBTEXT, fontsize=10)
        ax.set_ylabel('Packets (all polls)', color=SUBTEXT, fontsize=10, fontweight='bold')
        ax.grid(True, color=GRID_COLOR, linewidth=0.7, linestyle='--', alpha=0.8, axis='y')
        ax.tick_params(colors=TEXT_COLOR)
        for spine in ax.spines.values():
            spine.set_edgecolor(GRID_COLOR)
        lat_max_ms = pd.to_numeric(last.get('lat_max_usec'), errors='coerce')
        lat_max_str = f'  |  lat_max={lat_max_ms/1000:.1f}ms' if pd.notna(lat_max_ms) else ''
        save_page(pdf, fig, ax, f'US LATENCY HISTOGRAM — SFID {sfid}',
                  f'{scn}  |  All polls summed{lat_max_str}', **_sp(m))


def page_us_congestion(pdf, us, **m):
    cong_cols = ['cong_aqm_drop', 'cong_ce_marked', 'cong_ect0', 'cong_ect1',
                 'delta_cong_aqm_drop', 'delta_cong_ce_marked', 'delta_cong_ect0', 'delta_cong_ect1']
    # prefer delta_ versions if present
    aqm_col = 'delta_cong_aqm_drop' if 'delta_cong_aqm_drop' in us.columns else 'cong_aqm_drop'
    ce_col  = 'delta_cong_ce_marked' if 'delta_cong_ce_marked' in us.columns else 'cong_ce_marked'
    ect0    = 'delta_cong_ect0' if 'delta_cong_ect0' in us.columns else 'cong_ect0'
    ect1    = 'delta_cong_ect1' if 'delta_cong_ect1' in us.columns else 'cong_ect1'

    if aqm_col in us.columns and ce_col in us.columns:
        has_data = pd.to_numeric(us[aqm_col], errors='coerce').dropna().gt(0).any() or \
                   pd.to_numeric(us[ce_col],  errors='coerce').dropna().gt(0).any()
        if has_data:
            fig, ax = make_fig()
            plot_dual(ax, us, aqm_col, ce_col, 'AQM drop', 'CE marked', 'sfid', 'Packets')
            _annotate(ax, m)
            save_page(pdf, fig, ax, 'US CONGESTION — AQM DROPS & CE MARKED',
                      f'{m["modem_name"]} ({m["mac_fmt"]})', **_sp(m))

    if ect0 in us.columns and ect1 in us.columns:
        has_data = pd.to_numeric(us[ect0], errors='coerce').dropna().gt(0).any() or \
                   pd.to_numeric(us[ect1], errors='coerce').dropna().gt(0).any()
        if has_data:
            fig, ax = make_fig()
            plot_dual(ax, us, ect0, ect1, 'ECT(0)', 'ECT(1)', 'sfid', 'Packets')
            _annotate(ax, m)
            save_page(pdf, fig, ax, 'US ECT(0) & ECT(1) PACKETS',
                      f'{m["modem_name"]} ({m["mac_fmt"]})', **_sp(m))


def page_us_param_set(pdf, us, **m):
    rate_col = 'ps_max_rate'
    if rate_col not in us.columns or pd.to_numeric(us[rate_col], errors='coerce').dropna().empty:
        return
    df = us.copy()
    df[rate_col] = pd.to_numeric(df[rate_col], errors='coerce') / 1_000_000
    fig, ax = make_fig()
    plot_line(ax, df, rate_col, 'sfid', 'Mbps')
    save_page(pdf, fig, ax, 'US PARAM SET — MAX RATE (Mbps)',
              f'{m["modem_name"]} ({m["mac_fmt"]})', **_sp(m))

    buf_cols = ['ps_min_buffer', 'ps_target_buffer', 'ps_max_buffer']
    present  = [c for c in buf_cols if c in us.columns and
                pd.to_numeric(us[c], errors='coerce').dropna().any()]
    if present:
        fig, ax = make_fig()
        for i, col in enumerate(present):
            for j, (sfid, grp) in enumerate(us.groupby('sfid')):
                c = CHART_COLORS[(i * 3 + j) % len(CHART_COLORS)]
                ax.plot(grp['captured_utc'], pd.to_numeric(grp[col], errors='coerce'),
                        marker='o', markersize=3, linewidth=1.5, color=c,
                        label=f'{sfid} {col.replace("ps_", "")}')
        ax.set_ylabel('Bytes', color=SUBTEXT, fontsize=10, fontweight='bold')
        ax.set_xlabel('Time (UTC)', color=SUBTEXT, fontsize=10)
        ax.legend(fontsize=7, facecolor=BG_DARK, edgecolor=GRID_COLOR,
                  labelcolor=TEXT_COLOR, framealpha=0.9, ncol=2)
        fmt_ax(ax)
        save_page(pdf, fig, ax, 'US PARAM SET — BUFFER TARGETS',
                  f'{m["modem_name"]} ({m["mac_fmt"]})  |  min / target / max', **_sp(m))


# ---------------------------------------------------------------------------
# Kafka chart pages (DS from Kafka; US Kafka optional)
# ---------------------------------------------------------------------------
def page_kafka_throughput(pdf, kdf, direction, **m):
    col = 'delta_octets'
    if col not in kdf.columns or pd.to_numeric(kdf[col], errors='coerce').dropna().empty:
        return
    grp_col = 'sfid_label' if 'sfid_label' in kdf.columns else 'sfid'
    df = kdf.sort_values([grp_col, 'captured_utc']).copy()
    df['interval_s'] = df.groupby(grp_col)['captured_utc'].diff().dt.total_seconds().clip(lower=1)
    df['mbps'] = pd.to_numeric(df[col], errors='coerce').clip(lower=0) * 8 / df['interval_s'] / 1_000_000
    df['mbps'] = df['mbps'].fillna(0)
    fig, ax = make_fig()
    plot_line(ax, df, 'mbps', grp_col, 'Throughput (Mbps)')
    _annotate(ax, m)
    save_page(pdf, fig, ax, f'{direction.upper()} THROUGHPUT (Mbps)',
              f'{m["modem_name"]} ({m["mac_fmt"]})  |  Kafka delta_octets → Mbps',
              **_sp(m))


def page_kafka_latency(pdf, kdf, direction, **m):
    """Avg latency over time — already in ms from vCMTS Kafka."""
    grp_col = 'sfid_label' if 'sfid_label' in kdf.columns else 'sfid'
    _kw = {k: m[k] for k in ('mac_fmt', 'modem_name', 'session_start', 'session_end')}
    _kw['cmts_type'] = m.get('cmts_type', 'vcmts')

    for col, title in [('lat_avg_usec', f'{direction.upper()} LATENCY AVG (ms)')]:
        if col not in kdf.columns or pd.to_numeric(kdf[col], errors='coerce').dropna().empty:
            continue
        df = kdf.copy()
        df[col] = pd.to_numeric(df[col], errors='coerce') / 1000  # µs → ms
        # Filter out SFIDs with no latency data (all zeros)
        active = df.groupby(grp_col)[col].max()
        active_labels = active[active.fillna(0) > 0].index
        df = df[df[grp_col].isin(active_labels)]
        if df.empty:
            continue
        fig, ax = make_fig()
        plot_line(ax, df, col, grp_col, 'Latency (ms)')
        _annotate(ax, m)
        save_page(pdf, fig, ax, title,
                  f'{m["modem_name"]} ({m["mac_fmt"]})  |  vCMTS Kafka µs→ms', **_kw)


def page_kafka_latency_histogram(pdf, kdf, direction, **m):
    kdf = _test_only(kdf)
    bin_cols = [f'lat_bin{i}' for i in range(1, 17)]
    present  = [c for c in bin_cols if c in kdf.columns]
    if not present:
        return
    grp_col = 'sfid_label' if 'sfid_label' in kdf.columns else 'sfid'
    _kw = {k: m[k] for k in ('mac_fmt', 'modem_name', 'session_start', 'session_end')}
    _kw['cmts_type'] = m.get('cmts_type', 'vcmts')

    # vCMTS fixed DS bin edges in ms (bin N covers prev_edge – this_edge)
    x_labels = [f'Bin {i+1}' for i in range(len(present))]

    has_bins = kdf[present].apply(pd.to_numeric, errors='coerce').notna().any(axis=1)
    for name, grp in kdf[has_bins].groupby(grp_col):
        # Skip WiFi traffic flows
        if 'WFT' in str(name).upper():
            continue
        grp = grp.sort_values('captured_utc')
        last = grp.iloc[-1]
        # Diff cumulative bins across all rows, skip first (no baseline), sum all poll deltas
        cum = grp[present].apply(pd.to_numeric, errors='coerce')
        bins = [int(cum[c].diff().clip(lower=0).sum()) for c in present]
        lat_max = pd.to_numeric(grp['lat_max_usec'].max(), errors='coerce') if 'lat_max_usec' in grp.columns else float('nan')
        lat_avg = pd.to_numeric(grp['lat_avg_usec'], errors='coerce').mean() if 'lat_avg_usec' in grp.columns else float('nan')
        subtitle = 'All polls summed'
        if pd.notna(lat_max) and lat_max > 0:
            subtitle += f'  |  avg={lat_avg/1000:.2f}ms  max={lat_max/1000:.2f}ms'
        fig, ax = make_fig()
        x = range(len(present))
        bars = ax.bar(x, bins, color=ACCENT, edgecolor=BG_DARK, linewidth=0.5, width=0.7)
        max_val = max(bins) if any(b > 0 for b in bins) else 1
        for bar, val in zip(bars, bins):
            if val > 0:
                ax.text(bar.get_x() + bar.get_width() / 2,
                        bar.get_height() + max_val * 0.01,
                        f'{val:,}', ha='center', va='bottom', fontsize=6, color=TEXT_COLOR)
        ax.set_xticks(list(x))
        ax.set_xticklabels(x_labels, rotation=45, ha='right', fontsize=7, color=TEXT_COLOR)
        ax.set_xlabel('Latency Range', color=SUBTEXT, fontsize=10)
        ax.set_ylabel('Packets (all polls)', color=SUBTEXT, fontsize=10, fontweight='bold')
        ax.grid(True, color=GRID_COLOR, linewidth=0.7, linestyle='--', alpha=0.8, axis='y')
        ax.tick_params(colors=TEXT_COLOR)
        for spine in ax.spines.values():
            spine.set_edgecolor(GRID_COLOR)
        save_page(pdf, fig, ax,
                  f'{direction.upper()} LATENCY HISTOGRAM — {name}',
                  subtitle, **_kw)


def page_kafka_congestion(pdf, kdf, direction, **m):
    grp_col = 'sfid_label' if 'sfid_label' in kdf.columns else 'sfid'
    _kw = {k: m[k] for k in ('mac_fmt', 'modem_name', 'session_start', 'session_end')}
    _kw['cmts_type'] = m.get('cmts_type', 'vcmts')

    for raw_col, title in [('cong_aqm_drop',  f'{direction.upper()} AQM DROPPED PACKETS'),
                           ('cong_ce_marked', f'{direction.upper()} AQM MARKED PACKETS')]:
        if raw_col not in kdf.columns:
            continue
        df = kdf.copy()
        df[raw_col] = df.groupby(grp_col)[raw_col].transform(
            lambda s: pd.to_numeric(s, errors='coerce').diff().clip(lower=0))
        if df[raw_col].dropna().empty or not df[raw_col].gt(0).any():
            continue
        # Filter out SFIDs with no drops
        active = df.groupby(grp_col)[raw_col].max()
        active_labels = active[active.fillna(0) > 0].index
        df = df[df[grp_col].isin(active_labels)]
        fig, ax = make_fig()
        for i, (name, grp) in enumerate(df.groupby(grp_col)):
            grp = grp.sort_values('captured_utc')
            # Drop row 1 per SFID (NaN from diff — no prior baseline)
            grp = grp[grp[raw_col].notna()]
            if grp.empty:
                continue
            color = CHART_COLORS[i % len(CHART_COLORS)]
            intervals = grp['captured_utc'].diff().dt.total_seconds().dropna()
            bar_width = (intervals.median() if not intervals.empty else 15) * 0.4 / 86400
            ax.bar(grp['captured_utc'], grp[raw_col],
                   width=bar_width, color=color, alpha=0.85, label=str(name))
        ax.set_ylabel('Packets / poll', color=SUBTEXT, fontsize=10, fontweight='bold')
        ax.set_xlabel('Time (UTC)', color=SUBTEXT, fontsize=10)
        ax.legend(fontsize=8, facecolor=BG_DARK, edgecolor=GRID_COLOR,
                  labelcolor=TEXT_COLOR, framealpha=0.9)
        fmt_ax(ax)
        _annotate(ax, m)
        save_page(pdf, fig, ax, title,
                  f'{m["modem_name"]} ({m["mac_fmt"]})  |  Kafka delta per poll', **_kw)


# ---------------------------------------------------------------------------
# API-callable entry point
# ---------------------------------------------------------------------------
def generate_report(session_id: str, session_name: str, conn) -> str:
    """Generate PDF for a session and return the output path. No prompts."""
    sessions = _list_sessions(conn)
    row = sessions[sessions['id'].str.startswith(session_id)]
    if row.empty:
        raise ValueError(f'Session not found: {session_id}')
    sid  = row.iloc[0]['id']
    meta = row.iloc[0]
    us         = _load_snmp(conn, sid)
    k_us, k_ds = _load_kafka(conn, sid)
    test_start, cooldown_start = _get_phase_times(conn, sid)

    cmts_type  = meta['cmts_type']
    mac_fmt    = meta['mac']
    modem_name = MODEM_NAMES.get(mac_fmt, mac_fmt)

    all_times = pd.concat([
        us['captured_utc'] if not us.empty else pd.Series(dtype='datetime64[ns]'),
        k_ds['captured_utc'] if not k_ds.empty else pd.Series(dtype='datetime64[ns]'),
    ]).dropna()
    if all_times.empty:
        raise ValueError('No data rows found for this session')

    session_start = all_times.min().strftime('%Y-%m-%d %H:%M UTC')
    session_end   = all_times.max().strftime('%Y-%m-%d %H:%M UTC')
    duration_secs = int((all_times.max() - all_times.min()).total_seconds())
    hours, rem    = divmod(duration_secs, 3600)
    duration_str  = f'{hours}h {rem // 60}m'
    total_polls   = us['poll_index'].nunique() if 'poll_index' in us.columns else len(us)
    us_sfids = sorted(us['sfid'].unique(), key=lambda x: int(x) if str(x).isdigit() else 0) if not us.empty else []
    ds_sfids = sorted(k_ds['sfid'].unique(), key=lambda x: int(x) if str(x).isdigit() else 0) if not k_ds.empty else []

    m = dict(mac_fmt=mac_fmt, modem_name=modem_name,
             session_start=session_start, session_end=session_end,
             cmts_type=cmts_type, test_start=test_start, cooldown_start=cooldown_start)

    toc = [
        ('3',  'Session Summary',         'SFID/SCN, Peak Mbps, WAvg/Max latency, P50/P99/P99.9, AQM/CE/ECT/Policed drops, Loss%'),
        ('4',  'US Flow Throughput',      'SNMP delta_flow_octets → Mbps per US service flow'),
        ('5',  'US Policed Drop & Delay', 'SNMP policed drop and delay packet counts per US flow'),
        ('6',  'US AQM Dropped Packets',  'SNMP AQM drop counters per US service flow'),
        ('7',  'US Latency Histogram',    'SNMP 16-bin latency distribution (all polls summed, per SFID)'),
        ('8',  'US Congestion — AQM & CE','SNMP AQM drops and CE marked packets per US flow'),
        ('9',  'DS Throughput (Mbps)',    'Kafka delta_octets → Mbps per DS flow'),
        ('10', 'DS Latency Avg (ms)',     'Kafka average latency per DS flow'),
        ('11', 'DS Latency Histogram',    'Kafka 16-bin latency distribution (all polls summed, per SFID)'),
        ('12', 'DS AQM Dropped Packets',  'Kafka AQM drop packets per DS flow'),
        ('13', 'DS AQM Marked Packets',   'Kafka CE marked packets per DS flow'),
    ]

    safe_name   = re.sub(r'[^\w\-]', '_', session_name).strip('_')
    reports_dir = os.path.join(HERE, 'reports')
    os.makedirs(reports_dir, exist_ok=True)
    out_path = os.path.join(reports_dir, f'report_{cmts_type}_{sid[:8]}_{safe_name}.pdf')

    with PdfPages(out_path) as pdf:
        page_cover(pdf, mac_fmt, modem_name, session_start, session_end,
                   duration_str, total_polls, us_sfids, ds_sfids, cmts_type, session_name, sid)
        page_toc(pdf, mac_fmt, modem_name, session_start, session_end, toc, cmts_type)
        page_summary(pdf, us, k_us, k_ds, **m)
        page_us_flow_stats(pdf, us, **m)
        page_us_latency_histogram(pdf, us, **m)
        page_us_congestion(pdf, us, **m)
        page_kafka_throughput(pdf, k_ds, 'downstream', **m)
        page_kafka_latency(pdf, k_ds, 'downstream', **m)
        page_kafka_latency_histogram(pdf, k_ds, 'downstream', **m)
        page_kafka_congestion(pdf, k_ds, 'downstream', **m)
        d = pdf.infodict()
        d['Title']   = f'{cmts_type.upper()} SNMP+Kafka Report — {modem_name} ({mac_fmt})'
        d['Author']  = 'aphillips — Spectrum Access Engineering'
        d['Subject'] = session_name

    return out_path


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def main():
    sid, meta, us, k_us, k_ds, test_start, cooldown_start = pick_session(sys.argv[1] if len(sys.argv) > 1 else None)

    cmts_type  = meta['cmts_type']
    mac_fmt    = meta['mac']
    modem_name = MODEM_NAMES.get(mac_fmt, mac_fmt)

    if len(sys.argv) > 2:
        session_name = sys.argv[2]
    else:
        session_name = input('Session name [Enter for default]: ').strip()
    if not session_name:
        session_name = f'{cmts_type.upper()} SNMP+Kafka Telemetry Report'

    print(f'\nSession: {sid}  [{cmts_type}]  MAC: {mac_fmt}')

    # Session time bounds from SNMP + Kafka
    all_times = pd.concat([
        us['captured_utc'],
        k_ds['captured_utc'] if not k_ds.empty else pd.Series(dtype='datetime64[ns]'),
    ]).dropna()
    session_start = all_times.min().strftime('%Y-%m-%d %H:%M UTC')
    session_end   = all_times.max().strftime('%Y-%m-%d %H:%M UTC')
    duration_secs = int((all_times.max() - all_times.min()).total_seconds())
    hours, rem    = divmod(duration_secs, 3600)
    duration_str  = f'{hours}h {rem // 60}m'
    total_polls   = us['poll_index'].nunique() if 'poll_index' in us.columns else len(us)

    us_sfids = sorted(us['sfid'].unique(), key=lambda x: int(x) if str(x).isdigit() else 0)
    ds_sfids = sorted(k_ds['sfid'].unique(), key=lambda x: int(x) if str(x).isdigit() else 0) \
               if not k_ds.empty else []

    m = dict(mac_fmt=mac_fmt, modem_name=modem_name,
             session_start=session_start, session_end=session_end,
             cmts_type=cmts_type,
             test_start=test_start, cooldown_start=cooldown_start)

    toc = [
        ('3',  'Session Summary',             'SFID/SCN, Peak Mbps, WAvg/Max latency, P50/P99/P99.9, AQM/CE/ECT0/ECT1/Policed drops, Loss%'),
        ('4',  'US Flow Throughput',          'SNMP delta_flow_octets → Mbps per US service flow'),
        ('5',  'US Policed Drop & Delay',     'SNMP policed drop and delay packet counts per US flow'),
        ('6',  'US AQM Dropped Packets',      'SNMP AQM drop counters per US service flow'),
        ('7',  'US Latency Max (ms)',          'SNMP peak latency per AQM-enabled US flow over time'),
        ('8',  'US Latency Histogram',        'SNMP 16-bin latency distribution (all polls summed, per SFID)'),
        ('9',  'US Congestion — AQM & CE',    'SNMP AQM drops and CE marked packets per US flow'),
        ('10', 'US Param Set — Max Rate',     'SNMP active param set max rate (Mbps) per US flow'),
        ('11', 'DS Throughput (Mbps)',        'Kafka delta_octets → Mbps per DS flow'),
        ('12', 'DS Latency Avg (ms)',         'Kafka average latency per DS flow'),
        ('13', 'DS Latency Histogram',        'Kafka 16-bin latency distribution (all polls summed, per SFID)'),
        ('14', 'DS AQM Dropped Packets',      'Kafka AQM drop packets per DS flow'),
        ('15', 'DS AQM Marked Packets',       'Kafka CE marked packets per DS flow'),
    ]

    safe_name = re.sub(r'[^\w\-]', '_', session_name).strip('_')
    reports_dir = os.path.join(HERE, 'reports')
    os.makedirs(reports_dir, exist_ok=True)
    out_path  = os.path.join(reports_dir, f'report_{cmts_type}_{sid}_{safe_name}.pdf')

    with PdfPages(out_path) as pdf:
        page_cover(pdf, mac_fmt, modem_name, session_start, session_end,
                   duration_str, total_polls, us_sfids, ds_sfids,
                   cmts_type, session_name, sid)
        page_toc(pdf, mac_fmt, modem_name, session_start, session_end, toc, cmts_type)
        page_summary(pdf, us, k_us, k_ds, **m)

        # US — SNMP
        page_us_flow_stats(pdf, us, **m)
        page_us_latency_histogram(pdf, us, **m)
        page_us_congestion(pdf, us, **m)

        # DS — Kafka
        page_kafka_throughput(pdf, k_ds, 'downstream', **m)
        page_kafka_latency(pdf, k_ds, 'downstream', **m)
        page_kafka_latency_histogram(pdf, k_ds, 'downstream', **m)
        page_kafka_congestion(pdf, k_ds, 'downstream', **m)

        d = pdf.infodict()
        d['Title']   = f'{cmts_type.upper()} SNMP+Kafka Report — {modem_name} ({mac_fmt})'
        d['Author']  = 'aphillips — Spectrum Access Engineering'
        d['Subject'] = session_name

    print(f'\nPDF saved: {out_path}')


if __name__ == '__main__':
    main()
