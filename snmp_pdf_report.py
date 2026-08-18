"""
snmp_pdf_report.py
==================
Generates a PDF report from a CM Collector session directory.
Supports both iCMTS (snmp_us + snmp_ds) and vCMTS (snmp_us + kafka) sessions.

Usage:
    python snmp_pdf_report.py                        # auto-finds latest session in results/
    python snmp_pdf_report.py 206a949223b8           # specific MAC (latest session)
    python snmp_pdf_report.py path/to/session/dir    # specific session directory
    python snmp_pdf_report.py path/to/snmp_us_*.csv  # specific US CSV

Requirements:
    pip install pandas matplotlib
"""
import os
import sys
import glob
import re
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.backends.backend_pdf import PdfPages
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))

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

MODEM_NAMES  = {
    '0cb9379c64b4': 'Lab CM (iCMTS)',
    '206a949223b8': 'Lab CM (vCMTS)',
}

# ---------------------------------------------------------------------------
# Session discovery — returns a session dict
# ---------------------------------------------------------------------------
def _session_from_dir(d):
    """Inspect a directory and return a session dict, or None if not a valid session."""
    us_files    = sorted(glob.glob(os.path.join(d, 'snmp_us_*.csv')))
    ds_files    = sorted(glob.glob(os.path.join(d, 'snmp_ds_*.csv')))
    kafka_files = sorted(glob.glob(os.path.join(d, 'kafka_*.csv')))
    if not us_files:
        return None

    us_path    = us_files[-1]
    ds_path    = ds_files[-1] if ds_files else None
    kafka_path = kafka_files[-1] if kafka_files else None

    # Determine type and MAC from directory name or filename
    mac_m = re.search(r'([0-9a-f]{12})', os.path.basename(us_path))
    mac   = mac_m.group(1) if mac_m else 'unknown'

    # vcmts: has kafka, no ds  |  icmts: has ds, no kafka
    if kafka_path and not ds_path:
        cmts_type = 'vcmts'
    elif ds_path and not kafka_path:
        cmts_type = 'icmts'
    elif ds_path and kafka_path:
        cmts_type = 'vcmts'   # both present — vcmts with legacy ds stub
    else:
        return None           # only snmp_us with no ds or kafka — skip

    return {
        'cmts_type':   cmts_type,
        'mac':         mac,
        'us_path':     us_path,
        'ds_path':     ds_path,
        'kafka_path':  kafka_path,
        'session_dir': d,
    }


def find_session(arg=None):
    """Locate a session and return its session dict."""

    def _latest_session(mac_glob):
        dirs = sorted(glob.glob(os.path.join(HERE, 'results', mac_glob, '*')))
        for d in reversed(dirs):
            if os.path.isdir(d):
                s = _session_from_dir(d)
                if s:
                    return s
        return None

    if arg:
        # Explicit CSV file — derive session dir from it
        if arg.endswith('.csv'):
            path = arg if os.path.isabs(arg) else os.path.join(HERE, arg)
            s = _session_from_dir(os.path.dirname(path))
            if s:
                return s
            print(f'Could not build session from: {path}')
            sys.exit(1)

        # Session directory
        if os.path.isdir(arg):
            s = _session_from_dir(arg)
            if s:
                return s
            print(f'No valid session files found in {arg}')
            sys.exit(1)

        # MAC address
        mac = re.sub(r'[:\-.]', '', arg).lower()
        s = _latest_session(f'{mac}_*')
        if s:
            return s
        print(f'No session found for MAC {mac}')
        sys.exit(1)

    # Auto-find latest session across all MACs
    dirs = sorted(glob.glob(os.path.join(HERE, 'results', '*', '*')))
    for d in reversed(dirs):
        if os.path.isdir(d):
            s = _session_from_dir(d)
            if s:
                return s

    print('No valid session found under results/')
    sys.exit(1)

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
    hax.text(0.5, 0.62, title,
             transform=hax.transAxes, fontsize=16, fontweight='bold',
             color='white', ha='center', va='center', fontfamily='DejaVu Sans')
    if subtitle:
        hax.text(0.5, 0.15, subtitle,
                 transform=hax.transAxes, fontsize=8,
                 color=TEXT_COLOR, ha='center', va='center', fontstyle='italic')

def add_footer(fig, mac_fmt, modem_name, session_start, session_end, cmts_type='iCMTS'):
    fax = fig.add_axes([0, 0, 1, 0.04])
    fax.set_facecolor('#0a1628')
    fax.axis('off')
    fax.text(0.5, 0.5,
             f'{cmts_type.upper()} SNMP Report  |  {modem_name} ({mac_fmt})  |  {session_start} — {session_end}  |  aphillips — Charter Access Engineering',
             transform=fax.transAxes, fontsize=7, color='#445566',
             ha='center', va='center')

def make_fig():
    fig, ax = plt.subplots(figsize=(11, 5.8), subplot_kw={'facecolor': BG_PANEL})
    fig.patch.set_facecolor(BG_DARK)
    fig.subplots_adjust(top=0.88, bottom=0.18, left=0.09, right=0.97)
    style_ax(ax)
    return fig, ax

def save_page(pdf, fig, ax, header_title, subtitle, mac_fmt, modem_name, session_start, session_end, cmts_type='iCMTS'):
    add_header(fig, header_title, subtitle)
    add_footer(fig, mac_fmt, modem_name, session_start, session_end, cmts_type)
    pdf.savefig(fig, facecolor=fig.get_facecolor())
    plt.close(fig)

def plot_line(ax, df, y_col, group_col, ylabel, poll_col='poll_index'):
    """Line chart grouped by group_col, x-axis is poll_index or timestamp."""
    x_col = 'captured_utc' if 'captured_utc' in df.columns else poll_col
    for i, (name, grp) in enumerate(df.groupby(group_col)):
        color = CHART_COLORS[i % len(CHART_COLORS)]
        ax.plot(grp[x_col], grp[y_col],
                marker='o', markersize=4, linewidth=2, color=color, label=str(name),
                markerfacecolor='white', markeredgecolor=color, markeredgewidth=1.5)
        ax.fill_between(grp[x_col], grp[y_col], alpha=0.08, color=color)
    ax.set_ylabel(ylabel, color=SUBTEXT, fontsize=10, fontweight='bold')
    ax.set_xlabel('Time (UTC)', color=SUBTEXT, fontsize=10)
    ax.legend(fontsize=8, facecolor=BG_DARK, edgecolor=GRID_COLOR,
              labelcolor=TEXT_COLOR, framealpha=0.9)
    fmt_ax(ax)

def plot_dual(ax, df, col_a, col_b, label_a, label_b, group_col, ylabel):
    """Two metrics per group — solid for col_a, dashed for col_b."""
    for i, (name, grp) in enumerate(df.groupby(group_col)):
        c = CHART_COLORS[i % len(CHART_COLORS)]
        x = grp['captured_utc']
        ax.plot(x, grp[col_a], marker='o', markersize=4, linewidth=2,
                color=c, label=f'{name} {label_a}',
                markerfacecolor='white', markeredgecolor=c, markeredgewidth=1.5)
        ax.plot(x, grp[col_b], marker='x', markersize=4, linewidth=1.5,
                linestyle='--', color=c, alpha=0.7, label=f'{name} {label_b}')
    ax.set_ylabel(ylabel, color=SUBTEXT, fontsize=10, fontweight='bold')
    ax.set_xlabel('Time (UTC)', color=SUBTEXT, fontsize=10)
    ax.legend(fontsize=7, facecolor=BG_DARK, edgecolor=GRID_COLOR,
              labelcolor=TEXT_COLOR, framealpha=0.9, ncol=2)
    fmt_ax(ax)

# ---------------------------------------------------------------------------
# Page builders — Cover + TOC
# ---------------------------------------------------------------------------
def page_cover(pdf, mac_fmt, modem_name, session_start, session_end,
               duration_str, total_polls, us_sfids, ds_sfids, cmts_type='icmts', session_name=None):
    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor(BG_PANEL)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_facecolor(BG_PANEL)
    ax.axis('off')

    title_str = 'vCMTS SNMP SESSION REPORT' if cmts_type == 'vcmts' else 'iCMTS SNMP SESSION REPORT'
    sub_str   = 'Charter Communications  •  Access Engineering  •  vCMTS EFT' if cmts_type == 'vcmts' \
                else 'Charter Communications  •  Access Engineering  •  iCMTS EFT'

    ax.text(0.5, 0.935, title_str,
            transform=ax.transAxes, fontsize=26, fontweight='bold',
            color='white', ha='center', va='center', fontfamily='DejaVu Sans')
    ax.text(0.5, 0.865, sub_str,
            transform=ax.transAxes, fontsize=12, color=TEXT_COLOR,
            ha='center', va='center', fontfamily='DejaVu Sans', fontstyle='italic')
    if session_name:
        ax.text(0.5, 0.815, session_name,
                transform=ax.transAxes, fontsize=14, fontweight='bold',
                color=ACCENT, ha='center', va='center', fontfamily='DejaVu Sans')
    ax.axhline(y=0.79, xmin=0.05, xmax=0.95, color=ACCENT, linewidth=1.5)

    ds_label = 'Kafka (DS)' if cmts_type == 'vcmts' else ', '.join(str(s) for s in sorted(ds_sfids))
    labels = [
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
        y -= 0.062

    ax.text(0.5, 0.02,
            f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M")}   |   aphillips — Charter Access Engineering',
            transform=ax.transAxes, fontsize=8, color='#445566', ha='center', va='center')

    pdf.savefig(fig, facecolor=fig.get_facecolor())
    plt.close(fig)


def page_toc(pdf, mac_fmt, modem_name, session_start, session_end, contents, cmts_type='icmts'):
    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor(BG_PANEL)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_facecolor(BG_PANEL)
    ax.axis('off')

    ax.text(0.5, 0.935, 'TABLE OF CONTENTS',
            transform=ax.transAxes, fontsize=22, fontweight='bold',
            color='white', ha='center', va='center', fontfamily='DejaVu Sans')
    ax.axhline(y=0.88, xmin=0.05, xmax=0.95, color=ACCENT, linewidth=1)

    # Fit all entries — shrink row height if many entries
    n   = len(contents)
    gap = min(0.075, 0.78 / max(n, 1))
    y   = 0.80
    for i, (page, title, desc) in enumerate(contents):
        row_color = '#112240' if i % 2 == 0 else '#0d1b2a'
        ax.axhspan(y - gap * 0.4, y + gap * 0.55, facecolor=row_color, alpha=1.0)
        ax.text(0.07, y + 0.005, f'pg {page}',
                transform=ax.transAxes, fontsize=9, fontweight='bold',
                color=ACCENT, va='center', ha='center',
                bbox=dict(boxstyle='round,pad=0.3', facecolor='#0d2a4a',
                          edgecolor=ACCENT, linewidth=1))
        ax.text(0.15, y + 0.005, title,
                transform=ax.transAxes, fontsize=10, fontweight='bold',
                color=TEXT_COLOR, va='center', fontfamily='DejaVu Sans')
        ax.text(0.15, y - gap * 0.35, desc,
                transform=ax.transAxes, fontsize=8,
                color=SUBTEXT, va='center', fontstyle='italic')
        ax.axhline(y=y - gap * 0.4, xmin=0.05, xmax=0.95,
                   color=GRID_COLOR, linewidth=0.5, linestyle=':')
        y -= gap

    label = cmts_type.upper()
    ax.text(0.5, 0.02,
            f'{label} SNMP Report  |  {modem_name} ({mac_fmt})  |  {session_start} — {session_end}',
            transform=ax.transAxes, fontsize=8, color='#445566', ha='center', va='center')

    pdf.savefig(fig, facecolor=fig.get_facecolor())
    plt.close(fig)

# ---------------------------------------------------------------------------
# US chart pages
# ---------------------------------------------------------------------------
def _meta(mac_fmt, modem_name, session_start, session_end):
    return mac_fmt, modem_name, session_start, session_end


def _delta_mb(df, col):
    """Return df with col replaced by per-sfid poll-to-poll delta in MB."""
    df = df.copy()
    df[col] = pd.to_numeric(df[col], errors='coerce')
    df[col] = df.groupby('sfid')[col].diff().clip(lower=0) / 1e6
    return df


def _total_gb_label(df, col):
    """Subtitle string: total GB per sfid over session."""
    df = df.copy()
    df[col] = pd.to_numeric(df[col], errors='coerce')
    totals = df.groupby('sfid')[col].sum() / 1e3  # MB deltas → GB
    return '  |  '.join(f'SFID {s}: {v:.2f} GB' for s, v in totals.items())


def page_us_flow_stats(pdf, us, **m):
    """US flow pkts, octets, policed drop/delay, AQM drop."""
    _sp = {k: m[k] for k in ('mac_fmt','modem_name','session_start','session_end')}
    _sp['cmts_type'] = m.get('cmts_type', 'icmts')

    if 'flow_octets' in us.columns and not us['flow_octets'].isna().all():
        us_d = _delta_mb(us, 'flow_octets')
        fig, ax = make_fig()
        plot_line(ax, us_d, 'flow_octets', 'sfid', 'MB / poll')
        save_page(pdf, fig, ax, 'US FLOW THROUGHPUT (MB/poll)',
                  f'{m["modem_name"]} ({m["mac_fmt"]})  |  {_total_gb_label(us_d, "flow_octets")}', **_sp)

    fig, ax = make_fig()
    plot_dual(ax, us, 'flow_policed_drop', 'flow_policed_delay',
              'drop', 'delay', 'sfid', 'Packets')
    save_page(pdf, fig, ax, 'US POLICED DROP & DELAY',
              f'{m["modem_name"]} ({m["mac_fmt"]})  |  US Service Flows', **_sp)

    fig, ax = make_fig()
    plot_line(ax, us, 'flow_aqm_drop', 'sfid', 'Packets')
    save_page(pdf, fig, ax, 'US AQM DROPPED PACKETS',
              f'{m["modem_name"]} ({m["mac_fmt"]})  |  US Service Flows', **_sp)


def page_us_latency_max(pdf, us, **m):
    """US latency max ms per SFID over time."""
    col = 'lat_max_usec'
    if col not in us.columns or us[col].isna().all():
        return
    us = us.copy()
    us[col] = pd.to_numeric(us[col], errors='coerce') / 1000
    fig, ax = make_fig()
    plot_line(ax, us, col, 'sfid', 'Latency (ms)')
    save_page(pdf, fig, ax, 'US LATENCY MAX (ms)',
              f'{m["modem_name"]} ({m["mac_fmt"]})  |  AQM-enabled SFIDs only',
              **{k: m[k] for k in ('mac_fmt','modem_name','session_start','session_end')},
              cmts_type=m.get('cmts_type', 'icmts'))


def page_us_latency_histogram(pdf, us, **m):
    """US latency bin histogram — last poll, one chart per AQM SFID."""
    bin_cols = [f'lat_bin{i}' for i in range(1, 17)]
    present  = [c for c in bin_cols if c in us.columns]
    if not present:
        return

    # Only SFIDs that have any bin data
    has_bins = us[present].notna().any(axis=1)
    sfids    = us.loc[has_bins, 'sfid'].unique()
    if len(sfids) == 0:
        return

    for sfid in sfids:
        sfid_df = us[(us['sfid'] == sfid) & has_bins]
        if sfid_df.empty:
            continue
        last = sfid_df.iloc[-1]
        bins = [pd.to_numeric(last.get(c, 0), errors='coerce') / 1000 or 0 for c in present]
        scn  = last.get('ps_scn', '') or last.get('lat_bin_scn', '') or str(sfid)

        fig, ax = make_fig()
        bars = ax.bar(range(1, len(present) + 1), bins, color=ACCENT,
                      edgecolor=BG_DARK, linewidth=0.5, width=0.7)
        max_val = max(bins) if any(b > 0 for b in bins) else 1
        for bar, val in zip(bars, bins):
            if val > 0:
                ax.text(bar.get_x() + bar.get_width() / 2,
                        bar.get_height() + max_val * 0.01,
                        f'{val:.3f}', ha='center', va='bottom',
                        fontsize=7, color=TEXT_COLOR)
        ax.set_xlabel('Bin', color=SUBTEXT, fontsize=10)
        ax.set_ylabel('ms', color=SUBTEXT, fontsize=10, fontweight='bold')
        ax.grid(True, color=GRID_COLOR, linewidth=0.7, linestyle='--', alpha=0.8, axis='y')
        ax.tick_params(colors=TEXT_COLOR)
        for spine in ax.spines.values():
            spine.set_edgecolor(GRID_COLOR)
        save_page(pdf, fig, ax,
                  f'US LATENCY HISTOGRAM — SFID {sfid}',
                  f'{scn}  |  Last poll @ {last["captured_utc"]}',
                  **{k: m[k] for k in ('mac_fmt','modem_name','session_start','session_end')},
                  cmts_type=m.get('cmts_type', 'icmts'))


def page_us_congestion(pdf, us, **m):
    """US congestion — AQM drop, CE marked, ECT0/ECT1."""
    cong_cols = ['cong_aqm_drop', 'cong_ce_marked', 'cong_ect0', 'cong_ect1',
                 'cong_scn_marked', 'cong_sanctioned']
    present = [c for c in cong_cols if c in us.columns and not us[c].isna().all()]
    if not present:
        return

    _sp = {k: m[k] for k in ('mac_fmt','modem_name','session_start','session_end')}
    _sp['cmts_type'] = m.get('cmts_type', 'icmts')

    fig, ax = make_fig()
    plot_dual(ax, us, 'cong_aqm_drop', 'cong_ce_marked',
              'AQM drop', 'CE marked', 'sfid', 'Packets')
    save_page(pdf, fig, ax, 'US CONGESTION — AQM DROPS & CE MARKED',
              f'{m["modem_name"]} ({m["mac_fmt"]})', **_sp)

    if 'cong_ect0' in us.columns and 'cong_ect1' in us.columns:
        fig, ax = make_fig()
        plot_dual(ax, us, 'cong_ect0', 'cong_ect1',
                  'ECT(0)', 'ECT(1)', 'sfid', 'Packets')
        save_page(pdf, fig, ax, 'US ECT(0) & ECT(1) PACKETS',
                  f'{m["modem_name"]} ({m["mac_fmt"]})', **_sp)


def page_us_param_set(pdf, us, **m):
    """US param set — max rate, priority, buffer targets."""
    rate_col = 'ps_max_rate_64' if ('ps_max_rate_64' in us.columns and
                                     not us['ps_max_rate_64'].isna().all()) else 'ps_max_rate'
    if rate_col not in us.columns or us[rate_col].isna().all():
        return

    us = us.copy()
    us[rate_col] = pd.to_numeric(us[rate_col], errors='coerce') / 1_000_000  # bps → Mbps

    _sp = {k: m[k] for k in ('mac_fmt','modem_name','session_start','session_end')}
    _sp['cmts_type'] = m.get('cmts_type', 'icmts')

    fig, ax = make_fig()
    plot_line(ax, us, rate_col, 'sfid', 'Mbps')
    save_page(pdf, fig, ax, 'US PARAM SET — MAX RATE (Mbps)',
              f'{m["modem_name"]} ({m["mac_fmt"]})  |  Active param set (type 2)', **_sp)

    buf_cols = ['ps_min_buffer', 'ps_target_buffer', 'ps_max_buffer']
    present  = [c for c in buf_cols if c in us.columns and not us[c].isna().all()]
    if present:
        fig, ax = make_fig()
        for i, col in enumerate(present):
            for j, (sfid, grp) in enumerate(us.groupby('sfid')):
                c = CHART_COLORS[(i * 3 + j) % len(CHART_COLORS)]
                ax.plot(grp['captured_utc'], pd.to_numeric(grp[col], errors='coerce'),
                        marker='o', markersize=3, linewidth=1.5, color=c,
                        label=f'{sfid} {col.replace("ps_","")}')
        ax.set_ylabel('Bytes', color=SUBTEXT, fontsize=10, fontweight='bold')
        ax.set_xlabel('Time (UTC)', color=SUBTEXT, fontsize=10)
        ax.legend(fontsize=7, facecolor=BG_DARK, edgecolor=GRID_COLOR,
                  labelcolor=TEXT_COLOR, framealpha=0.9, ncol=2)
        fmt_ax(ax)
        save_page(pdf, fig, ax, 'US PARAM SET — BUFFER TARGETS',
                  f'{m["modem_name"]} ({m["mac_fmt"]})  |  min / target / max', **_sp)

# ---------------------------------------------------------------------------
# DS chart pages
# ---------------------------------------------------------------------------
def page_ds_flow_stats(pdf, ds, **m):
    """DS flow octets, policed drop/delay, AQM drop."""
    _sp = {k: m[k] for k in ('mac_fmt','modem_name','session_start','session_end')}
    _sp['cmts_type'] = m.get('cmts_type', 'icmts')

    if 'flow_octets' in ds.columns and not ds['flow_octets'].isna().all():
        ds_d = _delta_mb(ds, 'flow_octets')
        fig, ax = make_fig()
        plot_line(ax, ds_d, 'flow_octets', 'sfid', 'MB / poll')
        save_page(pdf, fig, ax, 'DS FLOW THROUGHPUT (MB/poll)',
                  f'{m["modem_name"]} ({m["mac_fmt"]})  |  {_total_gb_label(ds_d, "flow_octets")}', **_sp)

    if all(c in ds.columns for c in ('flow_policed_drop', 'flow_policed_delay')):
        fig, ax = make_fig()
        plot_dual(ax, ds, 'flow_policed_drop', 'flow_policed_delay',
                  'drop', 'delay', 'sfid', 'Packets')
        save_page(pdf, fig, ax, 'DS POLICED DROP & DELAY',
                  f'{m["modem_name"]} ({m["mac_fmt"]})  |  DS Service Flows', **_sp)

    if 'flow_aqm_drop' in ds.columns and not ds['flow_aqm_drop'].isna().all():
        fig, ax = make_fig()
        plot_line(ax, ds, 'flow_aqm_drop', 'sfid', 'Packets')
        save_page(pdf, fig, ax, 'DS AQM DROPPED PACKETS',
                  f'{m["modem_name"]} ({m["mac_fmt"]})  |  DS Service Flows', **_sp)


def page_ds_congestion(pdf, ds, **m):
    """DS congestion — AQM drop, CE marked, ECT0/ECT1."""
    cong_cols = ['cong_aqm_drop', 'cong_ce_marked', 'cong_ect0', 'cong_ect1']
    present   = [c for c in cong_cols if c in ds.columns and not ds[c].isna().all()]
    if not present:
        return

    _sp = {k: m[k] for k in ('mac_fmt','modem_name','session_start','session_end')}
    _sp['cmts_type'] = m.get('cmts_type', 'icmts')

    fig, ax = make_fig()
    plot_dual(ax, ds, 'cong_aqm_drop', 'cong_ce_marked',
              'AQM drop', 'CE marked', 'sfid', 'Packets')
    save_page(pdf, fig, ax, 'DS CONGESTION — AQM DROPS & CE MARKED',
              f'{m["modem_name"]} ({m["mac_fmt"]})', **_sp)

    if 'cong_ect0' in present and 'cong_ect1' in present:
        fig, ax = make_fig()
        plot_dual(ax, ds, 'cong_ect0', 'cong_ect1',
                  'ECT(0)', 'ECT(1)', 'sfid', 'Packets')
        save_page(pdf, fig, ax, 'DS ECT(0) & ECT(1) PACKETS',
                  f'{m["modem_name"]} ({m["mac_fmt"]})', **_sp)


def page_ds_latency(pdf, ds, **m):
    """DS latency — max µs and bin histogram (AQM-enabled SFIDs only)."""
    _sp = {k: m[k] for k in ('mac_fmt','modem_name','session_start','session_end')}
    _sp['cmts_type'] = m.get('cmts_type', 'icmts')

    if 'lat_max_usec' in ds.columns and not ds['lat_max_usec'].isna().all():
        ds = ds.copy()
        ds['lat_max_usec'] = pd.to_numeric(ds['lat_max_usec'], errors='coerce') / 1000
        fig, ax = make_fig()
        plot_line(ax, ds, 'lat_max_usec', 'sfid', 'Latency (ms)')
        save_page(pdf, fig, ax, 'DS LATENCY MAX (ms)',
                  f'{m["modem_name"]} ({m["mac_fmt"]})  |  AQM-enabled SFIDs only', **_sp)

    bin_cols = [f'lat_bin{i}' for i in range(1, 17)]
    present  = [c for c in bin_cols if c in ds.columns]
    if not present:
        return

    has_bins = ds[present].notna().any(axis=1)
    sfids    = ds.loc[has_bins, 'sfid'].unique()
    for sfid in sfids:
        sfid_df = ds[(ds['sfid'] == sfid) & has_bins]
        if sfid_df.empty:
            continue
        last = sfid_df.iloc[-1]
        bins = [pd.to_numeric(last.get(c, 0), errors='coerce') / 1000 or 0 for c in present]
        scn  = last.get('ps_scn', '') or last.get('lat_bin_scn', '') or str(sfid)

        fig, ax = make_fig()
        bars    = ax.bar(range(1, len(present) + 1), bins, color='#34a853',
                         edgecolor=BG_DARK, linewidth=0.5, width=0.7)
        max_val = max(bins) if any(b > 0 for b in bins) else 1
        for bar, val in zip(bars, bins):
            if val > 0:
                ax.text(bar.get_x() + bar.get_width() / 2,
                        bar.get_height() + max_val * 0.01,
                        f'{val:.3f}', ha='center', va='bottom',
                        fontsize=7, color=TEXT_COLOR)
        ax.set_xlabel('Bin', color=SUBTEXT, fontsize=10)
        ax.set_ylabel('ms', color=SUBTEXT, fontsize=10, fontweight='bold')
        ax.grid(True, color=GRID_COLOR, linewidth=0.7, linestyle='--', alpha=0.8, axis='y')
        ax.tick_params(colors=TEXT_COLOR)
        for spine in ax.spines.values():
            spine.set_edgecolor(GRID_COLOR)
        save_page(pdf, fig, ax,
                  f'DS LATENCY HISTOGRAM — SFID {sfid}',
                  f'{scn}  |  Last poll @ {last["captured_utc"]}',
                  **{k: m[k] for k in ('mac_fmt','modem_name','session_start','session_end')},
                  cmts_type=m.get('cmts_type', 'icmts'))

# ---------------------------------------------------------------------------
# Kafka chart pages (vcmts DS + US from Kafka)
# ---------------------------------------------------------------------------
def page_kafka_throughput(pdf, kdf, direction, **m):
    """Delta octets → Mbps over time, grouped by sfid_label."""
    col = 'delta_octets'
    if col not in kdf.columns or kdf[col].isna().all():
        return
    kdf = kdf.copy()
    kdf['mbps'] = pd.to_numeric(kdf[col], errors='coerce') * 8 / 15 / 1_000_000
    label = direction.upper()
    fig, ax = make_fig()
    grp_col = 'sfid_label' if 'sfid_label' in kdf.columns else 'sfid'
    plot_line(ax, kdf, 'mbps', grp_col, 'Throughput (Mbps)')
    save_page(pdf, fig, ax, f'{label} THROUGHPUT (Mbps)',
              f'{m["modem_name"]} ({m["mac_fmt"]})  |  Kafka delta_octets → Mbps',
              **{k: m[k] for k in ('mac_fmt','modem_name','session_start','session_end')},
              cmts_type=m.get('cmts_type', 'vcmts'))


def page_kafka_latency_avg(pdf, kdf, direction, **m):
    """Average latency (ms) over time per flow."""
    col = 'lat_avg_usec'
    if col not in kdf.columns or kdf[col].isna().all():
        return
    kdf = kdf.copy()
    kdf[col] = pd.to_numeric(kdf[col], errors='coerce') / 1000
    label   = direction.upper()
    grp_col = 'sfid_label' if 'sfid_label' in kdf.columns else 'sfid'
    fig, ax = make_fig()
    plot_line(ax, kdf, col, grp_col, 'Latency (ms)')
    save_page(pdf, fig, ax, f'{label} LATENCY AVG (ms)',
              f'{m["modem_name"]} ({m["mac_fmt"]})  |  Kafka lat_avg_usec',
              **{k: m[k] for k in ('mac_fmt','modem_name','session_start','session_end')},
              cmts_type=m.get('cmts_type', 'vcmts'))


def page_kafka_latency_histogram(pdf, kdf, direction, **m):
    """16-bin latency histogram — last sample per sfid_label."""
    bin_cols = [f'lat_bin{i}' for i in range(1, 17)]
    present  = [c for c in bin_cols if c in kdf.columns]
    if not present:
        return
    label   = direction.upper()
    grp_col = 'sfid_label' if 'sfid_label' in kdf.columns else 'sfid'
    _sp     = {k: m[k] for k in ('mac_fmt','modem_name','session_start','session_end')}
    _sp['cmts_type'] = m.get('cmts_type', 'vcmts')

    has_bins = kdf[present].notna().any(axis=1)
    for name, grp in kdf[has_bins].groupby(grp_col):
        last = grp.iloc[-1]
        bins = [pd.to_numeric(last.get(c, 0), errors='coerce') / 1000 or 0 for c in present]
        fig, ax = make_fig()
        bars    = ax.bar(range(1, len(present) + 1), bins, color=ACCENT,
                         edgecolor=BG_DARK, linewidth=0.5, width=0.7)
        max_val = max(bins) if any(b > 0 for b in bins) else 1
        for bar, val in zip(bars, bins):
            if val > 0:
                ax.text(bar.get_x() + bar.get_width() / 2,
                        bar.get_height() + max_val * 0.01,
                        f'{val:.3f}', ha='center', va='bottom',
                        fontsize=7, color=TEXT_COLOR)
        ax.set_xlabel('Bin', color=SUBTEXT, fontsize=10)
        ax.set_ylabel('ms', color=SUBTEXT, fontsize=10, fontweight='bold')
        ax.grid(True, color=GRID_COLOR, linewidth=0.7, linestyle='--', alpha=0.8, axis='y')
        ax.tick_params(colors=TEXT_COLOR)
        for spine in ax.spines.values():
            spine.set_edgecolor(GRID_COLOR)
        save_page(pdf, fig, ax,
                  f'{label} LATENCY HISTOGRAM — {name}',
                  f'Last sample @ {last["captured_utc"]}', **_sp)


def page_kafka_congestion(pdf, kdf, direction, **m):
    """AQM drop and marked packets over time per flow."""
    drop_col   = 'cong_aqm_drop'
    marked_col = 'cong_ce_marked'
    if drop_col not in kdf.columns and marked_col not in kdf.columns:
        return
    label   = direction.upper()
    grp_col = 'sfid_label' if 'sfid_label' in kdf.columns else 'sfid'
    _sp     = {k: m[k] for k in ('mac_fmt','modem_name','session_start','session_end')}
    _sp['cmts_type'] = m.get('cmts_type', 'vcmts')

    if drop_col in kdf.columns and not kdf[drop_col].isna().all():
        fig, ax = make_fig()
        plot_line(ax, kdf, drop_col, grp_col, 'Packets')
        save_page(pdf, fig, ax, f'{label} AQM DROPPED PACKETS',
                  f'{m["modem_name"]} ({m["mac_fmt"]})  |  Kafka aqm_drop_pkts', **_sp)

    if marked_col in kdf.columns and not kdf[marked_col].isna().all():
        fig, ax = make_fig()
        plot_line(ax, kdf, marked_col, grp_col, 'Packets')
        save_page(pdf, fig, ax, f'{label} AQM MARKED PACKETS',
                  f'{m["modem_name"]} ({m["mac_fmt"]})  |  Kafka aqm_marked_pkts', **_sp)


# ---------------------------------------------------------------------------
# CSV loaders
# ---------------------------------------------------------------------------
def _load_csv(path):
    """Read SNMP CSV skipping comment lines, parse captured_utc, coerce numerics."""
    df = pd.read_csv(path, comment='#', parse_dates=['captured_utc'])
    skip = {'captured_utc', 'target_ip', 'target_label', 'cmts_type',
            'sfid', 'ps_scn', 'lat_bin_scn'}
    for c in df.columns:
        if c not in skip:
            df[c] = pd.to_numeric(df[c], errors='coerce')
    df['sfid'] = df['sfid'].astype(str)
    return df.sort_values('captured_utc').reset_index(drop=True)


# Kafka metric column → SNMP-equivalent name used by chart functions
_KAFKA_COL_MAP = {
    'total_octets':   'flow_octets',
    'total_pkts':     'flow_pkts',
    'aqm_drop_pkts':  'cong_aqm_drop',
    'aqm_marked_pkts':'cong_ce_marked',
    'sanctioned_pkts':'cong_sanctioned',
    'lat_max_usec':   'lat_max_usec',
    'lat_avg_usec':   'lat_avg_usec',
    **{f'lat_bin{str(i).zfill(2)}': f'lat_bin{i}' for i in range(1, 17)},
}


def _load_kafka_csv(path):
    """Load Kafka pivot CSV and return (us_df, ds_df) with SNMP-compatible column names."""
    df = pd.read_csv(path, comment='#', parse_dates=['captured_utc'])
    num_skip = {'captured_utc', 'dir', 'sfIndex', 'sfid', 'scn',
                'mdName', 'node', 'pod', 'cluster'}
    for c in df.columns:
        if c not in num_skip:
            df[c] = pd.to_numeric(df[c], errors='coerce')

    df['sfid'] = df['sfid'].astype(str)
    # Use scn as label where available, fall back to sfid
    df['sfid_label'] = df.apply(
        lambda r: r['scn'] if pd.notna(r.get('scn')) and str(r.get('scn', '')).strip()
                  else r['sfid'], axis=1)

    # Rename to SNMP-compatible names
    df = df.rename(columns=_KAFKA_COL_MAP)
    df = df.sort_values('captured_utc').reset_index(drop=True)

    us = df[df['dir'] == 'upstream'].copy()
    ds = df[df['dir'] == 'downstream'].copy()
    return us, ds


def main():
    sess      = find_session(sys.argv[1] if len(sys.argv) > 1 else None)
    cmts_type = sess['cmts_type']
    session_name = input('Session name (e.g. "Netflix L4S Gaming Test") [Enter for default]: ').strip()
    if not session_name:
        session_name = f'{cmts_type.upper()} SNMP Telemetry Report'
    mac       = sess['mac']
    mac_fmt   = ':'.join(mac[i:i+2] for i in range(0, 12, 2)).upper()
    modem_name = MODEM_NAMES.get(mac, mac_fmt)

    print(f'Session: {sess["session_dir"]}  [{cmts_type}]')

    # --- load data ---
    us = _load_csv(sess['us_path'])
    if cmts_type == 'vcmts':
        k_us, k_ds = _load_kafka_csv(sess['kafka_path'])
        ds = k_ds   # DS comes entirely from Kafka for vcmts
    else:
        ds    = _load_csv(sess['ds_path'])
        k_us  = k_ds = None

    # --- session metadata ---
    all_times     = pd.concat([us['captured_utc'],
                                ds['captured_utc'] if ds is not None else pd.Series(dtype='datetime64[ns]')]).dropna()
    session_start = all_times.min().strftime('%Y-%m-%d %H:%M UTC')
    session_end   = all_times.max().strftime('%Y-%m-%d %H:%M UTC')
    duration_secs = int((all_times.max() - all_times.min()).total_seconds())
    hours, rem    = divmod(duration_secs, 3600)
    duration_str  = f'{hours}h {rem // 60}m'
    total_polls   = us['poll_index'].nunique() if 'poll_index' in us.columns else len(us)

    us_sfids = sorted(us['sfid'].unique(), key=lambda x: int(x) if str(x).isdigit() else 0)
    ds_sfids = sorted(ds['sfid'].unique(), key=lambda x: int(x) if str(x).isdigit() else 0) \
               if ds is not None and not ds.empty else []

    m = dict(mac_fmt=mac_fmt, modem_name=modem_name,
             session_start=session_start, session_end=session_end,
             cmts_type=cmts_type)

    # --- build TOC ---
    if cmts_type == 'vcmts':
        toc = [
            ('3',  'US Flow Octets',              'Cumulative octets per US service flow (SNMP)'),
            ('4',  'US Policed Drop & Delay',      'Policed drop and delay packet counts per US flow'),
            ('5',  'US AQM Dropped Packets',       'AQM drop counters per US service flow'),
            ('6',  'US Latency Max (ms)',           'Peak latency per AQM-enabled US flow over time'),
            ('7',  'US Latency Histogram',          'Bin distribution of US latency in ms (last poll, per SFID)'),
            ('8',  'US Congestion — AQM & CE',      'AQM drops and CE marked packets per US flow'),
            ('9',  'US Param Set — Max Rate',       'Active param set max rate (Mbps) per US flow'),
            ('10', 'US Throughput (Mbps)',          'Kafka delta_octets → Mbps per US flow'),
            ('11', 'US Latency Avg (ms)',           'Kafka average latency per US flow over time'),
            ('12', 'US Latency Histogram (Kafka)',  'Kafka 16-bin latency distribution (last sample)'),
            ('13', 'US AQM Dropped Packets',        'Kafka aqm_drop_pkts per US flow'),
            ('14', 'US AQM Marked Packets',         'Kafka aqm_marked_pkts per US flow'),
            ('15', 'DS Throughput (Mbps)',          'Kafka delta_octets → Mbps per DS flow'),
            ('16', 'DS Latency Avg (ms)',           'Kafka average latency per DS flow over time'),
            ('17', 'DS Latency Histogram (Kafka)',  'Kafka 16-bin latency distribution (last sample)'),
            ('18', 'DS AQM Dropped Packets',        'Kafka aqm_drop_pkts per DS flow'),
            ('19', 'DS AQM Marked Packets',         'Kafka aqm_marked_pkts per DS flow'),
        ]
    else:
        toc = [
            ('3',  'US Flow Octets',               'Cumulative octets per US service flow'),
            ('4',  'US Policed Drop & Delay',       'Policed drop and delay packet counts per US flow'),
            ('5',  'US AQM Dropped Packets',        'AQM drop counters per US service flow'),
            ('6',  'US Latency Max (ms)',            'Peak latency per AQM-enabled US flow over time'),
            ('7',  'US Latency Histogram',           'Bin distribution of US latency in ms (last poll, per SFID)'),
            ('8',  'US Congestion — AQM & CE',       'AQM drops and CE marked packets per US flow'),
            ('9',  'US ECT(0) & ECT(1)',             'ECN capable transport packet counts per US flow'),
            ('10', 'US Param Set — Max Rate',        'Active param set max rate (Mbps) per US flow'),
            ('11', 'US Param Set — Buffer Targets',  'Min / target / max buffer bytes per US flow'),
            ('12', 'DS Flow Octets',                 'Cumulative octets per DS service flow'),
            ('13', 'DS Policed Drop & Delay',        'Policed drop and delay packet counts per DS flow'),
            ('14', 'DS AQM Dropped Packets',         'AQM drop counters per DS service flow'),
            ('15', 'DS Congestion — AQM & CE',       'AQM drops and CE marked packets per DS flow'),
            ('16', 'DS ECT(0) & ECT(1)',             'ECN capable transport packet counts per DS flow'),
            ('17', 'DS Latency Max (ms)',             'Peak latency per AQM-enabled DS flow over time'),
            ('18', 'DS Latency Histogram',            'Bin distribution of DS latency in ms (last poll, per SFID)'),
        ]

    safe_name = re.sub(r'[^\w\-]', '_', session_name).strip('_')
    out_path = os.path.join(sess['session_dir'], f'report_{cmts_type}_{mac}_{safe_name}.pdf')

    with PdfPages(out_path) as pdf:
        page_cover(pdf, mac_fmt, modem_name, session_start, session_end,
                   duration_str, total_polls, us_sfids, ds_sfids,
                   cmts_type=cmts_type, session_name=session_name)
        page_toc(pdf, mac_fmt, modem_name, session_start, session_end, toc, cmts_type=cmts_type)

        # US pages (SNMP — same for both types)
        page_us_flow_stats(pdf, us, **m)
        page_us_latency_max(pdf, us, **m)
        page_us_latency_histogram(pdf, us, **m)
        page_us_congestion(pdf, us, **m)
        page_us_param_set(pdf, us, **m)

        if cmts_type == 'vcmts':
            # US Kafka charts
            page_kafka_throughput(pdf, k_us, 'upstream', **m)
            page_kafka_latency_avg(pdf, k_us, 'upstream', **m)
            page_kafka_latency_histogram(pdf, k_us, 'upstream', **m)
            page_kafka_congestion(pdf, k_us, 'upstream', **m)
            # DS Kafka charts
            page_kafka_throughput(pdf, k_ds, 'downstream', **m)
            page_kafka_latency_avg(pdf, k_ds, 'downstream', **m)
            page_kafka_latency_histogram(pdf, k_ds, 'downstream', **m)
            page_kafka_congestion(pdf, k_ds, 'downstream', **m)
        else:
            # DS from SNMP
            page_ds_flow_stats(pdf, ds, **m)
            page_ds_congestion(pdf, ds, **m)
            page_ds_latency(pdf, ds, **m)

        d = pdf.infodict()
        d['Title']   = f'{cmts_type.upper()} SNMP Report — {modem_name} ({mac_fmt})'
        d['Author']  = 'aphillips — Charter Access Engineering'
        d['Subject'] = session_name

    print(f'PDF saved: {out_path}')


if __name__ == '__main__':
    main()
