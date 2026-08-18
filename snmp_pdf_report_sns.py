"""
snmp_pdf_report_sns.py
======================
Seaborn version of snmp_pdf_report.py.
Generates a PDF report from a CM Collector session directory.
Supports both iCMTS (snmp_us + snmp_ds) and vCMTS (snmp_us + kafka) sessions.

Usage:
    python snmp_pdf_report_sns.py                        # auto-finds latest session in results/
    python snmp_pdf_report_sns.py 206a949223b8           # specific MAC (latest session)
    python snmp_pdf_report_sns.py path/to/session/dir    # specific session directory
    python snmp_pdf_report_sns.py path/to/snmp_us_*.csv  # specific US CSV

Requirements:
    pip install pandas matplotlib seaborn
"""
import os
import sys
import glob
import re
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as mticker
import seaborn as sns
from matplotlib.backends.backend_pdf import PdfPages
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))

# ---------------------------------------------------------------------------
# Theme
# ---------------------------------------------------------------------------
BG_DARK      = '#07111f'
BG_PANEL     = '#0d1f35'
BG_CARD      = '#112844'
GRID_COLOR   = '#1a3352'
TEXT_COLOR   = '#e8eaed'
SUBTEXT      = '#8ab4d4'
ACCENT       = '#1a73e8'
ACCENT2      = '#00c6ff'
ACCENT_GREEN = '#34a853'
ACCENT_WARN  = '#fa7b17'

SNS_PALETTE = [
    '#1a73e8', '#34a853', '#fa7b17', '#ea4335',
    '#a142f4', '#00c6ff', '#f538a0',
]

# Glow layers: each line gets 3 passes — wide faint, medium, sharp bright
_GLOW_ALPHAS  = [0.08, 0.18, 1.0]
_GLOW_WIDTHS  = [8,    3,    1.8]
_FILL_ALPHA   = 0.12

sns.set_theme(style='dark', rc={
    'axes.facecolor':        BG_PANEL,
    'figure.facecolor':      BG_DARK,
    'axes.edgecolor':        GRID_COLOR,
    'axes.labelcolor':       SUBTEXT,
    'xtick.color':           SUBTEXT,
    'ytick.color':           SUBTEXT,
    'xtick.labelsize':       8,
    'ytick.labelsize':       8,
    'grid.color':            GRID_COLOR,
    'grid.linestyle':        '--',
    'grid.linewidth':        0.5,
    'grid.alpha':            0.6,
    'text.color':            TEXT_COLOR,
    'legend.facecolor':      BG_CARD,
    'legend.edgecolor':      ACCENT,
    'legend.labelcolor':     TEXT_COLOR,
    'legend.fontsize':       8,
    'legend.framealpha':     0.92,
    'font.family':           'DejaVu Sans',
    'axes.titlesize':        11,
    'axes.labelsize':        10,
})

MODEM_NAMES = {
    '0cb9379c64b4': 'Lab CM (iCMTS)',
    '206a949223b8': 'Lab CM (vCMTS)',
}

# ---------------------------------------------------------------------------
# Session discovery
# ---------------------------------------------------------------------------
def _session_from_dir(d):
    us_files    = sorted(glob.glob(os.path.join(d, 'snmp_us_*.csv')))
    ds_files    = sorted(glob.glob(os.path.join(d, 'snmp_ds_*.csv')))
    kafka_files = sorted(glob.glob(os.path.join(d, 'kafka_*.csv')))
    if not us_files:
        return None

    us_path    = us_files[-1]
    ds_path    = ds_files[-1] if ds_files else None
    kafka_path = kafka_files[-1] if kafka_files else None

    mac_m = re.search(r'([0-9a-f]{12})', os.path.basename(us_path))
    mac   = mac_m.group(1) if mac_m else 'unknown'

    if kafka_path and not ds_path:
        cmts_type = 'vcmts'
    elif ds_path and not kafka_path:
        cmts_type = 'icmts'
    elif ds_path and kafka_path:
        cmts_type = 'vcmts'
    else:
        return None

    return {
        'cmts_type':   cmts_type,
        'mac':         mac,
        'us_path':     us_path,
        'ds_path':     ds_path,
        'kafka_path':  kafka_path,
        'session_dir': d,
    }


def find_session(arg=None):
    def _latest_session(mac_glob):
        dirs = sorted(glob.glob(os.path.join(HERE, 'results', mac_glob, '*')))
        for d in reversed(dirs):
            if os.path.isdir(d):
                s = _session_from_dir(d)
                if s:
                    return s
        return None

    if arg:
        if arg.endswith('.csv'):
            path = arg if os.path.isabs(arg) else os.path.join(HERE, arg)
            s = _session_from_dir(os.path.dirname(path))
            if s:
                return s
            print(f'Could not build session from: {path}')
            sys.exit(1)

        if os.path.isdir(arg):
            s = _session_from_dir(arg)
            if s:
                return s
            print(f'No valid session files found in {arg}')
            sys.exit(1)

        mac = re.sub(r'[:\-.]', '', arg).lower()
        s = _latest_session(f'{mac}_*')
        if s:
            return s
        print(f'No session found for MAC {mac}')
        sys.exit(1)

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
def make_fig():
    fig, ax = plt.subplots(figsize=(11, 5.8))
    fig.patch.set_facecolor(BG_DARK)
    ax.set_facecolor(BG_PANEL)
    fig.subplots_adjust(top=0.88, bottom=0.20, left=0.10, right=0.97)
    for spine in ax.spines.values():
        spine.set_edgecolor(GRID_COLOR)
        spine.set_linewidth(0.8)
    ax.tick_params(colors=SUBTEXT, labelsize=8, length=3)
    return fig, ax


def fmt_ax(ax):
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=40, ha='right',
             color=SUBTEXT, fontsize=8)
    ax.grid(True, color=GRID_COLOR, linewidth=0.5, linestyle='--', alpha=0.6)
    ax.set_axisbelow(True)
    # Subtle inner border highlight on top/right
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_edgecolor(ACCENT)
    ax.spines['left'].set_linewidth(1.2)
    ax.spines['bottom'].set_edgecolor(GRID_COLOR)


def _gradient_header(fig, y0, height, color_left, color_right):
    """Draw a horizontal gradient rectangle as the header background."""
    import numpy as np
    from matplotlib.patches import FancyBboxPatch
    hax = fig.add_axes([0, y0, 1, height])
    hax.set_xlim(0, 1)
    hax.set_ylim(0, 1)
    hax.axis('off')
    # Gradient via imshow
    grad = np.linspace(0, 1, 256).reshape(1, -1)
    hax.imshow(grad, aspect='auto', extent=[0, 1, 0, 1],
               cmap=plt.cm.colors.LinearSegmentedColormap.from_list(
                   'hdr', [color_left, color_right]),
               zorder=0)
    return hax


def add_header(fig, title, subtitle=None):
    import numpy as np
    hax = fig.add_axes([0, 0.91, 1, 0.09])
    hax.set_xlim(0, 1); hax.set_ylim(0, 1)
    hax.axis('off')
    # Gradient background
    grad = np.linspace(0, 1, 256).reshape(1, -1)
    cmap = plt.matplotlib.colors.LinearSegmentedColormap.from_list(
        'hdr', ['#0d2a5e', ACCENT])
    hax.imshow(grad, aspect='auto', extent=[0, 1, 0, 1], cmap=cmap, zorder=0)
    # Left accent bar
    hax.axvline(x=0.003, color=ACCENT2, linewidth=4, zorder=2)
    hax.text(0.5, 0.63, title, transform=hax.transAxes,
             fontsize=15, fontweight='bold', color='white',
             ha='center', va='center', zorder=3,
             fontfamily='DejaVu Sans')
    if subtitle:
        hax.text(0.5, 0.18, subtitle, transform=hax.transAxes,
                 fontsize=7.5, color='#b8d4f0',
                 ha='center', va='center', fontstyle='italic', zorder=3)


def add_footer(fig, mac_fmt, modem_name, session_start, session_end, cmts_type='icmts'):
    import numpy as np
    fax = fig.add_axes([0, 0, 1, 0.035])
    fax.set_xlim(0, 1); fax.set_ylim(0, 1)
    fax.axis('off')
    grad = np.linspace(0, 1, 256).reshape(1, -1)
    cmap = plt.matplotlib.colors.LinearSegmentedColormap.from_list(
        'ftr', ['#050e1a', '#0a1e35'])
    fax.imshow(grad, aspect='auto', extent=[0, 1, 0, 1], cmap=cmap, zorder=0)
    fax.axhline(y=0.95, color=ACCENT, linewidth=0.8, alpha=0.5, zorder=2)
    fax.text(0.5, 0.42,
             f'{cmts_type.upper()} SNMP Report  │  {modem_name} ({mac_fmt})'
             f'  │  {session_start} — {session_end}  │  aphillips — Charter Access Engineering',
             transform=fax.transAxes, fontsize=6.5, color='#4a7090',
             ha='center', va='center', zorder=3)


def save_page(pdf, fig, ax, header_title, subtitle,
              mac_fmt, modem_name, session_start, session_end, cmts_type='icmts'):
    add_header(fig, header_title, subtitle)
    add_footer(fig, mac_fmt, modem_name, session_start, session_end, cmts_type)
    pdf.savefig(fig, facecolor=fig.get_facecolor(), dpi=150)
    plt.close(fig)


def _style_legend(ax):
    leg = ax.get_legend()
    if not leg:
        return
    leg.get_frame().set_facecolor(BG_CARD)
    leg.get_frame().set_edgecolor(ACCENT)
    leg.get_frame().set_linewidth(0.8)
    for txt in leg.get_texts():
        txt.set_color(TEXT_COLOR)
        txt.set_fontsize(8)


def plot_line(ax, df, y_col, group_col, ylabel):
    """Glow-effect line chart grouped by group_col."""
    x_col  = 'captured_utc' if 'captured_utc' in df.columns else 'poll_index'
    groups = list(df[group_col].unique())

    for i, g in enumerate(groups):
        grp   = df[df[group_col] == g].sort_values(x_col)
        c     = SNS_PALETTE[i % len(SNS_PALETTE)]
        xvals = grp[x_col]
        yvals = pd.to_numeric(grp[y_col], errors='coerce')

        # Glow layers
        for alpha, lw in zip(_GLOW_ALPHAS, _GLOW_WIDTHS):
            ax.plot(xvals, yvals, color=c, linewidth=lw, alpha=alpha,
                    solid_capstyle='round')

        # Markers on top
        ax.plot(xvals, yvals, 'o', color=c, markersize=4,
                markerfacecolor='white', markeredgecolor=c,
                markeredgewidth=1.4, zorder=5)

        # Gradient fill
        ax.fill_between(xvals, yvals, alpha=_FILL_ALPHA, color=c, zorder=1)

        # Label last point
        if not grp.empty:
            ax.annotate(str(g),
                        xy=(xvals.iloc[-1], yvals.iloc[-1]),
                        xytext=(6, 0), textcoords='offset points',
                        fontsize=7, color=c, va='center', fontweight='bold')

    ax.set_ylabel(ylabel, color=SUBTEXT, fontsize=10, fontweight='bold', labelpad=8)
    ax.set_xlabel('Time (UTC)', color=SUBTEXT, fontsize=9, labelpad=6)
    fmt_ax(ax)


def plot_dual(ax, df, col_a, col_b, label_a, label_b, group_col, ylabel):
    """Two metrics per group with glow — solid col_a, dashed col_b."""
    x_col  = 'captured_utc'
    groups = list(df[group_col].unique())
    for i, g in enumerate(groups):
        grp = df[df[group_col] == g].sort_values(x_col)
        c   = SNS_PALETTE[i % len(SNS_PALETTE)]
        x   = grp[x_col]
        ya  = pd.to_numeric(grp[col_a], errors='coerce')
        yb  = pd.to_numeric(grp[col_b], errors='coerce')

        for alpha, lw in zip(_GLOW_ALPHAS, _GLOW_WIDTHS):
            ax.plot(x, ya, color=c, linewidth=lw, alpha=alpha,
                    solid_capstyle='round')
        ax.plot(x, ya, 'o', color=c, markersize=3.5,
                markerfacecolor='white', markeredgecolor=c,
                markeredgewidth=1.2, zorder=5, label=f'{g} {label_a}')
        ax.fill_between(x, ya, alpha=_FILL_ALPHA, color=c)

        for alpha, lw in zip(_GLOW_ALPHAS, _GLOW_WIDTHS):
            ax.plot(x, yb, color=c, linewidth=lw * 0.7,
                    alpha=alpha * 0.6, linestyle='--')
        ax.plot(x, yb, 'x', color=c, markersize=4, alpha=0.8,
                zorder=5, label=f'{g} {label_b}')

    handles, labels = ax.get_legend_handles_labels()
    ax.legend(handles, labels, fontsize=7, ncol=2,
              facecolor=BG_CARD, edgecolor=ACCENT,
              labelcolor=TEXT_COLOR, framealpha=0.92)
    ax.set_ylabel(ylabel, color=SUBTEXT, fontsize=10, fontweight='bold', labelpad=8)
    ax.set_xlabel('Time (UTC)', color=SUBTEXT, fontsize=9, labelpad=6)
    fmt_ax(ax)


def plot_bars(ax, bins, color, ylabel):
    """Modern gradient bar chart for latency histograms."""
    import numpy as np
    x      = list(range(1, len(bins) + 1))
    max_v  = max(bins) if any(b > 0 for b in bins) else 1

    # Draw bars with a vertical gradient effect via stacked thin imshow
    bars = ax.bar(x, bins, color=color, edgecolor=BG_DARK,
                  linewidth=0.6, width=0.72, alpha=0.25, zorder=2)
    bars2 = ax.bar(x, bins, color=color, edgecolor='none',
                   linewidth=0, width=0.72, alpha=0.85, zorder=3)

    # Glow top edge
    for xi, val in zip(x, bins):
        if val > 0:
            ax.plot([xi - 0.36, xi + 0.36], [val, val],
                    color=color, linewidth=2.5, alpha=0.9,
                    solid_capstyle='round', zorder=4)
            ax.text(xi, val + max_v * 0.025, f'{val:.3f}',
                    ha='center', va='bottom', fontsize=6.5,
                    color=TEXT_COLOR, fontweight='bold', zorder=5)

    ax.set_xlabel('Latency Bin', color=SUBTEXT, fontsize=9, labelpad=6)
    ax.set_ylabel(ylabel, color=SUBTEXT, fontsize=10, fontweight='bold', labelpad=8)
    ax.set_xlim(0.3, len(bins) + 0.7)
    ax.grid(True, color=GRID_COLOR, linewidth=0.5, linestyle='--', alpha=0.6, axis='y')
    ax.tick_params(colors=SUBTEXT, labelsize=8)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_edgecolor(ACCENT)
    ax.spines['bottom'].set_edgecolor(GRID_COLOR)

# ---------------------------------------------------------------------------
# Cover + TOC
# ---------------------------------------------------------------------------
def page_cover(pdf, mac_fmt, modem_name, session_start, session_end,
               duration_str, total_polls, us_sfids, ds_sfids,
               cmts_type='icmts', session_name=None):
    import numpy as np
    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor(BG_DARK)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_facecolor(BG_DARK)
    ax.axis('off')

    # Full-page gradient background
    grad = np.linspace(0, 1, 256).reshape(-1, 1)
    cmap_bg = plt.matplotlib.colors.LinearSegmentedColormap.from_list(
        'bg', [BG_DARK, '#0d2040'])
    ax.imshow(grad, aspect='auto', extent=[0, 1, 0, 1],
              cmap=cmap_bg, transform=ax.transAxes, zorder=0, alpha=0.6)

    # Top accent bar with gradient
    bar = fig.add_axes([0, 0.88, 1, 0.12])
    bar.set_xlim(0, 1); bar.set_ylim(0, 1); bar.axis('off')
    grad_h = np.linspace(0, 1, 256).reshape(1, -1)
    cmap_h = plt.matplotlib.colors.LinearSegmentedColormap.from_list(
        'hdr', ['#0a2060', ACCENT, ACCENT2])
    bar.imshow(grad_h, aspect='auto', extent=[0, 1, 0, 1], cmap=cmap_h, zorder=0)
    bar.axhline(y=0.02, color=ACCENT2, linewidth=2, alpha=0.8)

    title_str = 'vCMTS SNMP SESSION REPORT' if cmts_type == 'vcmts' else 'iCMTS SNMP SESSION REPORT'
    sub_str   = ('Charter Communications  •  Access Engineering  •  vCMTS EFT'
                 if cmts_type == 'vcmts' else
                 'Charter Communications  •  Access Engineering  •  iCMTS EFT')

    bar.text(0.5, 0.62, title_str, transform=bar.transAxes,
             fontsize=24, fontweight='bold', color='white',
             ha='center', va='center')
    bar.text(0.5, 0.18, sub_str, transform=bar.transAxes,
             fontsize=10, color='#b8d4f0', ha='center', va='center', fontstyle='italic')

    # Session name badge
    if session_name:
        ax.text(0.5, 0.83, session_name, transform=ax.transAxes,
                fontsize=16, fontweight='bold', color=ACCENT2,
                ha='center', va='center',
                bbox=dict(boxstyle='round,pad=0.5', facecolor='#0a2040',
                          edgecolor=ACCENT2, linewidth=1.5, alpha=0.9))

    # Divider
    ax.axhline(y=0.78, xmin=0.05, xmax=0.95, color=ACCENT, linewidth=0.8, alpha=0.5)

    # Info cards — two columns
    ds_label = ('Kafka (DS)' if cmts_type == 'vcmts'
                else ', '.join(str(s) for s in sorted(ds_sfids)))
    left_cards = [
        ('MODEM',         modem_name),
        ('MAC ADDRESS',   mac_fmt),
        ('CMTS TYPE',     cmts_type.upper()),
        ('DURATION',      duration_str),
        ('TOTAL POLLS',   str(total_polls)),
    ]
    right_cards = [
        ('SESSION START', session_start),
        ('SESSION END',   session_end),
        ('US SFIDs',      ', '.join(str(s) for s in sorted(us_sfids))),
        ('DS SFIDs',      ds_label),
    ]

    def _card(ax, x, y, label, value, w=0.38, h=0.072):
        ax.add_patch(plt.matplotlib.patches.FancyBboxPatch(
            (x, y - h * 0.6), w, h,
            boxstyle='round,pad=0.01',
            facecolor=BG_CARD, edgecolor=ACCENT,
            linewidth=0.8, transform=ax.transAxes, zorder=2, alpha=0.9))
        ax.text(x + 0.012, y + h * 0.18, label, transform=ax.transAxes,
                fontsize=6.5, color=SUBTEXT, fontweight='bold', va='center',
                zorder=3, alpha=0.9)
        ax.text(x + 0.012, y - h * 0.22, value, transform=ax.transAxes,
                fontsize=9.5, color='white', va='center',
                fontfamily='monospace', zorder=3)

    y = 0.72
    for label, value in left_cards:
        _card(ax, 0.05, y, label, value)
        y -= 0.085
    y = 0.72
    for label, value in right_cards:
        _card(ax, 0.55, y, label, value)
        y -= 0.085

    # Footer
    ax.text(0.5, 0.03,
            f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M")}   │   aphillips — Charter Access Engineering',
            transform=ax.transAxes, fontsize=8, color='#2a4a6a',
            ha='center', va='center')

    pdf.savefig(fig, facecolor=fig.get_facecolor(), dpi=150)
    plt.close(fig)


def page_toc(pdf, mac_fmt, modem_name, session_start, session_end,
             contents, cmts_type='icmts'):
    import numpy as np
    fig = plt.figure(figsize=(11, 8.5))
    fig.patch.set_facecolor(BG_DARK)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_facecolor(BG_DARK)
    ax.axis('off')

    # Header bar
    hbar = fig.add_axes([0, 0.88, 1, 0.12])
    hbar.set_xlim(0, 1); hbar.set_ylim(0, 1); hbar.axis('off')
    grad = np.linspace(0, 1, 256).reshape(1, -1)
    cmap = plt.matplotlib.colors.LinearSegmentedColormap.from_list(
        'hdr', ['#0a2060', ACCENT])
    hbar.imshow(grad, aspect='auto', extent=[0, 1, 0, 1], cmap=cmap)
    hbar.axhline(y=0.02, color=ACCENT2, linewidth=2, alpha=0.7)
    hbar.text(0.5, 0.55, 'TABLE OF CONTENTS', transform=hbar.transAxes,
              fontsize=20, fontweight='bold', color='white', ha='center', va='center')
    hbar.text(0.5, 0.15, f'{cmts_type.upper()} SNMP Report  │  {modem_name} ({mac_fmt})',
              transform=hbar.transAxes, fontsize=8, color='#b8d4f0',
              ha='center', va='center', fontstyle='italic')

    n   = len(contents)
    gap = min(0.072, 0.80 / max(n, 1))
    y   = 0.845

    for i, (page, title, desc) in enumerate(contents):
        # Alternating row background
        row_c = BG_CARD if i % 2 == 0 else BG_PANEL
        ax.add_patch(plt.matplotlib.patches.FancyBboxPatch(
            (0.03, y - gap * 0.52), 0.94, gap * 0.92,
            boxstyle='round,pad=0.005',
            facecolor=row_c, edgecolor=GRID_COLOR,
            linewidth=0.4, transform=ax.transAxes, zorder=1))

        # Page number badge
        ax.text(0.075, y, f'{page}', transform=ax.transAxes,
                fontsize=9, fontweight='bold', color='white',
                va='center', ha='center', zorder=3,
                bbox=dict(boxstyle='round,pad=0.35', facecolor=ACCENT,
                          edgecolor=ACCENT2, linewidth=0.8))

        # Left accent line
        ax.plot([0.11, 0.112], [y - gap * 0.38, y + gap * 0.38],
                color=ACCENT2, linewidth=2.5, transform=ax.transAxes,
                solid_capstyle='round', zorder=3)

        ax.text(0.125, y + gap * 0.1, title, transform=ax.transAxes,
                fontsize=9.5, fontweight='bold', color=TEXT_COLOR,
                va='center', zorder=3)
        ax.text(0.125, y - gap * 0.32, desc, transform=ax.transAxes,
                fontsize=7.5, color=SUBTEXT, va='center',
                fontstyle='italic', zorder=3)
        y -= gap

    ax.text(0.5, 0.025,
            f'{session_start} — {session_end}',
            transform=ax.transAxes, fontsize=7.5, color='#2a4a6a',
            ha='center', va='center')

    pdf.savefig(fig, facecolor=fig.get_facecolor(), dpi=150)
    plt.close(fig)

# ---------------------------------------------------------------------------
# US SNMP chart pages
# ---------------------------------------------------------------------------
def _sp(m):
    """Extract save_page kwargs from m dict."""
    return {k: m[k] for k in ('mac_fmt', 'modem_name', 'session_start', 'session_end',
                               'cmts_type')}


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
    totals = df.groupby('sfid')[col].sum() / 1e3
    return '  |  '.join(f'SFID {s}: {v:.2f} GB' for s, v in totals.items())


def page_us_flow_stats(pdf, us, **m):
    sp = _sp(m)
    if 'flow_octets' in us.columns and not us['flow_octets'].isna().all():
        us_d = _delta_mb(us, 'flow_octets')
        fig, ax = make_fig()
        plot_line(ax, us_d, 'flow_octets', 'sfid', 'MB / poll')
        save_page(pdf, fig, ax, 'US FLOW THROUGHPUT (MB/poll)',
                  f'{m["modem_name"]} ({m["mac_fmt"]})  |  {_total_gb_label(us_d, "flow_octets")}', **sp)

    if all(c in us.columns for c in ('flow_policed_drop', 'flow_policed_delay')):
        fig, ax = make_fig()
        plot_dual(ax, us, 'flow_policed_drop', 'flow_policed_delay',
                  'drop', 'delay', 'sfid', 'Packets')
        save_page(pdf, fig, ax, 'US POLICED DROP & DELAY',
                  f'{m["modem_name"]} ({m["mac_fmt"]})  |  US Service Flows', **sp)

    if 'flow_aqm_drop' in us.columns and not us['flow_aqm_drop'].isna().all():
        fig, ax = make_fig()
        plot_line(ax, us, 'flow_aqm_drop', 'sfid', 'Packets')
        save_page(pdf, fig, ax, 'US AQM DROPPED PACKETS',
                  f'{m["modem_name"]} ({m["mac_fmt"]})  |  US Service Flows', **sp)


def page_us_latency_max(pdf, us, **m):
    col = 'lat_max_usec'
    if col not in us.columns or us[col].isna().all():
        return
    us = us.copy()
    us[col] = pd.to_numeric(us[col], errors='coerce') / 1000
    fig, ax = make_fig()
    plot_line(ax, us, col, 'sfid', 'Latency (ms)')
    save_page(pdf, fig, ax, 'US LATENCY MAX (ms)',
              f'{m["modem_name"]} ({m["mac_fmt"]})  |  AQM-enabled SFIDs only', **_sp(m))


def page_us_latency_histogram(pdf, us, **m):
    bin_cols = [f'lat_bin{i}' for i in range(1, 17)]
    present  = [c for c in bin_cols if c in us.columns]
    if not present:
        return
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
        plot_bars(ax, bins, ACCENT, 'ms')
        save_page(pdf, fig, ax,
                  f'US LATENCY HISTOGRAM — SFID {sfid}',
                  f'{scn}  |  Last poll @ {last["captured_utc"]}', **_sp(m))


def page_us_congestion(pdf, us, **m):
    cong_cols = ['cong_aqm_drop', 'cong_ce_marked', 'cong_ect0', 'cong_ect1',
                 'cong_scn_marked', 'cong_sanctioned']
    present = [c for c in cong_cols if c in us.columns and not us[c].isna().all()]
    if not present:
        return
    sp = _sp(m)
    if 'cong_aqm_drop' in present and 'cong_ce_marked' in present:
        fig, ax = make_fig()
        plot_dual(ax, us, 'cong_aqm_drop', 'cong_ce_marked',
                  'AQM drop', 'CE marked', 'sfid', 'Packets')
        save_page(pdf, fig, ax, 'US CONGESTION — AQM DROPS & CE MARKED',
                  f'{m["modem_name"]} ({m["mac_fmt"]})', **sp)
    if 'cong_ect0' in present and 'cong_ect1' in present:
        fig, ax = make_fig()
        plot_dual(ax, us, 'cong_ect0', 'cong_ect1',
                  'ECT(0)', 'ECT(1)', 'sfid', 'Packets')
        save_page(pdf, fig, ax, 'US ECT(0) & ECT(1) PACKETS',
                  f'{m["modem_name"]} ({m["mac_fmt"]})', **sp)


def page_us_param_set(pdf, us, **m):
    rate_col = ('ps_max_rate_64'
                if 'ps_max_rate_64' in us.columns and not us['ps_max_rate_64'].isna().all()
                else 'ps_max_rate')
    if rate_col not in us.columns or us[rate_col].isna().all():
        return
    us = us.copy()
    us[rate_col] = pd.to_numeric(us[rate_col], errors='coerce') / 1_000_000
    sp = _sp(m)
    fig, ax = make_fig()
    plot_line(ax, us, rate_col, 'sfid', 'Mbps')
    save_page(pdf, fig, ax, 'US PARAM SET — MAX RATE (Mbps)',
              f'{m["modem_name"]} ({m["mac_fmt"]})  |  Active param set (type 2)', **sp)

    buf_cols = ['ps_min_buffer', 'ps_target_buffer', 'ps_max_buffer']
    present  = [c for c in buf_cols if c in us.columns and not us[c].isna().all()]
    if present:
        fig, ax = make_fig()
        for i, col in enumerate(present):
            for j, (sfid, grp) in enumerate(us.groupby('sfid')):
                c = SNS_PALETTE[(i * 3 + j) % len(SNS_PALETTE)]
                sns.lineplot(data=grp, x='captured_utc',
                             y=pd.to_numeric(grp[col], errors='coerce'),
                             ax=ax, color=c, linewidth=1.5,
                             marker='o', markersize=3,
                             label=f'{sfid} {col.replace("ps_", "")}',
                             legend=False)
        handles, labels = ax.get_legend_handles_labels()
        ax.legend(handles, labels, fontsize=7, ncol=2,
                  facecolor=BG_DARK, edgecolor=GRID_COLOR,
                  labelcolor=TEXT_COLOR, framealpha=0.9)
        ax.set_ylabel('Bytes', color=SUBTEXT, fontsize=10, fontweight='bold')
        ax.set_xlabel('Time (UTC)', color=SUBTEXT, fontsize=10)
        fmt_ax(ax)
        save_page(pdf, fig, ax, 'US PARAM SET — BUFFER TARGETS',
                  f'{m["modem_name"]} ({m["mac_fmt"]})  |  min / target / max', **sp)

# ---------------------------------------------------------------------------
# DS SNMP chart pages
# ---------------------------------------------------------------------------
def page_ds_flow_stats(pdf, ds, **m):
    sp = _sp(m)
    if 'flow_octets' in ds.columns and not ds['flow_octets'].isna().all():
        ds_d = _delta_mb(ds, 'flow_octets')
        fig, ax = make_fig()
        plot_line(ax, ds_d, 'flow_octets', 'sfid', 'MB / poll')
        save_page(pdf, fig, ax, 'DS FLOW THROUGHPUT (MB/poll)',
                  f'{m["modem_name"]} ({m["mac_fmt"]})  |  {_total_gb_label(ds_d, "flow_octets")}', **sp)

    if all(c in ds.columns for c in ('flow_policed_drop', 'flow_policed_delay')):
        fig, ax = make_fig()
        plot_dual(ax, ds, 'flow_policed_drop', 'flow_policed_delay',
                  'drop', 'delay', 'sfid', 'Packets')
        save_page(pdf, fig, ax, 'DS POLICED DROP & DELAY',
                  f'{m["modem_name"]} ({m["mac_fmt"]})  |  DS Service Flows', **sp)

    if 'flow_aqm_drop' in ds.columns and not ds['flow_aqm_drop'].isna().all():
        fig, ax = make_fig()
        plot_line(ax, ds, 'flow_aqm_drop', 'sfid', 'Packets')
        save_page(pdf, fig, ax, 'DS AQM DROPPED PACKETS',
                  f'{m["modem_name"]} ({m["mac_fmt"]})  |  DS Service Flows', **sp)


def page_ds_congestion(pdf, ds, **m):
    cong_cols = ['cong_aqm_drop', 'cong_ce_marked', 'cong_ect0', 'cong_ect1']
    present   = [c for c in cong_cols if c in ds.columns and not ds[c].isna().all()]
    if not present:
        return
    sp = _sp(m)
    if 'cong_aqm_drop' in present and 'cong_ce_marked' in present:
        fig, ax = make_fig()
        plot_dual(ax, ds, 'cong_aqm_drop', 'cong_ce_marked',
                  'AQM drop', 'CE marked', 'sfid', 'Packets')
        save_page(pdf, fig, ax, 'DS CONGESTION — AQM DROPS & CE MARKED',
                  f'{m["modem_name"]} ({m["mac_fmt"]})', **sp)
    if 'cong_ect0' in present and 'cong_ect1' in present:
        fig, ax = make_fig()
        plot_dual(ax, ds, 'cong_ect0', 'cong_ect1',
                  'ECT(0)', 'ECT(1)', 'sfid', 'Packets')
        save_page(pdf, fig, ax, 'DS ECT(0) & ECT(1) PACKETS',
                  f'{m["modem_name"]} ({m["mac_fmt"]})', **sp)


def page_ds_latency(pdf, ds, **m):
    sp = _sp(m)
    if 'lat_max_usec' in ds.columns and not ds['lat_max_usec'].isna().all():
        ds = ds.copy()
        ds['lat_max_usec'] = pd.to_numeric(ds['lat_max_usec'], errors='coerce') / 1000
        fig, ax = make_fig()
        plot_line(ax, ds, 'lat_max_usec', 'sfid', 'Latency (ms)')
        save_page(pdf, fig, ax, 'DS LATENCY MAX (ms)',
                  f'{m["modem_name"]} ({m["mac_fmt"]})  |  AQM-enabled SFIDs only', **sp)

    bin_cols = [f'lat_bin{i}' for i in range(1, 17)]
    present  = [c for c in bin_cols if c in ds.columns]
    if not present:
        return
    has_bins = ds[present].notna().any(axis=1)
    for sfid in ds.loc[has_bins, 'sfid'].unique():
        sfid_df = ds[(ds['sfid'] == sfid) & has_bins]
        if sfid_df.empty:
            continue
        last = sfid_df.iloc[-1]
        bins = [pd.to_numeric(last.get(c, 0), errors='coerce') / 1000 or 0 for c in present]
        scn  = last.get('ps_scn', '') or last.get('lat_bin_scn', '') or str(sfid)
        fig, ax = make_fig()
        plot_bars(ax, bins, '#34a853', 'ms')
        save_page(pdf, fig, ax,
                  f'DS LATENCY HISTOGRAM — SFID {sfid}',
                  f'{scn}  |  Last poll @ {last["captured_utc"]}', **sp)


# ---------------------------------------------------------------------------
# Kafka chart pages (vcmts)
# ---------------------------------------------------------------------------
def page_kafka_throughput(pdf, kdf, direction, **m):
    col = 'delta_octets'
    if col not in kdf.columns or kdf[col].isna().all():
        return
    kdf = kdf.copy()
    kdf['mbps']  = pd.to_numeric(kdf[col], errors='coerce') * 8 / 15 / 1_000_000
    grp_col      = 'sfid_label' if 'sfid_label' in kdf.columns else 'sfid'
    label        = direction.upper()
    fig, ax      = make_fig()
    plot_line(ax, kdf, 'mbps', grp_col, 'Throughput (Mbps)')
    save_page(pdf, fig, ax, f'{label} THROUGHPUT (Mbps)',
              f'{m["modem_name"]} ({m["mac_fmt"]})  |  Kafka delta_octets → Mbps', **_sp(m))


def page_kafka_latency_avg(pdf, kdf, direction, **m):
    col = 'lat_avg_usec'
    if col not in kdf.columns or kdf[col].isna().all():
        return
    kdf = kdf.copy()
    kdf[col] = pd.to_numeric(kdf[col], errors='coerce') / 1000
    grp_col  = 'sfid_label' if 'sfid_label' in kdf.columns else 'sfid'
    label    = direction.upper()
    fig, ax  = make_fig()
    plot_line(ax, kdf, col, grp_col, 'Latency (ms)')
    save_page(pdf, fig, ax, f'{label} LATENCY AVG (ms)',
              f'{m["modem_name"]} ({m["mac_fmt"]})  |  Kafka lat_avg_usec', **_sp(m))


def page_kafka_latency_histogram(pdf, kdf, direction, **m):
    bin_cols = [f'lat_bin{i}' for i in range(1, 17)]
    present  = [c for c in bin_cols if c in kdf.columns]
    if not present:
        return
    grp_col  = 'sfid_label' if 'sfid_label' in kdf.columns else 'sfid'
    label    = direction.upper()
    has_bins = kdf[present].notna().any(axis=1)
    for name, grp in kdf[has_bins].groupby(grp_col):
        last = grp.iloc[-1]
        bins = [pd.to_numeric(last.get(c, 0), errors='coerce') / 1000 or 0 for c in present]
        fig, ax = make_fig()
        plot_bars(ax, bins, ACCENT, 'ms')
        save_page(pdf, fig, ax,
                  f'{label} LATENCY HISTOGRAM — {name}',
                  f'Last sample @ {last["captured_utc"]}', **_sp(m))


def page_kafka_congestion(pdf, kdf, direction, **m):
    drop_col   = 'cong_aqm_drop'
    marked_col = 'cong_ce_marked'
    if drop_col not in kdf.columns and marked_col not in kdf.columns:
        return
    grp_col = 'sfid_label' if 'sfid_label' in kdf.columns else 'sfid'
    label   = direction.upper()
    sp      = _sp(m)
    if drop_col in kdf.columns and not kdf[drop_col].isna().all():
        fig, ax = make_fig()
        plot_line(ax, kdf, drop_col, grp_col, 'Packets')
        save_page(pdf, fig, ax, f'{label} AQM DROPPED PACKETS',
                  f'{m["modem_name"]} ({m["mac_fmt"]})  |  Kafka aqm_drop_pkts', **sp)
    if marked_col in kdf.columns and not kdf[marked_col].isna().all():
        fig, ax = make_fig()
        plot_line(ax, kdf, marked_col, grp_col, 'Packets')
        save_page(pdf, fig, ax, f'{label} AQM MARKED PACKETS',
                  f'{m["modem_name"]} ({m["mac_fmt"]})  |  Kafka aqm_marked_pkts', **sp)

# ---------------------------------------------------------------------------
# CSV loaders
# ---------------------------------------------------------------------------
def _load_csv(path):
    df   = pd.read_csv(path, comment='#', parse_dates=['captured_utc'])
    skip = {'captured_utc', 'target_ip', 'target_label', 'cmts_type',
            'sfid', 'ps_scn', 'lat_bin_scn'}
    for c in df.columns:
        if c not in skip:
            df[c] = pd.to_numeric(df[c], errors='coerce')
    df['sfid'] = df['sfid'].astype(str)
    return df.sort_values('captured_utc').reset_index(drop=True)


_KAFKA_COL_MAP = {
    'total_octets':    'flow_octets',
    'total_pkts':      'flow_pkts',
    'aqm_drop_pkts':   'cong_aqm_drop',
    'aqm_marked_pkts': 'cong_ce_marked',
    'sanctioned_pkts': 'cong_sanctioned',
    'lat_max_usec':    'lat_max_usec',
    'lat_avg_usec':    'lat_avg_usec',
    **{f'lat_bin{str(i).zfill(2)}': f'lat_bin{i}' for i in range(1, 17)},
}


def _load_kafka_csv(path):
    df       = pd.read_csv(path, comment='#', parse_dates=['captured_utc'])
    num_skip = {'captured_utc', 'dir', 'sfIndex', 'sfid', 'scn',
                'mdName', 'node', 'pod', 'cluster'}
    for c in df.columns:
        if c not in num_skip:
            df[c] = pd.to_numeric(df[c], errors='coerce')
    df['sfid'] = df['sfid'].astype(str)
    df['sfid_label'] = df.apply(
        lambda r: r['scn'] if pd.notna(r.get('scn')) and str(r.get('scn', '')).strip()
                  else r['sfid'], axis=1)
    df = df.rename(columns=_KAFKA_COL_MAP)
    df = df.sort_values('captured_utc').reset_index(drop=True)
    return df[df['dir'] == 'upstream'].copy(), df[df['dir'] == 'downstream'].copy()


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def main():
    sess         = find_session(sys.argv[1] if len(sys.argv) > 1 else None)
    cmts_type    = sess['cmts_type']
    session_name = input('Session name (e.g. "Netflix L4S Gaming Test") [Enter for default]: ').strip()
    if not session_name:
        session_name = f'{cmts_type.upper()} SNMP Telemetry Report'

    mac        = sess['mac']
    mac_fmt    = ':'.join(mac[i:i+2] for i in range(0, 12, 2)).upper()
    modem_name = MODEM_NAMES.get(mac, mac_fmt)
    print(f'Session: {sess["session_dir"]}  [{cmts_type}]')

    us = _load_csv(sess['us_path'])
    if cmts_type == 'vcmts':
        k_us, k_ds = _load_kafka_csv(sess['kafka_path'])
        ds = k_ds
    else:
        ds   = _load_csv(sess['ds_path'])
        k_us = k_ds = None

    all_times     = pd.concat([us['captured_utc'],
                                ds['captured_utc'] if ds is not None
                                else pd.Series(dtype='datetime64[ns]')]).dropna()
    session_start = all_times.min().strftime('%Y-%m-%d %H:%M UTC')
    session_end   = all_times.max().strftime('%Y-%m-%d %H:%M UTC')
    duration_secs = int((all_times.max() - all_times.min()).total_seconds())
    hours, rem    = divmod(duration_secs, 3600)
    duration_str  = f'{hours}h {rem // 60}m'
    total_polls   = us['poll_index'].nunique() if 'poll_index' in us.columns else len(us)

    us_sfids = sorted(us['sfid'].unique(), key=lambda x: int(x) if str(x).isdigit() else 0)
    ds_sfids = (sorted(ds['sfid'].unique(), key=lambda x: int(x) if str(x).isdigit() else 0)
                if ds is not None and not ds.empty else [])

    m = dict(mac_fmt=mac_fmt, modem_name=modem_name,
             session_start=session_start, session_end=session_end,
             cmts_type=cmts_type)

    if cmts_type == 'vcmts':
        toc = [
            ('3',  'US Flow Octets',             'Cumulative octets per US service flow (SNMP)'),
            ('4',  'US Policed Drop & Delay',     'Policed drop and delay packet counts per US flow'),
            ('5',  'US AQM Dropped Packets',      'AQM drop counters per US service flow'),
            ('6',  'US Latency Max (ms)',          'Peak latency per AQM-enabled US flow over time'),
            ('7',  'US Latency Histogram',         'Bin distribution of US latency in ms (last poll)'),
            ('8',  'US Congestion — AQM & CE',     'AQM drops and CE marked packets per US flow'),
            ('9',  'US Param Set — Max Rate',      'Active param set max rate (Mbps) per US flow'),
            ('10', 'US Throughput (Mbps)',         'Kafka delta_octets → Mbps per US flow'),
            ('11', 'US Latency Avg (ms)',          'Kafka average latency per US flow over time'),
            ('12', 'US Latency Histogram (Kafka)', 'Kafka 16-bin latency distribution (last sample)'),
            ('13', 'US AQM Dropped Packets',       'Kafka aqm_drop_pkts per US flow'),
            ('14', 'US AQM Marked Packets',        'Kafka aqm_marked_pkts per US flow'),
            ('15', 'DS Throughput (Mbps)',         'Kafka delta_octets → Mbps per DS flow'),
            ('16', 'DS Latency Avg (ms)',          'Kafka average latency per DS flow over time'),
            ('17', 'DS Latency Histogram (Kafka)', 'Kafka 16-bin latency distribution (last sample)'),
            ('18', 'DS AQM Dropped Packets',       'Kafka aqm_drop_pkts per DS flow'),
            ('19', 'DS AQM Marked Packets',        'Kafka aqm_marked_pkts per DS flow'),
        ]
    else:
        toc = [
            ('3',  'US Flow Octets',              'Cumulative octets per US service flow'),
            ('4',  'US Policed Drop & Delay',      'Policed drop and delay packet counts per US flow'),
            ('5',  'US AQM Dropped Packets',       'AQM drop counters per US service flow'),
            ('6',  'US Latency Max (ms)',           'Peak latency per AQM-enabled US flow over time'),
            ('7',  'US Latency Histogram',          'Bin distribution of US latency in ms (last poll)'),
            ('8',  'US Congestion — AQM & CE',      'AQM drops and CE marked packets per US flow'),
            ('9',  'US ECT(0) & ECT(1)',            'ECN capable transport packet counts per US flow'),
            ('10', 'US Param Set — Max Rate',       'Active param set max rate (Mbps) per US flow'),
            ('11', 'US Param Set — Buffer Targets', 'Min / target / max buffer bytes per US flow'),
            ('12', 'DS Flow Octets',                'Cumulative octets per DS service flow'),
            ('13', 'DS Policed Drop & Delay',       'Policed drop and delay packet counts per DS flow'),
            ('14', 'DS AQM Dropped Packets',        'AQM drop counters per DS service flow'),
            ('15', 'DS Congestion — AQM & CE',      'AQM drops and CE marked packets per DS flow'),
            ('16', 'DS ECT(0) & ECT(1)',            'ECN capable transport packet counts per DS flow'),
            ('17', 'DS Latency Max (ms)',            'Peak latency per AQM-enabled DS flow over time'),
            ('18', 'DS Latency Histogram',           'Bin distribution of DS latency in ms (last poll)'),
        ]

    safe_name = re.sub(r'[^\w\-]', '_', session_name).strip('_')
    out_path  = os.path.join(sess['session_dir'], f'report_sns_{cmts_type}_{mac}_{safe_name}.pdf')

    with PdfPages(out_path) as pdf:
        page_cover(pdf, mac_fmt, modem_name, session_start, session_end,
                   duration_str, total_polls, us_sfids, ds_sfids,
                   cmts_type=cmts_type, session_name=session_name)
        page_toc(pdf, mac_fmt, modem_name, session_start, session_end,
                 toc, cmts_type=cmts_type)

        page_us_flow_stats(pdf, us, **m)
        page_us_latency_max(pdf, us, **m)
        page_us_latency_histogram(pdf, us, **m)
        page_us_congestion(pdf, us, **m)
        page_us_param_set(pdf, us, **m)

        if cmts_type == 'vcmts':
            page_kafka_throughput(pdf, k_us, 'upstream', **m)
            page_kafka_latency_avg(pdf, k_us, 'upstream', **m)
            page_kafka_latency_histogram(pdf, k_us, 'upstream', **m)
            page_kafka_congestion(pdf, k_us, 'upstream', **m)
            page_kafka_throughput(pdf, k_ds, 'downstream', **m)
            page_kafka_latency_avg(pdf, k_ds, 'downstream', **m)
            page_kafka_latency_histogram(pdf, k_ds, 'downstream', **m)
            page_kafka_congestion(pdf, k_ds, 'downstream', **m)
        else:
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
