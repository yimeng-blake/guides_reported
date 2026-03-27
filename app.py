"""
Earnings Guidance Philosophy Analyzer — Web Interface
Streamlit app that wraps the CLI analyzer with interactive charts and downloads.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from io import BytesIO, StringIO

from earnings_guidance_analyzer import build_all_data, write_excel_to_bytes

# ── Translations ───────────────────────────────────────────────────────
_TR = {
    # Sidebar
    "sidebar_title":        {"en": "## 📊 Guidance Analyzer",         "zh": "## 📊 业绩指引分析器"},
    "ticker_label":         {"en": "Ticker Symbol",                   "zh": "股票代码"},
    "analyze_btn":          {"en": "🔍 Analyze",                      "zh": "🔍 开始分析"},
    "sidebar_desc":         {"en": "Analyzes management guidance vs actual results using SEC 8-K filings.\nPowered by Claude AI for universal filing format support.",
                             "zh": "基于SEC 8-K文件分析管理层业绩指引与实际结果的对比。\n由Claude AI驱动，支持通用文件格式解析。"},
    # Tab names
    "tab_dashboard":        {"en": "📋 Dashboard",                    "zh": "📋 仪表盘"},
    "tab_guide_vs_actual":  {"en": "📊 Guidance vs Actuals",          "zh": "📊 指引 vs 实际"},
    "tab_fy_walk":          {"en": "📈 FY Guidance Walk",             "zh": "📈 全年指引演变"},
    "tab_conservatism":     {"en": "🎯 Conservatism Score",           "zh": "🎯 保守程度评分"},
    "tab_market":           {"en": "💹 Market Reaction",              "zh": "💹 市场反应"},
    "tab_seasonal":         {"en": "📅 Seasonal Patterns",            "zh": "📅 季节性规律"},
    "tab_metric":           {"en": "📐 Metric Accuracy",              "zh": "📐 指标精度"},
    # Dashboard
    "recent_track":         {"en": "Recent Track Record",             "zh": "近期业绩追踪"},
    "current_streak":       {"en": "Current streak:",                 "zh": "当前连续:"},
    "consecutive":          {"en": "consecutive",                     "zh": "连续"},
    "fy_revision_pattern":  {"en": "FY Guidance Revision Pattern",    "zh": "全年指引修正规律"},
    "fiscal_year":          {"en": "Fiscal Year",                     "zh": "财年"},
    "initial_guide":        {"en": "Initial Guide",                   "zh": "初始指引"},
    "revision":             {"en": "Revision",                        "zh": "修正"},
    "total_delta":          {"en": "Total Δ",                         "zh": "总变动 Δ"},
    "investor_takeaway":    {"en": "Investor Takeaway",               "zh": "投资者要点"},
    "download_data":        {"en": "Download Data",                   "zh": "下载数据"},
    "download_excel":       {"en": "📥 Download Excel",               "zh": "📥 下载Excel"},
    "download_json":        {"en": "📥 Download JSON",                "zh": "📥 下载JSON"},
    # Guidance vs Actuals
    "guided_vs_actual_title": {"en": "{metric}: Guided vs Actual per Quarter",
                               "zh": "{metric}: 每季度指引 vs 实际"},
    "guided_vs_actual_desc":  {"en": "Each quarter, management issues forward guidance for **next quarter's** revenue (a dollar range). This chart compares the **guided midpoint** (dark bar) against the **actual reported result** (red bar). The table below shows the exact numbers, the dollar and percentage difference vs. midpoint, and the verdict:",
                               "zh": "每个季度，管理层会对**下一季度**的营收发布指引（一个金额范围）。此图表将**指引中值**（深色柱）与**实际报告结果**（红色柱）进行对比。下表展示具体数字、与中值的金额和百分比差异，以及判定结果:"},
    "beat_desc":            {"en": "= above the high end",            "zh": "= 超出指引上限"},
    "inrange_desc":         {"en": "= within the guided range",       "zh": "= 在指引范围内"},
    "miss_desc":            {"en": "= below the low end",             "zh": "= 低于指引下限"},
    "guided_midpoint":      {"en": "Guided Midpoint",                 "zh": "指引中值"},
    "quarter":              {"en": "Quarter",                         "zh": "季度"},
    "date":                 {"en": "Date",                            "zh": "日期"},
    "guide_low":            {"en": "Guide Low",                       "zh": "指引下限"},
    "guide_high":           {"en": "Guide High",                      "zh": "指引上限"},
    "midpoint":             {"en": "Midpoint",                        "zh": "中值"},
    "actual":               {"en": "Actual",                          "zh": "实际"},
    "diff_dollar":          {"en": "Diff ($M)",                       "zh": "差额($M)"},
    "diff_pct":             {"en": "Diff (%)",                        "zh": "差额(%)"},
    "verdict":              {"en": "Verdict",                         "zh": "判定"},
    "op_margin":            {"en": "Op Margin",                       "zh": "营业利润率"},
    "yoy_pct":              {"en": "YoY %",                           "zh": "同比%"},
    # FY Guidance Walk
    "fy_walk_title":        {"en": "Full-Year Revenue Guidance Evolution",
                             "zh": "全年营收指引演变"},
    "fy":                   {"en": "FY",                              "zh": "财年"},
    "rev_num":              {"en": "Rev #",                           "zh": "修正#"},
    "given_at":             {"en": "Given At",                        "zh": "发布于"},
    "fy_rev":               {"en": "FY Rev ($M)",                     "zh": "全年营收($M)"},
    "action":               {"en": "Action",                          "zh": "动作"},
    "chg_vs_prior_dollar":  {"en": "Chg vs Prior ($M)",              "zh": "环比变动($M)"},
    "chg_vs_prior_pct":     {"en": "Chg vs Prior (%)",               "zh": "环比变动(%)"},
    "chg_vs_initial_pct":   {"en": "Chg vs Initial (%)",             "zh": "较初始变动(%)"},
    "mgmt_raises":          {"en": "Management typically **raises** full-year guidance.",
                             "zh": "管理层通常会**上调**全年指引。"},
    "mgmt_lowers":          {"en": "Management typically **lowers** full-year guidance.",
                             "zh": "管理层通常会**下调**全年指引。"},
    "mgmt_maintains":       {"en": "Management typically **maintains** full-year guidance.",
                             "zh": "管理层通常会**维持**全年指引。"},
    "avg_change_initial_final": {"en": "Average total change from initial to final:",
                                 "zh": "从初始到最终的平均总变动:"},
    "across_n_fy":          {"en": "across {n} fiscal years.",        "zh": "跨{n}个财年。"},
    # Conservatism Score
    "conservatism_title":   {"en": "Guidance Conservatism Score (0-100)",
                             "zh": "指引保守程度评分 (0-100)"},
    "beat_magnitude":       {"en": "Beat Magnitude (0–33)",           "zh": "超出幅度 (0–33)"},
    "beat_mag_desc":        {"en": "How much actuals exceed guidance", "zh": "实际超出指引的幅度"},
    "range_width":          {"en": "Range Width (0–33)",              "zh": "范围宽度 (0–33)"},
    "range_width_desc":     {"en": "How wide the guidance range is",  "zh": "指引范围的宽窄"},
    "consistency":          {"en": "Consistency (0–33)",              "zh": "一致性 (0–33)"},
    "consistency_desc":     {"en": "How consistently they beat",      "zh": "超出指引的稳定性"},
    "total_score":          {"en": "Total Score",                     "zh": "总分"},
    "rolling_4q":           {"en": "Rolling 4Q Avg",                  "zh": "滚动4季均值"},
    "sandbagging":          {"en": "Sandbagging",                     "zh": "故意压低"},
    "conservative":         {"en": "Conservative",                    "zh": "保守"},
    "straight_shooter":     {"en": "Straight Shooter",                "zh": "实事求是"},
    "aggressive":           {"en": "Aggressive",                      "zh": "激进"},
    # Market Reaction
    "market_title":         {"en": "Stock Price Reaction to Earnings","zh": "财报后股价反应"},
    "rev_surprise_pct":     {"en": "Revenue Surprise (%)",            "zh": "营收意外(%)"},
    "oneday_return":        {"en": "1-Day Stock Return (%)",          "zh": "次日股价回报(%)"},
    "avg_1d_return_on":     {"en": "Avg 1D Return on",               "zh": "平均次日回报 "},
    "surprise_pct":         {"en": "Surprise (%)",                    "zh": "意外(%)"},
    "pre_close":            {"en": "Pre-Close",                       "zh": "盘前收盘价"},
    "post_1d":              {"en": "Post 1D",                         "zh": "次日收盘价"},
    "return_1d":            {"en": "1D Return (%)",                   "zh": "次日回报(%)"},
    "return_3d":            {"en": "3D Return (%)",                   "zh": "3日回报(%)"},
    "strongly_priced_in":   {"en": "strongly priced in",              "zh": "已被充分定价"},
    "partially_priced_in":  {"en": "partially priced in",             "zh": "已被部分定价"},
    "not_priced_in":        {"en": "not priced in",                   "zh": "尚未被定价"},
    # Seasonal Patterns
    "seasonal_title":       {"en": "Guidance Accuracy by Fiscal Quarter",
                             "zh": "各财季指引精度"},
    "n":                    {"en": "N",                               "zh": "样本数"},
    "avg_surprise":         {"en": "Avg Surprise (%)",                "zh": "平均意外(%)"},
    "beat_rate":            {"en": "Beat Rate (%)",                   "zh": "超出率(%)"},
    "max_beat":             {"en": "Max Beat (%)",                    "zh": "最大超出(%)"},
    "max_miss":             {"en": "Max Miss (%)",                    "zh": "最大低于(%)"},
    "most_conservative":    {"en": "Most conservative:",              "zh": "最保守:"},
    "most_accurate":        {"en": "Most accurate:",                  "zh": "最精确:"},
    # Metric Accuracy
    "metric_accuracy_title":{"en": "Guidance Accuracy Across Metrics","zh": "各指标指引精度"},
    "avg_error":            {"en": "Avg |Error|",                     "zh": "平均|误差|"},
    "metric":               {"en": "Metric",                         "zh": "指标"},
    "n_quarters":           {"en": "N Quarters",                      "zh": "季度数"},
    "miss_rate":            {"en": "Miss Rate (%)",                   "zh": "低于率(%)"},
    "most_conservative_guided": {"en": "Most conservatively guided:", "zh": "最保守指引:"},
    "most_accurate_guided":     {"en": "Most accurately guided:",     "zh": "最精确指引:"},
    # Landing page
    "landing_title":        {"en": "📊 Guidance Philosophy Analyzer", "zh": "📊 业绩指引分析器"},
    "landing_desc":         {"en": "This tool analyzes how company management sets earnings guidance relative to actual results — revealing whether they sandbag, play it straight, or guide aggressively.",
                             "zh": "本工具分析公司管理层如何设定业绩指引，通过对比指引与实际结果，揭示管理层是在故意压低预期、实事求是，还是激进指引。"},
    "landing_instruction":  {"en": "Enter a ticker symbol in the sidebar and click **Analyze** to get started.",
                             "zh": "在侧边栏输入股票代码，点击 **开始分析** 即可开始。"},
    # Progress messages
    "fetching_filings":     {"en": "Fetching SEC filings…",          "zh": "正在获取SEC文件…"},
    "parsing_filing":       {"en": "Parsing filing",                  "zh": "正在解析文件"},
    "building_analysis":    {"en": "Building analysis…",              "zh": "正在构建分析…"},
    "flat":                 {"en": "flat",                            "zh": "持平"},
    "raise":                {"en": "RAISE",                           "zh": "上调"},
    "lower":                {"en": "LOWER",                           "zh": "下调"},
    "reaffirm":             {"en": "REAFFIRM",                        "zh": "重申"},
    "initial":              {"en": "INITIAL",                         "zh": "初始"},
    # Tab commentaries — Guidance vs Actuals
    "commentary_gva": {
        "en": "Over the last {n} quarters, {ticker} beat guidance <strong>{beats}</strong> times, landed in-range <strong>{inrange}</strong> times, and missed <strong>{misses}</strong> time(s). The average upside vs. midpoint is <strong>{avg_diff}%</strong>, with a max beat of <strong>{max_beat}%</strong>{tail}.",
        "zh": "在过去{n}个季度中，{ticker}共<strong>{beats}</strong>次超出指引，<strong>{inrange}</strong>次落入范围内，<strong>{misses}</strong>次低于指引。平均超出中值<strong>{avg_diff}%</strong>，最大超出为<strong>{max_beat}%</strong>{tail}。",
    },
    "commentary_gva_tail_miss": {
        "en": " and a worst miss of <strong>{max_miss}%</strong>",
        "zh": "，最大低于为<strong>{max_miss}%</strong>",
    },
    "commentary_gva_tail_smallest_beat": {
        "en": " and a smallest beat of <strong>{min_beat}%</strong>",
        "zh": "，最小超出为<strong>{min_beat}%</strong>",
    },
    # Tab commentaries — FY Guidance Walk
    "commentary_fy_walk": {
        "en": "Across {n} fiscal years tracked, {ticker} has {raise_n} raise(s), {lower_n} lower(s), and {reaff_n} reaffirmation(s) in total. The average initial-to-final change is <strong>{avg_chg}%</strong>.",
        "zh": "在追踪的{n}个财年中，{ticker}共有{raise_n}次上调、{lower_n}次下调和{reaff_n}次重申。从初始到最终的平均变动为<strong>{avg_chg}%</strong>。",
    },
    # Tab commentaries — Conservatism Score
    "commentary_cs_avg":    {"en": "Average Score:",                   "zh": "平均评分:"},
    "commentary_cs_latest": {"en": "Latest Score:",                    "zh": "最新评分:"},
    "commentary_cs_trend":  {"en": "Trend:",                           "zh": "趋势:"},
    "commentary_cs_dir_more":  {"en": "more conservative",             "zh": "越来越保守"},
    "commentary_cs_dir_less":  {"en": "less conservative",             "zh": "越来越不保守"},
    "commentary_cs_dir_stable":{"en": "stable",                        "zh": "稳定"},
    "pts_per_q":            {"en": "pts/quarter",                      "zh": "分/季度"},
    # Tab commentaries — Market Reaction
    "commentary_mr_corr":   {"en": "Surprise-Return Correlation:",     "zh": "意外-回报相关性:"},
    "commentary_mr_eff":    {"en": "Management's guidance conservatism is <strong>{eff}</strong> by the market.",
                             "zh": "管理层的指引保守性已被市场<strong>{eff}</strong>。"},
    # Tab commentaries — Seasonal Patterns
    "commentary_sp_cons":   {"en": "Most conservative:",               "zh": "最保守:"},
    "commentary_sp_acc":    {"en": "Most accurate:",                   "zh": "最精确:"},
    "commentary_sp_detail": {"en": "avg surprise",                     "zh": "平均意外"},
    # Tab commentaries — Metric Accuracy
    "commentary_ma_cons":   {"en": "Most conservatively guided:",      "zh": "最保守指引:"},
    "commentary_ma_acc":    {"en": "Most accurately guided:",          "zh": "最精确指引:"},
    "commentary_ma_avg":    {"en": "avg",                              "zh": "平均"},
    "commentary_ma_err":    {"en": "avg |error|",                      "zh": "平均|误差|"},
    # Investor Takeaway narrative
    "takeaway_philosophy":  {"en": "{ticker} has a <strong>{archetype}</strong> guidance philosophy (conservatism score: {score}/100).",
                             "zh": "{ticker} 具有<strong>{archetype}</strong>的指引风格（保守程度评分: {score}/100）。"},
    "takeaway_beat_record": {"en": "Over {total} quarters, they beat guidance {beats} times ({pct}%) with an avg {diff}% upside vs midpoint.",
                             "zh": "在{total}个季度中，{beats}次超出指引（{pct}%），平均超出中值{diff}%。"},
    "takeaway_fy_raises":   {"en": "Management typically {direction} full-year guidance throughout the year, with an avg total change of {chg}% from initial to final.",
                             "zh": "管理层通常在年内{direction}全年指引，从初始到最终的平均总变动为{chg}%。"},
    "takeaway_dir_raises":  {"en": "raises",                          "zh": "上调"},
    "takeaway_dir_lowers":  {"en": "lowers",                          "zh": "下调"},
    "takeaway_priced_in":   {"en": "The market appears to have priced in this conservatism (r={corr}), resulting in muted reactions to beats.",
                             "zh": "市场似乎已经消化了这种保守性（r={corr}），导致对超预期的反应较为温和。"},
    "takeaway_not_priced":  {"en": "The market still rewards guidance beats meaningfully (r={corr}).",
                             "zh": "市场仍然对超出指引给予显著奖励（r={corr}）。"},
    # Section description blocks
    "desc_guided_vs_actual": {
        "en": 'Each quarter, management issues forward guidance for <strong>next quarter\'s</strong> revenue (a dollar range). This chart compares the <strong>guided midpoint</strong> (dark bar) against the <strong>actual reported result</strong> (red bar). The table below shows the exact numbers, the dollar and percentage difference vs. midpoint, and the verdict:',
        "zh": '每个季度，管理层会对<strong>下一季度</strong>的营收发布指引（一个金额范围）。此图表将<strong>指引中值</strong>（深色柱）与<strong>实际报告结果</strong>（红色柱）进行对比。下表展示具体数字、与中值的金额和百分比差异，以及判定结果:',
    },
    "desc_fy_walk": {
        "en": 'At each earnings call, management typically updates their <strong>full-year revenue guidance</strong>. Each line on the chart represents one fiscal year, showing how the FY revenue guide evolved from the initial estimate through subsequent quarterly revisions. An upward-sloping line means management raised guidance throughout the year (common for conservative/"sandbagging" companies). A flat or declining line suggests the initial guide was more aggressive or that business conditions deteriorated.',
        "zh": '每次财报电话会议上，管理层通常会更新其<strong>全年营收指引</strong>。图表中每条线代表一个财年，展示全年营收指引如何从初始估计值经过后续季度修正逐步演变。向上倾斜的线表示管理层在年内持续上调指引（保守型/"压低预期"的公司常见此模式）。平坦或下降的线则表明初始指引较为激进，或者业务状况出现恶化。',
    },
    "desc_conservatism": {
        "en": 'A composite score (0–100) measuring how conservative management\'s guidance is each quarter. Built from three sub-scores:<br>&nbsp;&nbsp;• <strong>Beat Magnitude (0–33):</strong> How much did the actual result exceed the guided midpoint? Bigger beats = higher score.<br>&nbsp;&nbsp;• <strong>Range Width (0–33):</strong> How wide is the guided range relative to the midpoint? Wider range = more cushion = higher score.<br>&nbsp;&nbsp;• <strong>Consistency (0–33):</strong> Rolling 4-quarter beat rate. Consistently beating = higher score.',
        "zh": '一个综合评分（0-100），衡量管理层每季度指引的保守程度。由三个子评分构成:<br>&nbsp;&nbsp;• <strong>超出幅度 (0–33):</strong> 实际结果超出指引中值的程度。超出越多 = 评分越高。<br>&nbsp;&nbsp;• <strong>范围宽度 (0–33):</strong> 指引范围相对于中值的宽度。范围越宽 = 缓冲越大 = 评分越高。<br>&nbsp;&nbsp;• <strong>一致性 (0–33):</strong> 滚动4季度超出率。持续超出 = 评分越高。',
    },
    "desc_conservatism_bands": {
        "en": 'The colored bands show the classification:',
        "zh": '色带表示分类:',
    },
    "desc_conservatism_tail": {
        "en": 'The red dashed line is the rolling 4-quarter average, smoothing out quarter-to-quarter noise.',
        "zh": '红色虚线为滚动4季度均值，用于平滑季度间的波动。',
    },
    "desc_market": {
        "en": 'How does the stock move after earnings? Each dot is one quarter: the x-axis shows the <strong>revenue surprise</strong> (actual vs. guided midpoint), the y-axis shows the <strong>1-day stock return</strong> (close after earnings vs. close before). If dots cluster along a positive diagonal, the market rewards bigger beats. If returns are scattered regardless of surprise size, it means the market has already <strong>priced in</strong> the conservatism and looks past the headline beat. The <strong>correlation coefficient (r)</strong> quantifies this: |r| &lt; 0.3 = strongly priced in, |r| &gt; 0.6 = market still reacting.',
        "zh": '财报发布后股价如何变动？每个点代表一个季度: x轴为<strong>营收意外</strong>（实际值 vs 指引中值），y轴为<strong>次日股价回报</strong>（财报后收盘价 vs 财报前收盘价）。如果点沿正对角线分布，说明市场奖励更大的超预期。如果回报与意外大小无关地分散，说明市场已经<strong>定价</strong>了这种保守性，忽略了表面上的超预期。<strong>相关系数(r)</strong>量化了这一点: |r| &lt; 0.3 = 已被充分定价，|r| &gt; 0.6 = 市场仍在反应。',
    },
    "desc_seasonal": {
        "en": 'Do certain quarters get more conservative guidance than others? This groups all historical quarters by <strong>Q1 / Q2 / Q3 / Q4</strong> and compares the average surprise (actual minus guided midpoint as a %). A higher bar means management tends to under-guide more in that quarter. Seasonality often emerges because of budget cycles (initial FY guide in Q4 earnings tends to be most conservative) or business patterns (e.g., holiday quarters may have more predictable revenue, leading to tighter guidance).',
        "zh": '某些季度的指引是否比其他季度更保守？本分析将所有历史季度按<strong>Q1/Q2/Q3/Q4</strong>分组，比较平均意外值（实际值减去指引中值的百分比）。柱越高，表示管理层在该季度的指引越趋于保守。季节性规律通常源于预算周期（Q4财报中发布的初始全年指引往往最保守）或业务模式（如节假日季度的营收更可预测，指引更为精确）。',
    },
    "desc_metric_accuracy": {
        "en": 'Management often guides on multiple metrics — revenue, operating margin, EPS, etc. Are they equally conservative across all of them, or do they sandbag one metric more than others? <strong>Avg Surprise</strong> shows the directional bias (positive = consistently under-guiding), while <strong>Avg |Error|</strong> measures raw accuracy regardless of direction. A high surprise with a low |error| means consistently beating by the same small amount — very controlled guidance.',
        "zh": '管理层通常对多个指标提供指引——营收、营业利润率、每股收益等。他们在所有指标上同样保守，还是某些指标比其他指标更故意压低？<strong>平均意外</strong>显示方向性偏差（正值 = 持续低于实际），而<strong>平均|误差|</strong>衡量不分方向的原始精度。高意外值配合低|误差|意味着每次都以相似的小幅度超出——非常可控的指引。',
    },
}

def T(key: str, **kwargs) -> str:
    """Look up a translated string for the current language."""
    lang = st.session_state.get("lang", "en")
    text = _TR.get(key, {}).get(lang) or _TR.get(key, {}).get("en", key)
    if kwargs:
        text = text.format(**kwargs)
    return text

# ── Page config ─────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Guidance Analyzer",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Theme state ────────────────────────────────────────────────────────
if "dark_mode" not in st.session_state:
    st.session_state["dark_mode"] = False

_dark = st.session_state["dark_mode"]

# ── Custom CSS with CSS variables for theming ──────────────────────────
_theme_vars = """
    :root {
        --bg: #f8f9fa;  --bg-card: #ffffff;  --bg-input: #16213e;
        --fg: #1a1a2e;  --fg-muted: #666666;  --fg-body: #333333;
        --border: #e0e0e0;  --shadow: rgba(0,0,0,0.08);
        --tab-bg: #ffffff;  --tab-fg: #555555;
        --desc-fg: #555;
    }
""" if not _dark else """
    :root {
        --bg: #0e1117;  --bg-card: #1a1d29;  --bg-input: #1a1d29;
        --fg: #e0e0e0;  --fg-muted: #9e9e9e;  --fg-body: #cccccc;
        --border: #2d3040;  --shadow: rgba(0,0,0,0.3);
        --tab-bg: #1a1d29;  --tab-fg: #9e9e9e;
        --desc-fg: #9e9e9e;
    }
"""

st.markdown(f"""
<style>
    {_theme_vars}

    /* App background */
    .stApp {{
        background-color: var(--bg);
    }}

    /* Sidebar styling (always dark) */
    section[data-testid="stSidebar"] {{
        background-color: #1a1a2e;
    }}
    section[data-testid="stSidebar"] * {{
        color: #e0e0e0 !important;
    }}
    section[data-testid="stSidebar"] .stTextInput label,
    section[data-testid="stSidebar"] .stButton button {{
        color: #ffffff !important;
    }}
    section[data-testid="stSidebar"] .stTextInput input {{
        background-color: #16213e;
        color: #ffffff !important;
        border: 1px solid #0f3460;
    }}
    section[data-testid="stSidebar"] .stButton button {{
        background-color: #e94560;
        color: #ffffff !important;
        border: none;
        font-weight: bold;
        width: 100%;
    }}
    section[data-testid="stSidebar"] .stButton button:hover {{
        background-color: #c73650;
    }}

    /* Tab styling */
    .stTabs [data-baseweb="tab-list"] {{
        gap: 0;
        background-color: var(--tab-bg);
        border-bottom: 2px solid var(--border);
        padding: 0 1rem;
    }}
    .stTabs [data-baseweb="tab"] {{
        padding: 0.75rem 1.25rem;
        color: var(--tab-fg);
        font-weight: 500;
        border-bottom: 3px solid transparent;
        background-color: transparent;
    }}
    .stTabs [data-baseweb="tab"]:hover {{
        color: var(--fg);
    }}
    .stTabs [aria-selected="true"] {{
        color: #e94560 !important;
        border-bottom: 3px solid #e94560 !important;
        font-weight: 700;
    }}

    /* Metric cards */
    div[data-testid="stMetric"] {{
        background-color: var(--bg-card);
        border: 1px solid var(--border);
        border-radius: 8px;
        padding: 1rem;
        box-shadow: 0 1px 3px var(--shadow);
    }}
    div[data-testid="stMetric"] label {{
        color: var(--fg-muted) !important;
        font-size: 0.85rem !important;
    }}
    div[data-testid="stMetric"] [data-testid="stMetricValue"] {{
        color: var(--fg) !important;
        font-size: 1.8rem !important;
        font-weight: 700 !important;
    }}
    div[data-testid="stMetric"] [data-testid="stMetricDelta"] {{
        font-size: 0.85rem !important;
    }}

    /* Dataframes */
    .stDataFrame {{
        border: 1px solid var(--border);
        border-radius: 8px;
        overflow: hidden;
    }}

    /* General text */
    .main .block-container {{
        padding-top: 2rem;
        color: var(--fg);
    }}
    h1, h2, h3, h4 {{
        color: var(--fg) !important;
    }}

    /* Hero banner */
    .hero-banner {{
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
        color: white;
        padding: 2rem 2.5rem;
        border-radius: 12px;
        margin-bottom: 1.5rem;
        box-shadow: 0 4px 15px rgba(0,0,0,0.15);
    }}
    .hero-banner h1 {{
        color: white !important;
        margin: 0;
        font-size: 2rem;
    }}
    .hero-banner .archetype {{
        color: #e94560;
        font-size: 2rem;
        font-weight: 700;
    }}
    .hero-banner .desc {{
        color: #b0b0b0;
        font-size: 1rem;
        margin-top: 0.5rem;
    }}

    /* Verdict badges */
    .verdict-beat {{
        background-color: #d4edda;
        color: #155724;
        padding: 2px 10px;
        border-radius: 12px;
        font-weight: 600;
        font-size: 0.85rem;
    }}
    .verdict-miss {{
        background-color: #f8d7da;
        color: #721c24;
        padding: 2px 10px;
        border-radius: 12px;
        font-weight: 600;
        font-size: 0.85rem;
    }}
    .verdict-range {{
        background-color: #fff3cd;
        color: #856404;
        padding: 2px 10px;
        border-radius: 12px;
        font-weight: 600;
        font-size: 0.85rem;
    }}

    /* Section headers */
    .section-header {{
        background-color: var(--bg-card);
        border-left: 4px solid #e94560;
        padding: 0.5rem 1rem;
        margin: 1.5rem 0 1rem 0;
        font-size: 1.1rem;
        font-weight: 600;
        color: var(--fg);
        border-radius: 0 6px 6px 0;
        box-shadow: 0 1px 3px var(--shadow);
    }}

    /* Narrative box */
    .narrative-box {{
        background-color: var(--bg-card);
        border: 1px solid var(--border);
        border-radius: 8px;
        padding: 1.25rem;
        margin: 1rem 0;
        line-height: 1.7;
        color: var(--fg-body);
        font-size: 0.95rem;
        box-shadow: 0 1px 3px var(--shadow);
    }}

    /* Streak display */
    .streak-container {{
        display: flex;
        gap: 6px;
        flex-wrap: wrap;
        margin: 0.5rem 0;
    }}
    .streak-item {{
        width: 36px;
        height: 36px;
        border-radius: 6px;
        display: flex;
        align-items: center;
        justify-content: center;
        font-weight: 700;
        font-size: 0.85rem;
    }}
    .streak-beat {{ background-color: #d4edda; color: #155724; }}
    .streak-miss {{ background-color: #f8d7da; color: #721c24; }}
    .streak-range {{ background-color: #fff3cd; color: #856404; }}

    /* Download section */
    .download-section {{
        background-color: var(--bg-card);
        border: 1px solid var(--border);
        border-radius: 8px;
        padding: 1rem;
        margin-top: 1rem;
    }}

    /* Description text under section headers */
    .desc-text {{
        font-size: 0.88rem;
        color: var(--desc-fg);
        margin: -0.5rem 0 1rem 0;
        line-height: 1.6;
    }}

    /* Hide streamlit branding but keep the sidebar toggle */
    #MainMenu {{ visibility: hidden; }}
    footer {{ visibility: hidden; }}
    /* Make header transparent but keep it functional for the sidebar toggle */
    header[data-testid="stHeader"] {{
        background: transparent !important;
        backdrop-filter: none !important;
    }}
    /* Hide toolbar items EXCEPT the sidebar expand button */
    header [data-testid="stToolbar"] > *:not(:has([data-testid="stExpandSidebarButton"])) {{
        display: none !important;
    }}
    header [data-testid="stDecoration"] {{ display: none; }}
    header [data-testid="stHeaderActionElements"] {{ display: none !important; }}

    /* Ensure sidebar expand button is always visible and clickable when sidebar is collapsed */
    button[data-testid="stExpandSidebarButton"] {{
        position: fixed !important;
        top: 12px !important;
        left: 12px !important;
        z-index: 999999 !important;
        width: 40px !important;
        height: 40px !important;
        min-width: 40px !important;
        min-height: 40px !important;
        background-color: #1a1a2e !important;
        color: white !important;
        border-radius: 8px !important;
        border: none !important;
        box-shadow: 0 2px 8px rgba(0,0,0,0.2) !important;
        display: flex !important;
        align-items: center !important;
        justify-content: center !important;
        cursor: pointer !important;
    }}
    button[data-testid="stExpandSidebarButton"]:hover {{
        background-color: #e94560 !important;
    }}
    button[data-testid="stExpandSidebarButton"] svg {{
        width: 20px !important;
        height: 20px !important;
        fill: white !important;
        stroke: white !important;
    }}
</style>
""", unsafe_allow_html=True)


# ── Sidebar ─────────────────────────────────────────────────────────────
if "lang" not in st.session_state:
    st.session_state["lang"] = "en"

with st.sidebar:
    # Title
    st.markdown(T("sidebar_title"))
    # Language + dark mode toggles
    tc1, tc2 = st.columns(2)
    with tc1:
        _cur = st.session_state["lang"]
        _label = "中文" if _cur == "en" else "EN"
        if st.button(f"🌐 {_label}", key="lang_toggle", use_container_width=True):
            st.session_state["lang"] = "zh" if _cur == "en" else "en"
            st.rerun()
    with tc2:
        _dm_label = "☀️ Light" if _dark else "🌙 Dark"
        if st.button(_dm_label, key="dark_toggle", use_container_width=True):
            st.session_state["dark_mode"] = not _dark
            st.rerun()
    st.markdown("---")
    ticker_input = st.text_input(T("ticker_label"), value="SNOW", placeholder="e.g. SNOW, AAOI, MDB")
    analyze_btn = st.button(T("analyze_btn"), use_container_width=True)
    st.markdown("---")
    st.markdown(f"""
    <div style="font-size: 0.8rem; color: var(--fg-muted);">
    {T("sidebar_desc")}
    </div>
    """, unsafe_allow_html=True)


# ── Helpers ─────────────────────────────────────────────────────────────

def verdict_color(v):
    if v == "BEAT": return "#d4edda"
    if v == "MISS": return "#f8d7da"
    if v == "IN-RANGE": return "#fff3cd"
    return "#f0f0f0"

def verdict_text_color(v):
    if v == "BEAT": return "#155724"
    if v == "MISS": return "#721c24"
    if v == "IN-RANGE": return "#856404"
    return "#666666"

def style_verdict_df(df):
    """Apply verdict-based row coloring to a dataframe."""
    def row_style(row):
        v = row.get("Verdict", "")
        bg = verdict_color(v)
        tc = verdict_text_color(v)
        if v in ("BEAT", "MISS", "IN-RANGE"):
            return [f"background-color: {bg}; color: {tc}"] * len(row)
        return [""] * len(row)
    return df.style.apply(row_style, axis=1)

def fmt_pct(v):
    if v is None: return "—"
    return f"{v:+.1f}%"

def fmt_dollar(v):
    if v is None: return "—"
    return f"${v:,.0f}M"


# ── Main logic ──────────────────────────────────────────────────────────

if analyze_btn and ticker_input:
    ticker = ticker_input.strip().upper()

    # Single placeholder that holds ALL loading UI — can be fully cleared
    loading_placeholder = st.empty()

    log_lines = []
    quarters_parsed = [0]

    # We need references to inner widgets, so we build them inside the placeholder
    # Using a container inside the empty placeholder
    loading_container = loading_placeholder.container()
    loading_container.markdown(f"""
    <div style="background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
                border-radius: 12px; padding: 1.5rem 2rem; margin-bottom: 1rem;
                color: white; box-shadow: 0 4px 15px rgba(0,0,0,0.15);">
        <div style="font-size: 1.3rem; font-weight: 700; margin-bottom: 0.25rem;">
            Analyzing {ticker}...
        </div>
        <div style="font-size: 0.85rem; color: #999;">
            Fetching SEC filings, parsing with Claude AI, computing analytics
        </div>
    </div>
    """, unsafe_allow_html=True)
    progress_bar = loading_container.progress(0)
    phase_text = loading_container.empty()
    log_area = loading_container.empty()

    total_to_parse = [0]  # track total for concurrent progress

    def progress_cb(msg):
        # Track phases and build a rich log
        msg_lower = msg.lower()
        if "Fetching 8-K" in msg:
            phase_text.markdown("**Phase 1/5** — Fetching SEC 8-K filings index...")
            progress_bar.progress(0.03)
        elif "fetching" in msg_lower and "edgar" in msg_lower and "exhibit" in msg_lower:
            phase_text.markdown(f"**Phase 1/5** — {msg.strip()}")
            progress_bar.progress(0.04)
        elif "fetching edgar filing" in msg_lower:
            # Per-EDGAR-filing progress: "Fetching EDGAR filing 3/15..."
            import re as _re
            m = _re.search(r"(\d+)/(\d+)", msg)
            if m:
                done, total = int(m.group(1)), int(m.group(2))
                pct = 0.04 + (done / total) * 0.04  # 0.04 to 0.08
                phase_text.markdown(f"**Phase 1/5** — Fetching older SEC filings ({done}/{total})...")
                progress_bar.progress(min(pct, 0.08))
        elif "fetching" in msg_lower and "exhibit texts from primary" in msg_lower:
            phase_text.markdown(f"**Phase 1/5** — {msg.strip()}")
            progress_bar.progress(0.06)
        elif "fetched" in msg_lower and "exhibit texts" in msg_lower:
            # "Fetched 10/200 exhibit texts..."
            import re as _re
            m = _re.search(r"(\d+)/(\d+)", msg)
            if m:
                done, total = int(m.group(1)), int(m.group(2))
                pct = 0.06 + (done / total) * 0.02  # 0.06 to 0.08
                phase_text.markdown(f"**Phase 1/5** — Fetching exhibit texts ({done}/{total})...")
                progress_bar.progress(min(pct, 0.08))
        elif "Fetching income" in msg:
            phase_text.markdown("**Phase 1/5** — Fetching income statements...")
            progress_bar.progress(0.09)
        elif "found" in msg_lower and "earnings releases" in msg_lower:
            phase_text.markdown(f"**Phase 1/5** — {msg.strip()}")
            progress_bar.progress(0.10)
        elif "[cached]" in msg_lower:
            quarters_parsed[0] += 1
            progress_bar.progress(min(0.10 + quarters_parsed[0] * 0.02, 0.70))
        elif "loaded" in msg_lower and "from cache" in msg_lower:
            phase_text.markdown(f"**Phase 2/5** — ⚡ {msg.strip()}")
        elif "concurrent" in msg_lower:
            # "Parsing N filings with Claude AI (concurrent)..."
            import re as _re
            m = _re.search(r"(\d+) filings", msg)
            if m:
                total_to_parse[0] = int(m.group(1))
            phase_text.markdown(f"**Phase 2/5** — {msg.strip()}")
            progress_bar.progress(0.15)
        elif "rate limited" in msg_lower:
            phase_text.markdown(f"**Phase 2/5** — ⏳ {msg.strip()}")
        elif "Parsing" in msg and "..." in msg:
            quarters_parsed[0] += 1
            # Extract the progress fraction from "(N/M)" if present
            import re as _re
            m = _re.search(r"\((\d+)/(\d+)\)", msg)
            if m:
                done, total = int(m.group(1)), int(m.group(2))
                pct = min(0.15 + (done / total) * 0.55, 0.70)
                phase_text.markdown(f"**Phase 2/5** — Parsing with Claude AI... ({done}/{total})")
            else:
                pct = min(0.10 + quarters_parsed[0] * 0.025, 0.70)
                date_part = msg.replace("Parsing ", "").replace("...", "").strip()
                date_part = date_part.replace("[EDGAR] ", "")
                phase_text.markdown(f"**Phase 2/5** — Parsing **{date_part}** ({quarters_parsed[0]} done)")
            progress_bar.progress(pct)
        elif msg.strip().startswith("FY") or msg.strip().startswith("CY"):
            clean = msg.strip()
            icon = "✅" if "guide:" in clean or "growth:" in clean else "📄"
            log_lines.append(f"{icon} {clean}")
            recent = log_lines[-6:]
            log_html = "<div style='background:#f8f9fa; border:1px solid #e0e0e0; border-radius:8px; padding:0.75rem 1rem; font-family:monospace; font-size:0.8rem; line-height:1.8; color:#333;'>"
            for line in recent:
                log_html += f"{line}<br>"
            log_html += "</div>"
            log_area.markdown(log_html, unsafe_allow_html=True)
        elif "normalizing" in msg_lower and "validating" in msg_lower:
            phase_text.markdown("**Phase 2/5** — Normalizing and validating parsed data...")
            progress_bar.progress(0.71)
        elif "quarterly guidance vs actuals" in msg_lower:
            phase_text.markdown("**Phase 2/5** — Building quarterly guidance vs actuals...")
            progress_bar.progress(0.73)
        elif "full-year" in msg_lower or "guidance walk" in msg_lower:
            phase_text.markdown("**Phase 3/5** — Building full-year guidance walk...")
            progress_bar.progress(0.75)
        elif "conservatism" in msg_lower:
            phase_text.markdown("**Phase 3/5** — Computing conservatism scores...")
            progress_bar.progress(0.80)
        elif "retrying" in msg_lower and "failed exhibit" in msg_lower:
            phase_text.markdown(f"**Phase 1/5** — ⏳ {msg.strip()}")
            progress_bar.progress(0.08)
        elif "[recovered]" in msg_lower:
            phase_text.markdown(f"**Phase 1/5** — ✅ {msg.strip()}")
        elif "stock prices" in msg_lower:
            phase_text.markdown("**Phase 4/5** — Fetching stock prices (concurrent)...")
            progress_bar.progress(0.85)
        elif "seasonal" in msg_lower:
            phase_text.markdown("**Phase 5/5** — Analyzing seasonal patterns...")
            progress_bar.progress(0.90)
        elif "accuracy" in msg_lower or "metric" in msg_lower:
            phase_text.markdown("**Phase 5/5** — Comparing cross-metric accuracy...")
            progress_bar.progress(0.95)

    data = build_all_data(ticker, progress_callback=progress_cb)

    # Fully clear the entire loading UI
    loading_placeholder.empty()

    st.session_state["data"] = data
    st.session_state["ticker"] = ticker


# ── Display results ─────────────────────────────────────────────────────

if "data" in st.session_state:
    data = st.session_state["data"]
    ticker = st.session_state["ticker"]

    guided = data["guided_rows"]
    rows = data["rows"]
    scores = data["scores"]

    if not guided:
        st.warning(f"No quarters with matched guidance data found for {ticker}. "
                   f"Found {len(rows)} earnings filings total.")
        st.stop()

    beats = sum(1 for r in guided if r["verdict"] == "BEAT")
    in_range = sum(1 for r in guided if r["verdict"] == "IN-RANGE")
    misses = sum(1 for r in guided if r["verdict"] == "MISS")
    total = len(guided)
    diffs = [r["rev_diff_pct"] for r in guided if r["rev_diff_pct"] is not None]
    avg_diff = sum(diffs) / len(diffs) if diffs else 0
    avg_score = sum(s["total_score"] for s in scores) / len(scores) if scores else 0
    _archetype_key = ("sandbagging" if avg_score > 66 else "conservative" if avg_score > 50
                      else "straight_shooter" if avg_score > 33 else "aggressive")
    archetype = T(_archetype_key)
    archetype_desc_map = {
        "sandbagging":      {"en": "Management systematically under-guides to create reliable beat-and-raise narratives.",
                             "zh": "管理层系统性地压低指引，以营造稳定的超预期叙事。"},
        "conservative":     {"en": "Management sets achievable targets with moderate upside built in.",
                             "zh": "管理层设定可实现的目标，预留适度上行空间。"},
        "straight_shooter": {"en": "Management aims for accurate guidance, resulting in a balanced beat/miss record.",
                             "zh": "管理层力求准确指引，超出与低于记录较为均衡。"},
        "aggressive":       {"en": "Management sets ambitious targets, frequently requiring downward revisions.",
                             "zh": "管理层设定激进目标，经常需要下调修正。"},
    }
    _lang = st.session_state.get("lang", "en")
    archetype_desc_text = archetype_desc_map.get(_archetype_key, {}).get(_lang, "")

    # ── Tab layout ───────────────────────────────────────────────────
    tabs = st.tabs([T("tab_dashboard"), T("tab_guide_vs_actual"), T("tab_fy_walk"),
                     T("tab_conservatism"), T("tab_market"),
                     T("tab_seasonal"), T("tab_metric")])

    # ═══════════════════════════════════════════════════════════════
    # TAB 0: DASHBOARD
    # ═══════════════════════════════════════════════════════════════
    with tabs[0]:
        # Hero banner
        st.markdown(f"""
        <div class="hero-banner">
            <h1>{ticker} — <span class="archetype">{archetype}</span></h1>
            <div class="desc">{archetype_desc_text}</div>
        </div>
        """, unsafe_allow_html=True)

        # Metric cards
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Quarters Analyzed", total)
        c2.metric("Beat Rate", f"{beats/total*100:.0f}%", f"{beats}B / {in_range}R / {misses}M")
        c3.metric("Avg Surprise", f"{avg_diff:+.1f}%")
        c4.metric("Conservatism Score", f"{avg_score:.0f}/100")
        c5.metric("Score Trend", f"{data['score_slope']:+.1f} pts/q")

        # Recent track record
        st.markdown(f'<div class="section-header">{T("recent_track")}</div>', unsafe_allow_html=True)
        recent = guided[-12:]
        streak_html = '<div class="streak-container">'
        for r in recent:
            v = r["verdict"]
            letter = {"BEAT": "B", "MISS": "M", "IN-RANGE": "R"}.get(v, "?")
            cls = {"BEAT": "streak-beat", "MISS": "streak-miss", "IN-RANGE": "streak-range"}.get(v, "")
            streak_html += f'<div class="streak-item {cls}" title="{r["fiscal_quarter"]}: {v}">{letter}</div>'
        streak_html += '</div>'
        st.markdown(streak_html, unsafe_allow_html=True)

        # Current streak
        if guided:
            streak_v = guided[-1]["verdict"]
            streak_n = 0
            for x in reversed(guided):
                if x["verdict"] == streak_v:
                    streak_n += 1
                else:
                    break
            st.markdown(f"**{T('current_streak')}** {streak_n} {T('consecutive')} **{streak_v}**")

        # FY guidance walk summary — detailed quarter-by-quarter
        fy_walk = data["fy_walk"]
        fy_walk_rows = data["fy_walk_rows"]
        total_raises = []
        for fy, revs in fy_walk.items():
            if len(revs) >= 2:
                # Only count FYs where first entry is Q4 of prior year (true initial)
                import re as _re
                m = _re.match(r"(CY|FY)(\d{4})", fy)
                if m:
                    yr = int(m.group(2))
                    sq = revs[0].get("source_q", "")
                    sq_m = _re.match(r"(CY|FY)(\d{4})-Q(\d)", sq)
                    if sq_m and int(sq_m.group(2)) == yr - 1 and int(sq_m.group(3)) == 4:
                        total_raises.append((revs[-1]["fy_rev"] / revs[0]["fy_rev"] - 1) * 100)
        if fy_walk_rows:
            st.markdown(f'<div class="section-header">{T("fy_revision_pattern")}</div>', unsafe_allow_html=True)

            # Build detailed HTML table showing each revision step
            fy_keys = sorted(fy_walk.keys(), reverse=True)
            # Find the max number of revisions across all FYs
            max_revs = max(len(fy_walk[fy]) for fy in fy_keys) if fy_keys else 0

            html = f"""
            <div class="narrative-box" style="padding: 0.75rem; overflow-x: auto;">
            <table style="width: 100%; border-collapse: collapse; font-size: 0.9rem;">
            <thead>
                <tr style="border-bottom: 2px solid var(--fg);">
                    <th style="text-align: left; padding: 8px 12px; color: var(--fg); font-weight: 700;">{T("fiscal_year")}</th>"""

            # Column headers: one per revision slot
            rev_labels = [T("initial_guide")]
            for i in range(1, max_revs):
                rev_labels.append(f"{T('revision')} {i}")
            rev_labels.append(T("total_delta"))

            for label in rev_labels:
                html += f'<th style="text-align: center; padding: 8px 10px; color: var(--fg); font-weight: 700;">{label}</th>'
            html += "</tr></thead><tbody>"

            for fy in fy_keys:
                revs = fy_walk[fy]
                if len(revs) < 1:
                    continue
                html += f'<tr style="border-bottom: 1px solid #e0e0e0;">'
                html += f'<td style="padding: 8px 12px; font-weight: 600; color: var(--fg);">{fy}</td>'

                initial = revs[0]["fy_rev"]
                # Check if this FY has a proper Q4 initial
                _fy_m = _re.match(r"(CY|FY)(\d{4})", fy)
                _has_q4 = False
                if _fy_m:
                    _yr = int(_fy_m.group(2))
                    _sq0 = revs[0].get("source_q", "")
                    _sq0_m = _re.match(r"(CY|FY)(\d{4})-Q(\d)", _sq0)
                    _has_q4 = (_sq0_m and int(_sq0_m.group(2)) == _yr - 1
                               and int(_sq0_m.group(3)) == 4)

                for i in range(max_revs):
                    if i < len(revs):
                        rev = revs[i]
                        val = rev["fy_rev"]
                        cell = f"${val:,.0f}M"
                        subtitle = f"<br><span style='font-size:0.75rem; color:#888;'>({rev['source_q']})</span>"

                        if i == 0 and _has_q4:
                            # Proper Q4 initial — neutral blue
                            bg = "#e8f4fd"
                            color = "#1a1a2e"
                        elif i == 0:
                            # No Q4 initial — gray to indicate incomplete
                            bg = "#f0f0f0"
                            color = "#666"
                        else:
                            chg = val - revs[i-1]["fy_rev"]
                            chg_pct = (chg / revs[i-1]["fy_rev"]) * 100
                            if chg_pct > 0.5:
                                bg = "#d4edda"
                                color = "#155724"
                                cell += f"<br><span style='font-size:0.75rem; font-weight:600;'>▲ +{chg_pct:.1f}%</span>"
                            elif chg_pct < -0.5:
                                bg = "#f8d7da"
                                color = "#721c24"
                                cell += f"<br><span style='font-size:0.75rem; font-weight:600;'>▼ {chg_pct:.1f}%</span>"
                            else:
                                bg = "#fff3cd"
                                color = "#856404"
                                cell += f"<br><span style='font-size:0.75rem; font-weight:600;'>= flat</span>"

                        html += f'<td style="text-align: center; padding: 8px 10px; background: {bg}; color: {color};">{cell}{subtitle}</td>'
                    else:
                        html += '<td style="text-align: center; padding: 8px 10px; color: #ccc;">—</td>'

                # Total change column
                if len(revs) >= 2:
                    total_chg = (revs[-1]["fy_rev"] / initial - 1) * 100
                    if total_chg > 0:
                        bg = "#d4edda"; color = "#155724"
                    elif total_chg < 0:
                        bg = "#f8d7da"; color = "#721c24"
                    else:
                        bg = "#fff3cd"; color = "#856404"
                    html += f'<td style="text-align: center; padding: 8px 10px; background: {bg}; color: {color}; font-weight: 700;">{total_chg:+.1f}%</td>'
                else:
                    html += '<td style="text-align: center; padding: 8px 10px; color: var(--fg-muted);">n/a</td>'

                html += "</tr>"

            html += "</tbody></table>"

            # Summary line
            if total_raises:
                avg_fy_raise = sum(total_raises) / len(total_raises)
                direction_key = "mgmt_raises" if avg_fy_raise > 0 else "mgmt_lowers"
                html += f'<div style="margin-top: 12px; padding-top: 10px; border-top: 1px solid var(--border); font-size: 0.9rem; color: var(--fg-body);">{T(direction_key)} {T("avg_change_initial_final")} <strong>{avg_fy_raise:+.1f}%</strong> {T("across_n_fy", n=len(total_raises))}</div>'

            html += "</div>"
            st.markdown(html, unsafe_allow_html=True)

        # Narrative
        st.markdown(f'<div class="section-header">{T("investor_takeaway")}</div>', unsafe_allow_html=True)
        parts = [T("takeaway_philosophy", ticker=ticker, archetype=archetype, score=f"{avg_score:.0f}")]
        parts.append(T("takeaway_beat_record", total=total, beats=beats, pct=f"{beats/total*100:.0f}", diff=f"{avg_diff:+.1f}"))
        if total_raises:
            direction = T("takeaway_dir_raises") if avg_fy_raise > 0 else T("takeaway_dir_lowers")
            parts.append(T("takeaway_fy_raises", direction=direction, chg=f"{avg_fy_raise:+.1f}"))
        corr = data["price_corr"]
        if corr is not None:
            if abs(corr) < 0.3:
                parts.append(T("takeaway_priced_in", corr=f"{corr:.2f}"))
            elif abs(corr) > 0.5:
                parts.append(T("takeaway_not_priced", corr=f"{corr:.2f}"))
        st.markdown(f'<div class="narrative-box">{" ".join(parts)}</div>', unsafe_allow_html=True)

        # Downloads
        st.markdown(f'<div class="section-header">{T("download_data")}</div>', unsafe_allow_html=True)
        dc1, dc2 = st.columns(2)
        with dc1:
            excel_bytes = write_excel_to_bytes(data, ticker)
            st.download_button(T("download_excel"), data=excel_bytes,
                             file_name=f"{ticker}_guidance_analysis.xlsx",
                             mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                             use_container_width=True)
        with dc2:
            df_csv = pd.DataFrame(data["rows"])
            csv_str = df_csv.to_csv(index=False)
            st.download_button("📥 Download CSV", data=csv_str,
                             file_name=f"{ticker}_guidance_data.csv",
                             mime="text/csv",
                             use_container_width=True)

    # ═══════════════════════════════════════════════════════════════
    # TAB 1: GUIDANCE VS ACTUALS
    # ═══════════════════════════════════════════════════════════════
    with tabs[1]:
        metric_name = data.get("revenue_metric_name", "Revenue")
        st.markdown(f'<div class="section-header">{T("guided_vs_actual_title", metric=metric_name)}</div>', unsafe_allow_html=True)
        st.markdown(f"""<div class="desc-text">
        {T("desc_guided_vs_actual")}
        <span style="background:#d4edda;color:#155724;padding:1px 6px;border-radius:4px;font-weight:600;">BEAT</span> {T("beat_desc")},
        <span style="background:#fff3cd;color:#856404;padding:1px 6px;border-radius:4px;font-weight:600;">IN-RANGE</span> {T("inrange_desc")},
        <span style="background:#f8d7da;color:#721c24;padding:1px 6px;border-radius:4px;font-weight:600;">MISS</span> {T("miss_desc")}.
        </div>""", unsafe_allow_html=True)

        # Chart
        chart_rows = [r for r in rows if r["guide_midpoint"] is not None and r["actual_revenue"] is not None]
        if chart_rows:
            fig = go.Figure()
            quarters = [r["fiscal_quarter"] for r in chart_rows]
            fig.add_trace(go.Bar(
                name="Guided Midpoint",
                x=quarters,
                y=[r["guide_midpoint"] for r in chart_rows],
                marker_color="#7aa2d4" if _dark else "#1a1a2e",
                opacity=0.7,
            ))
            fig.add_trace(go.Bar(
                name=f"Actual {metric_name}",
                x=quarters,
                y=[r["actual_revenue"] for r in chart_rows],
                marker_color="#e94560",
            ))
            fig.update_layout(
                barmode="group",
                template="plotly_dark" if _dark else "plotly_white",
                height=400,
                margin=dict(t=30, b=40),
                legend=dict(orientation="h", y=1.1),
                yaxis_title=f"{metric_name} ($M)",
            )
            st.plotly_chart(fig, use_container_width=True)

        # Table (most recent first)
        display_rows = []
        for r in reversed(rows):
            display_rows.append({
                "Quarter": r["fiscal_quarter"],
                "Date": r["filing_date"],
                "Guide Low": r["guide_low"],
                "Guide High": r["guide_high"],
                "Midpoint": r["guide_midpoint"],
                "Actual": r["actual_revenue"],
                "Diff ($M)": r["rev_diff_vs_mid"],
                "Diff (%)": r["rev_diff_pct"],
                "Verdict": r["verdict"],
                "Op Margin": r["op_margin_actual"],
                "YoY %": r.get("rev_yoy"),
            })
        df_display = pd.DataFrame(display_rows)
        st.dataframe(
            style_verdict_df(df_display).format({
                "Guide Low": "{:,.1f}", "Guide High": "{:,.1f}",
                "Midpoint": "{:,.1f}", "Actual": "{:,.1f}",
                "Diff ($M)": "{:+,.1f}", "Diff (%)": "{:+.1f}%",
                "Op Margin": "{:.1f}%", "YoY %": "{:+.1f}%",
            }, na_rep="—"),
            use_container_width=True,
            height=min(len(display_rows) * 38 + 40, 600),
        )

        # Commentary
        if guided:
            _diffs = [r["rev_diff_pct"] for r in guided if r["rev_diff_pct"] is not None]
            _max_beat = max(_diffs) if _diffs else 0
            _min_val = min(_diffs) if _diffs else 0
            # If there are actual misses, show "worst miss"; otherwise show "smallest beat"
            if misses > 0:
                _tail = T("commentary_gva_tail_miss", max_miss=f"{_min_val:+.1f}")
            else:
                _tail = T("commentary_gva_tail_smallest_beat", min_beat=f"{_min_val:+.1f}")
            st.markdown(f"""
            <div class="narrative-box">
            {T("commentary_gva", ticker=ticker, n=total, beats=beats, inrange=in_range, misses=misses,
               avg_diff=f"{avg_diff:+.1f}", max_beat=f"{_max_beat:+.1f}", tail=_tail)}
            </div>
            """, unsafe_allow_html=True)

    # ═══════════════════════════════════════════════════════════════
    # TAB 2: FY GUIDANCE WALK
    # ═══════════════════════════════════════════════════════════════
    with tabs[2]:
        st.markdown(f'<div class="section-header">{T("fy_walk_title")}</div>', unsafe_allow_html=True)
        st.markdown(f"""<div class="desc-text">
        {T("desc_fy_walk")}
        </div>""", unsafe_allow_html=True)

        fy_walk_rows = data["fy_walk_rows"]
        if not fy_walk_rows:
            st.info("No full-year guidance data found.")
        else:
            # Line chart per FY
            fy_keys = sorted(data["fy_walk"].keys())
            fig = go.Figure()
            colors = ["#e94560", "#1a1a2e", "#0f3460", "#548235", "#FFC000", "#7030A0"]
            for ci, fy in enumerate(fy_keys):
                revs = data["fy_walk"][fy]
                fig.add_trace(go.Scatter(
                    x=[r["source_q"] for r in revs],
                    y=[r["fy_rev"] for r in revs],
                    mode="lines+markers",
                    name=fy,
                    line=dict(color=colors[ci % len(colors)], width=3),
                    marker=dict(size=10),
                ))
            fig.update_layout(
                template="plotly_dark" if _dark else "plotly_white",
                height=400,
                margin=dict(t=30, b=40),
                yaxis_title="FY Revenue Guide Midpoint ($M)",
                legend=dict(orientation="h", y=1.1),
            )
            st.plotly_chart(fig, use_container_width=True)

            # Table (most recent first)
            fw_display = []
            for r in reversed(fy_walk_rows):
                fw_display.append({
                    "FY": r["fy_target"], "Rev #": r["revision_num"],
                    "Given At": r["source_q"], "Date": r["filing_date"],
                    "FY Rev ($M)": r["fy_rev"],
                    "Action": r["action"],
                    "Chg vs Prior ($M)": r["chg_vs_prior"],
                    "Chg vs Prior (%)": r["chg_vs_prior_pct"],
                    "Chg vs Initial (%)": r["chg_vs_initial_pct"],
                })
            df_fw = pd.DataFrame(fw_display)

            def style_action(row):
                a = row.get("Action", "")
                if a == "RAISE": return ["background-color: #d4edda; color: #155724"] * len(row)
                if a == "LOWER": return ["background-color: #f8d7da; color: #721c24"] * len(row)
                if a == "REAFFIRM": return ["background-color: #fff3cd; color: #856404"] * len(row)
                return ["background-color: #e8f4fd; color: #1a1a2e"] * len(row)

            st.dataframe(
                df_fw.style.apply(style_action, axis=1).format({
                    "FY Rev ($M)": "{:,.0f}",
                    "Chg vs Prior ($M)": "{:+,.0f}",
                    "Chg vs Prior (%)": "{:+.1f}%",
                    "Chg vs Initial (%)": "{:+.1f}%",
                }, na_rep="—"),
                use_container_width=True,
                height=min(len(fw_display) * 38 + 40, 500),
            )

            # Commentary
            _raise_n = sum(1 for r in fy_walk_rows if r["action"] == "RAISE")
            _lower_n = sum(1 for r in fy_walk_rows if r["action"] == "LOWER")
            _reaff_n = sum(1 for r in fy_walk_rows if r["action"] == "REAFFIRM")
            _avg_chg = f"{sum(total_raises)/len(total_raises):+.1f}" if total_raises else "0.0"
            st.markdown(f"""
            <div class="narrative-box">
            {T("commentary_fy_walk", n=len(data["fy_walk"]), ticker=ticker,
               raise_n=_raise_n, lower_n=_lower_n, reaff_n=_reaff_n, avg_chg=_avg_chg)}
            </div>
            """, unsafe_allow_html=True)

    # ═══════════════════════════════════════════════════════════════
    # TAB 3: CONSERVATISM SCORE
    # ═══════════════════════════════════════════════════════════════
    with tabs[3]:
        st.markdown(f'<div class="section-header">{T("conservatism_title")}</div>', unsafe_allow_html=True)
        st.markdown(f"""<div class="desc-text">
        {T("desc_conservatism")}<br>
        {T("desc_conservatism_bands")}
        <span style="background:#d4edda;padding:1px 6px;border-radius:4px;">66–100 {T("sandbagging")}</span>
        <span style="background:#fff3cd;padding:1px 6px;border-radius:4px;">50–66 {T("conservative")}</span>
        <span style="background:#e8e8e8;padding:1px 6px;border-radius:4px;">33–50 {T("straight_shooter")}</span>
        <span style="background:#f8d7da;padding:1px 6px;border-radius:4px;">0–33 {T("aggressive")}</span>.
        {T("desc_conservatism_tail")}
        </div>""", unsafe_allow_html=True)

        if not scores:
            st.info("No conservatism score data available.")
        else:
            # Chart
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=[s["fiscal_quarter"] for s in scores],
                y=[s["total_score"] for s in scores],
                mode="lines+markers",
                name="Total Score",
                line=dict(color="#1a1a2e", width=3),
                marker=dict(size=8),
            ))
            fig.add_trace(go.Scatter(
                x=[s["fiscal_quarter"] for s in scores],
                y=[s["rolling_4q"] for s in scores],
                mode="lines",
                name="Rolling 4Q Avg",
                line=dict(color="#e94560", width=2, dash="dash"),
            ))
            # Add threshold bands
            fig.add_hrect(y0=66, y1=100, fillcolor="#d4edda", opacity=0.15, line_width=0,
                         annotation_text="Sandbagging", annotation_position="top left")
            fig.add_hrect(y0=50, y1=66, fillcolor="#fff3cd", opacity=0.15, line_width=0,
                         annotation_text="Conservative", annotation_position="top left")
            fig.add_hrect(y0=33, y1=50, fillcolor="#e8e8e8", opacity=0.15, line_width=0,
                         annotation_text="Straight Shooter", annotation_position="top left")
            fig.add_hrect(y0=0, y1=33, fillcolor="#f8d7da", opacity=0.15, line_width=0,
                         annotation_text="Aggressive", annotation_position="top left")
            fig.update_layout(
                template="plotly_dark" if _dark else "plotly_white", height=450,
                margin=dict(t=30, b=40),
                yaxis=dict(range=[0, 105], title="Conservatism Score"),
                legend=dict(orientation="h", y=1.1),
            )
            st.plotly_chart(fig, use_container_width=True)

            # Interpretation
            _cs_label = T(_archetype_key)
            _cs_dir = T("commentary_cs_dir_more") if data["score_slope"] > 0.5 else T("commentary_cs_dir_less") if data["score_slope"] < -0.5 else T("commentary_cs_dir_stable")
            st.markdown(f"""
            <div class="narrative-box">
            <strong>{T("commentary_cs_avg")}</strong> {avg_score:.0f}/100 — <strong>{_cs_label}</strong><br>
            <strong>{T("commentary_cs_latest")}</strong> {scores[-1]["total_score"]}/100<br>
            <strong>{T("commentary_cs_trend")}</strong> {data["score_slope"]:+.1f} {T("pts_per_q")} ({_cs_dir})
            </div>
            """, unsafe_allow_html=True)

            # Table
            cs_display = [{
                "Quarter": s["fiscal_quarter"],
                "Beat (0-33)": s["beat_score"],
                "Width (0-33)": s["width_score"],
                "Consistency (0-33)": s["consistency_score"],
                "Total (0-100)": s["total_score"],
                "Rolling 4Q": s["rolling_4q"],
            } for s in reversed(scores)]
            st.dataframe(pd.DataFrame(cs_display), use_container_width=True,
                        height=min(len(cs_display) * 38 + 40, 400))

    # ═══════════════════════════════════════════════════════════════
    # TAB 4: MARKET REACTION
    # ═══════════════════════════════════════════════════════════════
    with tabs[4]:
        st.markdown(f'<div class="section-header">{T("market_title")}</div>', unsafe_allow_html=True)
        st.markdown(f"""<div class="desc-text">
        {T("desc_market")}
        </div>""", unsafe_allow_html=True)

        reactions = data["price_reactions"]
        if not reactions:
            st.info("No market reaction data available.")
        else:
            # Scatter plot
            valid = [p for p in reactions if p["rev_surprise_pct"] is not None and p["ret_1d"] is not None]
            if valid:
                fig = px.scatter(
                    x=[p["rev_surprise_pct"] for p in valid],
                    y=[p["ret_1d"] for p in valid],
                    color=[p["verdict"] for p in valid],
                    color_discrete_map={"BEAT": "#155724", "IN-RANGE": "#856404", "MISS": "#721c24"},
                    hover_name=[p["fiscal_quarter"] for p in valid],
                    labels={"x": "Revenue Surprise (%)", "y": "1-Day Stock Return (%)", "color": "Verdict"},
                    template="plotly_dark" if _dark else "plotly_white",
                )
                fig.update_traces(marker=dict(size=12, line=dict(width=1, color="white")))
                fig.update_layout(height=400, margin=dict(t=30, b=40))
                st.plotly_chart(fig, use_container_width=True)

            # Summary stats
            mc1, mc2, mc3 = st.columns(3)
            for col, verdict in zip([mc1, mc2, mc3], ["BEAT", "IN-RANGE", "MISS"]):
                rets = [p["ret_1d"] for p in reactions if p["verdict"] == verdict and p["ret_1d"] is not None]
                if rets:
                    col.metric(f"Avg 1D Return on {verdict}",
                             f"{sum(rets)/len(rets):+.1f}%",
                             f"n={len(rets)}")

            corr = data["price_corr"]
            if corr is not None:
                _eff_key = "strongly_priced_in" if abs(corr) < 0.3 else "partially_priced_in" if abs(corr) < 0.6 else "not_priced_in"
                st.markdown(f"""
                <div class="narrative-box">
                <strong>{T("commentary_mr_corr")}</strong> r = {corr:.2f}<br>
                {T("commentary_mr_eff", eff=T(_eff_key))}
                </div>
                """, unsafe_allow_html=True)

            # Table
            mr_display = [{
                "Quarter": p["fiscal_quarter"], "Date": p["earnings_date"],
                "Verdict": p["verdict"],
                "Surprise (%)": p["rev_surprise_pct"],
                "Pre-Close": p["pre_close"],
                "Post 1D": p["post_1d_close"],
                "1D Return (%)": p["ret_1d"],
                "3D Return (%)": p["ret_3d"],
            } for p in reversed(reactions)]
            st.dataframe(
                style_verdict_df(pd.DataFrame(mr_display)).format({
                    "Surprise (%)": "{:+.1f}%", "Pre-Close": "${:,.2f}",
                    "Post 1D": "${:,.2f}", "1D Return (%)": "{:+.1f}%",
                    "3D Return (%)": "{:+.1f}%",
                }, na_rep="—"),
                use_container_width=True,
                height=min(len(mr_display) * 38 + 40, 400),
            )

    # ═══════════════════════════════════════════════════════════════
    # TAB 5: SEASONAL PATTERNS
    # ═══════════════════════════════════════════════════════════════
    with tabs[5]:
        st.markdown(f'<div class="section-header">{T("seasonal_title")}</div>', unsafe_allow_html=True)
        st.markdown(f"""<div class="desc-text">
        {T("desc_seasonal")}
        </div>""", unsafe_allow_html=True)

        seasonal = data["seasonal_summary"]
        if not seasonal:
            st.info("No seasonal data available.")
        else:
            # Bar chart
            qs = sorted(seasonal.keys())
            avgs = [seasonal[q]["avg_surprise"] or 0 for q in qs]
            fig = go.Figure(go.Bar(
                x=qs, y=avgs,
                marker_color=["#155724" if v > 0 else "#721c24" for v in avgs],
                text=[f"{v:+.1f}%" for v in avgs],
                textposition="outside",
            ))
            fig.update_layout(
                template="plotly_dark" if _dark else "plotly_white", height=350,
                margin=dict(t=30, b=40),
                yaxis_title="Avg Surprise (%)",
                xaxis_title="Fiscal Quarter",
            )
            st.plotly_chart(fig, use_container_width=True)

            # Table
            sp_display = [{
                "Quarter": q,
                "N": seasonal[q]["n"],
                "Avg Surprise (%)": seasonal[q]["avg_surprise"],
                "Beat Rate (%)": seasonal[q]["beat_rate"],
                "Max Beat (%)": seasonal[q]["max_beat"],
                "Max Miss (%)": seasonal[q]["max_miss"],
            } for q in qs]
            st.dataframe(pd.DataFrame(sp_display).style.format({
                "Avg Surprise (%)": "{:+.1f}%", "Beat Rate (%)": "{:.0f}%",
                "Max Beat (%)": "{:+.1f}%", "Max Miss (%)": "{:+.1f}%",
            }, na_rep="—"), use_container_width=True)

            # Interpretation
            valid_seasonal = {k: v for k, v in seasonal.items() if v["avg_surprise"] is not None}
            if valid_seasonal:
                most_cons = max(valid_seasonal.items(), key=lambda x: x[1]["avg_surprise"])
                most_acc = min(valid_seasonal.items(), key=lambda x: abs(x[1]["avg_surprise"]))
                st.markdown(f"""
                <div class="narrative-box">
                <strong>{T("commentary_sp_cons")}</strong> {most_cons[0]} ({T("commentary_sp_detail")} {most_cons[1]['avg_surprise']:+.1f}%)<br>
                <strong>{T("commentary_sp_acc")}</strong> {most_acc[0]} ({T("commentary_sp_detail")} {most_acc[1]['avg_surprise']:+.1f}%)
                </div>
                """, unsafe_allow_html=True)

    # ═══════════════════════════════════════════════════════════════
    # TAB 6: METRIC ACCURACY
    # ═══════════════════════════════════════════════════════════════
    with tabs[6]:
        st.markdown(f'<div class="section-header">{T("metric_accuracy_title")}</div>', unsafe_allow_html=True)
        st.markdown(f"""<div class="desc-text">
        {T("desc_metric_accuracy")}
        </div>""", unsafe_allow_html=True)

        ms = data["metric_summary"]
        if not ms:
            st.info("No multi-metric data available.")
        else:
            # Grouped bar
            metrics_list = list(ms.keys())
            fig = go.Figure()
            fig.add_trace(go.Bar(
                name="Avg Surprise", x=metrics_list,
                y=[ms[m]["avg_surprise"] for m in metrics_list],
                marker_color="#7aa2d4" if _dark else "#1a1a2e",
            ))
            fig.add_trace(go.Bar(
                name="Avg |Error|", x=metrics_list,
                y=[ms[m]["avg_abs_error"] for m in metrics_list],
                marker_color="#e94560",
            ))
            fig.update_layout(
                template="plotly_dark" if _dark else "plotly_white", height=350, barmode="group",
                margin=dict(t=30, b=40),
                yaxis_title="Percentage",
                legend=dict(orientation="h", y=1.1),
            )
            st.plotly_chart(fig, use_container_width=True)

            # Table
            ma_display = [{
                "Metric": m,
                "N Quarters": ms[m]["n"],
                "Avg Surprise": f"{ms[m]['avg_surprise']:+.1f}",
                "Avg |Error|": f"{ms[m]['avg_abs_error']:.1f}",
                "Beat Rate (%)": ms[m]["beat_rate"],
                "Miss Rate (%)": ms[m]["miss_rate"],
            } for m in metrics_list]
            st.dataframe(pd.DataFrame(ma_display).style.format({
                "Beat Rate (%)": "{:.0f}%", "Miss Rate (%)": "{:.0f}%",
            }, na_rep="—"), use_container_width=True)

            most_cons = max(ms.items(), key=lambda x: x[1]["avg_surprise"])
            most_acc = min(ms.items(), key=lambda x: x[1]["avg_abs_error"])
            st.markdown(f"""
            <div class="narrative-box">
            <strong>{T("commentary_ma_cons")}</strong> {most_cons[0]} ({T("commentary_ma_avg")} {most_cons[1]['avg_surprise']:+.1f})<br>
            <strong>{T("commentary_ma_acc")}</strong> {most_acc[0]} ({T("commentary_ma_err")} {most_acc[1]['avg_abs_error']:.1f})
            </div>
            """, unsafe_allow_html=True)

else:
    # Landing page
    st.markdown(f"""
    <div style="text-align: center; padding: 4rem 2rem;">
        <h1 style="font-size: 2.5rem; color: var(--fg);">{T("landing_title")}</h1>
        <p style="font-size: 1.2rem; color: var(--fg-muted); max-width: 600px; margin: 1rem auto;">
            {T("landing_desc")}
        </p>
        <p style="font-size: 1rem; color: var(--fg-muted); margin-top: 2rem;">
            {T("landing_instruction")}
        </p>
    </div>
    """, unsafe_allow_html=True)
