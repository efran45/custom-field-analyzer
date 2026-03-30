"""
Jira Custom Field Analyzer
Connects to a Jira project and shows which custom fields are used most/least.
"""
import json
import requests
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from base64 import b64encode
from pathlib import Path

CONFIG_FILE = Path(__file__).parent / ".config.json"

# ── Colors ────────────────────────────────────────────────────────────────────
C_HIGH   = "#16a34a"
C_MED    = "#2563eb"
C_LOW    = "#d97706"
C_UNUSED = "#dc2626"

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Jira Field Analyzer",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
html, body, [class*="css"] { font-family: 'Inter', 'Segoe UI', sans-serif; }
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
.stApp { background-color: #f1f5f9; }

section[data-testid="stSidebar"] { background-color: #1e293b !important; }
section[data-testid="stSidebar"] * { color: #e2e8f0 !important; }
section[data-testid="stSidebar"] .stTextInput input,
section[data-testid="stSidebar"] .stSelectbox div {
    background: #334155; color: #f1f5f9 !important; border-color: #475569;
}
section[data-testid="stSidebar"] hr { border-color: #334155 !important; }

.kpi-card {
    background: #ffffff;
    border-radius: 14px;
    padding: 22px 18px 18px 18px;
    text-align: center;
    border-top: 4px solid var(--card-color, #2563eb);
    box-shadow: 0 2px 12px rgba(0,0,0,0.08);
    height: 130px;
    display: flex;
    flex-direction: column;
    justify-content: center;
}
.kpi-label {
    font-size: 0.72rem;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: #64748b;
    margin-bottom: 8px;
    font-weight: 600;
}
.kpi-value {
    font-size: 2.2rem;
    font-weight: 800;
    line-height: 1;
    color: var(--card-color, #1e293b);
}
.kpi-sub { font-size: 0.75rem; color: #94a3b8; margin-top: 6px; }

.section-header {
    background: linear-gradient(90deg, #e0f2fe 0%, transparent 100%);
    border-left: 4px solid #2563eb;
    padding: 10px 16px;
    border-radius: 0 8px 8px 0;
    margin: 24px 0 12px 0;
}
.section-title { font-size: 1.05rem; font-weight: 700; color: #1e293b; margin: 0; }
.section-sub   { font-size: 0.78rem; color: #64748b; margin: 2px 0 0 0; }
hr { border-color: #e2e8f0; }
</style>
""", unsafe_allow_html=True)


# ── Config persistence ────────────────────────────────────────────────────────
def load_config() -> dict:
    if CONFIG_FILE.exists():
        try:
            return json.loads(CONFIG_FILE.read_text())
        except Exception:
            return {}
    return {}


def save_config(cfg: dict):
    CONFIG_FILE.write_text(json.dumps(cfg, indent=2))


# ── Jira helpers ──────────────────────────────────────────────────────────────
def auth_headers(email: str, token: str) -> dict:
    creds = b64encode(f"{email}:{token}".encode()).decode()
    return {
        "Authorization": f"Basic {creds}",
        "Accept": "application/json",
    }


def jira_get(base_url: str, endpoint: str, headers: dict, params: dict = None):
    r = requests.get(f"{base_url.rstrip('/')}{endpoint}", headers=headers, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


@st.cache_data(ttl=300, show_spinner=False)
def fetch_projects(base_url: str, email: str, token: str) -> list[dict]:
    headers = auth_headers(email, token)
    data = jira_get(base_url, "/rest/api/3/project", headers)
    return sorted(data, key=lambda p: p["name"])


@st.cache_data(ttl=300, show_spinner=False)
def fetch_custom_fields(base_url: str, email: str, token: str) -> dict:
    """Return {field_id: field_name} for all custom fields."""
    headers = auth_headers(email, token)
    all_fields = jira_get(base_url, "/rest/api/3/field", headers)
    return {
        f["id"]: f.get("name", f["id"])
        for f in all_fields
        if f.get("custom", False) and f["id"].startswith("customfield_")
    }


def count_jql(base_url: str, headers: dict, jql: str) -> int:
    """Return the total number of issues matching a JQL query (fetches no issue data)."""
    r = requests.get(
        f"{base_url.rstrip('/')}/rest/api/3/search/jql",
        headers=headers,
        params={"jql": jql, "maxResults": 0, "fields": "id"},
        timeout=30,
    )
    if r.status_code in (400, 404):
        # Field may not support JQL filtering — treat as unknown
        return -1
    r.raise_for_status()
    return r.json().get("total", 0)


def analyze_fields(base_url: str, email: str, token: str,
                   project_key: str, custom_fields: dict) -> tuple[pd.DataFrame, int]:
    """
    For each custom field, ask Jira: how many tickets in this project
    have this field set? Uses maxResults=0 so no issue data is transferred.
    Returns (DataFrame, total_ticket_count).
    """
    headers = auth_headers(email, token)

    # Total tickets in project
    total = count_jql(base_url, headers, f"project = {project_key}")
    if total <= 0:
        return pd.DataFrame(), 0

    rows = []
    field_list = list(custom_fields.items())
    progress = st.progress(0, text="Analyzing fields…")

    for i, (fid, fname) in enumerate(field_list):
        # Extract numeric ID: customfield_10151 → 10151
        num_id = fid.replace("customfield_", "")
        jql = f"project = {project_key} AND cf[{num_id}] is not EMPTY"
        used = count_jql(base_url, headers, jql)

        rows.append({
            "field_id":   fid,
            "field_name": fname,
            "used":       max(used, 0),
            "unused":     total - max(used, 0) if used >= 0 else -1,
            "pct":        round(used / total * 100, 1) if used >= 0 else -1,
            "queryable":  used >= 0,
        })

        progress.progress((i + 1) / len(field_list),
                          text=f"Checking field {i + 1} of {len(field_list)}: {fname}")

    progress.empty()

    df = pd.DataFrame(rows)
    # Separate queryable vs non-queryable, sort queryable by usage
    queryable = df[df["queryable"]].sort_values("pct", ascending=False).reset_index(drop=True)
    queryable["rank"] = queryable.index + 1
    return queryable, total


def usage_color(pct: float) -> str:
    if pct >= 75: return C_HIGH
    if pct >= 25: return C_MED
    if pct > 0:   return C_LOW
    return C_UNUSED


def kpi_card(label: str, value: str, sub: str = "", color: str = C_MED):
    st.markdown(f"""
    <div class="kpi-card" style="--card-color:{color}">
        <div class="kpi-label">{label}</div>
        <div class="kpi-value">{value}</div>
        {"<div class='kpi-sub'>" + sub + "</div>" if sub else ""}
    </div>""", unsafe_allow_html=True)


def section_header(title: str, sub: str = ""):
    st.markdown(f"""
    <div class="section-header">
        <p class="section-title">{title}</p>
        {"<p class='section-sub'>" + sub + "</p>" if sub else ""}
    </div>""", unsafe_allow_html=True)


# ── Sidebar ────────────────────────────────────────────────────────────────────
saved = load_config()

with st.sidebar:
    st.markdown("## 🔍 Field Analyzer")
    st.markdown("---")
    st.markdown("### Jira Credentials")

    jira_url = st.text_input(
        "Jira URL",
        value=saved.get("jira_base_url", "https://yourcompany.atlassian.net"),
        placeholder="https://yourcompany.atlassian.net",
    )
    jira_email = st.text_input("Email", value=saved.get("jira_email", ""))
    jira_token = st.text_input(
        "API Token", type="password",
        help="https://id.atlassian.com/manage-profile/security/api-tokens",
    )

    connect_btn = st.button("Connect", use_container_width=True)

    st.markdown("---")

    # Project selector — only shown after a successful connection
    project_key = None
    if st.session_state.get("connected"):
        st.markdown("### Project")
        projects = st.session_state.get("projects", [])
        project_options = {f"{p['key']} — {p['name']}": p["key"] for p in projects}
        selected = st.selectbox("Select project", list(project_options.keys()))
        project_key = project_options[selected]

        st.markdown("---")
        run_btn = st.button("▶ Analyze Fields", type="primary", use_container_width=True)
    else:
        run_btn = False

    st.markdown("---")
    st.markdown("""
<div style='color:#94a3b8;font-size:0.72rem;line-height:1.7'>
Analyzes <b>custom fields only</b>.<br>
Usage = % of tickets where the field has any value set.<br><br>
<b style='color:#16a34a'>■</b> ≥75% used &nbsp;
<b style='color:#2563eb'>■</b> 25–74% &nbsp;
<b style='color:#d97706'>■</b> 1–24% &nbsp;
<b style='color:#dc2626'>■</b> 0% (never used)
</div>""", unsafe_allow_html=True)


# ── Main ───────────────────────────────────────────────────────────────────────
st.markdown("""
<h1 style='font-size:2rem;font-weight:800;margin-bottom:2px'>🔍 Jira Custom Field Analyzer</h1>
<p style='color:#64748b;margin-top:0'>Find which custom fields your team actually uses — and which are just noise.</p>
""", unsafe_allow_html=True)

# ── Connect ───────────────────────────────────────────────────────────────────
if connect_btn:
    if not jira_url or not jira_email or not jira_token:
        st.error("Please fill in all three credential fields.")
    else:
        with st.spinner("Connecting…"):
            try:
                headers = auth_headers(jira_email, jira_token)
                me = jira_get(jira_url, "/rest/api/3/myself", headers)
                projects = fetch_projects(jira_url, jira_email, jira_token)
                st.session_state["connected"] = True
                st.session_state["projects"]  = projects
                st.session_state["me"]        = me
                st.session_state["creds"]     = (jira_url, jira_email, jira_token)
                save_config({"jira_base_url": jira_url, "jira_email": jira_email})
                st.success(f"Connected as **{me.get('displayName', jira_email)}** — {len(projects)} projects found. Choose one in the sidebar.")
                st.rerun()
            except Exception as e:
                st.error(f"Connection failed: {e}")
                st.stop()

if not st.session_state.get("connected"):
    st.markdown("---")
    st.markdown("""
<div style='background:#fff;border-radius:14px;padding:32px;text-align:center;border:1px solid #e2e8f0;box-shadow:0 2px 12px rgba(0,0,0,0.06)'>
  <div style='font-size:3rem;margin-bottom:12px'>🔌</div>
  <h3 style='color:#1e293b;margin:0 0 8px 0'>Connect to Jira</h3>
  <p style='color:#64748b;margin:0'>Enter your credentials in the sidebar and click <b style='color:#2563eb'>Connect</b></p>
</div>""", unsafe_allow_html=True)
    st.stop()

if not run_btn:
    st.info("Select a project in the sidebar and click **▶ Analyze Fields**.")
    st.stop()

# ── Run analysis ──────────────────────────────────────────────────────────────
base_url, email, token = st.session_state["creds"]
me = st.session_state.get("me", {})

with st.spinner("Loading custom field definitions…"):
    try:
        custom_fields = fetch_custom_fields(base_url, email, token)
    except Exception as e:
        st.error(f"Failed to load fields: {e}")
        st.stop()

if not custom_fields:
    st.warning("No custom fields found in this Jira instance.")
    st.stop()

try:
    df, total_tickets = analyze_fields(base_url, email, token, project_key, custom_fields)
except Exception as e:
    st.error(f"Analysis failed: {e}")
    st.stop()

if df.empty or total_tickets == 0:
    st.warning(f"No tickets found in project **{project_key}**.")
    st.stop()
total_fields     = len(df)
fields_used      = len(df[df["pct"] > 0])
fields_never     = len(df[df["pct"] == 0])
fields_high      = len(df[df["pct"] >= 75])
avg_usage        = df["pct"].mean()

# ── Executive summary ─────────────────────────────────────────────────────────
st.markdown("---")
display_name = me.get("displayName", email)
st.markdown(f"""
<div style='display:flex;align-items:center;justify-content:space-between;margin-bottom:4px'>
  <h2 style='margin:0;font-size:1.3rem;color:#1e293b'>Results — <code>{project_key}</code></h2>
  <span style='color:#64748b;font-size:0.8rem'>
    {total_tickets:,} total tickets in project &nbsp;·&nbsp; connected as <b style='color:#2563eb'>{display_name}</b>
  </span>
</div>""", unsafe_allow_html=True)

c1, c2, c3, c4, c5 = st.columns(5)
with c1: kpi_card("Total Tickets", f"{total_tickets:,}", sub="in project", color=C_MED)
with c2: kpi_card("Custom Fields", f"{total_fields:,}", sub="in this instance", color=C_MED)
with c3: kpi_card("Fields in Use", f"{fields_used:,}", sub=f"{fields_used/total_fields*100:.0f}% of all fields", color=C_HIGH)
with c4: kpi_card("Never Used", f"{fields_never:,}", sub="0% usage in this project", color=C_UNUSED if fields_never else "#94a3b8")
with c5: kpi_card("Avg Usage", f"{avg_usage:.0f}%", sub="across all custom fields", color=C_MED)

st.markdown("<br>", unsafe_allow_html=True)

# ── Overview chart ────────────────────────────────────────────────────────────
section_header("Field Usage Overview", f"All {total_fields} custom fields ranked by % of tickets with a value set")

bar_colors = [usage_color(p) for p in df["pct"]]

fig_all = go.Figure(go.Bar(
    x=df["pct"],
    y=df["field_name"],
    orientation="h",
    marker_color=bar_colors,
    customdata=df[["used", "unused", "field_id"]].values,
    hovertemplate=(
        "<b>%{y}</b><br>"
        "Usage: %{x:.1f}%<br>"
        "Filled: %{customdata[0]:,} tickets<br>"
        "Empty: %{customdata[1]:,} tickets<br>"
        "Field ID: %{customdata[2]}<extra></extra>"
    ),
))
fig_all.update_layout(
    xaxis=dict(title="% of tickets with a value", range=[0, 100],
               ticksuffix="%", gridcolor="#e2e8f0", tickfont=dict(color="#64748b")),
    yaxis=dict(autorange="reversed", tickfont=dict(color="#1e293b", size=11)),
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    margin=dict(t=10, b=40, l=250, r=20),
    height=max(400, total_fields * 22),
    showlegend=False,
)
st.plotly_chart(fig_all, use_container_width=True, config={"displayModeBar": False}, key="overview_all")

# ── Top 15 / Bottom 15 ────────────────────────────────────────────────────────
col_top, col_bot = st.columns(2)

with col_top:
    section_header("Most Used Fields", "Top 15 by usage rate")
    top15 = df.head(15).sort_values("pct")
    fig_top = go.Figure(go.Bar(
        x=top15["pct"], y=top15["field_name"], orientation="h",
        marker_color=[usage_color(p) for p in top15["pct"]],
        hovertemplate="<b>%{y}</b><br>%{x:.1f}%<extra></extra>",
    ))
    fig_top.update_layout(
        xaxis=dict(title=None, range=[0, 100], ticksuffix="%",
                   gridcolor="#e2e8f0", tickfont=dict(color="#64748b")),
        yaxis=dict(tickfont=dict(color="#1e293b", size=11)),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(t=10, b=20, l=200, r=10),
        height=380,
        showlegend=False,
    )
    st.plotly_chart(fig_top, use_container_width=True, config={"displayModeBar": False}, key="top15")

with col_bot:
    section_header("Least Used Fields", "Bottom 15 — candidates for cleanup")
    bot15 = df.tail(15).sort_values("pct", ascending=False)
    fig_bot = go.Figure(go.Bar(
        x=bot15["pct"], y=bot15["field_name"], orientation="h",
        marker_color=[usage_color(p) for p in bot15["pct"]],
        hovertemplate="<b>%{y}</b><br>%{x:.1f}%<extra></extra>",
    ))
    fig_bot.update_layout(
        xaxis=dict(title=None, range=[0, 100], ticksuffix="%",
                   gridcolor="#e2e8f0", tickfont=dict(color="#64748b")),
        yaxis=dict(tickfont=dict(color="#1e293b", size=11)),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(t=10, b=20, l=200, r=10),
        height=380,
        showlegend=False,
    )
    st.plotly_chart(fig_bot, use_container_width=True, config={"displayModeBar": False}, key="bot15")

# ── Usage breakdown donut ─────────────────────────────────────────────────────
section_header("Usage Distribution", "How fields break down across usage tiers")

tier_counts = {
    "High (≥75%)":   fields_high,
    "Medium (25–74%)": len(df[(df["pct"] >= 25) & (df["pct"] < 75)]),
    "Low (1–24%)":   len(df[(df["pct"] > 0)  & (df["pct"] < 25)]),
    "Unused (0%)":   fields_never,
}
pie_col, insight_col = st.columns([1, 2])
with pie_col:
    fig_pie = go.Figure(go.Pie(
        labels=list(tier_counts.keys()),
        values=list(tier_counts.values()),
        hole=0.55,
        marker=dict(colors=[C_HIGH, C_MED, C_LOW, C_UNUSED],
                    line=dict(color="#f1f5f9", width=2)),
        textinfo="percent+label",
        textfont=dict(size=11),
        hovertemplate="%{label}: %{value} fields<extra></extra>",
        sort=False,
    ))
    fig_pie.update_layout(
        showlegend=False,
        paper_bgcolor="rgba(0,0,0,0)",
        margin=dict(t=10, b=10, l=10, r=10),
        height=260,
    )
    st.plotly_chart(fig_pie, use_container_width=True, config={"displayModeBar": False}, key="tier_pie")

with insight_col:
    st.markdown("<br>", unsafe_allow_html=True)
    for tier, count in tier_counts.items():
        color = [C_HIGH, C_MED, C_LOW, C_UNUSED][list(tier_counts.keys()).index(tier)]
        pct_of_fields = count / total_fields * 100 if total_fields else 0
        st.markdown(f"""
        <div style='display:flex;align-items:center;gap:12px;margin-bottom:14px'>
          <div style='width:14px;height:14px;border-radius:3px;background:{color};flex-shrink:0'></div>
          <div>
            <span style='font-weight:700;color:#1e293b'>{tier}</span>
            <span style='color:#64748b;margin-left:8px'>{count} fields ({pct_of_fields:.0f}%)</span>
          </div>
        </div>""", unsafe_allow_html=True)

# ── Never-used fields callout ─────────────────────────────────────────────────
never_used = df[df["pct"] == 0]
if not never_used.empty:
    st.markdown("<br>", unsafe_allow_html=True)
    section_header(
        f"🗑️ {len(never_used)} Fields Never Used in {project_key}",
        "These fields had no values set across all analyzed tickets — candidates for removal or hiding"
    )
    st.dataframe(
        never_used[["field_name", "field_id"]].rename(columns={"field_name": "Field Name", "field_id": "Field ID"}),
        use_container_width=True,
        hide_index=True,
    )

# ── Full data table ───────────────────────────────────────────────────────────
st.markdown("<br>", unsafe_allow_html=True)
section_header("Full Field Table", "All custom fields — sortable")

display_df = df[["rank", "field_name", "field_id", "used", "unused", "pct"]].rename(columns={
    "rank":       "Rank",
    "field_name": "Field Name",
    "field_id":   "Field ID",
    "used":       "Tickets with Value",
    "unused":     "Tickets without Value",
    "pct":        "Usage %",
})
st.dataframe(display_df, use_container_width=True, hide_index=True,
             column_config={"Usage %": st.column_config.ProgressColumn(
                 "Usage %", min_value=0, max_value=100, format="%.1f%%"
             )})

# ── Download ──────────────────────────────────────────────────────────────────
csv = display_df.to_csv(index=False)
st.download_button(
    label="⬇ Download CSV",
    data=csv,
    file_name=f"{project_key}_custom_field_usage.csv",
    mime="text/csv",
)
