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


@st.cache_data(ttl=300, show_spinner=False)
def fetch_screen_field_map(base_url: str, email: str, token: str,
                           project_key: str, sample_issue_key: str) -> tuple[dict, str | None]:
    """
    Return ({field_id: [screen_names]}, error_message).
    Uses createmeta (Create screen) and editmeta (Edit screen) — no admin needed.
    """
    headers = auth_headers(email, token)
    base = base_url.rstrip("/")
    field_screen_map: dict[str, set] = {}

    # ── Create screen ─────────────────────────────────────────────────────────
    # Get issue types for this project
    r = requests.get(f"{base}/rest/api/3/issue/createmeta/{project_key}/issuetypes",
                     headers=headers, params={"maxResults": 50}, timeout=30)
    if r.ok:
        issue_types = r.json().get("issueTypes", r.json().get("values", []))
        for it in issue_types:
            it_id = it.get("id")
            if not it_id:
                continue
            r2 = requests.get(
                f"{base}/rest/api/3/issue/createmeta/{project_key}/issuetypes/{it_id}",
                headers=headers, params={"maxResults": 200}, timeout=30)
            if not r2.ok:
                continue
            fields = r2.json().get("fields", r2.json().get("values", []))
            # fields can be a list or a dict keyed by field id
            if isinstance(fields, dict):
                fids = [fid for fid in fields if fid.startswith("customfield_")]
            else:
                fids = [f.get("fieldId", f.get("id", "")) for f in fields
                        if f.get("fieldId", f.get("id", "")).startswith("customfield_")]
            for fid in fids:
                field_screen_map.setdefault(fid, set()).add("Create Screen")

    # ── Edit screen ───────────────────────────────────────────────────────────
    if sample_issue_key:
        r = requests.get(f"{base}/rest/api/3/issue/{sample_issue_key}/editmeta",
                         headers=headers, timeout=30)
        if r.ok:
            fields = r.json().get("fields", {})
            for fid in fields:
                if fid.startswith("customfield_"):
                    field_screen_map.setdefault(fid, set()).add("Edit Screen")

    if not field_screen_map:
        return {}, "Could not retrieve screen field data from createmeta or editmeta."

    return {k: sorted(v) for k, v in field_screen_map.items()}, None


def fetch_issues_cursor(base_url: str, headers: dict, project_key: str,
                        max_issues: int) -> tuple[list, dict]:
    """
    Fetch issues using /rest/api/3/search/jql cursor pagination.
    Returns (issues, debug_info).
    """
    url = f"{base_url.rstrip('/')}/rest/api/3/search/jql"
    issues = []
    next_token = None
    debug = {"url": url, "status_code": None, "response_keys": [], "raw": "", "batches": 0}

    progress = st.progress(0, text="Fetching tickets…")

    while len(issues) < max_issues:
        params = {
            "jql": f"project = {project_key} ORDER BY created DESC",
            "maxResults": min(100, max_issues - len(issues)),
            "fields": "*all",
        }
        if next_token:
            params["nextPageToken"] = next_token

        try:
            r = requests.get(url, headers=headers, params=params, timeout=30)
            debug["status_code"] = r.status_code
            debug["raw"] = r.text[:800]
            r.raise_for_status()
            data = r.json()
            debug["response_keys"] = list(data.keys())
            debug["batches"] += 1
        except Exception as e:
            debug["raw"] = f"Exception: {e}"
            break

        batch = data.get("issues", [])
        issues.extend(batch)
        # Store a sample issue key for editmeta lookup
        if batch and not st.session_state.get("sample_issue_key"):
            st.session_state["sample_issue_key"] = batch[0]["key"]

        if data.get("isLast", True) or not batch:
            break
        next_token = data.get("nextPageToken")

        pct = min(len(issues) / max_issues, 1.0)
        progress.progress(pct, text=f"Fetched {len(issues):,} tickets…")

    progress.empty()
    return issues, debug


def is_empty(value) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    if isinstance(value, list):
        return len(value) == 0
    if isinstance(value, dict):
        inner = list(value.values())
        return all(v is None or v == "" for v in inner) if inner else True
    return False


def analyze_fields(base_url: str, email: str, token: str,
                   project_key: str, custom_fields: dict,
                   max_issues: int) -> tuple[pd.DataFrame, int, list]:
    """
    Fetch a sample of tickets, then count how many have each custom field set.
    Returns (DataFrame, total_fetched, debug_log).
    """
    headers = auth_headers(email, token)
    debug_log = []

    issues, dbg = fetch_issues_cursor(base_url, headers, project_key, max_issues)
    first_issue_fields = list(issues[0].get("fields", {}).keys())[:20] if issues else []
    custom_in_first = [k for k in first_issue_fields if k.startswith("customfield_")]
    dbg["issues_fetched"] = len(issues)
    dbg["first_issue_field_keys_sample"] = first_issue_fields
    dbg["custom_fields_in_first_issue"] = custom_in_first
    debug_log.append({"label": "Issue fetch", **dbg})

    if not issues:
        return pd.DataFrame(), 0, debug_log

    total = len(issues)

    # Only analyze fields whose keys actually appear in this project's issue payloads.
    # Fields on the project's screens are always present as keys (even if null);
    # fields not on any screen are absent entirely.
    project_field_ids = {
        key for issue in issues
        for key in issue.get("fields", {}).keys()
        if key.startswith("customfield_")
    }
    debug_log[0]["project_field_ids_found"] = len(project_field_ids)

    rows = []
    for fid in project_field_ids:
        fname = custom_fields.get(fid, fid)  # fall back to ID if name unknown
        used = sum(
            1 for issue in issues
            if not is_empty(issue.get("fields", {}).get(fid))
        )
        rows.append({
            "field_id":   fid,
            "field_name": fname,
            "used":       used,
            "unused":     total - used,
            "pct":        round(used / total * 100, 1),
        })

    df = (pd.DataFrame(rows)
            .sort_values("pct", ascending=False)
            .reset_index(drop=True))
    df["rank"] = df.index + 1
    return df, total, debug_log


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
        st.markdown("### Options")
        max_issues = st.slider("Tickets to sample", 50, 10000, 500, step=50)
        st.markdown("---")
        run_btn = st.button("▶ Analyze Fields", type="primary", use_container_width=True)
    else:
        run_btn = False
        max_issues = 200

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

debug_log = []
df = pd.DataFrame()
total_tickets = 0
analysis_error = None

try:
    df, total_tickets, debug_log = analyze_fields(base_url, email, token, project_key, custom_fields, max_issues)
except Exception as e:
    analysis_error = str(e)

with st.expander("🐛 Debug info", expanded=False):
    if analysis_error:
        st.error(f"Analysis exception: {analysis_error}")
    if not debug_log:
        st.warning("No debug entries collected — analysis may have crashed before any queries ran.")
    for entry in debug_log:
        st.markdown(f"**{entry['label']}**")
        st.json({k: v for k, v in entry.items() if k != "label"})

if analysis_error:
    st.stop()

if df.empty or total_tickets == 0:
    st.warning(f"No tickets found in project **{project_key}**. Check the debug info above.")
    st.stop()

# Fetch screen → field mapping using createmeta/editmeta (no admin needed)
sample_issue_key = st.session_state.get("sample_issue_key", "")
with st.spinner("Loading screen configuration…"):
    screen_map, screen_error = fetch_screen_field_map(base_url, email, token, project_key, sample_issue_key)

if screen_error:
    st.info(f"ℹ️ Screen breakdown unavailable: {screen_error}")

# Add screens column to df
if screen_map:
    df["screens"] = df["field_id"].map(lambda fid: ", ".join(screen_map.get(fid, ["Unknown"])))
else:
    df["screens"] = "—"
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
with c1: kpi_card("Tickets Sampled", f"{total_tickets:,}", sub="most recent", color=C_MED)
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

# ── By screen ─────────────────────────────────────────────────────────────────
if screen_map:
    st.markdown("<br>", unsafe_allow_html=True)
    section_header("Usage by Screen", "Fields grouped by which screen they appear on")

    all_screens = sorted({s for screens in screen_map.values() for s in screens})
    screen_tabs = st.tabs(all_screens)

    for tab, screen_name in zip(screen_tabs, all_screens):
        with tab:
            screen_df = df[df["screens"].str.contains(screen_name, na=False)].copy()
            if screen_df.empty:
                st.info(f"No custom fields found on {screen_name}.")
                continue

            # Mini bar chart
            screen_df_sorted = screen_df.sort_values("pct", ascending=True)
            fig = go.Figure(go.Bar(
                x=screen_df_sorted["pct"],
                y=screen_df_sorted["field_name"],
                orientation="h",
                marker_color=[usage_color(p) for p in screen_df_sorted["pct"]],
                hovertemplate="<b>%{y}</b><br>%{x:.1f}%<extra></extra>",
            ))
            fig.update_layout(
                xaxis=dict(title=None, range=[0, 100], ticksuffix="%",
                           gridcolor="#e2e8f0", tickfont=dict(color="#64748b")),
                yaxis=dict(tickfont=dict(color="#1e293b", size=11)),
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                margin=dict(t=10, b=20, l=220, r=10),
                height=max(300, len(screen_df) * 24),
                showlegend=False,
            )
            st.plotly_chart(fig, use_container_width=True,
                            config={"displayModeBar": False},
                            key=f"screen_chart_{screen_name.replace(' ', '_')}")

            screen_display = screen_df[["rank", "field_name", "field_id", "used", "unused", "pct"]].rename(columns={
                "rank": "Rank", "field_name": "Field Name", "field_id": "Field ID",
                "used": "With Value", "unused": "Without Value", "pct": "Usage %",
            })
            st.dataframe(screen_display, use_container_width=True, hide_index=True,
                         column_config={"Usage %": st.column_config.ProgressColumn(
                             "Usage %", min_value=0, max_value=100, format="%.1f%%")})

# ── Full data table ───────────────────────────────────────────────────────────
st.markdown("<br>", unsafe_allow_html=True)
section_header("Full Field Table", "All custom fields — sortable")

display_df = df[["rank", "field_name", "field_id", "used", "unused", "pct", "screens"]].rename(columns={
    "rank":       "Rank",
    "field_name": "Field Name",
    "field_id":   "Field ID",
    "used":       "Tickets with Value",
    "unused":     "Tickets without Value",
    "pct":        "Usage %",
    "screens":    "Screens",
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
