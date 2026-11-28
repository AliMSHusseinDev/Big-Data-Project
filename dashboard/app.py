import pandas as pd
import plotly.express as px
import streamlit as st
import pyodbc
from dashboard.data import control_state, day_data, with_times, read
from dashboard.monitor import monitor
from simulator.control import command
from simulator.kitchen import CONFIG, STATIONS

st.set_page_config(page_title="Kitchen Monitor", page_icon="🍽️", layout="wide")
PAGES = [
    "Overview",
    "Live Orders",
    "Kitchen Stations",
    "Analytics",
    "SLA Alerts",
    "Simulation Control",
    "Data Pipeline Monitor",
    "End-of-Day Analysis",
]
st.sidebar.title("Kitchen Monitor")
page = st.sidebar.radio("Workspace", PAGES)
st.title(page)


def act(action, state, settings=None):
    if not command(action, state.session_id, state.status, settings):
        st.toast("Another control changed the session. Showing current state.")
    st.rerun()


def controls(state):
    choices = {
        "Ready": ("Start Simulation", "Start"),
        "Running": ("Pause Simulation", "Pause"),
        "Paused": ("Resume Simulation", "Resume"),
        "Completed": ("Start New Day", "Start"),
    }
    label, action = choices.get(state.status, ("Closing…", ""))
    left, right = st.columns(2)
    if left.button(label, type="primary", disabled=not action, use_container_width=True):
        act(action, state)
    if right.button(
        "End Simulation", disabled=state.status not in ("Running", "Paused"), use_container_width=True
    ):
        act("End", state)
    with st.expander("Simulation settings", expanded=page == "Simulation Control"):
        modes = list(CONFIG["arrival_seconds"])
        speeds = [1, 2, 5, 10]
        with st.form("settings"):
            scenario = st.selectbox("Traffic scenario", modes, index=modes.index(state.scenario))
            speed = st.select_slider(
                "Simulation speed", options=speeds, value=int(state.speed), format_func=lambda s: f"{s}×"
            )
            rate = st.slider(
                "Customer arrival rate multiplier", 0.5, 2.0, float(state.arrival_factor), 0.1
            )
            if st.form_submit_button("Apply settings"):
                act("Settings", state, (scenario, speed, rate))
        if st.button("Send invalid demo record", disabled=state.status != "Running"):
            act("Invalid demo", state)
        st.caption(
            "Each new day sends one separate test record. Open Data Pipeline Monitor to see why NiFi rejected it."
        )


def chart(frame, kind, x, y, title, color=None, labels=None):
    if frame.empty:
        return
    figure = kind(
        frame,
        x=x,
        y=y,
        color=color,
        title=title,
        labels=labels,
        color_discrete_sequence=["#63C6AD", "#F4A261", "#82A9D1", "#D98FA1", "#C2B88C"],
    )
    figure.update_layout(margin=dict(l=4, r=8, t=48, b=6), height=305)
    if y == "waiting":
        peak = int(frame[y].max())
        figure.update_yaxes(range=[0, max(1, peak) * 1.08], dtick=max(1, (peak + 5) // 6))
        if kind == px.line:
            figure.update_traces(mode="lines+markers", line_shape="hv")
    st.plotly_chart(figure, use_container_width=True)


def stations(metrics):
    config = pd.DataFrame(
        [{"station_id": s["id"], "station": s["name"], "capacity": s["capacity"]} for s in STATIONS]
    )
    frame = config.merge(metrics.drop(columns="session_id"), on="station_id", how="left")
    for key in ("waiting", "preparing", "completed", "violations"):
        frame[key] = pd.to_numeric(frame[key]).fillna(0).astype(int)
    frame["utilization_pct"] = (100 * frame.preparing / frame.capacity).round()
    frame["load"] = [
        "Queued" if w else "At capacity" if p == c else "Available"
        for w, p, c in zip(frame.waiting, frame.preparing, frame.capacity)
    ]
    return frame


def metrics(data):
    orders, items = data["Orders"], data["OrderItems"]
    done = items[items.status == "READY"]
    alerts = data["SLAAlerts"]
    cards = [
        ("Orders", len(orders)),
        ("Completed orders", (orders.status == "READY").sum()),
        ("Menu items", len(items)),
        ("Waiting", (items.status == "PLACED").sum()),
        ("Preparing", (items.status == "PREPARING").sum()),
        ("Completed items", len(done)),
        ("Avg prep · sec", round(done.prep_seconds.mean()) if done.prep_seconds.count() else "—"),
        ("Total SLA violations", len(alerts)),
        ("Active alerts", (alerts.status == "Active").sum()),
        ("Resolved alerts", (alerts.status == "Resolved").sum()),
    ]
    for i, (name, value) in enumerate(cards):
        if i % 5 == 0:
            cols = st.columns(5)
        cols[i % 5].metric(name, value, border=True)
    if len(orders):
        affected = items.loc[items.item_id.isin(alerts.item_id), "order_id"].nunique()
        st.caption(
            f"{affected} of {len(orders)} orders have at least one late item ({affected / len(orders):.1%}). "
            "Each late item counts as one SLA violation."
        )


def order_table(items):
    cols = st.columns(4)
    match = cols[0].text_input("Order ID contains")
    items = items[items.order_id.str.contains(match, regex=False, na=False)] if match else items
    for col, key in zip(cols[1:], ["station_id", "category", "status"]):
        selected = col.multiselect(key.replace("_", " ").title(), sorted(items[key].unique()))
        if selected:
            items = items[items[key].isin(selected)]
    items = with_times(items.drop(columns="session_id")).sort_values("placed_at", ascending=False)
    items["sla_status"] = [
        "Over SLA" if pd.notna(p) and p > s else "Within limit" if pd.notna(p) else "Waiting"
        for p, s in zip(items.prep_seconds, items.sla_seconds)
    ]
    style = {"READY": "color:#63C6AD", "PREPARING": "color:#F4A261", "PLACED": "color:#82A9D1"}
    st.dataframe(
        items.style.map(style.get, subset=["status"]), hide_index=True, use_container_width=True
    )


def analytics(data, station):
    category = data["CategoryMetrics"]
    chart(category, px.bar, "category", "avg_prep", "Average completed prep · seconds")
    chart(
        station,
        px.bar,
        "station",
        "waiting",
        "Waiting by station",
        labels={"station": "Kitchen station", "waiting": "Menu items waiting"},
    )
    chart(station, px.bar, "station", "preparing", "Current preparation workload")
    chart(category, px.bar, "category", "violations", "SLA violations by category")
    chart(category, px.bar, "category", "completed", "Completed items by category")
    history = with_times(data["MetricHistory"]).sort_values("sample_time")
    chart(
        history,
        px.line,
        "sample_time",
        "waiting",
        "Observed queue length over time",
        "station_id",
        labels={
            "sample_time": "Simulated time (UTC)",
            "waiting": "Menu items waiting",
            "station_id": "Kitchen station",
        },
    )
    if not history.empty:
        st.caption(
            "Each point counts menu items waiting to start preparation at that station. A value of 2 means two items in line."
        )


def alerts(data):
    frame = data["SLAAlerts"].merge(
        data["OrderItems"][
            ["item_id", "order_id", "item_name", "station_id", "prep_seconds", "sla_seconds"]
        ],
        on="item_id",
    )
    return with_times(frame.drop(columns="session_id")).sort_values("alert_time", ascending=False)


def render():
    state = control_state()
    clock = (
        pd.to_datetime(state.sim_time, unit="s", utc=True)
        if pd.notna(state.sim_time)
        else "Awaiting opening"
    )
    st.caption(f"{state.status}   ·   {clock}   ·   {state.scenario}   ·   {state.speed}×")
    if page in ("Simulation Control", "Overview"):
        controls(state)
    if page == "Simulation Control":
        return
    sessions = read(
        "SELECT session_id,opened_at,closed_at,status FROM SimulationSessions ORDER BY opened_at DESC,session_id"
    )
    if page == "End-of-Day Analysis":
        sessions = sessions[sessions.status == "Completed"]
    if sessions.empty:
        if page == "Data Pipeline Monitor":
            monitor(None)
            return
        st.info(
            "End-of-day results appear after Spark confirms completion."
            if page == "End-of-Day Analysis"
            else "Start a simulation to see kitchen activity."
        )
        return
    dates = pd.to_datetime(sessions.opened_at, unit="s", utc=True)
    labels = {key: f"{date:%B} {date.day}, {date.year}" for key, date in zip(sessions.session_id, dates)}
    if page in (
        "End-of-Day Analysis",
        "Live Orders",
        "Analytics",
        "SLA Alerts",
        "Data Pipeline Monitor",
    ):
        ids = sessions.session_id.tolist()
        session = st.selectbox(
            "Business day",
            ids,
            index=ids.index(state.session_id) if state.session_id in ids else 0,
            format_func=labels.__getitem__,
        )
    else:
        session = state.session_id
        if session not in labels:
            st.info("Start a simulation to see kitchen activity.")
            return
        st.caption(f"Business day · {labels[session]}")
    if page == "Data Pipeline Monitor":
        monitor(session)
        return
    data = day_data(session)
    station = stations(data["StationMetrics"])
    if page == "Overview":
        metrics(data)
        chart(station, px.bar, "station", "preparing", "Station workload")
        st.subheader("Recent orders")
        st.dataframe(
            with_times(data["Orders"].drop(columns="session_id"))
            .sort_values("placed_at", ascending=False)
            .head(10),
            hide_index=True,
        )
        st.subheader("Recent alerts")
        st.dataframe(alerts(data).head(8), hide_index=True)
    elif page == "Live Orders":
        order_table(data["OrderItems"])
    elif page == "Kitchen Stations":
        st.dataframe(station, hide_index=True, use_container_width=True)
        chart(station, px.bar, "station", "utilization_pct", "Occupied preparation capacity · %")
    elif page == "SLA Alerts":
        frame = alerts(data)
        status = st.radio("Alert state", ["All", "Active", "Resolved"], horizontal=True)
        st.dataframe(
            frame if status == "All" else frame[frame.status == status],
            hide_index=True,
            use_container_width=True,
        )
    elif page == "Analytics":
        analytics(data, station)
    elif page == "End-of-Day Analysis":
        record = sessions[sessions.session_id == session].iloc[0]
        st.write(
            f"Service window · {pd.to_datetime(record.opened_at, unit='s', utc=True)} to {pd.to_datetime(record.closed_at, unit='s', utc=True)}"
        )
        metrics(data)
        items = data["OrderItems"]
        total = len(data["SLAAlerts"])
        busiest = station.loc[station.completed.idxmax(), "station"] if len(items) else "—"
        wait = items.wait_seconds.mean()
        st.write(f"Busiest by completed items · {busiest}")
        hours = with_times(data["Orders"]).placed_at.dropna().dt.floor("h").value_counts()
        if not hours.empty:
            peaks = ", ".join(
                t.strftime("%H:%M") for t in hours[hours == hours.max()].index.sort_values()
            )
            st.write(f"Peak arrival hours (UTC) · {peaks} · {hours.max()} orders per peak hour")
        st.write(f"SLA violations · {total} ({100 * total / max(1, len(items)):.1f}% of items)")
        st.write(
            f"Average wait · {wait:.1f} simulated seconds" if pd.notna(wait) else "Average wait · —"
        )
        peaks = data["MetricHistory"].groupby("station_id").waiting.max().rename("observed_peak")
        st.dataframe(station.join(peaks, on="station_id").fillna({"observed_peak": 0}), hide_index=True)
        analytics(data, station)


@st.fragment(run_every="4s")
def refresh():
    try:
        render()
    except pyodbc.Error:
        st.error("SQL Server is unavailable. Check the sql and schema service logs.")


refresh()
