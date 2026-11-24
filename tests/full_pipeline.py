"""Run inside the dashboard of an isolated kitchen-e2e Compose project."""

import time
from pathlib import Path
from streamlit.testing.v1 import AppTest
from dashboard.data import control_state, day_data, read
from simulator.control import command


def wait_for(check, seconds):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if check():
            return
        time.sleep(1)
    raise AssertionError(f"Condition timed out after {seconds} seconds")


def press(action, settings=None):
    state = control_state()
    assert command(action, state.session_id, state.status, settings)


if __name__ == "__main__":
    before = control_state()
    assert before.status in ("Completed", "Ready"), "Use an idle, isolated test stack"
    press("Settings", ("Kitchen Delay", 10, 2))
    press("Start")
    session = control_state().session_id
    wait_for(lambda: control_state().generated >= 5, 30)
    press("Invalid demo")
    press("Pause")
    frozen = control_state().sim_time
    time.sleep(2)
    assert control_state().sim_time == frozen
    press("Resume")
    wait_for(lambda: control_state().sim_time >= frozen + 10, 20)
    press("Pause")
    press("End")
    wait_for(lambda: control_state().status == "Completed", 420)
    data = day_data(session)
    assert len(data["Orders"]) and (data["Orders"].status == "READY").all()
    assert (data["OrderItems"].status == "READY").all()
    assert data["StationMetrics"].completed.sum() == len(data["OrderItems"])
    row = read(
        "SELECT expected_events,generated FROM SimulationSessions WHERE session_id=?", session
    ).iloc[0]
    count = read("SELECT COUNT(*) n FROM ProcessedEvents WHERE session_id=?", session).iloc[0, 0]
    assert row.expected_events == row.generated == count
    app = AppTest.from_file(str(Path("/app/dashboard/app.py")), default_timeout=25).run()
    for page in [
        "Overview",
        "Live Orders",
        "Kitchen Stations",
        "Analytics",
        "SLA Alerts",
        "Data Pipeline Monitor",
        "End-of-Day Analysis",
    ]:
        app.sidebar.radio[0].set_value(page).run()
        assert not app.exception, (page, app.exception)
    press("Start")
    next_day = control_state()
    assert next_day.session_id != session
    last_close = read("SELECT closed_at FROM SimulationSessions WHERE session_id=?", session).iloc[0, 0]
    assert next_day.sim_time > last_close
    assert len(day_data(session)["OrderItems"]) == len(data["OrderItems"])
    press("End")
    wait_for(lambda: control_state().status == "Completed", 420)
    print(
        "PASS: controls, final-event barrier, database results, dashboard pages, independent day and history."
    )
