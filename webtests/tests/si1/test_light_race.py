# Copyright (C) 2022 Rainer Garus
#
# This file is part of the ooresults Python package, a software to
# compute results of orienteering events.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.


import asyncio
import bz2
import datetime
import json
import pathlib
import re
import ssl
import time

import pytest
import requests
import urllib3
import websockets
from selenium import webdriver
from selenium.webdriver.common.by import By

from webtests.pageobjects.competitors import CompetitorPage
from webtests.pageobjects.courses import CoursePage
from webtests.pageobjects.entries import EntryPage
from webtests.pageobjects.events import EventPage
from webtests.pageobjects.tabs import Tabs


EVENT_NAME = "Light Race Test"
EVENT_KEY = "test-light-key"
CHIP = "87654321"
CONTROL = "101"
COURSE = "TestCourse"
CLASS = COURSE  # auto-created by add_course for light events
FIRST_NAME = "Jan"
LAST_NAME = "Meier"

UNKNOWN_CHIP = "99999999"   # not registered to any competitor
ASSIGN_FIRST = "Alice"
ASSIGN_LAST = "Test"


def send_card_read(event_key: str, chip: str, controls: list[str]) -> None:
    pad = 8 - len(controls)
    code = ["Check", "Start"] + controls + [""] * pad + ["Finish"]
    times = (
        ["09:00:00", "10:00:00"]
        + ["10:01:00"] * len(controls)
        + [""] * pad
        + ["10:05:00"]
    )

    msg = json.dumps({"key": event_key, "code": code, "time": times, "card": chip})

    async def _send():
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        async with websockets.connect("wss://localhost:8081/demo", ssl=ctx) as ws:
            await ws.send(msg)

    asyncio.run(_send())


@pytest.fixture(scope="module")
def setup_light_event(page: webdriver.Remote) -> None:
    """Create light event + course + class + competitor; teardown deletes the event."""

    # 1. Events tab — delete stale data, create light event
    Tabs(page=page).select(text="Events")
    event_page = EventPage(page=page)
    event_page.delete_events()
    dialog = event_page.actions.add()
    dialog.enter_values(name=EVENT_NAME, date="2026-02-28", key=EVENT_KEY, light=True)
    dialog.submit()
    event_page.table.select_row(2)  # select the new event

    # 2. Courses tab — create course with single control
    # (adding a course to a light event auto-creates a matching class)
    Tabs(page=page).select(text="Courses")
    course_page = CoursePage(page=page)
    dialog = course_page.actions.add()
    dialog.enter_values(name=COURSE, controls=CONTROL)
    dialog.submit()

    # 3. Competitors tab — create competitor with chip
    Tabs(page=page).select(text="Competitors")
    comp_page = CompetitorPage(page=page)
    dialog = comp_page.actions.add()
    dialog.enter_values(first_name=FIRST_NAME, last_name=LAST_NAME, chip=CHIP)
    dialog.submit()

    yield

    # Teardown
    Tabs(page=page).select(text="Events")
    event_page = EventPage(page=page)
    event_page.delete_events()


@pytest.fixture
def entry_page_clean(page: webdriver.Remote, setup_light_event: None) -> EntryPage:
    """Navigate to Entries tab; delete any existing entries before each test."""
    Tabs(page=page).select(text="Entries")
    ep = EntryPage(page=page)
    ep.delete_entries()
    return ep


def test_auto_register_on_valid_card_read(
    entry_page_clean: EntryPage,
) -> None:
    """
    Valid card read on a light event → entry auto-created with OK status.

    Setup: light event, course with control 101, class Runners, competitor Jan Meier (chip 87654321).
    Action: simulate card read with chip 87654321 punching control 101.
    Expected: entries table shows 1 entry: Jan Meier, class Runners, status OK.
    """
    send_card_read(event_key=EVENT_KEY, chip=CHIP, controls=[CONTROL])

    # The entries table does not auto-refresh; poll by clicking Reload until the
    # entry appears (server processes the card read asynchronously).
    deadline = time.monotonic() + 10
    while True:
        entry_page_clean.actions.reload()
        time.sleep(0.5)  # wait for XHR to complete
        if entry_page_clean.table.nr_of_rows() > 1:
            break
        if time.monotonic() > deadline:
            pytest.fail("Entry did not appear within 10 s after card read")

    assert entry_page_clean.table.nr_of_rows() == 2  # 1 header + 1 data row
    row = entry_page_clean.table.row(i=2)  # first data row
    assert row[1] == FIRST_NAME  # First name
    assert row[2] == LAST_NAME  # Last name
    assert row[5] == CHIP  # Chip
    assert row[7] == CLASS  # Class
    assert row[10] == "OK"  # Status


def test_si2_needs_assignment_form(
    page: webdriver.Remote,
    entry_page_clean: EntryPage,
) -> None:
    """
    Unknown chip on a light event → /si2 shows yellow assignment form;
    filling it in and clicking Assign creates the entry.

    Setup:  light event with one course (control 101) and one competitor (Jan Meier, 87654321).
    Action: send card read for unknown chip 99999999 via /demo WebSocket;
            open /si2 in a second browser window;
            wait for the yellow form;
            type first/last name, click Assign.
    Expected: form disappears; Entries table shows chip 99999999 with class TestCourse,
              first name Alice, last name Test.
    """
    # 1. Discover event_id from the /si1 page HTML
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    r = requests.get("https://localhost:8080/si1", auth=("admin", "admin"), verify=False)
    m = re.search(r'var event_id = "(\d+)"', r.text)
    assert m, "Could not find event_id in /si1 response"
    event_id = m.group(1)

    # 2. Open /si2 in a new browser window
    original_window = page.current_window_handle
    page.execute_script("window.open('');")
    page.switch_to.window(page.window_handles[-1])
    page.get(f"https://admin:admin@localhost:8080/si2?id={event_id}")

    # 3. Wait for WebSocket to connect (messages table must be present)
    deadline = time.monotonic() + 15
    while True:
        try:
            page.find_element(By.ID, "si2.messages")
            break
        except Exception:
            pass
        time.sleep(0.5)
        if time.monotonic() > deadline:
            pytest.fail("/si2 page did not connect within 15 s")

    # 4. Send a card read with an unknown chip → server emits needs_assignment
    send_card_read(event_key=EVENT_KEY, chip=UNKNOWN_CHIP, controls=[CONTROL])

    # 5. Wait for the yellow assignment form to appear
    deadline = time.monotonic() + 15
    while True:
        try:
            page.find_element(By.ID, "si2.firstName")
            break
        except Exception:
            pass
        time.sleep(0.5)
        if time.monotonic() > deadline:
            pytest.fail("needs_assignment form did not appear on /si2 within 15 s")

    # 6. Fill in name (class dropdown already shows CLASS — only one class exists)
    first_input = page.find_element(By.ID, "si2.firstName")
    first_input.clear()
    first_input.send_keys(ASSIGN_FIRST)
    last_input = page.find_element(By.ID, "si2.lastName")
    last_input.clear()
    last_input.send_keys(ASSIGN_LAST)

    # 7. Click Assign → browser sends assignEntry JSON over WebSocket
    page.find_element(By.XPATH, "//button[text()='Assign']").click()

    # 8. Wait for yellow form to disappear (server re-renders page without pending_assignment)
    deadline = time.monotonic() + 10
    while True:
        if not page.find_elements(By.ID, "si2.firstName"):
            break
        time.sleep(0.5)
        if time.monotonic() > deadline:
            pytest.fail("Assignment form did not disappear within 10 s after Assign click")

    # 9. Close the /si2 window and return to the admin page
    page.close()
    page.switch_to.window(original_window)

    # 10. Poll the Entries table until the assigned entry is visible
    deadline = time.monotonic() + 10
    while True:
        entry_page_clean.actions.reload()
        time.sleep(0.5)
        for i in range(1, entry_page_clean.table.nr_of_rows() + 1):
            row = entry_page_clean.table.row(i=i)
            if len(row) > 7 and row[5] == UNKNOWN_CHIP and row[7] == CLASS:
                assert row[1] == ASSIGN_FIRST
                assert row[2] == ASSIGN_LAST
                return
        if time.monotonic() > deadline:
            pytest.fail(
                f"Assigned entry for chip {UNKNOWN_CHIP} did not appear within 10 s"
            )


# ---------------------------------------------------------------------------
# Log-file replay test
# ---------------------------------------------------------------------------

LOG_FILE = (
    pathlib.Path(__file__).resolve().parents[3]
    / "docs"
    / "user"
    / "data"
    / "cardreader-2023-01-15.log"
)
LOG_EVENT_NAME = "Log Race Test"
LOG_EVENT_KEY = "log-race-key"
LOG_COURSE = "LogCourse"
LOG_CLASS = LOG_COURSE  # auto-created by add_course for light events
LOG_CONTROLS = "121-124-122-123"
LOG_COMPETITORS = [
    ("7379879", "Alice", "Smith"),
    ("7509749", "Bob", "Jones"),  # missing punch 122 → unassigned
    ("7223344", "Carol", "White"),
    ("7076815", "David", "Brown"),
    ("7579050", "Eve", "Green"),
    ("219403", "Frank", "Black"),
]
CHIP_MISSING_PUNCH = "7509749"


def send_log_file(event_key: str, log_path: pathlib.Path) -> None:
    async def _send():
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        headers = {
            "Content-Type": "application/octet-stream",
            "X-Event-Key": event_key,
            "X-Suffix": ".json",
        }
        async with websockets.connect(
            "wss://localhost:8081/cardreader", ssl=ctx, additional_headers=headers
        ) as ws:
            # 1. announce reader connected (matches real reader protocol)
            connected = {
                "entryType": "readerConnected",
                "entryTime": datetime.datetime.now().astimezone().isoformat(),
            }
            await ws.send(bz2.compress(json.dumps(connected).encode()))
            await ws.recv()

            # 2. send each log entry
            for line in log_path.read_text().splitlines():
                if not line.strip():
                    continue
                await ws.send(bz2.compress(line.encode()))
                await ws.recv()  # wait for server ack before sending next

    asyncio.run(_send())


@pytest.fixture(scope="module")
def setup_log_race_event(page: webdriver.Remote) -> None:
    """Create light event + course + class + 6 competitors; teardown deletes the event."""

    # 1. Events tab — delete stale data, create light event
    Tabs(page=page).select(text="Events")
    event_page = EventPage(page=page)
    event_page.delete_events()
    dialog = event_page.actions.add()
    dialog.enter_values(name=LOG_EVENT_NAME, date="2023-01-15", key=LOG_EVENT_KEY, light=True)
    dialog.submit()
    event_page.table.select_row(2)  # select the new event

    # 2. Courses tab — create course with four controls
    # (adding a course to a light event auto-creates a matching class)
    Tabs(page=page).select(text="Courses")
    course_page = CoursePage(page=page)
    dialog = course_page.actions.add()
    dialog.enter_values(name=LOG_COURSE, controls=LOG_CONTROLS)
    dialog.submit()

    # 3. Competitors tab — create 6 competitors
    Tabs(page=page).select(text="Competitors")
    comp_page = CompetitorPage(page=page)
    for chip, first_name, last_name in LOG_COMPETITORS:
        dialog = comp_page.actions.add()
        dialog.enter_values(first_name=first_name, last_name=last_name, chip=chip)
        dialog.submit()

    yield

    # Teardown
    Tabs(page=page).select(text="Events")
    event_page = EventPage(page=page)
    event_page.delete_events()


@pytest.fixture
def log_entry_page(page: webdriver.Remote, setup_log_race_event: None) -> EntryPage:
    """Navigate to Entries tab; delete any existing entries before the test."""
    Tabs(page=page).select(text="Entries")
    ep = EntryPage(page=page)
    ep.delete_entries()
    return ep


def test_log_file_replay(log_entry_page: EntryPage) -> None:
    """
    Replay cardreader log file through /cardreader WebSocket → 6 entries created.

    Setup: light event with course 121-124-122-123, class LogRunners, 6 competitors.
    Action: replay cardreader-2023-01-15.log via /cardreader WebSocket.
    Expected:
      - 6 entries auto-created (one per card read in the log)
      - 5 entries have status OK (all four controls punched)
      - 1 entry (chip 7509749, missing punch 122) has empty class column
    """
    send_log_file(LOG_EVENT_KEY, LOG_FILE)

    deadline = time.monotonic() + 20
    while True:
        log_entry_page.actions.reload()
        time.sleep(0.5)
        if log_entry_page.table.nr_of_rows() >= 8:
            break
        if time.monotonic() > deadline:
            pytest.fail("Entries did not appear within 20 s after log file replay")

    # 2 group-header rows (LogRunners, Unassigned) + 6 data rows = 8
    assert log_entry_page.table.nr_of_rows() == 8

    ok_count = 0
    missing_punch_class = None
    for i in range(1, 9):
        row = log_entry_page.table.row(i=i)
        if len(row) == 1:  # group header row — skip
            continue
        if row[10] == "OK":
            ok_count += 1
        if row[5] == CHIP_MISSING_PUNCH:
            missing_punch_class = row[7]

    assert ok_count == 5
    assert missing_punch_class == ""
