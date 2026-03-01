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
import ssl
import time

import pytest
import websockets
from selenium import webdriver

from webtests.pageobjects.classes import ClassPage
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
CLASS = "Runners"
FIRST_NAME = "Jan"
LAST_NAME = "Meier"


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
    Tabs(page=page).select(text="Courses")
    course_page = CoursePage(page=page)
    dialog = course_page.actions.add()
    dialog.enter_values(name=COURSE, controls=CONTROL)
    dialog.submit()

    # 3. Classes tab — create class assigned to course
    Tabs(page=page).select(text="Classes")
    class_page = ClassPage(page=page)
    dialog = class_page.actions.add()
    dialog.enter_values(name=CLASS, course=COURSE)
    dialog.submit()

    # 4. Competitors tab — create competitor with chip
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
LOG_CLASS = "LogRunners"
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
    Tabs(page=page).select(text="Courses")
    course_page = CoursePage(page=page)
    dialog = course_page.actions.add()
    dialog.enter_values(name=LOG_COURSE, controls=LOG_CONTROLS)
    dialog.submit()

    # 3. Classes tab — create class assigned to course
    Tabs(page=page).select(text="Classes")
    class_page = ClassPage(page=page)
    dialog = class_page.actions.add()
    dialog.enter_values(name=LOG_CLASS, course=LOG_COURSE)
    dialog.submit()

    # 4. Competitors tab — create 6 competitors
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
