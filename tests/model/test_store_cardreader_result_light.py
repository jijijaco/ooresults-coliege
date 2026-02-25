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


import datetime
from collections.abc import Iterator

import pytest

from ooresults import model
from ooresults.otypes.class_params import ClassParams
from ooresults.otypes.competitor_type import CompetitorType
from ooresults.otypes.result_type import CardReaderMessage
from ooresults.otypes.result_type import PersonRaceResult
from ooresults.otypes.result_type import ResultStatus
from ooresults.otypes.result_type import SplitTime
from ooresults.otypes.result_type import SpStatus
from ooresults.otypes.start_type import PersonRaceStart
from ooresults.repo.sqlite_repo import SqliteRepo


entry_time = datetime.datetime(2015, 1, 1, 13, 38, 59, tzinfo=datetime.timezone.utc)
s1 = datetime.datetime(2015, 1, 1, 12, 38, 59, tzinfo=datetime.timezone.utc)
c1 = datetime.datetime(2015, 1, 1, 12, 39, 1, tzinfo=datetime.timezone.utc)
c2 = datetime.datetime(2015, 1, 1, 12, 39, 3, tzinfo=datetime.timezone.utc)
c3 = datetime.datetime(2015, 1, 1, 12, 39, 5, tzinfo=datetime.timezone.utc)
f1 = datetime.datetime(2015, 1, 1, 12, 39, 7, tzinfo=datetime.timezone.utc)

CONTROL_CARD = "9876"


def t(a: datetime.datetime, b: datetime.datetime) -> int:
    diff = b.replace(microsecond=0) - a.replace(microsecond=0)
    return int(diff.total_seconds())


@pytest.fixture
def db() -> Iterator[SqliteRepo]:
    model.db = SqliteRepo(db=":memory:")
    yield model.db
    model.db.close()


@pytest.fixture
def event_id(db: SqliteRepo) -> int:
    with db.transaction():
        return db.add_event(
            name="Light Event",
            date=datetime.date(year=2015, month=1, day=1),
            key="4711",
            publish=False,
            series=None,
            fields=[],
            light=True,
        )


@pytest.fixture
def course_id(db: SqliteRepo, event_id: int) -> int:
    with db.transaction():
        return db.add_course(
            event_id=event_id,
            name="Bahn A",
            length=4500,
            climb=90,
            controls=["101", "102", "103"],
        )


@pytest.fixture
def class_id(db: SqliteRepo, event_id: int, course_id: int) -> int:
    with db.transaction():
        return db.add_class(
            event_id=event_id,
            name="Elite",
            short_name="E",
            course_id=course_id,
            params=ClassParams(),
        )


@pytest.fixture
def competitor(db: SqliteRepo) -> CompetitorType:
    with db.transaction():
        competitor_id = db.add_competitor(
            first_name="Jane",
            last_name="Doe",
            club_id=None,
            gender="F",
            year=1990,
            chip=CONTROL_CARD,
        )
        return db.get_competitor(id=competitor_id)


def _ok_result() -> PersonRaceResult:
    """A result with all three controls punched and valid start/finish."""
    return PersonRaceResult(
        status=ResultStatus.FINISHED,
        punched_start_time=s1,
        punched_finish_time=f1,
        si_punched_start_time=s1,
        si_punched_finish_time=f1,
        time=None,
        split_times=[
            SplitTime(
                control_code="101",
                punch_time=c1,
                si_punch_time=c1,
                status=SpStatus.ADDITIONAL,
            ),
            SplitTime(
                control_code="102",
                punch_time=c2,
                si_punch_time=c2,
                status=SpStatus.ADDITIONAL,
            ),
            SplitTime(
                control_code="103",
                punch_time=c3,
                si_punch_time=c3,
                status=SpStatus.ADDITIONAL,
            ),
        ],
    )


def _missing_punch_result() -> PersonRaceResult:
    """A result with control 102 missing."""
    return PersonRaceResult(
        status=ResultStatus.FINISHED,
        punched_start_time=s1,
        punched_finish_time=f1,
        si_punched_start_time=s1,
        si_punched_finish_time=f1,
        time=None,
        split_times=[
            SplitTime(
                control_code="101",
                punch_time=c1,
                si_punch_time=c1,
                status=SpStatus.ADDITIONAL,
            ),
            SplitTime(
                control_code="103",
                punch_time=c3,
                si_punch_time=c3,
                status=SpStatus.ADDITIONAL,
            ),
        ],
    )


def test_auto_register_on_ok_result(
    db: SqliteRepo,
    event_id: int,
    course_id: int,
    class_id: int,
    competitor: CompetitorType,
):
    """Competitor chip matches a unique course; entry is auto-created."""
    item = CardReaderMessage(
        entry_type="cardRead",
        entry_time=entry_time,
        control_card=CONTROL_CARD,
        result=_ok_result(),
    )

    status, event, res = model.results.store_cardreader_result(
        event_key="4711", item=item
    )

    assert status == "cardRead"
    assert event.id == event_id
    assert res["light_status"] == "ok_registered"
    assert res["status"] == ResultStatus.OK
    assert res["firstName"] == "Jane"
    assert res["lastName"] == "Doe"
    assert res["class"] == "Elite"
    assert res["error"] is None
    assert res["missingControls"] == []
    assert res["time"] == t(s1, f1)

    with db.transaction():
        entries = db.get_entries(event_id=event_id)
    assert len(entries) == 1
    assert entries[0].chip == CONTROL_CARD
    assert entries[0].class_name == "Elite"
    assert entries[0].first_name == "Jane"


def test_unassigned_on_unknown_chip(
    db: SqliteRepo,
    event_id: int,
    course_id: int,
    class_id: int,
):
    """No competitor registered with this chip; unassigned entry is created."""
    item = CardReaderMessage(
        entry_type="cardRead",
        entry_time=entry_time,
        control_card="000000",
        result=_ok_result(),
    )

    status, event, res = model.results.store_cardreader_result(
        event_key="4711", item=item
    )

    assert status == "cardRead"
    assert event.id == event_id
    assert res["light_status"] == "unassigned"
    assert res["error"] == "Control card unknown"
    assert res["firstName"] is None
    assert res["class"] is None

    with db.transaction():
        entries = db.get_entries(event_id=event_id)
    assert len(entries) == 1
    assert entries[0].chip == "000000"
    assert entries[0].class_name is None


def test_unassigned_on_missing_punch(
    db: SqliteRepo,
    event_id: int,
    course_id: int,
    class_id: int,
    competitor: CompetitorType,
):
    """Competitor found but result has a missing punch; no matching class."""
    item = CardReaderMessage(
        entry_type="cardRead",
        entry_time=entry_time,
        control_card=CONTROL_CARD,
        result=_missing_punch_result(),
    )

    status, event, res = model.results.store_cardreader_result(
        event_key="4711", item=item
    )

    assert status == "cardRead"
    assert event.id == event_id
    assert res["light_status"] == "unassigned"
    assert res["error"] == "No unique matching course"
    assert res["firstName"] is None
    assert res["class"] is None

    with db.transaction():
        entries = db.get_entries(event_id=event_id)
    assert len(entries) == 1
    assert entries[0].chip == CONTROL_CARD
    assert entries[0].class_name is None


def test_unassigned_on_multiple_matching_classes(
    db: SqliteRepo,
    event_id: int,
    course_id: int,
    class_id: int,
    competitor: CompetitorType,
):
    """Two classes both match the result; entry is unassigned (ambiguous)."""
    with db.transaction():
        db.add_class(
            event_id=event_id,
            name="Open",
            short_name="O",
            course_id=course_id,
            params=ClassParams(),
        )

    item = CardReaderMessage(
        entry_type="cardRead",
        entry_time=entry_time,
        control_card=CONTROL_CARD,
        result=_ok_result(),
    )

    status, event, res = model.results.store_cardreader_result(
        event_key="4711", item=item
    )

    assert status == "cardRead"
    assert event.id == event_id
    assert res["light_status"] == "unassigned"
    assert res["error"] == "No unique matching course"
    assert res["class"] is None

    with db.transaction():
        entries = db.get_entries(event_id=event_id)
    assert len(entries) == 1
    assert entries[0].chip == CONTROL_CARD
    assert entries[0].class_name is None


def test_second_reading_if_entry_already_exists(
    db: SqliteRepo,
    event_id: int,
    course_id: int,
    class_id: int,
    competitor: CompetitorType,
):
    """Chip already has an entry in the event; second reading is detected."""
    # pre-populate an entry with the same chip
    with db.transaction():
        db.add_entry_result(
            event_id=event_id,
            chip=CONTROL_CARD,
            result=PersonRaceResult(status=ResultStatus.FINISHED),
            start=PersonRaceStart(),
        )

    item = CardReaderMessage(
        entry_type="cardRead",
        entry_time=entry_time,
        control_card=CONTROL_CARD,
        result=_ok_result(),
    )

    status, event, res = model.results.store_cardreader_result(
        event_key="4711", item=item
    )

    assert status == "cardRead"
    assert event.id == event_id
    assert res["light_status"] == "second_reading"
    assert res["firstName"] is None
    assert res["class"] is None

    # no new entry should have been added
    with db.transaction():
        entries = db.get_entries(event_id=event_id)
    assert len(entries) == 1
