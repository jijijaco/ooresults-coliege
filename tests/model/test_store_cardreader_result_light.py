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


def test_needs_assignment_on_unknown_chip(
    db: SqliteRepo,
    event_id: int,
    course_id: int,
    class_id: int,
):
    """No competitor registered with this chip; needs_assignment with entry_id and classes."""
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
    assert res["light_status"] == "needs_assignment"
    assert res["firstName"] is None
    assert res["lastName"] is None
    assert res["class"] is None
    assert "entry_id" in res
    assert isinstance(res["entry_id"], int)
    assert len(res["classes"]) == 1
    assert res["classes"][0]["name"] == "Elite"
    assert res["classes"][0]["id"] == class_id

    with db.transaction():
        entries = db.get_entries(event_id=event_id)
    assert len(entries) == 1
    assert entries[0].chip == "000000"
    assert entries[0].class_name is None


def test_needs_assignment_on_missing_punch(
    db: SqliteRepo,
    event_id: int,
    course_id: int,
    class_id: int,
    competitor: CompetitorType,
):
    """Competitor found but result has a missing punch; no unique class match → needs_assignment."""
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
    assert res["light_status"] == "needs_assignment"
    assert res["firstName"] == "Jane"
    assert res["lastName"] == "Doe"
    assert res["class"] is None
    assert "entry_id" in res
    assert isinstance(res["entry_id"], int)
    assert len(res["classes"]) == 1
    assert res["classes"][0]["name"] == "Elite"

    with db.transaction():
        entries = db.get_entries(event_id=event_id)
    assert len(entries) == 1
    assert entries[0].chip == CONTROL_CARD
    assert entries[0].class_name is None


def test_needs_assignment_on_multiple_matching_classes(
    db: SqliteRepo,
    event_id: int,
    course_id: int,
    class_id: int,
    competitor: CompetitorType,
):
    """Two classes both match the result; needs_assignment returned (ambiguous)."""
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
    assert res["light_status"] == "needs_assignment"
    assert res["firstName"] == "Jane"
    assert res["lastName"] == "Doe"
    assert res["class"] is None
    assert "entry_id" in res
    assert len(res["classes"]) == 2

    with db.transaction():
        entries = db.get_entries(event_id=event_id)
    assert len(entries) == 1
    assert entries[0].chip == CONTROL_CARD
    assert entries[0].class_name is None


def test_second_reading_creates_new_entry_and_preserves_old(
    db: SqliteRepo,
    event_id: int,
    course_id: int,
    class_id: int,
    competitor: CompetitorType,
):
    """Chip already has an entry; second reading creates a new unassigned entry and
    leaves the existing entry untouched."""
    # pre-populate an entry for person A (already registered)
    with db.transaction():
        existing_entry_id = db.add_entry(
            event_id=event_id,
            competitor_id=competitor.id,
            class_id=class_id,
            club_id=None,
            not_competing=False,
            chip=CONTROL_CARD,
            fields={},
            result=PersonRaceResult(status=ResultStatus.OK),
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
    assert res["light_status"] == "needs_assignment"
    assert res["firstName"] is None
    assert res["lastName"] is None
    assert res["class"] is None
    assert "entry_id" in res
    assert len(res["classes"]) == 1
    assert res["classes"][0]["name"] == "Elite"

    # Both entries must still exist: person A's entry + the new unassigned one
    with db.transaction():
        entries = db.get_entries(event_id=event_id)
    assert len(entries) == 2
    entry_ids = {e.id for e in entries}
    assert existing_entry_id in entry_ids
    new_entry_id = res["entry_id"]
    assert new_entry_id in entry_ids
    assert new_entry_id != existing_entry_id


@pytest.fixture
def competitor_2(db: SqliteRepo) -> CompetitorType:
    with db.transaction():
        competitor_id = db.add_competitor(
            first_name="John",
            last_name="Smith",
            club_id=None,
            gender="M",
            year=None,
            chip="0000",
        )
        return db.get_competitor(id=competitor_id)


# ---------------------------------------------------------------------------
# assign_entry_to_light_entry tests
# ---------------------------------------------------------------------------


def test_assign_entry_creates_entry_for_new_competitor(
    db: SqliteRepo,
    event_id: int,
    course_id: int,
    class_id: int,
):
    """No competitor with this name; an unassigned entry exists; competitor is created and
    entry is replaced with a registered one."""
    with db.transaction():
        unassigned_id = db.add_entry_result(
            event_id=event_id,
            chip=CONTROL_CARD,
            result=_ok_result(),
            start=PersonRaceStart(),
        )

    event, res = model.results.assign_entry_to_light_entry(
        event_key="4711",
        entry_id=unassigned_id,
        first_name="New",
        last_name="Person",
        class_id=class_id,
    )

    assert event.id == event_id
    assert res["light_status"] == "ok_registered"
    assert res["status"] == ResultStatus.OK
    assert res["firstName"] == "New"
    assert res["lastName"] == "Person"
    assert res["class"] == "Elite"
    assert res["error"] is None

    with db.transaction():
        entries = db.get_entries(event_id=event_id)
    assert len(entries) == 1
    assert entries[0].chip == CONTROL_CARD
    assert entries[0].class_name == "Elite"
    assert entries[0].first_name == "New"


def test_assign_entry_updates_chip_for_existing_competitor(
    db: SqliteRepo,
    event_id: int,
    course_id: int,
    class_id: int,
    competitor_2: CompetitorType,
):
    """Competitor exists with a different chip; unassigned entry exists with CONTROL_CARD;
    chip is updated on the competitor and entry is replaced with a registered one."""
    with db.transaction():
        unassigned_id = db.add_entry_result(
            event_id=event_id,
            chip=CONTROL_CARD,
            result=_ok_result(),
            start=PersonRaceStart(),
        )

    event, res = model.results.assign_entry_to_light_entry(
        event_key="4711",
        entry_id=unassigned_id,
        first_name="John",
        last_name="Smith",
        class_id=class_id,
    )

    assert event.id == event_id
    assert res["light_status"] == "ok_registered"
    assert res["firstName"] == "John"
    assert res["lastName"] == "Smith"
    assert res["class"] == "Elite"

    with db.transaction():
        updated = db.get_competitor(id=competitor_2.id)
    assert updated.chip == CONTROL_CARD

    with db.transaction():
        entries = db.get_entries(event_id=event_id)
    assert len(entries) == 1
    assert entries[0].chip == CONTROL_CARD
    assert entries[0].class_name == "Elite"


def test_assign_entry_only_deletes_targeted_entry(
    db: SqliteRepo,
    event_id: int,
    course_id: int,
    class_id: int,
    competitor: CompetitorType,
):
    """When the same chip has multiple entries (e.g. after second reading), assigning
    one entry_id must not touch the other entries for the same chip."""
    # Entry for person A (already registered)
    with db.transaction():
        person_a_entry_id = db.add_entry(
            event_id=event_id,
            competitor_id=competitor.id,
            class_id=class_id,
            club_id=None,
            not_competing=False,
            chip=CONTROL_CARD,
            fields={},
            result=PersonRaceResult(status=ResultStatus.OK),
            start=PersonRaceStart(),
        )
    # New unassigned entry from the second reading
    with db.transaction():
        second_entry_id = db.add_entry_result(
            event_id=event_id,
            chip=CONTROL_CARD,
            result=_ok_result(),
            start=PersonRaceStart(),
        )

    event, res = model.results.assign_entry_to_light_entry(
        event_key="4711",
        entry_id=second_entry_id,
        first_name="New",
        last_name="Runner",
        class_id=class_id,
    )

    assert res["light_status"] == "ok_registered"
    assert res["firstName"] == "New"

    with db.transaction():
        entries = db.get_entries(event_id=event_id)
    # person A's entry + the newly registered entry (second reading unassigned was replaced)
    assert len(entries) == 2
    entry_ids = {e.id for e in entries}
    assert person_a_entry_id in entry_ids
    # The unassigned second_entry_id was deleted and replaced by a new registered entry
    assert second_entry_id not in entry_ids


def test_assign_entry_computes_ok_result(
    db: SqliteRepo,
    event_id: int,
    course_id: int,
    class_id: int,
):
    """Assigning an entry with a complete punch set produces ResultStatus.OK."""
    with db.transaction():
        unassigned_id = db.add_entry_result(
            event_id=event_id,
            chip=CONTROL_CARD,
            result=_ok_result(),
            start=PersonRaceStart(),
        )

    event, res = model.results.assign_entry_to_light_entry(
        event_key="4711",
        entry_id=unassigned_id,
        first_name="Alice",
        last_name="Wonder",
        class_id=class_id,
    )

    assert res["light_status"] == "ok_registered"
    assert res["status"] == ResultStatus.OK
    assert res["time"] == t(s1, f1)
    assert res["missingControls"] == []
