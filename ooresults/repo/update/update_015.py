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


import logging
import sqlite3


VERSION = 15


def update(db: sqlite3.Connection) -> None:
    # add foreign keys
    # remove unique constraint (event_id, competitor_id) from entries

    c = db.cursor()
    try:
        c.execute("BEGIN EXCLUSIVE TRANSACTION")

        c.execute(
            """
            CREATE TABLE clubs_x (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE
            )""",
        )
        c.execute(
            """
            CREATE TABLE competitors_x (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                first_name TEXT NOT NULL,
                last_name TEXT NOT NULL,
                club_id INTEGER REFERENCES clubs_x(id),
                gender TEXT,
                year INTEGER,
                chip TEXT,
                UNIQUE (first_name, last_name)
            )""",
        )
        c.execute(
            """
            CREATE TABLE events_x (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                date TEXT,
                key TEXT UNIQUE,
                publish BOOL NOT NULL,
                series TEXT,
                fields BLOB NOT NULL,
                streaming_address TEXT,
                streaming_key TEXT,
                streaming_enabled BOOL
            )""",
        )
        c.execute(
            """
            CREATE TABLE courses_x (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id INTEGER NOT NULL REFERENCES events_x(id),
                name TEXT NOT NULL,
                length FLOAT,
                climb FLOAT,
                controls BLOB NOT NULL,
                UNIQUE (event_id, name)
            )""",
        )
        c.execute(
            """
            CREATE TABLE classes_x (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id INTEGER NOT NULL REFERENCES events_x(id),
                name TEXT NOT NULL,
                short_name TEXT,
                course_id INTEGER REFERENCES courses_x(id),
                params BLOB NOT NULL,
                UNIQUE (event_id, name)
            )""",
        )
        c.execute(
            """
            CREATE TABLE entries_x (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id INTEGER NOT NULL REFERENCES events_x(id),
                competitor_id INTEGER REFERENCES competitors_x(id),
                class_id INTEGER REFERENCES classes_x(id),
                club_id INTEGER REFERENCES clubs_x(id),
                not_competing BOOL NOT NULL,
                result BLOB NOT NULL,
                start BLOB NOT NULL,
                chip TEXT,
                fields BLOB NOT NULL
            )""",
        )

        c.execute("INSERT INTO clubs_x SELECT * FROM clubs")
        c.execute("INSERT INTO competitors_x SELECT * FROM competitors")
        c.execute("INSERT INTO events_x SELECT * FROM events")
        c.execute("INSERT INTO courses_x SELECT * FROM courses")
        c.execute("INSERT INTO classes_x SELECT * FROM classes")
        c.execute("INSERT INTO entries_x SELECT * FROM entries")

        c.execute("DROP TABLE entries")
        c.execute("DROP TABLE classes")
        c.execute("DROP TABLE courses")
        c.execute("DROP TABLE events")
        c.execute("DROP TABLE competitors")
        c.execute("DROP TABLE clubs")

        c.execute("ALTER TABLE clubs_x RENAME TO clubs")
        c.execute("ALTER TABLE competitors_x RENAME TO competitors")
        c.execute("ALTER TABLE events_x RENAME TO events")
        c.execute("ALTER TABLE courses_x RENAME TO courses")
        c.execute("ALTER TABLE classes_x RENAME TO classes")
        c.execute("ALTER TABLE entries_x RENAME TO entries")

        c.execute(
            """
            CREATE INDEX entries_idx1 ON entries(
                event_id,
                competitor_id
            )""",
        )

        # database integrity check
        c.execute("PRAGMA integrity_check")
        value = c.fetchone()
        assert list(value) == ["ok"], list(value)

        # foreign key check
        c.execute("PRAGMA foreign_key_check")
        value = c.fetchone()
        assert value is None, list(value)

        # version
        sql = "UPDATE version SET value=?"
        c.execute(sql, [VERSION])
        db.commit()

        # compress database
        c.execute("VACUUM")
        db.commit()

    except:
        logging.exception(f"Error during DB update to version {VERSION}")
        db.rollback()
        raise
    finally:
        c.close()
