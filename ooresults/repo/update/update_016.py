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


VERSION = 16


def update(db: sqlite3.Connection) -> None:
    # add light column to events table

    c = db.cursor()
    try:
        c.execute("BEGIN EXCLUSIVE TRANSACTION")

        c.execute("ALTER TABLE events ADD COLUMN light INTEGER DEFAULT 0")

        # version
        sql = "UPDATE version SET value=?"
        c.execute(sql, [VERSION])
        db.commit()

    except:
        logging.exception(f"Error during DB update to version {VERSION}")
        db.rollback()
        raise
    finally:
        c.close()