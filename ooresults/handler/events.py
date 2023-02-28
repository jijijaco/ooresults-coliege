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
import logging
import pathlib

import web
from web.utils import Storage

from ooresults.handler import model
from ooresults.repo.repo import ConstraintError


templates = pathlib.Path(__file__).resolve().parent.parent / "templates"
render = web.template.render(templates, globals={"str": str})


def update():
    return render.events_table(model.get_events())


class Update:
    def POST(self):
        """Update data"""
        return update()


class Add:
    def POST(self):
        """Add or edit entry"""
        data = web.input()
        print(data)
        try:
            fields = data.fields.split(",") if data.fields != "" else []
            fields = [f.strip() for f in fields]
            if data.id == "":
                model.add_event(
                    name=data.name,
                    date=datetime.datetime.strptime(data.date, "%Y-%m-%d").date(),
                    key=data.key if data.key != "" else None,
                    publish=data.publish == "yes",
                    series=data.series if data.series != "" else None,
                    fields=fields,
                )
            else:
                model.update_event(
                    id=int(data.id),
                    name=data.name,
                    date=datetime.datetime.strptime(data.date, "%Y-%m-%d").date(),
                    key=data.key if data.key != "" else None,
                    publish=data.publish == "yes",
                    series=data.series if data.series != "" else None,
                    fields=fields,
                )
        except ConstraintError as e:
            raise web.conflict(str(e))
        except:
            logging.exception("Internal server error")
            raise

        return update()


class Delete:
    def POST(self):
        """Delete entry"""
        data = web.input()
        model.delete_event(int(data.id))
        return update()


class FillEditForm:
    def POST(self):
        """Query data to fill add or edit form"""
        data = web.input()
        print(data)
        if data.id == "":
            event = Storage(
                {
                    "id": "",
                    "name": "",
                    "date": "",
                    "key": "",
                    "publish": False,
                    "series": "",
                    "fields": "",
                }
            )
        else:
            event = model.get_event(int(data.id))[0]

        return render.add_event(event)
