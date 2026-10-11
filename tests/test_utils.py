import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from decimal import Decimal
from typing import Any
from unittest import TestCase, main
from uuid import UUID
from lilota.utils import to_json


@dataclass
class Item:
    name: str
    price: Decimal


@dataclass
class Order:
    id: UUID
    items: list[Item]


class Customer:
    def __init__(self, name: str, since: date) -> None:
        self.name = name
        self.since = since

    def as_dict(self) -> dict[str, Any]:
        return {"name": self.name, "since": self.since}


class UtilsTestCase(TestCase):
    def test_to_json___with_json_values___should_return_values_unchanged(self):
        for value in [None, True, 42, 1.5, "done", [1, 2, 3], {"a": [1, {"b": None}]}]:
            with self.subTest(value=value):
                self.assertEqual(to_json(value), value)

    def test_to_json___with_tuple___should_return_list(self):
        self.assertEqual(to_json((1, "two", (3, 4))), [1, "two", [3, 4]])

    def test_to_json___with_datetime_date_and_time___should_return_iso_format(self):
        value = {
            "datetime": datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc),
            "date": date(2026, 1, 2),
            "time": time(3, 4, 5),
        }

        self.assertEqual(
            to_json(value),
            {
                "datetime": "2026-01-02T03:04:05+00:00",
                "date": "2026-01-02",
                "time": "03:04:05",
            },
        )

    def test_to_json___with_uuid_and_decimal___should_return_text(self):
        value = [UUID("0a1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4d"), Decimal("9.99")]

        self.assertEqual(
            to_json(value), ["0a1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4d", "9.99"]
        )

    def test_to_json___with_dataclass_in_dataclass___should_return_dictionaries(self):
        order = Order(
            id=UUID("0a1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4d"),
            items=[Item(name="Book", price=Decimal("9.99"))],
        )

        self.assertEqual(
            to_json(order),
            {
                "id": "0a1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4d",
                "items": [{"name": "Book", "price": "9.99"}],
            },
        )

    def test_to_json___with_model_protocol___should_return_dictionary(self):
        customer = Customer(name="Ada", since=date(2026, 1, 2))

        self.assertEqual(to_json(customer), {"name": "Ada", "since": "2026-01-02"})

    def test_to_json___with_unsupported_type___should_raise(self):
        with self.assertRaises(TypeError) as raised:
            to_json({"values": {1, 2, 3}})

        self.assertTrue(str(raised.exception).startswith("Unsupported type: set."))


if __name__ == "__main__":
    main()
