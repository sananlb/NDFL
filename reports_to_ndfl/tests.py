from datetime import datetime
from decimal import Decimal

from django.test import SimpleTestCase

from reports_to_ndfl.parsers.ib_parser import IBParser


def _trade(
    *,
    trade_id: str,
    operation: str,
    symbol: str,
    dt_obj: datetime,
    quantity: str,
    price: str,
    cbr_rate: str = "100",
    commission: str = "0",
    currency: str = "USD",
    income_code: str = "1530",
):
    return {
        "trade_id": trade_id,
        "operation": operation,
        "symbol": symbol,
        "datetime_obj": dt_obj,
        "quantity": Decimal(quantity),
        "price": Decimal(price),
        "commission": Decimal(commission),
        "cbr_rate": Decimal(cbr_rate),
        "currency": currency,
        "income_code": income_code,
        "multiplier": Decimal("1"),
        "instr_kind": "stock",
        "isin": "",
        "instr_nm": symbol,
    }


class IBParserConversionLinksTests(SimpleTestCase):
    def test_conversion_preserves_links_to_original_buys(self):
        parser = IBParser(request=None, user=None, target_year=2024)

        trades = [
            _trade(
                trade_id="BUY_1",
                operation="buy",
                symbol="RAC",
                dt_obj=datetime(2020, 5, 1, 10, 0, 0),
                quantity="10",
                price="1",
            ),
            _trade(
                trade_id="SELL_1",
                operation="sell",
                symbol="RACAU",
                dt_obj=datetime(2024, 6, 2, 10, 0, 0),
                quantity="10",
                price="2",
            ),
        ]
        conversions = [
            {
                "datetime_obj": datetime(2023, 1, 1, 10, 0, 0),
                "old_ticker": "RAC",
                "new_ticker": "RACAU",
                "old_qty_removed": Decimal("10"),
                "new_qty_received": Decimal("10"),
                "old_isin": "US0000000001",
                "new_isin": "US0000000002",
                "comment": "RAC -> RACAU",
            }
        ]

        history, _total_profit, _profit_by_code = parser._build_fifo_history(trades, conversions)

        self.assertIn("RACAU", history)
        trade_events = [
            e for e in history["RACAU"] if e.get("display_type") == "trade" and e.get("event_details")
        ]
        details_by_id = {e["event_details"].get("trade_id"): e["event_details"] for e in trade_events}

        self.assertIn("BUY_1", details_by_id)
        self.assertIn("SELL_1", details_by_id)

        buy_details = details_by_id["BUY_1"]
        sell_details = details_by_id["SELL_1"]

        self.assertTrue(buy_details.get("is_relevant_for_target_year"))
        self.assertTrue(sell_details.get("is_relevant_for_target_year"))
        self.assertIn("BUY_1", sell_details.get("used_buy_ids", []))
        self.assertTrue(buy_details.get("link_colors"))
        self.assertTrue(sell_details.get("link_colors"))

    def test_conversion_chains_preserve_links_to_original_buys(self):
        parser = IBParser(request=None, user=None, target_year=2024)

        trades = [
            _trade(
                trade_id="BUY_A",
                operation="buy",
                symbol="A",
                dt_obj=datetime(2020, 1, 1, 10, 0, 0),
                quantity="5",
                price="10",
            ),
            _trade(
                trade_id="SELL_C",
                operation="sell",
                symbol="C",
                dt_obj=datetime(2024, 1, 10, 10, 0, 0),
                quantity="5",
                price="12",
            ),
        ]
        conversions = [
            {
                "datetime_obj": datetime(2022, 6, 1, 10, 0, 0),
                "old_ticker": "A",
                "new_ticker": "B",
                "old_qty_removed": Decimal("5"),
                "new_qty_received": Decimal("5"),
                "old_isin": "US000000000A",
                "new_isin": "US000000000B",
                "comment": "A -> B",
            },
            {
                "datetime_obj": datetime(2023, 6, 1, 10, 0, 0),
                "old_ticker": "B",
                "new_ticker": "C",
                "old_qty_removed": Decimal("5"),
                "new_qty_received": Decimal("5"),
                "old_isin": "US000000000B",
                "new_isin": "US000000000C",
                "comment": "B -> C",
            },
        ]

        history, _total_profit, _profit_by_code = parser._build_fifo_history(trades, conversions)

        self.assertIn("C", history)
        trade_events = [e for e in history["C"] if e.get("display_type") == "trade" and e.get("event_details")]
        details_by_id = {e["event_details"].get("trade_id"): e["event_details"] for e in trade_events}

        self.assertIn("BUY_A", details_by_id)
        self.assertIn("SELL_C", details_by_id)

        sell_details = details_by_id["SELL_C"]
        self.assertIn("BUY_A", sell_details.get("used_buy_ids", []))
