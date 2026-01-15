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

    def test_conversion_with_ratio_preserves_fifo_matching(self):
        """
        Тест на конвертацию с соотношением (например, reverse split 10:1).

        Сценарий (как с CWEB):
        - Покупка 1: 500 акций (BUY_1)
        - Покупка 2: 200 акций (BUY_2)
        - Покупка 3: 300 акций (BUY_3)
        - Покупка 4: 500 акций (BUY_4)
        - Конвертация 10:1: 1500 old -> 150 new
        - После конвертации: 50 + 20 + 30 + 50 = 150 акций
        - Продажа 1: 100 акций -> по FIFO должны использоваться BUY_1 (50), BUY_2 (20), BUY_3 (30), но НЕ BUY_4
        """
        parser = IBParser(request=None, user=None, target_year=2024)

        trades = [
            _trade(
                trade_id="BUY_1",
                operation="buy",
                symbol="CWEB.OLD",
                dt_obj=datetime(2022, 1, 3, 10, 0, 0),
                quantity="500",
                price="11.82",
            ),
            _trade(
                trade_id="BUY_2",
                operation="buy",
                symbol="CWEB.OLD",
                dt_obj=datetime(2022, 3, 14, 10, 0, 0),
                quantity="200",
                price="4.145",
            ),
            _trade(
                trade_id="BUY_3",
                operation="buy",
                symbol="CWEB.OLD",
                dt_obj=datetime(2022, 3, 18, 10, 0, 0),
                quantity="300",
                price="9",
            ),
            _trade(
                trade_id="BUY_4",
                operation="buy",
                symbol="CWEB.OLD",
                dt_obj=datetime(2022, 5, 10, 10, 0, 0),
                quantity="500",
                price="4.52",
            ),
            _trade(
                trade_id="SELL_1",
                operation="sell",
                symbol="CWEB",
                dt_obj=datetime(2024, 7, 27, 10, 0, 0),
                quantity="100",
                price="61",
            ),
        ]
        conversions = [
            {
                "datetime_obj": datetime(2022, 5, 30, 10, 0, 0),
                "old_ticker": "CWEB.OLD",
                "new_ticker": "CWEB",
                "old_qty_removed": Decimal("1500"),
                "new_qty_received": Decimal("150"),  # 10:1 reverse split
                "old_isin": "US0000000001",
                "new_isin": "US0000000002",
                "comment": "1500 CWEB.OLD -> 150 CWEB",
            }
        ]

        history, _total_profit, _profit_by_code = parser._build_fifo_history(trades, conversions)

        self.assertIn("CWEB", history)
        trade_events = [
            e for e in history["CWEB"] if e.get("display_type") == "trade" and e.get("event_details")
        ]
        details_by_id = {e["event_details"].get("trade_id"): e["event_details"] for e in trade_events}

        self.assertIn("SELL_1", details_by_id)
        sell_details = details_by_id["SELL_1"]
        used_buy_ids = sell_details.get("used_buy_ids", [])

        # Продажа 100 акций по FIFO после конвертации 10:1:
        # BUY_1: 500 old -> 50 new (полностью использовано)
        # BUY_2: 200 old -> 20 new (полностью использовано)
        # BUY_3: 300 old -> 30 new (полностью использовано)
        # Итого: 100 акций
        # BUY_4 НЕ должен использоваться!
        self.assertIn("BUY_1", used_buy_ids)
        self.assertIn("BUY_2", used_buy_ids)
        self.assertIn("BUY_3", used_buy_ids)
        self.assertNotIn("BUY_4", used_buy_ids, "BUY_4 не должен использоваться при FIFO продаже 100 акций")

    def test_conversion_without_prior_buys_creates_virtual_lot(self):
        """
        Тест на конвертацию когда покупки были до периода отчёта.

        Сценарий (как с LFCHY.CNV → 2628):
        - Покупки LFCHY.CNV были до периода отчёта (не в файле)
        - Конвертация: 1000 LFCHY.CNV → 5000 2628
        - Продажа: 5000 акций 2628
        - Система должна создать виртуальный лот для 5000 акций
        """
        parser = IBParser(request=None, user=None, target_year=2022)

        trades = [
            _trade(
                trade_id="SELL_1",
                operation="sell",
                symbol="2628",
                dt_obj=datetime(2022, 12, 18, 22, 37, 32),
                quantity="5000",
                price="12.12",
                currency="HKD",
            ),
        ]
        conversions = [
            {
                "datetime_obj": datetime(2022, 10, 19, 10, 0, 0),
                "old_ticker": "LFCHY.CNV",
                "new_ticker": "2628",
                "old_qty_removed": Decimal("1000"),
                "new_qty_received": Decimal("5000"),  # 1:5 split
                "old_isin": "US0000000001",
                "new_isin": "US0000000002",
                "comment": "1000 LFCHY.CNV -> 5000 2628",
            }
        ]

        history, _total_profit, _profit_by_code = parser._build_fifo_history(trades, conversions)

        self.assertIn("2628", history)
        trade_events = [
            e for e in history["2628"] if e.get("display_type") == "trade" and e.get("event_details")
        ]
        details_by_id = {e["event_details"].get("trade_id"): e["event_details"] for e in trade_events}

        # Продажа должна существовать и не быть short sale
        self.assertIn("SELL_1", details_by_id)
        sell_details = details_by_id["SELL_1"]
        fifo_cost_str = sell_details.get("fifo_cost_rub_str", "")
        # Не должно быть "шорт" в строке - акции должны быть из виртуального лота
        self.assertNotIn("шорт", fifo_cost_str.lower(), "Продажа не должна открывать шорт")
