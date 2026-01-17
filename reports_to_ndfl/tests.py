from datetime import datetime
from collections import defaultdict
from decimal import Decimal

from django.test import SimpleTestCase

from reports_to_ndfl.parsers.ib_parser import IBParser
from reports_to_ndfl.views import _attach_dividend_fees


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
    **extra_fields,
):
    trade = {
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
    trade.update(extra_fields)
    return trade


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
        trade_events = [e for e in history["CWEB"] if e.get("display_type") == "trade" and e.get("event_details")]
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


class IBParserExpiredOptionsTests(SimpleTestCase):
    def test_short_option_expiration_is_included_in_target_year_history(self):
        """
        SHORT опцион, открытый продажей в прошлом году и закрытый погашением (Ep) в целевом году,
        должен попадать в историю и в расчёт финреза по году закрытия.
        """
        parser = IBParser(request=None, user=None, target_year=2025)

        symbol = "FMC 17JAN25 80 C"
        group_symbol = f"OPTION_{symbol}"
        trades = [
            _trade(
                trade_id="SELL_OPEN",
                operation="sell",
                symbol=symbol,
                dt_obj=datetime(2024, 10, 30, 10, 0, 0),
                quantity="1",
                price="1",
                income_code="1532",
                group_symbol=group_symbol,
            ),
            _trade(
                trade_id="BUY_EXP",
                operation="buy",
                symbol=symbol,
                dt_obj=datetime(2025, 1, 17, 10, 0, 0),
                quantity="1",
                price="0",
                income_code="1532",
                group_symbol=group_symbol,
                is_expired=True,
            ),
        ]

        history, total_profit, profit_by_code = parser._build_fifo_history(trades, conversions=[], acquisitions=[])

        self.assertIn(group_symbol, history)
        trade_events = [e for e in history[group_symbol] if e.get("display_type") == "trade" and e.get("event_details")]
        details_by_id = {e["event_details"].get("trade_id"): e["event_details"] for e in trade_events}

        self.assertIn("SELL_OPEN", details_by_id)
        self.assertIn("BUY_EXP", details_by_id)

        sell_details = details_by_id["SELL_OPEN"]
        buy_details = details_by_id["BUY_EXP"]

        self.assertEqual(sell_details.get("short_close_year"), 2025)
        self.assertTrue(sell_details.get("is_relevant_for_target_year"))
        self.assertTrue(buy_details.get("is_expired"))
        self.assertTrue(buy_details.get("is_relevant_for_target_year"))
        self.assertTrue(sell_details.get("is_in_pdf_range"))
        self.assertTrue(buy_details.get("is_in_pdf_range"))

        self.assertEqual(profit_by_code.get("1532"), Decimal("100.00"))
        self.assertEqual(total_profit, Decimal("100.00"))


class IBParserDividendMatchingTests(SimpleTestCase):
    def test_extract_symbol_isin_allows_space_before_paren(self):
        parser = IBParser(request=None, user=None, target_year=2020)
        ticker, isin = parser._extract_symbol_isin("D05 (SG1L01001701) Наличный дивиденд SGD 0.18")
        self.assertEqual(ticker, "D05")
        self.assertEqual(isin, "SG1L01001701")

    def test_normalize_dividend_description_strips_fee_tax_and_trailing_parens(self):
        parser = IBParser(request=None, user=None, target_year=2025)
        self.assertEqual(
            parser._normalize_dividend_description(
                "GLTR(US37949E2046) Наличный дивиденд USD 0.61982 на акцию - FEE"
            ),
            "GLTR(US37949E2046) Наличный дивиденд USD 0.61982 на акцию",
        )
        self.assertEqual(
            parser._normalize_dividend_description(
                "CMCSA(US20030N1019) Выплата в качестве дивиденда (Обыкновенный дивиденд)"
            ),
            "CMCSA(US20030N1019) Выплата в качестве дивиденда",
        )
        self.assertEqual(
            parser._normalize_dividend_description(
                "FMC(US3024913036) Выплата в качестве дивиденда - US Налог"
            ),
            "FMC(US3024913036) Выплата в качестве дивиденда",
        )

    def test_withholding_tax_matches_by_description_not_just_date_symbol(self):
        parser = IBParser(request=None, user=None, target_year=2025)
        parser._get_cbr_rate = lambda _currency, _dt_obj: Decimal("1")

        sections = {
            "Дивиденды": [
                {
                    "header": ["Валюта", "Дата", "Описание", "Сумма"],
                    "data": [
                        [
                            "USD",
                            "2025-04-23",
                            "CMCSA(US20030N1019) Наличный дивиденд USD 0.33 на акцию (Обыкновенный дивиденд)",
                            "63.03",
                        ],
                        [
                            "USD",
                            "2025-04-23",
                            "CMCSA(US20030N1019) Выплата в качестве дивиденда (Обыкновенный дивиденд)",
                            "2.97",
                        ],
                    ],
                }
            ],
            "Удерживаемый налог": [
                {
                    "header": ["Валюта", "Дата", "Описание", "Сумма", "Код"],
                    "data": [
                        [
                            "USD",
                            "2025-04-23",
                            "CMCSA(US20030N1019) Наличный дивиденд USD 0.33 на акцию - US Налог",
                            "-18.91",
                            "",
                        ],
                        [
                            "USD",
                            "2025-04-23",
                            "CMCSA(US20030N1019) Выплата в качестве дивиденда - US Налог",
                            "-0.89",
                            "",
                        ],
                    ],
                }
            ],
        }

        dividends = parser._parse_dividends(sections)
        self.assertEqual(len(dividends), 2)

        dividends_by_key = {d["dividend_key"]: d for d in dividends}
        self.assertEqual(
            dividends_by_key["CMCSA(US20030N1019) Наличный дивиденд USD 0.33 на акцию"]["tax_amount"],
            Decimal("-18.91"),
        )
        self.assertEqual(
            dividends_by_key["CMCSA(US20030N1019) Выплата в качестве дивиденда"]["tax_amount"],
            Decimal("-0.89"),
        )

    def test_dividend_fee_fallback_matches_nearest_date_for_repeated_description(self):
        dividend_events = [
            {
                "date": datetime(2025, 1, 8).date(),
                "ticker": "MRK",
                "currency": "USD",
                "dividend_key": "MRK(US58933Y1055) Наличный дивиденд USD 0.81 на акцию",
                "dividend_match_key": "2025-01-08|USD|MRK(US58933Y1055) Наличный дивиденд USD 0.81 на акцию",
            },
            {
                "date": datetime(2025, 4, 7).date(),
                "ticker": "MRK",
                "currency": "USD",
                "dividend_key": "MRK(US58933Y1055) Наличный дивиденд USD 0.81 на акцию",
                "dividend_match_key": "2025-04-07|USD|MRK(US58933Y1055) Наличный дивиденд USD 0.81 на акцию",
            },
        ]
        dividend_commissions_data = {
            "Комиссия по дивидендам (MRK)": {
                "amount_by_currency": {"USD": Decimal("-1")},
                "amount_rub": Decimal("-1"),
                "details": [
                    {
                        "date": "08.04.2025",
                        "date_obj": datetime(2025, 4, 8).date(),
                        "amount": Decimal("-1"),
                        "currency": "USD",
                        "amount_rub": Decimal("-100"),
                        "comment": "MRK(US58933Y1055) Наличный дивиденд USD 0.81 на акцию - FEE",
                        "dividend_key": "MRK(US58933Y1055) Наличный дивиденд USD 0.81 на акцию",
                        # намеренно неверная дата в match_key, чтобы проверять fallback
                        "dividend_match_key": "2025-04-08|USD|MRK(US58933Y1055) Наличный дивиденд USD 0.81 на акцию",
                    }
                ],
            }
        }

        _attach_dividend_fees(dividend_events, dividend_commissions_data)

        self.assertNotIn("fee_rub", dividend_events[0])
        self.assertEqual(dividend_events[1].get("fee_rub"), Decimal("-100"))

    def test_dividend_fee_matching_report_detects_fee_without_dividends(self):
        dividend_events = []
        dividend_commissions_data = {
            "Комиссия по дивидендам (X)": {
                "amount_by_currency": {"USD": Decimal("-1")},
                "amount_rub": Decimal("-1"),
                "details": [
                    {
                        "date": "01.01.2025",
                        "date_obj": datetime(2025, 1, 1).date(),
                        "amount": Decimal("-1"),
                        "currency": "USD",
                        "amount_rub": Decimal("-10"),
                        "comment": "X(US0000000000) Cash Dividend - FEE",
                        "dividend_key": "X(US0000000000) Cash Dividend",
                        "dividend_match_key": "2025-01-01|USD|X(US0000000000) Cash Dividend",
                    }
                ],
            }
        }
        report = _attach_dividend_fees(dividend_events, dividend_commissions_data)
        self.assertFalse(report["ok"])
        self.assertEqual(report["total_fee_details"], 1)
        self.assertEqual(report["unmatched_fee_details"], 1)

    def test_attach_dividend_fees_is_idempotent(self):
        dividend_events = [
            {
                "date": datetime(2025, 10, 7).date(),
                "ticker": "GLTR",
                "currency": "USD",
                "dividend_key": "GLTR(US37949E2046) Наличный дивиденд USD 3.910916 на акцию",
                "dividend_match_key": "2025-10-07|USD|GLTR(US37949E2046) Наличный дивиденд USD 3.910916 на акцию",
            }
        ]
        dividend_commissions_data = {
            "Комиссия по дивидендам (GLTR)": {
                "amount_by_currency": {"USD": Decimal("-1")},
                "amount_rub": Decimal("-1"),
                "details": [
                    {
                        "date": "07.10.2025",
                        "date_obj": datetime(2025, 10, 7).date(),
                        "amount": Decimal("-1"),
                        "currency": "USD",
                        "amount_rub": Decimal("-120"),
                        "comment": "GLTR(US37949E2046) Наличный дивиденд USD 3.910916 на акцию - FEE",
                        "dividend_key": "GLTR(US37949E2046) Наличный дивиденд USD 3.910916 на акцию",
                        "dividend_match_key": "2025-10-07|USD|GLTR(US37949E2046) Наличный дивиденд USD 3.910916 на акцию",
                    }
                ],
            }
        }
        report1 = _attach_dividend_fees(dividend_events, dividend_commissions_data)
        report2 = _attach_dividend_fees(dividend_events, dividend_commissions_data)
        self.assertTrue(report1["ok"])
        self.assertTrue(report2["ok"])
        self.assertEqual(dividend_events[0].get("fee_rub"), Decimal("-120"))

    def test_parse_fees_includes_adr_fee_near_target_year_dividend(self):
        parser = IBParser(request=None, user=None, target_year=2024)
        parser._get_cbr_rate = lambda _currency, _dt_obj: Decimal("1")

        sections = {
            "Сборы/комиссии": [
                {
                    "header": ["Subtitle", "Валюта", "Дата", "Описание", "Сумма"],
                    "data": [
                        ["Другие сборы", "USD", "2024-12-23", "HSBK(US46627J3023) Плата ADR USD 0.02 на акцию", "-5.78"],
                    ],
                }
            ]
        }

        dividend_commissions = defaultdict(
            lambda: {"amount_by_currency": defaultdict(Decimal), "amount_rub": Decimal(0), "details": []}
        )
        other_commissions = defaultdict(lambda: {"currencies": defaultdict(Decimal), "total_rub": Decimal(0), "raw_events": []})

        parser._parse_fees(sections, other_commissions, dividend_commissions)

        # ADR fee не должен считаться дивидендной комиссией, так как нет явного упоминания дивиденда/-FEE.
        details = [d for cat in dividend_commissions.values() for d in cat.get("details", [])]
        self.assertEqual(len(details), 0)
        self.assertIn("Другие сборы", other_commissions)
        self.assertEqual(other_commissions["Другие сборы"]["currencies"]["USD"], Decimal("-5.78"))

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

    def test_conversion_with_ib_ticker_rename(self):
        """
        Тест на конвертацию когда IB переименовал тикеры в CSV.

        IB в CSV использует текущий тикер для всех сделок (даже тех, что были до конвертации).
        Пример: покупки записаны как CWEB, но конвертация говорит CWEB.OLD -> CWEB.
        Система должна понять, что покупки CWEB на самом деле относятся к CWEB.OLD.
        """
        parser = IBParser(request=None, user=None, target_year=2022)

        trades = [
            # Покупки до сплита - IB записывает их с текущим тикером CWEB
            _trade(
                trade_id="BUY_1",
                operation="buy",
                symbol="CWEB",  # IB пишет текущий тикер, хотя покупка была до сплита
                dt_obj=datetime(2022, 1, 3, 10, 0, 0),
                quantity="500",
                price="11.82",
            ),
            _trade(
                trade_id="BUY_2",
                operation="buy",
                symbol="CWEB",
                dt_obj=datetime(2022, 3, 14, 10, 0, 0),
                quantity="200",
                price="4.145",
            ),
            # Продажа после сплита
            _trade(
                trade_id="SELL_1",
                operation="sell",
                symbol="CWEB",
                dt_obj=datetime(2022, 7, 27, 10, 0, 0),
                quantity="50",
                price="61",
            ),
        ]
        conversions = [
            {
                # Конвертация использует OLD тикер для старых акций
                "datetime_obj": datetime(2022, 5, 30, 10, 0, 0),
                "old_ticker": "CWEB.OLD",  # Старый тикер
                "new_ticker": "CWEB",       # Новый тикер (совпадает с тем, что в сделках)
                "old_qty_removed": Decimal("700"),  # 500 + 200
                "new_qty_received": Decimal("70"),  # 10:1 reverse split
                "old_isin": "US0000000001",
                "new_isin": "US0000000002",
                "comment": "700 CWEB.OLD -> 70 CWEB",
            }
        ]

        history, _total_profit, _profit_by_code = parser._build_fifo_history(trades, conversions)

        self.assertIn("CWEB", history)
        trade_events = [
            e for e in history["CWEB"] if e.get("display_type") == "trade" and e.get("event_details")
        ]
        details_by_id = {e["event_details"].get("trade_id"): e["event_details"] for e in trade_events}

        # Покупки должны быть связаны с продажей через конвертацию
        self.assertIn("SELL_1", details_by_id)
        sell_details = details_by_id["SELL_1"]
        used_buy_ids = sell_details.get("used_buy_ids", [])

        # Продажа 50 акций после сплита 10:1
        # BUY_1: 500 old -> 50 new (полностью)
        # BUY_2: не нужен
        self.assertIn("BUY_1", used_buy_ids)
        # Не должно быть шорта - акции должны найтись через конвертацию
        fifo_cost_str = sell_details.get("fifo_cost_rub_str", "")
        self.assertNotIn("шорт", fifo_cost_str.lower(), "Продажа не должна открывать шорт")

    def test_conversion_finds_lots_by_isin_when_ticker_changed(self):
        """
        Тест на поиск лотов по ISIN когда тикер сменился без корп. действия.

        Сценарий (как с LFC → LFCHY):
        - Покупка под тикером LFC (ISIN US16939P1066)
        - Тикер сменился на LFCHY (тот же ISIN) - нет корп. действия в CSV
        - Конвертация LFCHY → 2628
        - Система должна найти лоты LFC по ISIN и связать с продажей 2628
        """
        parser = IBParser(request=None, user=None, target_year=2022)

        trades = [
            _trade(
                trade_id="BUY_LFC",
                operation="buy",
                symbol="LFC",  # Старый тикер
                dt_obj=datetime(2021, 6, 1, 10, 0, 0),
                quantity="1000",
                price="10.58",
            ),
            _trade(
                trade_id="SELL_2628",
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
                "datetime_obj": datetime(2022, 10, 6, 19, 45, 0),
                "old_ticker": "LFCHY",  # Новый тикер (тот же ISIN что и LFC)
                "new_ticker": "LFCHY.CNV",
                "old_qty_removed": Decimal("1000"),
                "new_qty_received": Decimal("1000"),
                "old_isin": "US16939P1066",  # Тот же ISIN что и у LFC!
                "new_isin": "US16939P10CV",
                "comment": "LFCHY -> LFCHY.CNV",
            },
            {
                "datetime_obj": datetime(2022, 10, 19, 20, 25, 0),
                "old_ticker": "LFCHY.CNV",
                "new_ticker": "2628",
                "old_qty_removed": Decimal("1000"),
                "new_qty_received": Decimal("5000"),  # 1:5
                "old_isin": "US16939P10CV",
                "new_isin": "CNE1000002L3",
                "comment": "LFCHY.CNV -> 2628 5:1",
            },
        ]

        # Важно: передаём symbol_to_isin чтобы парсер знал что LFC и LFCHY имеют одинаковый ISIN
        symbol_to_isin = {
            "LFC": "US16939P1066",
            "LFCHY": "US16939P1066",  # Тот же ISIN!
            "LFCHY.CNV": "US16939P10CV",
            "2628": "CNE1000002L3",
        }

        history, _total_profit, _profit_by_code = parser._build_fifo_history(
            trades, conversions, symbol_to_isin=symbol_to_isin
        )

        self.assertIn("2628", history)
        trade_events = [
            e for e in history["2628"] if e.get("display_type") == "trade" and e.get("event_details")
        ]
        details_by_id = {e["event_details"].get("trade_id"): e["event_details"] for e in trade_events}

        self.assertIn("SELL_2628", details_by_id)
        sell_details = details_by_id["SELL_2628"]
        used_buy_ids = sell_details.get("used_buy_ids", [])

        # Продажа должна быть связана с покупкой LFC через ISIN
        self.assertIn("BUY_LFC", used_buy_ids, "Покупка LFC должна быть связана с продажей 2628 по ISIN")
        # Не должно быть шорта
        fifo_cost_str = sell_details.get("fifo_cost_rub_str", "")
        self.assertNotIn("шорт", fifo_cost_str.lower(), "Продажа не должна открывать шорт")

    def test_parse_corporate_actions_not_filters_isin_change_with_suffix(self):
        """
        Тест что конвертация с суффиксом тикера (.CNV) НЕ фильтруется если ISIN меняется.

        Сценарий:
        - LFCHY → LFCHY.CNV (количество 1000 → 1000, но ISIN меняется!)
        - old_isin: US16939P1066
        - new_isin: US16939P10CV
        - Это реальная конвертация, НЕ техническое переименование
        """
        parser = IBParser(request=None, user=None, target_year=2022)

        sections = {
            "Корпоративные действия": [
                {
                    "header": ["Дата/Время", "Описание", "Количество"],
                    "data": [
                        # Списание LFCHY
                        ["2022-10-06, 19:45:00", "LFCHY(US16939P1066) CASH and STOCK MERGER (Voluntary) - DIFFERENT ISIN LFCHY.CNV(US16939P10CV)", "-1000"],
                        # Получение LFCHY.CNV
                        ["2022-10-06, 19:45:00", "LFCHY(US16939P1066) CASH and STOCK MERGER (Voluntary) - DIFFERENT ISIN LFCHY.CNV(US16939P10CV)", "1000"],
                    ],
                }
            ]
        }

        conversions, acquisitions = parser._parse_corporate_actions(sections)
        self.assertEqual(acquisitions, [])

        # Должна быть 1 конвертация LFCHY → LFCHY.CNV
        self.assertEqual(len(conversions), 1, "Конвертация LFCHY → LFCHY.CNV не должна фильтроваться")
        conv = conversions[0]
        self.assertEqual(conv["old_ticker"], "LFCHY")
        self.assertEqual(conv["new_ticker"], "LFCHY.CNV")
        self.assertEqual(conv["old_isin"], "US16939P1066")
        self.assertEqual(conv["new_isin"], "US16939P10CV")

    def test_parse_corporate_actions_uses_symbol_column_when_description_has_only_old_ticker(self):
        """
        Реальный кейс IB: в строке получения новый тикер/ISIN есть в колонках Symbol/Security ID,
        а в Description фигурирует только старый тикер (в т.ч. дублированный в скобках).

        Ожидание: парсер должен построить конвертацию old->new и НЕ отфильтровать её как "техническое переименование".
        """
        parser = IBParser(request=None, user=None, target_year=2025)

        sections = {
            "Корпоративные действия": [
                {
                    "header": [
                        "Дата/Время",
                        "Класс актива",
                        "Символ",
                        "Идентификатор ценной бумаги",
                        "Описание",
                        "Количество",
                        "Валюта",
                        "Выручка",
                        "Стоимость",
                    ],
                    "data": [
                        [
                            "2023-04-13, 07:00:00",
                            "Stocks",
                            "ADC.SUB8",
                            "AU251811SUB8",
                            "ADC.SUB8(AU251811SUB8) MERGER EVENT (ADC.SUB8, something, AU251811SUB8)",
                            "-15000",
                            "AUD",
                            "0",
                            "0",
                        ],
                        [
                            "2023-04-13, 07:00:00",
                            "Warrants",
                            "ADCO",
                            "AU0000271165",
                            "ADC.SUB8(AU251811SUB8) MERGER EVENT (ADC.SUB8, something, AU251811SUB8)",
                            "15000",
                            "AUD",
                            "0",
                            "0",
                        ],
                    ],
                }
            ]
        }

        conversions, acquisitions = parser._parse_corporate_actions(sections)
        self.assertEqual(acquisitions, [])
        self.assertEqual(len(conversions), 1)

        conv = conversions[0]
        self.assertEqual(conv["old_ticker"], "ADC.SUB8")
        self.assertEqual(conv["new_ticker"], "ADCO")
        self.assertEqual(conv["old_qty_removed"], Decimal("15000"))
        self.assertEqual(conv["new_qty_received"], Decimal("15000"))
        self.assertEqual(conv["old_isin"], "AU251811SUB8")
        self.assertEqual(conv["new_isin"], "AU0000271165")
        self.assertEqual(conv.get("asset_class_from"), "Stocks")
        self.assertEqual(conv.get("asset_class_to"), "Warrants")


class IBParserWarrantCorporateActionsTests(SimpleTestCase):
    def test_subscription_then_conversion_to_warrant_preserves_cost_basis(self):
        parser = IBParser(request=None, user=None, target_year=2025)

        acquisitions = [
            {
                "datetime_obj": datetime(2024, 6, 1, 10, 0, 0),
                "ticker": "ADC.SUB8",
                "isin": "AU0000000001",
                "quantity": Decimal("1500"),
                "currency": "RUB",
                "cost": Decimal("75"),
                "value": Decimal("0"),
                "comment": "subscription",
                "asset_class": "Stocks",
                "source_ticker": "ADC.RTS8",
                "source_isin": "AU0000000002",
                "type": "subscription",
            }
        ]

        conversions = [
            {
                "datetime_obj": datetime(2024, 7, 1, 10, 0, 0),
                "old_ticker": "ADC.SUB8",
                "new_ticker": "ADCO",
                "old_qty_removed": Decimal("1500"),
                "new_qty_received": Decimal("1500"),
                "old_isin": "AU0000000001",
                "new_isin": "AU0000000003",
                "comment": "ADC.SUB8 -> ADCO",
                "asset_class_from": "Stocks",
                "asset_class_to": "Warrants",
            }
        ]

        trades = [
            _trade(
                trade_id="SELL_ADCO",
                operation="sell",
                symbol="WARRANT_ADCO",
                dt_obj=datetime(2025, 1, 10, 10, 0, 0),
                quantity="1500",
                price="1",
                currency="RUB",
                cbr_rate="1",
                income_code="1532",
            )
        ]

        history, _total_profit, _profit_by_code = parser._build_fifo_history(trades, conversions, acquisitions)

        self.assertIn("WARRANT_ADCO", history)
        trade_events = [
            e for e in history["WARRANT_ADCO"] if e.get("display_type") == "trade" and e.get("event_details")
        ]
        details_by_id = {e["event_details"].get("trade_id"): e["event_details"] for e in trade_events}

        self.assertIn("SELL_ADCO", details_by_id)
        sell_details = details_by_id["SELL_ADCO"]
        fifo_cost = sell_details.get("fifo_cost_rub_decimal")
        self.assertIsNotNone(fifo_cost)
        self.assertEqual(fifo_cost.quantize(Decimal("0.01")), Decimal("75.00"))
