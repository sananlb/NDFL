import csv
import xml.etree.ElementTree as ET
from collections import defaultdict
from decimal import Decimal, InvalidOperation, ROUND_HALF_EVEN


TWO_PLACES = Decimal("0.01")

CURRENCY_NUMERIC_CODES = {
    "AUD": "036",
    "CAD": "124",
    "EUR": "978",
    "GBP": "826",
    "KZT": "398",
    "RUB": "643",
    "USD": "840",
}


def _decimal(value):
    if value is None:
        return Decimal(0)
    raw = str(value).strip()
    if not raw or raw == "--":
        return Decimal(0)
    is_negative = raw.startswith("(") and raw.endswith(")")
    raw = raw.strip("()").replace(",", "")
    try:
        result = Decimal(raw)
    except InvalidOperation:
        return Decimal(0)
    return -result if is_negative else result


def _money(value):
    result = value.quantize(TWO_PLACES, rounding=ROUND_HALF_EVEN)
    if result == 0:
        return Decimal("0.00")
    return result


def _movement_row():
    return {
        "start": Decimal(0),
        "credited": Decimal(0),
        "debited": Decimal(0),
        "end": Decimal(0),
    }


def _add_signed_amount(rows, currency, amount):
    currency = (currency or "").strip().upper()
    if not currency or amount == 0:
        return
    if amount > 0:
        rows[currency]["credited"] += amount
    else:
        rows[currency]["debited"] += abs(amount)


def _finalize_rows(rows):
    finalized = []
    for currency in sorted(rows.keys()):
        data = rows[currency]
        start = _money(data["start"])
        credited = _money(data["credited"])
        debited = _money(data["debited"])
        end = _money(data["end"])

        # Hide dormant currencies that have no reportable movement and no balance change.
        if credited == 0 and debited == 0 and start == end:
            continue

        finalized.append({
            "currency": currency,
            "currency_code": CURRENCY_NUMERIC_CODES.get(currency, ""),
            "start": start,
            "credited": credited,
            "debited": debited,
            "end": end,
        })
    return finalized


def _text(node, tag, default=""):
    if node is None:
        return default
    child = node.find(tag)
    if child is None or child.text is None:
        return default
    return child.text.strip()


def _is_ffg_fx_trade(node):
    instr_kind = _text(node, "instr_kind").strip().lower()
    instr_type = _text(node, "instr_type").strip()
    instr_name = _text(node, "instr_nm")
    return instr_kind == "валюта" or instr_type == "6" or "/" in instr_name


def _split_currency_pair(pair):
    if not pair or "/" not in pair:
        return "", ""
    left, right = pair.split("/", 1)
    return left.strip().upper(), right.strip().upper()


def calculate_ffg_movement_from_path(path, account_number=""):
    root = ET.parse(path).getroot()
    account = (
        root.findtext(".//plainAccountInfoData/client_code")
        or account_number
        or ""
    ).strip()

    cash_rows = defaultdict(_movement_row)
    asset_rows = defaultdict(_movement_row)
    instrument_currency = {}

    for node in root.findall("cash_flows_json/node"):
        currency = _text(node, "curr").upper()
        if not currency:
            continue
        cash_rows[currency]["start"] += _decimal(_text(node, "curr_at_start"))
        cash_rows[currency]["end"] += _decimal(_text(node, "curr_at_end"))

    intercompany_flows = defaultdict(Decimal)
    has_cash_flow_details = False
    for node in root.findall("cash_flows/detailed/node"):
        flow_type = _text(node, "type_id").lower()
        if flow_type in ("block", "unblock"):
            continue

        currency = _text(node, "currency").upper()
        amount = _decimal(_text(node, "amount"))
        if not currency or amount == 0:
            continue

        has_cash_flow_details = True
        if flow_type == "intercompany":
            intercompany_flows[currency] += amount
        else:
            _add_signed_amount(cash_rows, currency, amount)

    for currency, amount in intercompany_flows.items():
        _add_signed_amount(cash_rows, currency, amount)

    if not has_cash_flow_details:
        for node in root.findall("cash_flows_json/node"):
            currency = _text(node, "curr").upper()
            _add_signed_amount(cash_rows, currency, _decimal(_text(node, "curr_flowed")))

    for node in root.findall("trades/detailed/node"):
        operation = _text(node, "operation").lower()
        trade_currency = _text(node, "curr_c").upper()
        amount = _decimal(_text(node, "summ"))
        quantity = _decimal(_text(node, "q"))

        ticker = _text(node, "instr_nm")
        isin = _text(node, "isin")
        if trade_currency:
            if ticker:
                instrument_currency[ticker] = trade_currency
            if isin:
                instrument_currency[isin] = trade_currency

        if _is_ffg_fx_trade(node):
            base_currency, quote_currency = _split_currency_pair(ticker)
            quote_currency = trade_currency or quote_currency
            if operation == "buy":
                _add_signed_amount(cash_rows, base_currency, quantity)
                _add_signed_amount(cash_rows, quote_currency, -amount)
            elif operation == "sell":
                _add_signed_amount(cash_rows, base_currency, -quantity)
                _add_signed_amount(cash_rows, quote_currency, amount)
            continue

        if operation == "buy":
            _add_signed_amount(cash_rows, trade_currency, -amount)
            asset_rows[trade_currency]["credited"] += amount
        elif operation == "sell":
            _add_signed_amount(cash_rows, trade_currency, amount)
            asset_rows[trade_currency]["debited"] += amount

    for node in root.findall("commissions/detailed/node"):
        currency = _text(node, "currency").upper()
        amount = _decimal(_text(node, "sum"))
        if amount:
            _add_signed_amount(cash_rows, currency, -abs(amount))

    for node in root.findall("securities_flows_json/node"):
        ticker = _text(node, "ticker")
        isin = _text(node, "isin")
        currency = (
            _text(node, "security_currency").upper()
            or instrument_currency.get(isin, "")
            or instrument_currency.get(ticker, "")
        )
        if not currency:
            continue

        quantity_at_start = _decimal(_text(node, "quantity_at_start"))
        quantity_at_end = _decimal(_text(node, "quantity_at_end"))
        price_at_start = _decimal(_text(node, "security_price_at_start"))
        price_at_end = _decimal(_text(node, "security_price"))

        asset_rows[currency]["start"] += quantity_at_start * price_at_start
        asset_rows[currency]["end"] += quantity_at_end * price_at_end

    return {
        "broker": "Freedom Finance Global",
        "account_number": account,
        "cash_rows": _finalize_rows(cash_rows),
        "asset_rows": _finalize_rows(asset_rows),
    }


def _parse_csv_sections(path):
    sections = {}
    with open(path, "r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        for row in reader:
            if not row or not row[0].strip():
                continue
            section_name = row[0].strip()
            row_type = row[1].strip() if len(row) > 1 else ""
            if row_type == "Header":
                sections.setdefault(section_name, []).append({"header": row[2:], "rows": []})
            elif row_type == "Data" and section_name in sections:
                sections[section_name][-1]["rows"].append(row[2:])
    return sections


def _header_map(header):
    return {name.strip(): idx for idx, name in enumerate(header or []) if name}


def _get(row, header, names):
    for name in names:
        idx = header.get(name)
        if idx is not None and idx < len(row):
            return row[idx]
    return ""


def _is_stock_asset_class(asset_class):
    return asset_class in ("Акции", "Stocks")


def calculate_ib_movement_from_path(path, account_number=""):
    sections = _parse_csv_sections(path)
    account = account_number or ""
    for block in sections.get("Информация о счете", []) + sections.get("Account Information", []):
        header = _header_map(block.get("header"))
        for row in block.get("rows", []):
            field_name = _get(row, header, ["Название поля", "Field Name"])
            if field_name in ("Счет", "Account"):
                account = _get(row, header, ["Значение поля", "Field Value"]) or account

    cash_rows = defaultdict(_movement_row)
    cash_start_labels = {"Начальная сумма средств", "Starting Cash"}
    cash_end_labels = {"Остаток средств на конец периода", "Ending Cash"}
    cash_ignored_labels = {
        "Конечная расчетная сумма средств",
        "Ending Settled Cash",
        "Начальная залоговая стоимость",
        "Starting Collateral",
        "Операции с чист. кредитованными ценными бумагами",
        "Net Stock Lending Activity",
        "Конечная залоговая стоимость",
        "Ending Collateral",
        "Чистый остаток наличных средств",
        "Net Cash Balance",
        "Чистый расчетный остаток наличных средств",
        "Net Settled Cash Balance",
        "Прибыль/убытки при пересчете валюты",
        "Forex P/L Details",
    }

    for block in sections.get("Отчет о денежных средствах", []) + sections.get("Cash Report", []):
        header = _header_map(block.get("header"))
        for row in block.get("rows", []):
            label = _get(row, header, ["Валютная сводка", "Currency Summary"])
            currency = _get(row, header, ["Валюта", "Currency"]).upper()
            amount = _decimal(_get(row, header, ["Всего", "Total"]))
            if not currency or currency in ("ОТЧЕТ ПО БАЗОВОЙ ВАЛЮТЕ", "BASE CURRENCY SUMMARY"):
                continue
            if label in cash_start_labels:
                cash_rows[currency]["start"] += amount
            elif label in cash_end_labels:
                cash_rows[currency]["end"] += amount
            elif label in cash_ignored_labels:
                continue
            else:
                _add_signed_amount(cash_rows, currency, amount)

    symbol_currency = {}
    for section_name in ("Открытые позиции", "Open Positions", "Данные о чистой позиции по акциям"):
        for block in sections.get(section_name, []):
            header = _header_map(block.get("header"))
            for row in block.get("rows", []):
                symbol = _get(row, header, ["Символ", "Symbol"])
                currency = _get(row, header, ["Валюта", "Currency"]).upper()
                if symbol and currency:
                    symbol_currency[symbol] = currency

    for block in sections.get("Сделки", []) + sections.get("Trades", []):
        header = _header_map(block.get("header"))
        for row in block.get("rows", []):
            symbol = _get(row, header, ["Символ", "Symbol"])
            currency = _get(row, header, ["Валюта", "Currency"]).upper()
            if symbol and currency:
                symbol_currency[symbol] = currency

    asset_rows = defaultdict(_movement_row)

    for block in sections.get("Рыночная переоценка: отчет об эффективности", []) + sections.get("Mark-to-Market Performance Summary", []):
        header = _header_map(block.get("header"))
        for row in block.get("rows", []):
            asset_class = _get(row, header, ["Класс актива", "Asset Class"])
            if not _is_stock_asset_class(asset_class):
                continue

            symbol = _get(row, header, ["Символ", "Symbol"])
            currency = symbol_currency.get(symbol, "")
            if not currency:
                continue

            previous_quantity = _decimal(_get(row, header, ["Предыд. Количество", "Prior Quantity"]))
            current_quantity = _decimal(_get(row, header, ["Текущ. Количество", "Current Quantity"]))
            previous_price = _decimal(_get(row, header, ["Предыд. Цена", "Prior Price"]))
            current_price = _decimal(_get(row, header, ["Текущ. Цена", "Current Price"]))

            asset_rows[currency]["start"] += previous_quantity * previous_price
            asset_rows[currency]["end"] += current_quantity * current_price

    for block in sections.get("Сделки", []) + sections.get("Trades", []):
        header = _header_map(block.get("header"))
        for row in block.get("rows", []):
            discriminator = _get(row, header, ["DataDiscriminator"])
            if discriminator and discriminator != "Order":
                continue

            asset_class = _get(row, header, ["Класс актива", "Asset Class"])
            if not _is_stock_asset_class(asset_class):
                continue

            currency = _get(row, header, ["Валюта", "Currency"]).upper()
            quantity = _decimal(_get(row, header, ["Количество", "Quantity"]))
            proceeds = _decimal(_get(row, header, ["Выручка", "Proceeds"]))
            if not currency or proceeds == 0:
                continue

            if quantity > 0:
                asset_rows[currency]["credited"] += abs(proceeds)
            elif quantity < 0:
                asset_rows[currency]["debited"] += abs(proceeds)

    return {
        "broker": "Interactive Brokers",
        "account_number": account,
        "cash_rows": _finalize_rows(cash_rows),
        "asset_rows": _finalize_rows(asset_rows),
    }


def calculate_movement_report_tables(user, broker_type, target_year):
    from .models import BrokerReport

    reports = BrokerReport.objects.filter(
        user=user,
        broker_type=broker_type,
        year=target_year,
    ).order_by("account_number", "original_filename")

    tables = []
    for report in reports:
        if not report.report_file:
            continue
        path = report.report_file.path
        if broker_type == "ib":
            table = calculate_ib_movement_from_path(path, report.account_number)
        else:
            table = calculate_ffg_movement_from_path(path, report.account_number)
        if table["cash_rows"] or table["asset_rows"]:
            table["source_filename"] = report.original_filename
            tables.append(table)
    return tables
