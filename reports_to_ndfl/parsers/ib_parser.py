import csv
import re
from collections import defaultdict, deque
from datetime import datetime, date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

from currency_CBRF.models import Currency
from ..FFG_ndfl import _get_exchange_rate_for_date
from .base import BaseBrokerParser


class IBParser(BaseBrokerParser):
    def process(self):
        reports = list(self._get_reports())
        if not reports:
            from django.contrib import messages
            messages.info(self.request, "У вас нет загруженных IB отчетов для анализа истории.")
            empty_commissions = defaultdict(lambda: {'amount_by_currency': defaultdict(Decimal), 'amount_rub': Decimal(0), 'details': []})
            empty_other = defaultdict(lambda: {'currencies': defaultdict(Decimal), 'total_rub': Decimal(0), 'raw_events': []})
            empty_profit_by_code = {'1530': Decimal(0), '1532': Decimal(0)}
            return {}, [], Decimal(0), Decimal(0), False, empty_commissions, empty_other, Decimal(0), empty_profit_by_code

        sections = {}
        for report in reports:
            report_sections = self._parse_csv_sections(report.report_file.path)
            for key, blocks in report_sections.items():
                sections.setdefault(key, [])
                sections[key].extend(blocks)

        dividend_commissions = defaultdict(lambda: {'amount_by_currency': defaultdict(Decimal), 'amount_rub': Decimal(0), 'details': []})
        other_commissions = defaultdict(lambda: {'currencies': defaultdict(Decimal), 'total_rub': Decimal(0), 'raw_events': []})
        total_other_commissions_rub = Decimal(0)

        symbol_to_isin, symbol_to_name, symbol_to_multiplier = self._parse_instrument_info(sections)
        trades = self._parse_trades(sections, other_commissions, symbol_to_isin, symbol_to_name, symbol_to_multiplier)
        dividends = self._parse_dividends(sections)
        conversions, acquisitions = self._parse_corporate_actions(sections, symbol_to_name)
        self._parse_interest(sections, other_commissions)
        dividend_accrual_payments = self._parse_dividend_accrual_payments(sections)
        self._parse_fees(sections, other_commissions, dividend_commissions, dividend_accrual_payments)

        instrument_event_history, total_sales_profit, profit_by_income_code = self._build_fifo_history(
            trades, conversions, acquisitions, symbol_to_isin, symbol_to_name
        )

        total_other_commissions_rub = sum((data.get('total_rub', Decimal(0)) for data in other_commissions.values()), Decimal(0))
        total_dividends_rub = sum((d.get('amount_rub', Decimal(0)) for d in dividends), Decimal(0))

        return (
            instrument_event_history,
            dividends,
            total_dividends_rub,
            total_sales_profit,
            False,
            dividend_commissions,
            other_commissions,
            total_other_commissions_rub,
            profit_by_income_code,
        )

    def _get_reports(self):
        from ..models import BrokerReport
        return BrokerReport.objects.filter(user=self.user, broker_type='ib').order_by('year', 'uploaded_at')

    def _parse_csv_sections(self, file_path):
        sections = {}
        current_section = None
        current_block = None

        with open(file_path, 'r', encoding='utf-8-sig', newline='') as handle:
            reader = csv.reader(handle)
            for row in reader:
                if not row or not row[0].strip():
                    continue
                section_name = self._canonical_section_name(row[0].strip())
                row_type = row[1].strip() if len(row) > 1 else ''

                if row_type == 'Header':
                    current_section = section_name
                    current_block = {'header': row[2:], 'data': []}
                    sections.setdefault(current_section, []).append(current_block)
                elif row_type == 'Data' and current_block:
                    current_block['data'].append(row[2:])

        return sections

    def _canonical_section_name(self, section_name):
        aliases = {
            'Trades': 'Сделки',
            'Dividends': 'Дивиденды',
            'Withholding Tax': 'Удерживаемый налог',
            'Interest': 'Процент',
            'Transfers': 'Переводы',
            'Corporate Actions': 'Корпоративные действия',
        }
        return aliases.get(section_name, section_name)

    def _header_map(self, header_row):
        return {name.strip(): idx for idx, name in enumerate(header_row or []) if name}

    def _get_value(self, row, header_map, keys):
        for key in keys:
            idx = header_map.get(key)
            if idx is not None and idx < len(row):
                return row[idx]
        return ''

    def _parse_decimal(self, value):
        if value is None:
            return Decimal(0)
        raw = str(value).strip()
        if not raw:
            return Decimal(0)
        is_negative = raw.startswith('(') and raw.endswith(')')
        raw = raw.strip('()')
        raw = raw.replace(',', '')
        try:
            val = Decimal(raw)
        except InvalidOperation:
            return Decimal(0)
        return -val if is_negative else val

    def _parse_datetime(self, value):
        if not value:
            return None
        normalized = value.replace(', ', ' ').strip()
        for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d'):
            try:
                dt = datetime.strptime(normalized, fmt)
                return dt
            except ValueError:
                continue
        return None

    def _get_cbr_rate(self, currency_code, dt_obj):
        if not currency_code or currency_code.upper() == 'RUB':
            return Decimal('1')
        if not isinstance(dt_obj, (datetime, date)):
            return None
        curr = Currency.objects.filter(char_code=currency_code.upper()).first()
        if not curr:
            return None
        _, _, rate_val = _get_exchange_rate_for_date(self.request, curr, dt_obj.date(), f"для {currency_code}")
        return rate_val

    def _parse_instrument_info(self, sections):
        """Парсит секцию 'Информация о финансовом инструменте' и возвращает словари symbol -> ISIN, symbol -> название, symbol -> множитель."""
        symbol_to_isin = {}
        symbol_to_name = {}
        symbol_to_multiplier = {}
        info_blocks = sections.get('Информация о финансовом инструменте') or []
        if not info_blocks:
            return symbol_to_isin, symbol_to_name, symbol_to_multiplier

        for block in info_blocks:
            header_map = self._header_map(block.get('header', []))
            for row in block.get('data', []):
                symbol = self._get_value(row, header_map, ['Символ', 'Symbol'])
                isin = self._get_value(row, header_map, ['Идентификатор ценной бумаги', 'Security ID'])
                description = self._get_value(row, header_map, ['Описание', 'Description'])
                multiplier = self._get_value(row, header_map, ['Множитель', 'Multiplier'])
                if symbol:
                    symbol = symbol.strip()
                    if isin:
                        symbol_to_isin[symbol] = isin.strip()
                    if description:
                        symbol_to_name[symbol] = description.strip()
                    if multiplier:
                        symbol_to_multiplier[symbol] = self._parse_decimal(multiplier)

        return symbol_to_isin, symbol_to_name, symbol_to_multiplier

    def _parse_trades(self, sections, other_commissions, symbol_to_isin=None, symbol_to_name=None, symbol_to_multiplier=None):
        trades = []
        trades_blocks = sections.get('Сделки') or []
        if symbol_to_isin is None:
            symbol_to_isin = {}
        if symbol_to_name is None:
            symbol_to_name = {}
        if symbol_to_multiplier is None:
            symbol_to_multiplier = {}
        if not trades_blocks:
            return trades

        trade_index = 1
        for block in trades_blocks:
            header_map = self._header_map(block.get('header', []))
            for row in block.get('data', []):
                discriminator = self._get_value(row, header_map, ['DataDiscriminator'])
                if discriminator and discriminator != 'Order':
                    continue

                asset_class = self._get_value(row, header_map, ['Класс актива', 'Asset Class'])
                if asset_class and asset_class in ('Forex',):
                    self._record_commission_from_trade(row, header_map, other_commissions)
                    continue
                if asset_class and asset_class not in ('Акции', 'Stocks', 'Опционы на акции и индексы', 'Stock Options', 'Варранты', 'Warrants'):
                    continue

                currency = self._get_value(row, header_map, ['Валюта', 'Currency']).upper()
                symbol = self._get_value(row, header_map, ['Символ', 'Symbol']).strip()
                group_symbol = symbol
                if asset_class in ('Опционы на акции и индексы', 'Stock Options'):
                    group_symbol = f"OPTION_{symbol}"
                elif asset_class in ('Варранты', 'Warrants'):
                    group_symbol = f"WARRANT_{symbol}"
                datetime_raw = self._get_value(row, header_map, ['Дата/Время', 'Date/Time'])
                quantity_raw = self._get_value(row, header_map, ['Количество', 'Quantity'])
                price_raw = self._get_value(row, header_map, ['Цена транзакции', 'T. Price', 'Trade Price'])
                commission_raw = self._get_value(row, header_map, ['Комиссия/плата', 'Comm/Fee', 'Комиссия в USD'])
                proceeds_raw = self._get_value(row, header_map, ['Выручка', 'Proceeds'])
                basis_raw = self._get_value(row, header_map, ['Базис', 'Basis'])
                code_raw = self._get_value(row, header_map, ['Код', 'Code'])

                quantity = self._parse_decimal(quantity_raw)
                if quantity == 0:
                    continue

                dt_obj = self._parse_datetime(datetime_raw)

                # Проверяем код сделки: Ep = Expired (истёкший опцион)
                # Для истёкших опционов используем стандартную логику по знаку quantity:
                # - Положительный quantity = закрытие шорта = buy
                # - Отрицательный quantity = закрытие лонга = sell
                is_expired = 'Ep' in (code_raw or '')

                operation = 'buy' if quantity > 0 else 'sell'
                trade_id = f"IB_{symbol}_{dt_obj.strftime('%Y%m%d%H%M%S') if dt_obj else trade_index}_{trade_index}"
                cbr_rate = self._get_cbr_rate(currency, dt_obj) or Decimal(0)

                price = self._parse_decimal(price_raw)
                proceeds = self._parse_decimal(proceeds_raw)

                # Для опционов и варрантов: используем множитель из секции "Информация о финансовом инструменте"
                # IB уже учитывает множитель в колонке Proceeds, но не в Price
                is_option = asset_class in ('Опционы на акции и индексы', 'Stock Options')
                is_warrant = asset_class in ('Варранты', 'Warrants')
                is_pfi = is_option or is_warrant  # ПФИ = производные финансовые инструменты
                if is_option:
                    # Для опционов пытаемся получить множитель из инструментальной информации
                    multiplier = symbol_to_multiplier.get(symbol)
                    if multiplier is None:
                        # Если не нашли, используем стандартное значение 100
                        multiplier = Decimal(100)
                elif is_warrant:
                    # Для варрантов берём множитель из инструментальной информации (обычно 1)
                    multiplier = symbol_to_multiplier.get(symbol) or Decimal(1)
                else:
                    # Для акций множитель всегда 1
                    multiplier = Decimal(1)

                # Код дохода: 1530 для акций, 1532 для ПФИ (опционов, варрантов)
                income_code = '1532' if is_pfi else '1530'

                # Округляем комиссию до сотых для соответствия с отчетом IB
                commission = abs(self._parse_decimal(commission_raw))
                commission = commission.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) if commission else Decimal(0)

                # Парсим Базис для истёкших опционов (стоимость покупки)
                basis = self._parse_decimal(basis_raw)

                trade = {
                    'trade_id': trade_id,
                    'datetime_obj': dt_obj,
                    'operation': operation,
                    'symbol': symbol,
                    'group_symbol': group_symbol,
                    'quantity': abs(quantity),
                    'price': price,
                    'proceeds': abs(proceeds),
                    'multiplier': multiplier,
                    'commission': commission,
                    'currency': currency,
                    'cbr_rate': cbr_rate,
                    'instr_kind': asset_class,
                    'income_code': income_code,
                    'isin': symbol_to_isin.get(symbol, ''),
                    'instr_nm': symbol_to_name.get(symbol, symbol),
                    'is_expired': is_expired,
                    'basis': basis,  # Стоимость покупки (для истёкших опционов)
                }
                trades.append(trade)
                trade_index += 1

        trades.sort(key=lambda x: x.get('datetime_obj') or datetime.min)
        return trades

    def _parse_dividends(self, sections):
        dividends = []
        dividends_blocks = sections.get('Дивиденды') or []
        tax_blocks = sections.get('Удерживаемый налог') or []

        tax_by_match_key = defaultdict(Decimal)
        tax_by_fallback_key = defaultdict(Decimal)
        for block in tax_blocks:
            header_map_tax = self._header_map(block.get('header', []))
            for row in block.get('data', []):
                date_raw = self._get_value(row, header_map_tax, ['Дата', 'Date'])
                desc = self._get_value(row, header_map_tax, ['Описание', 'Description'])
                currency = self._get_value(row, header_map_tax, ['Валюта', 'Currency']).upper()
                amount = self._parse_decimal(self._get_value(row, header_map_tax, ['Сумма', 'Amount']))
                ticker, _ = self._extract_symbol_isin(desc)
                dt_obj = self._parse_datetime(date_raw)
                if not dt_obj or dt_obj.year != self.target_year or not ticker or amount == 0:
                    continue
                normalized_desc = self._normalize_dividend_description(desc)
                match_key = self._make_dividend_match_key(dt_obj.date(), currency, normalized_desc)
                if match_key:
                    tax_by_match_key[match_key] += amount
                tax_by_fallback_key[(dt_obj.date(), ticker, currency)] += amount

        if not dividends_blocks:
            return dividends

        dividend_fallback_key_counts = defaultdict(int)
        for block in dividends_blocks:
            header_map = self._header_map(block.get('header', []))
            for row in block.get('data', []):
                date_raw = self._get_value(row, header_map, ['Дата', 'Date'])
                desc = self._get_value(row, header_map, ['Описание', 'Description'])
                currency = self._get_value(row, header_map, ['Валюта', 'Currency']).upper()
                amount = self._parse_decimal(self._get_value(row, header_map, ['Сумма', 'Amount']))
                dt_obj = self._parse_datetime(date_raw)
                if not dt_obj or dt_obj.year != self.target_year:
                    continue

                ticker, isin = self._extract_symbol_isin(desc)
                if not ticker:
                    continue

                dividend_key = self._normalize_dividend_description(desc)
                dividend_match_key = self._make_dividend_match_key(dt_obj.date(), currency, dividend_key)
                tax_amount = tax_by_match_key.get(dividend_match_key, Decimal(0)) if dividend_match_key else Decimal(0)
                fallback_key = (dt_obj.date(), ticker, currency)
                dividend_fallback_key_counts[fallback_key] += 1
                cbr_rate = self._get_cbr_rate(currency, dt_obj) or Decimal(0)
                amount_rub = (amount * cbr_rate).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) if cbr_rate else Decimal(0)

                dividends.append({
                    'date': dt_obj.date(),
                    'ticker': ticker,
                    'instrument_name': isin or '',
                    'amount': amount,
                    'tax_amount': tax_amount,
                    'currency': currency,
                    'cbr_rate_str': f"{cbr_rate:.4f}" if cbr_rate else '-',
                    'amount_rub': amount_rub,
                    'dividend_key': dividend_key,  # Ключ для связывания с комиссией
                    'dividend_match_key': dividend_match_key,  # Ключ для точного сопоставления (дата+валюта+описание)
                    '_dividend_tax_fallback_key': fallback_key,
                })

        # Fallback: если точного совпадения по описанию нет, то берём налог по (дата, тикер, валюта)
        # только когда на эту дату ровно одна строка дивиденда для тикера (иначе будет дублирование).
        for div in dividends:
            if div.get('tax_amount'):
                continue
            fallback_key = div.get('_dividend_tax_fallback_key')
            if fallback_key and dividend_fallback_key_counts.get(fallback_key, 0) == 1:
                div['tax_amount'] = tax_by_fallback_key.get(fallback_key, Decimal(0))
            div.pop('_dividend_tax_fallback_key', None)

        dividends.sort(key=lambda x: (x.get('date') or date.min, x.get('ticker') or ''))
        return dividends

    def _parse_dividend_accrual_payments(self, sections):
        """Парсит секцию 'Изменения в начислениях дивидендов' и возвращает набор платежей.

        Возвращает set of tuples: (ticker, abs(payment_amount))
        Эти платежи (ADR fees и др.) связаны с дивидендами.
        """
        payments = set()
        accrual_blocks = sections.get('Изменения в начислениях дивидендов') or []
        if not accrual_blocks:
            return payments

        for block in accrual_blocks:
            header_map = self._header_map(block.get('header', []))
            for row in block.get('data', []):
                symbol = self._get_value(row, header_map, ['Символ', 'Symbol'])
                payment = self._parse_decimal(self._get_value(row, header_map, ['Платеж', 'Payment']))
                if symbol and payment and payment != 0:
                    # Сохраняем абсолютное значение для сопоставления
                    payments.add((symbol.strip(), abs(payment)))
        return payments

    def _parse_fees(self, sections, other_commissions, dividend_commissions, dividend_accrual_payments=None):
        """Парсит секцию Сборы/комиссии.

        ADR fees и комиссии связанные с дивидендами идут в dividend_commissions.
        Остальные комиссии идут в other_commissions.

        dividend_accrual_payments - набор (ticker, amount) из секции "Изменения в начислениях дивидендов"
        для определения связи комиссии с дивидендами.
        """
        if dividend_accrual_payments is None:
            dividend_accrual_payments = set()

        fee_blocks = sections.get('Сборы/комиссии') or []
        if not fee_blocks:
            return
        for block in fee_blocks:
            header_map = self._header_map(block.get('header', []))
            for row in block.get('data', []):
                subtitle = self._get_value(row, header_map, ['Subtitle']) or 'Прочие комиссии'
                currency = self._get_value(row, header_map, ['Валюта', 'Currency']).upper()
                date_raw = self._get_value(row, header_map, ['Дата', 'Date'])
                description = self._get_value(row, header_map, ['Описание', 'Description'])
                amount = self._parse_decimal(self._get_value(row, header_map, ['Сумма', 'Amount']))
                dt_obj = self._parse_datetime(date_raw)
                if not dt_obj or amount == 0:
                    continue
                # Учитываем комиссии за целевой год И за декабрь предыдущего года
                # (IB может включить декабрьские комиссии в отчёт следующего года из-за ребилинга)
                is_target_year = dt_obj.year == self.target_year
                is_prev_december = dt_obj.year == self.target_year - 1 and dt_obj.month == 12
                if not (is_target_year or is_prev_december):
                    continue

                # Извлекаем тикер из описания для проверки
                ticker, isin = self._extract_symbol_isin(description)

                # Проверяем, связана ли комиссия с дивидендами:
                # 1. По названию (содержит "дивиденд", "dividend", или "- FEE" в конце)
                # 2. Или если платёж есть в секции "Изменения в начислениях дивидендов"
                desc_lower = (description or '').lower()
                is_dividend_related_by_name = (
                    bool(re.search(r'\s*-\s*FEE\s*$', description or '', flags=re.IGNORECASE))
                    or 'дивиденд' in desc_lower
                    or 'dividend' in desc_lower
                )
                # Проверяем наличие платежа в "Изменениях в начислениях дивидендов"
                is_in_dividend_accruals = ticker and (ticker, abs(amount)) in dividend_accrual_payments
                is_dividend_related = is_dividend_related_by_name or is_in_dividend_accruals

                if is_dividend_related:
                    # ticker и isin уже извлечены выше
                    category = ticker or "Комиссии по дивидендам"
                    dividend_key = self._normalize_dividend_description(description)
                    dividend_match_key = self._make_dividend_match_key(dt_obj.date(), currency, dividend_key)
                    self._add_dividend_commission(
                        dividend_commissions,
                        category,
                        amount,
                        currency,
                        dt_obj,
                        description,
                        isin,
                        dividend_key,
                        dividend_match_key,
                    )
                else:
                    self._add_other_commission(other_commissions, subtitle, amount, currency, dt_obj, description)

    def _parse_interest(self, sections, other_commissions):
        """Парсит проценты (Interest) и добавляет их в other_commissions.

        Положительные суммы = кредитные проценты (доход)
        Отрицательные суммы = дебетовые проценты/маржа (расход)
        Все учитываются в общей сумме прочих расходов/доходов.
        """
        interest_blocks = sections.get('Процент') or []
        if not interest_blocks:
            return
        for block in interest_blocks:
            header_map = self._header_map(block.get('header', []))
            for row in block.get('data', []):
                currency = self._get_value(row, header_map, ['Валюта', 'Currency']).upper()
                date_raw = self._get_value(row, header_map, ['Дата', 'Date'])
                description = self._get_value(row, header_map, ['Описание', 'Description'])
                amount = self._parse_decimal(self._get_value(row, header_map, ['Сумма', 'Amount']))
                dt_obj = self._parse_datetime(date_raw)
                if not dt_obj or dt_obj.year != self.target_year or amount == 0:
                    continue
                # Определяем категорию по знаку суммы
                if amount > 0:
                    category = 'Кредитные проценты (доход)'
                else:
                    category = 'Проценты за маржинальное кредитование'
                self._add_other_commission(other_commissions, category, amount, currency, dt_obj, description)

    def _extract_symbol_isin(self, description):
        if not description:
            return None, None
        match = re.match(r'^([A-Z0-9.\-]+)\s*\(([A-Z0-9]{12})\)', description.strip())
        if not match:
            return None, None
        return match.group(1), match.group(2)

    def _make_dividend_match_key(self, dt_value, currency, normalized_description):
        if not dt_value or not normalized_description:
            return None
        currency_code = (currency or '').upper().strip()
        if not currency_code:
            return None
        if isinstance(dt_value, datetime):
            dt_value = dt_value.date()
        if not isinstance(dt_value, date):
            return None
        return f"{dt_value.isoformat()}|{currency_code}|{normalized_description}"

    def _parse_corporate_actions(self, sections, symbol_to_name=None):
        """Парсит корпоративные действия.

        Возвращает кортеж (conversions, acquisitions):
        - conversions: конвертации одного инструмента в другой (с переносом стоимости)
        - acquisitions: получения инструментов через корп. действия (выдача прав, подписка)
        """
        if symbol_to_name is None:
            symbol_to_name = {}
        corp_blocks = sections.get('Корпоративные действия') or []
        if not corp_blocks:
            return [], []

        # Парсим все корп. действия с полной информацией
        all_events = []
        for block in corp_blocks:
            header_map = self._header_map(block.get('header', []))
            for row in block.get('data', []):
                asset_class = self._get_value(row, header_map, ['Класс актива', 'Asset Class'])
                # Пропускаем итоговые строки
                if asset_class in ('Всего', 'Всего в USD', 'Total', 'Total in USD'):
                    continue

                symbol = self._get_value(row, header_map, ['Символ', 'Symbol'])
                symbol = symbol.strip() if symbol else ''
                description = self._get_value(row, header_map, ['Описание', 'Description'])
                quantity = self._parse_decimal(self._get_value(row, header_map, ['Количество', 'Quantity']))
                date_raw = self._get_value(row, header_map, ['Дата/Время', 'Date/Time'])
                currency = self._get_value(row, header_map, ['Валюта', 'Currency']).upper()
                proceeds = self._parse_decimal(self._get_value(row, header_map, ['Выручка', 'Proceeds']))
                value = self._parse_decimal(self._get_value(row, header_map, ['Стоимость', 'Value']))
                row_security_id = self._get_value(row, header_map, ['Идентификатор ценной бумаги', 'Security ID'])
                row_security_id = row_security_id.strip() if isinstance(row_security_id, str) else ''
                if row_security_id and not re.match(r'^[A-Z0-9]{12}$', row_security_id):
                    row_security_id = ''

                dt_obj = self._parse_datetime(date_raw)
                if not dt_obj or quantity == 0:
                    continue

                # Ищем паттерны: TICKER(ISIN) и (TICKER, описание, ISIN)
                pairs_raw = re.findall(r'([A-Z0-9\\.]+)\(([A-Z0-9]{12})\)', description or '')
                alt_pairs = re.findall(r'\(([A-Z0-9\\.]+),\s*[^,]+,\s*([A-Z0-9]{12})\)', description or '')
                pairs_raw.extend(alt_pairs)
                # Убираем дубли (в IB Description часто повторяются одинаковые пары)
                pairs = []
                seen_pairs = set()
                for tck, isn in pairs_raw:
                    key = (tck, isn)
                    if key in seen_pairs:
                        continue
                    seen_pairs.add(key)
                    pairs.append((tck, isn))
                if len(pairs) < 1:
                    continue

                # Определяем тип события по описанию
                # Убираем последние скобки с названием инструмента, чтобы не путать
                # слова типа "SUBSCRIPTION" в названии с типом действия "Подписка"
                action_part = re.sub(r'\s*\([^()]+\)\s*$', '', description or '')
                action_lower = action_part.lower()
                is_rights_issue = 'выдача прав' in action_lower or 'rights issue' in action_lower
                is_subscription = 'подписка' in action_lower or 'subscription' in action_lower
                is_merger = 'слияние' in action_lower or 'merger' in action_lower
                is_spinoff = 'спин-офф' in action_lower or 'spin-off' in action_lower or 'spinoff' in action_lower

                # Определяем "тикер строки" (какой инструмент именно списан/получен в этой записи).
                # В реальном IB CSV он есть в колонке "Символ". Если колонки нет (например, в тестах),
                # восстанавливаем по знаку количества: для списаний берём первый тикер, для получений — последний.
                row_ticker = None
                row_isin = None
                other_ticker = None
                other_isin = None

                if symbol:
                    row_ticker = symbol
                    for tck, isn in pairs:
                        if tck == row_ticker:
                            row_isin = isn
                            break
                    if row_isin is None and row_security_id:
                        row_isin = row_security_id
                    other = next(((tck, isn) for (tck, isn) in reversed(pairs) if tck != row_ticker), None)
                    if other is None:
                        other = next(((tck, isn) for (tck, isn) in pairs if tck != row_ticker), None)
                    if other:
                        other_ticker, other_isin = other
                else:
                    if len(pairs) >= 2:
                        if quantity < 0:
                            row_ticker, row_isin = pairs[0]
                            other_ticker, other_isin = pairs[-1]
                        else:
                            row_ticker, row_isin = pairs[-1]
                            other_ticker, other_isin = pairs[0]
                    else:
                        row_ticker, row_isin = pairs[0]
                    if row_isin is None and row_security_id:
                        row_isin = row_security_id

                # Нормализуем: old/new пары для удобства дальнейшей агрегации.
                old_ticker = old_isin = new_ticker = new_isin = None
                if row_ticker and other_ticker and row_ticker != other_ticker:
                    if quantity < 0:
                        old_ticker, old_isin = row_ticker, row_isin
                        new_ticker, new_isin = other_ticker, other_isin
                    else:
                        old_ticker, old_isin = other_ticker, other_isin
                        new_ticker, new_isin = row_ticker, row_isin

                all_events.append({
                    'dt_obj': dt_obj,
                    'date': dt_obj.date(),
                    'asset_class': asset_class,
                    'currency': currency,
                    'symbol': symbol,
                    'description': description,
                    'quantity': quantity,
                    'proceeds': proceeds,
                    'value': value,
                    'row_ticker': row_ticker,
                    'row_isin': row_isin,
                    'other_ticker': other_ticker,
                    'other_isin': other_isin,
                    'old_ticker': old_ticker,
                    'old_isin': old_isin,
                    'new_ticker': new_ticker,
                    'new_isin': new_isin,
                    'is_rights_issue': is_rights_issue,
                    'is_subscription': is_subscription,
                    'is_merger': is_merger,
                    'is_spinoff': is_spinoff,
                    'pairs': pairs,
                })

        # Группируем события по дате и нормализованному описанию для связывания пар
        # Ключ: (дата, базовое описание без конкретного тикера результата)
        def normalize_description(desc):
            """Извлекаем базовое описание для связывания событий."""
            # Убираем последние скобки с результатом
            match = re.match(r'^(.+?)\s*\([^()]+\)\s*$', desc or '')
            return match.group(1).strip() if match else (desc or '').strip()

        events_by_base = defaultdict(list)
        for ev in all_events:
            base_desc = normalize_description(ev['description'])
            key = (ev['date'], base_desc)
            events_by_base[key].append(ev)

        # Собираем конвертации и приобретения
        conversions = []
        acquisitions = []
        processed_events = set()

        for key, events in events_by_base.items():
            date, base_desc = key

            # Случай 1: выдача прав / spin-off (получение без списания, себестоимость = 0)
            for ev in events:
                ev_id = id(ev)
                if ev_id in processed_events:
                    continue
                if ev.get('quantity', Decimal(0)) <= 0:
                    continue
                if not (ev.get('is_rights_issue') or ev.get('is_spinoff')):
                    continue

                acquisitions.append({
                    'datetime_obj': ev['dt_obj'],
                    'ticker': ev.get('row_ticker') or '',
                    'isin': ev.get('row_isin') or '',
                    'quantity': ev['quantity'],
                    'currency': ev.get('currency') or 'USD',
                    'cost': Decimal(0),
                    'value': ev.get('value', Decimal(0)),
                    'comment': ev.get('description', ''),
                    'asset_class': ev.get('asset_class', ''),
                    'source_ticker': ev.get('other_ticker') or '',
                    'source_isin': ev.get('other_isin') or '',
                    'type': 'rights_issue' if ev.get('is_rights_issue') else 'spinoff',
                })
                processed_events.add(ev_id)

            # Случай 2: подписка (списание прав + получение подписки, себестоимость = оплаченная сумма)
            subscription_removals = [e for e in events if e.get('is_subscription') and e.get('quantity', Decimal(0)) < 0]
            subscription_receives = [e for e in events if e.get('is_subscription') and e.get('quantity', Decimal(0)) > 0]

            for receive in subscription_receives:
                receive_id = id(receive)
                if receive_id in processed_events:
                    continue

                matched_removal = None
                for removal in subscription_removals:
                    removal_id = id(removal)
                    if removal_id in processed_events:
                        continue
                    if (
                        removal.get('old_ticker') == receive.get('old_ticker')
                        and removal.get('new_ticker') == receive.get('new_ticker')
                    ):
                        matched_removal = removal
                        break

                if matched_removal is None and subscription_removals:
                    matched_removal = next((r for r in subscription_removals if id(r) not in processed_events), None)

                cost_paid = Decimal(0)
                if matched_removal:
                    for amt in (matched_removal.get('proceeds', Decimal(0)), matched_removal.get('value', Decimal(0))):
                        if amt:
                            cost_paid = abs(amt)
                            break

                acq_data = {
                    'datetime_obj': receive['dt_obj'],
                    'ticker': receive.get('row_ticker') or '',
                    'isin': receive.get('row_isin') or '',
                    'quantity': receive['quantity'],
                    'currency': receive.get('currency') or (matched_removal.get('currency') if matched_removal else 'USD'),
                    'cost': cost_paid,
                    'value': receive.get('value', Decimal(0)),
                    'comment': receive.get('description', ''),
                    'asset_class': receive.get('asset_class', ''),
                    'source_ticker': (matched_removal.get('row_ticker') if matched_removal else '') or '',
                    'source_isin': (matched_removal.get('row_isin') if matched_removal else '') or '',
                    'type': 'subscription',
                }
                print(f"[DEBUG] Creating subscription acquisition: ticker={acq_data['ticker']}, cost={cost_paid}, source={acq_data['source_ticker']}")
                acquisitions.append(acq_data)

                processed_events.add(receive_id)
                if matched_removal:
                    processed_events.add(id(matched_removal))

            # Случай 3: конвертация (перенос стоимости)
            # Исключаем подписку/выдачу прав/spin-off: они обрабатываются как acquisitions.
            conversion_acc = defaultdict(lambda: {
                'datetime_obj': None,
                'old_qty_removed': Decimal(0),
                'new_qty_received': Decimal(0),
                'currency': '',
                'proceeds': Decimal(0),
                'value': Decimal(0),
                'comment': '',
                'asset_class_from': '',
                'asset_class_to': '',
            })

            # Сначала собираем события конвертации, разделяя на "с тикерами" и "без тикеров"
            conversion_events = []
            for ev in events:
                if id(ev) in processed_events:
                    continue
                if ev.get('is_subscription') or ev.get('is_rights_issue') or ev.get('is_spinoff'):
                    continue
                conversion_events.append(ev)

            # Находим строки получения с разными тикерами (они содержат информацию о конвертации)
            receives_with_tickers = []
            removals_without_tickers = []
            for ev in conversion_events:
                old_ticker = ev.get('old_ticker')
                new_ticker = ev.get('new_ticker')
                qty = ev.get('quantity', Decimal(0))
                has_valid_tickers = old_ticker and new_ticker and old_ticker != new_ticker

                if has_valid_tickers:
                    receives_with_tickers.append(ev)
                elif qty < 0:
                    # Строка списания без валидных тикеров - попробуем связать с получением
                    removals_without_tickers.append(ev)

            # Связываем строки списания без тикеров со строками получения по row_ticker
            for removal in removals_without_tickers:
                removal_ticker = removal.get('row_ticker')
                if not removal_ticker:
                    continue
                # Ищем строку получения, где old_ticker совпадает с тикером списания
                for receive in receives_with_tickers:
                    if receive.get('old_ticker') == removal_ticker:
                        # Нашли пару - копируем тикеры из получения в списание
                        removal['old_ticker'] = receive.get('old_ticker')
                        removal['new_ticker'] = receive.get('new_ticker')
                        removal['old_isin'] = receive.get('old_isin')
                        removal['new_isin'] = receive.get('new_isin')
                        break

            # Теперь обрабатываем все события конвертации
            for ev in conversion_events:
                old_ticker = ev.get('old_ticker')
                new_ticker = ev.get('new_ticker')
                old_isin = ev.get('old_isin')
                new_isin = ev.get('new_isin')
                if not old_ticker or not new_ticker or old_ticker == new_ticker:
                    continue

                pair_key = (old_ticker, new_ticker, old_isin or '', new_isin or '')
                acc = conversion_acc[pair_key]
                if acc['datetime_obj'] is None or ev.get('dt_obj') < acc['datetime_obj']:
                    acc['datetime_obj'] = ev.get('dt_obj')
                acc['comment'] = acc['comment'] or (ev.get('description') or '')

                qty = ev.get('quantity', Decimal(0))
                if qty < 0:
                    acc['old_qty_removed'] += abs(qty)
                    acc['currency'] = ev.get('currency') or acc['currency']
                    acc['proceeds'] = ev.get('proceeds', Decimal(0))
                    acc['asset_class_from'] = ev.get('asset_class') or acc['asset_class_from']
                else:
                    acc['new_qty_received'] += qty
                    acc['value'] = ev.get('value', Decimal(0))
                    acc['asset_class_to'] = ev.get('asset_class') or acc['asset_class_to']

            for (old_ticker, new_ticker, old_isin, new_isin), acc in conversion_acc.items():
                if acc['old_qty_removed'] <= 0 or acc['new_qty_received'] <= 0:
                    continue
                is_technical = self._is_technical_rename(
                    old_ticker, new_ticker, old_isin, new_isin,
                    acc['old_qty_removed'], acc['new_qty_received'], symbol_to_name
                )
                if is_technical:
                    continue
                conversions.append({
                    'datetime_obj': acc['datetime_obj'],
                    'old_ticker': old_ticker,
                    'new_ticker': new_ticker,
                    'old_isin': old_isin,
                    'new_isin': new_isin,
                    'old_qty_removed': acc['old_qty_removed'],
                    'new_qty_received': acc['new_qty_received'],
                    'currency': acc['currency'],
                    'proceeds': acc['proceeds'],
                    'value': acc['value'],
                    'comment': acc['comment'],
                    'asset_class_from': acc.get('asset_class_from', ''),
                    'asset_class_to': acc.get('asset_class_to', ''),
                })

        return conversions, acquisitions

    def _is_technical_rename(self, old_ticker, new_ticker, old_isin, new_isin, old_qty, new_qty, symbol_to_name):
        """Проверяет, является ли изменение техническим переименованием тикера."""
        # 1. Если названия инструментов совпадают и количество почти не меняется
        old_name = symbol_to_name.get(old_ticker, '').strip()
        new_name = symbol_to_name.get(new_ticker, '').strip()
        if old_name and new_name and old_name == new_name:
            if old_qty > 0 and new_qty > 0:
                change_ratio = abs(1 - (new_qty / old_qty))
                if change_ratio < Decimal('0.01'):
                    return True

        # 2. Новый тикер = старый + суффикс, и ISIN не меняется
        if new_ticker.startswith(old_ticker + '.'):
            if not (old_isin and new_isin and old_isin != new_isin):
                return True

        # 3. Количество не меняется и ISIN не меняется
        if old_qty > 0 and new_qty > 0:
            qty_diff = abs(old_qty - new_qty)
            isin_changed = old_isin and new_isin and old_isin != new_isin
            if qty_diff < Decimal('0.01') and not isin_changed:
                return True

        # 4. Один тикер содержится в другом, количество почти не меняется, ISIN не меняется
        if old_ticker in new_ticker or new_ticker in old_ticker:
            if old_qty > 0 and new_qty > 0:
                change_ratio = abs(1 - (new_qty / old_qty))
                isin_changed = old_isin and new_isin and old_isin != new_isin
                if change_ratio < Decimal('0.10') and not isin_changed:
                    return True

        return False

    def _record_commission_from_trade(self, row, header_map, other_commissions):
        currency = self._get_value(row, header_map, ['Валюта', 'Currency']).upper()
        datetime_raw = self._get_value(row, header_map, ['Дата/Время', 'Date/Time'])
        commission_raw = self._get_value(row, header_map, ['Комиссия/плата', 'Comm/Fee', 'Комиссия в USD'])
        amount = self._parse_decimal(commission_raw)
        # Округляем комиссию до сотых
        amount = amount.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) if amount else Decimal(0)
        dt_obj = self._parse_datetime(datetime_raw)
        if not dt_obj or dt_obj.year != self.target_year or amount == 0:
            return
        self._add_other_commission(other_commissions, 'FX комиссии', amount, currency, dt_obj, 'Forex trade commission')

    def _add_other_commission(self, other_commissions, category, amount, currency, dt_obj, description):
        """Добавляет запись в other_commissions.

        Сохраняем оригинальный знак суммы:
        - Отрицательные = расходы (комиссии, проценты за маржу)
        - Положительные = доходы (кредитные проценты)
        """
        cbr_rate = self._get_cbr_rate(currency, dt_obj) or Decimal(0)
        amount_rub = (amount * cbr_rate).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) if cbr_rate else Decimal(0)
        other_commissions[category]['currencies'][currency] += amount
        other_commissions[category]['total_rub'] += amount_rub
        other_commissions[category]['raw_events'].append({
            'date': dt_obj.date(),
            'amount': amount,
            'currency': currency,
            'amount_rub': amount_rub,
            'description': description,
        })

    def _extract_ticker_from_fee_description(self, description):
        """Извлекает тикер из описания комиссии.

        Примеры:
        - "HSBK(US46627J3023) Наличный дивиденд USD 2.258938 на акцию - FEE" -> "HSBK"
        - "GLTR.OLD(US37949E2046) Наличный дивиденд USD 3.910916 на акцию - FEE" -> "GLTR.OLD"
        """
        if not description:
            return None
        match = re.match(r'^([A-Z0-9.]+)\(', description.strip())
        if match:
            return match.group(1)
        return None

    def _normalize_dividend_description(self, description):
        """Нормализует описание дивидендного события для связывания (дивиденд/налог/FEE).

        Убирает окончания:
        - " - FEE" (для комиссий)
        - " - XX Налог"/" - XX Tax" (для удержанного налога)
        - завершающие скобки "(...)" (тип/классификация выплаты)

        Примеры:
        - "GLTR(US37949E2046) Наличный дивиденд USD 0.61982 на акцию - FEE"
          -> "GLTR(US37949E2046) Наличный дивиденд USD 0.61982 на акцию"
        - "GLTR(US37949E2046) Наличный дивиденд USD 0.61982 на акцию (Обыкновенный дивиденд)"
          -> "GLTR(US37949E2046) Наличный дивиденд USD 0.61982 на акцию"
        - "FMC(US3024913036) Выплата в качестве дивиденда - US Налог"
          -> "FMC(US3024913036) Выплата в качестве дивиденда"
        """
        if not description:
            return None
        desc = description.strip()
        # Убираем " - FEE"
        desc = re.sub(r'\s*-\s*FEE\s*$', '', desc, flags=re.IGNORECASE)
        # Убираем " - XX Налог" / " - XX Tax" в конце
        desc = re.sub(r'\s*-\s*[A-Z]{2}\s*(налог|tax)\s*$', '', desc, flags=re.IGNORECASE)
        # Убираем завершающие скобки (тип выплаты): "(...)" в конце (в т.ч. несколько подряд)
        desc = re.sub(r'(?:\s*\([^)]*\)\s*)+$', '', desc)
        return desc.strip() if desc else None

    def _add_dividend_commission(
        self,
        dividend_commissions,
        category,
        amount,
        currency,
        dt_obj,
        description,
        isin=None,
        dividend_key=None,
        dividend_match_key=None,
    ):
        """Добавляет запись в dividend_commissions.

        Структура совместима с FFG: amount_by_currency, amount_rub, details.
        Сохраняем оригинальный знак: отрицательные = расходы.
        dividend_key - нормализованное описание для связывания с дивидендом.
        """
        cbr_rate = self._get_cbr_rate(currency, dt_obj) or Decimal(0)
        amount_rub = (amount * cbr_rate).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) if cbr_rate else Decimal(0)
        dividend_commissions[category]['amount_by_currency'][currency] += amount
        dividend_commissions[category]['amount_rub'] += amount_rub
        # Сохраняем ISIN на уровне категории для поиска по ISIN
        if isin and 'isin' not in dividend_commissions[category]:
            dividend_commissions[category]['isin'] = isin
        dividend_commissions[category]['details'].append({
            'date': dt_obj.strftime('%d.%m.%Y'),
            'date_obj': dt_obj.date(),
            'amount': amount,
            'currency': currency,
            'amount_rub': amount_rub,
            'comment': description,
            'isin': isin,
            'dividend_key': dividend_key,  # Ключ для связывания с дивидендом
            'dividend_match_key': dividend_match_key,  # Точный ключ (дата+валюта+описание)
        })

    def _build_fifo_history(self, trades, conversions, acquisitions=None, symbol_to_isin=None, symbol_to_name=None):
        buy_lots = defaultdict(deque)
        short_sales = defaultdict(deque)
        instrument_events = defaultdict(list)
        trade_details_by_id = {}
        symbols_with_sales_in_target_year = set()
        used_buy_ids_for_target_year = set()
        total_sales_profit_rub = Decimal(0)

        if acquisitions is None:
            acquisitions = []
        if symbol_to_isin is None:
            symbol_to_isin = {}
        if symbol_to_name is None:
            symbol_to_name = {}

        # Дополняем symbol_to_isin из конвертаций.
        # Это важно когда тикер сменился (LFC → LFCHY) и старый тикер
        # отсутствует в секции "Информация о финансовом инструменте" текущего отчёта.
        for conv in conversions:
            old_ticker = conv.get('old_ticker', '')
            old_isin = conv.get('old_isin', '')
            new_ticker = conv.get('new_ticker', '')
            new_isin = conv.get('new_isin', '')
            if old_ticker and old_isin and old_ticker not in symbol_to_isin:
                symbol_to_isin[old_ticker] = old_isin
            if new_ticker and new_isin and new_ticker not in symbol_to_isin:
                symbol_to_isin[new_ticker] = new_isin

        # Дополняем symbol_to_isin из acquisitions
        for acq in acquisitions:
            ticker = acq.get('ticker', '')
            isin = acq.get('isin', '')
            if ticker and isin and ticker not in symbol_to_isin:
                symbol_to_isin[ticker] = isin

        # Объединяем все события (сделки, конвертации, acquisitions) и сортируем по дате
        conversions_by_date = sorted(conversions, key=lambda x: x.get('datetime_obj') or datetime.min)
        acquisitions_by_date = sorted(acquisitions, key=lambda x: x.get('datetime_obj') or datetime.min)
        conversion_idx = 0
        acquisition_idx = 0

        for trade in trades:
            # Обрабатываем конвертации и acquisitions в хронологическом порядке до текущей сделки
            trade_dt = trade.get('datetime_obj') or datetime.max
            while True:
                next_conv = conversions_by_date[conversion_idx] if conversion_idx < len(conversions_by_date) else None
                next_acq = acquisitions_by_date[acquisition_idx] if acquisition_idx < len(acquisitions_by_date) else None

                next_conv_dt = (next_conv.get('datetime_obj') if next_conv else None) or datetime.min
                next_acq_dt = (next_acq.get('datetime_obj') if next_acq else None) or datetime.min

                has_conv = next_conv is not None and next_conv_dt <= trade_dt
                has_acq = next_acq is not None and next_acq_dt <= trade_dt

                if not has_conv and not has_acq:
                    break

                if has_conv and (not has_acq or next_conv_dt <= next_acq_dt):
                    self._apply_conversion(next_conv, buy_lots, instrument_events, symbol_to_isin, symbol_to_name)
                    conversion_idx += 1
                else:
                    self._apply_acquisition(next_acq, buy_lots, instrument_events, symbol_to_isin, symbol_to_name)
                    acquisition_idx += 1

            dt_obj = trade.get('datetime_obj')
            symbol = trade.get('group_symbol') or trade.get('symbol') or 'UNKNOWN'
            quantity = trade.get('quantity', Decimal(0))
            price = trade.get('price', Decimal(0))
            commission = trade.get('commission', Decimal(0))
            cbr_rate = trade.get('cbr_rate', Decimal(0))
            multiplier = trade.get('multiplier', Decimal(1))
            proceeds = trade.get('proceeds', price * quantity * multiplier)

            if trade['operation'] == 'buy':
                # Покрываем открытые шорты (если есть)
                remaining = quantity
                # Для опционов: сумма = цена * количество * множитель (100)
                cost_shares_rub = (proceeds * cbr_rate).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                cost_comm_rub = (commission * cbr_rate).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                cost_per_share_rub = Decimal(0)
                if quantity:
                    cost_per_share_rub = (cost_shares_rub + cost_comm_rub) / quantity

                while remaining > 0 and short_sales[symbol]:
                    short_entry = short_sales[symbol][0]
                    cover_qty = min(remaining, short_entry['qty_remaining'])
                    remaining -= cover_qty
                    short_entry['qty_remaining'] -= cover_qty

                    sell_details = short_entry.get('sell_details')
                    if sell_details:
                        sell_details['fifo_cost_rub_decimal'] += (cover_qty * cost_per_share_rub)
                        used_buy_ids = sell_details.setdefault('used_buy_ids', [])
                        if trade.get('trade_id') not in used_buy_ids:
                            used_buy_ids.append(trade.get('trade_id'))
                        if short_entry.get('sell_year') == self.target_year:
                            used_buy_ids_for_target_year.add(trade.get('trade_id'))

                    if short_entry['qty_remaining'] <= 0:
                        if sell_details:
                            sell_details['fifo_cost_rub_str'] = f"{sell_details['fifo_cost_rub_decimal']:.2f} (шорт, покр.)"
                        short_sales[symbol].popleft()
                    elif sell_details:
                        sell_details['fifo_cost_rub_str'] = f"Частично открытый шорт (тек. расх.: {sell_details['fifo_cost_rub_decimal']:.2f} RUB)"

                quantity_for_lots = remaining

                if quantity_for_lots > 0:
                    buy_lots[symbol].append({
                        'q_remaining': quantity_for_lots,
                        'cost_per_share_rub': cost_per_share_rub,
                        'lot_id': trade.get('trade_id'),
                    })
                fifo_cost_rub = None
                fifo_cost_str = None
                used_buy_ids = []
            else:
                # Для истёкших опционов: если нет лота покупки, создаём виртуальный на основе Базиса
                is_expired = trade.get('is_expired', False)
                basis = trade.get('basis', Decimal(0))
                if is_expired and not buy_lots[symbol] and basis > 0:
                    # Создаём виртуальную покупку на основе Базиса
                    # Базис в IB указан в валюте сделки, переводим в рубли
                    basis_rub = (basis * cbr_rate).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) if cbr_rate else Decimal(0)
                    cost_per_share_rub = (basis_rub / quantity) if quantity else Decimal(0)
                    virtual_lot_id = f"VIRTUAL_BUY_{trade.get('trade_id')}"
                    buy_lots[symbol].append({
                        'q_remaining': quantity,
                        'cost_per_share_rub': cost_per_share_rub,
                        'lot_id': virtual_lot_id,
                    })
                    # Добавляем событие виртуальной покупки для отображения
                    instrument_events[symbol].append({
                        'display_type': 'trade',
                        'datetime_obj': dt_obj,  # Дата та же (покупка была ранее, но отображаем здесь)
                        'event_details': {
                            'date': dt_obj.strftime('%Y-%m-%d %H:%M:%S') if dt_obj else '-',
                            'trade_id': virtual_lot_id,
                            'operation': 'buy',
                            'symbol': trade.get('symbol'),
                            'instr_nm': trade.get('instr_nm') or symbol,
                            'isin': trade.get('isin', ''),
                            'instr_kind': trade.get('instr_kind'),
                            'income_code': trade.get('income_code', '1530'),
                            'p': Decimal(0),  # Цена неизвестна
                            'curr_c': trade.get('currency'),
                            'cbr_rate': cbr_rate,
                            'q': quantity,
                            'multiplier': multiplier,
                            'summ': basis,  # Базис = стоимость покупки
                            'commission': Decimal(0),
                            'fifo_cost_rub_decimal': None,
                            'fifo_cost_rub_str': None,
                            'is_relevant_for_target_year': True,  # Покупка релевантна для истёкшего опциона
                            'used_buy_ids': [],
                            'link_colors': [],
                            'is_virtual_buy': True,  # Флаг виртуальной покупки
                            'virtual_buy_comment': 'Покупка опциона (из отчёта за предыдущий период)',
                        },
                    })
                    if virtual_lot_id:
                        trade_details_by_id[virtual_lot_id] = instrument_events[symbol][-1]['event_details']

                remaining = quantity
                fifo_cost_rub = Decimal(0)
                used_buy_ids = []
                commission_rub = (commission * cbr_rate).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) if cbr_rate else Decimal(0)
                while remaining > 0 and buy_lots[symbol]:
                    lot = buy_lots[symbol][0]
                    take = min(remaining, lot['q_remaining'])
                    fifo_cost_rub += (take * lot['cost_per_share_rub'])
                    lot['q_remaining'] -= take
                    remaining -= take
                    # Получаем ID оригинальных покупок (из конвертации или напрямую)
                    if lot.get('source_lot_ids'):
                        # Лот создан конвертацией - берём ID оригинальных покупок
                        for sid in lot['source_lot_ids']:
                            if sid not in used_buy_ids:
                                used_buy_ids.append(sid)
                    else:
                        # Обычный лот - берём его собственный lot_id
                        lot_id = lot.get('lot_id')
                        if lot_id and lot_id not in used_buy_ids:
                            used_buy_ids.append(lot_id)
                    if lot['q_remaining'] <= 0:
                        buy_lots[symbol].popleft()
                if remaining > 0:
                    short_sales[symbol].append({
                        'sell_id': trade.get('trade_id'),
                        'qty_remaining': remaining,
                        'sell_year': dt_obj.year if dt_obj else None,
                        'sell_details': None,
                    })
                    fifo_cost_rub += commission_rub
                    if fifo_cost_rub == commission_rub:
                        fifo_cost_str = f"Открытый шорт (расх.: {commission_rub:.2f} RUB)"
                    else:
                        fifo_cost_str = f"Частично открытый шорт (тек. расх.: {fifo_cost_rub:.2f} RUB)"
                else:
                    fifo_cost_str = f"{fifo_cost_rub:.2f}"

                if dt_obj and dt_obj.year == self.target_year:
                    symbols_with_sales_in_target_year.add(symbol)

            event_details = {
                'date': dt_obj.strftime('%Y-%m-%d %H:%M:%S') if dt_obj else '-',
                'trade_id': trade.get('trade_id'),
                'operation': trade.get('operation'),
                'symbol': trade.get('symbol'),
                'instr_nm': trade.get('instr_nm') or symbol,
                'isin': trade.get('isin', ''),
                'instr_kind': trade.get('instr_kind'),
                'income_code': trade.get('income_code', '1530'),
                'p': price,
                'curr_c': trade.get('currency'),
                'cbr_rate': cbr_rate,
                'q': quantity,
                'multiplier': multiplier,
                'summ': proceeds,  # Для опционов уже включает множитель 100
                'commission': commission,
                'fifo_cost_rub_decimal': fifo_cost_rub,
                'fifo_cost_rub_str': fifo_cost_str,
                'is_relevant_for_target_year': bool(dt_obj and dt_obj.year == self.target_year and trade.get('operation') == 'sell'),
                'used_buy_ids': used_buy_ids,
                'link_colors': [],
                'is_expired': trade.get('is_expired', False),  # Флаг истёкшего опциона
            }
            instrument_events[symbol].append({
                'display_type': 'trade',
                'datetime_obj': dt_obj,
                'event_details': event_details,
            })
            if event_details.get('trade_id'):
                trade_details_by_id[event_details['trade_id']] = event_details
                if trade.get('operation') == 'sell' and short_sales[symbol]:
                    last_short = short_sales[symbol][-1]
                    if last_short.get('sell_id') == event_details['trade_id']:
                        last_short['sell_details'] = event_details

        # Обрабатываем оставшиеся конвертации/acquisitions после всех сделок (тоже по времени)
        while conversion_idx < len(conversions_by_date) or acquisition_idx < len(acquisitions_by_date):
            next_conv = conversions_by_date[conversion_idx] if conversion_idx < len(conversions_by_date) else None
            next_acq = acquisitions_by_date[acquisition_idx] if acquisition_idx < len(acquisitions_by_date) else None

            next_conv_dt = (next_conv.get('datetime_obj') if next_conv else None) or datetime.max
            next_acq_dt = (next_acq.get('datetime_obj') if next_acq else None) or datetime.max

            if next_conv and (not next_acq or next_conv_dt <= next_acq_dt):
                self._apply_conversion(next_conv, buy_lots, instrument_events, symbol_to_isin, symbol_to_name)
                conversion_idx += 1
            elif next_acq:
                self._apply_acquisition(next_acq, buy_lots, instrument_events, symbol_to_isin, symbol_to_name)
                acquisition_idx += 1
            else:
                break

        for symbol, events in instrument_events.items():
            for event in events:
                if event.get('display_type') != 'trade':
                    continue
                details = event.get('event_details') or {}
                if details.get('operation') == 'sell':
                    dt_obj = event.get('datetime_obj')
                    if dt_obj and dt_obj.year == self.target_year:
                        used_buy_ids_for_target_year.update(details.get('used_buy_ids', []))

        for symbol, events in instrument_events.items():
            for event in events:
                if event.get('display_type') != 'trade':
                    continue
                details = event.get('event_details') or {}
                if details.get('operation') == 'buy':
                    trade_id = details.get('trade_id')
                    details['is_relevant_for_target_year'] = trade_id in used_buy_ids_for_target_year

        for symbol, events in instrument_events.items():
            if symbol in symbols_with_sales_in_target_year:
                for event in events:
                    if event.get('display_type') == 'conversion_info':
                        details = event.get('event_details') or {}
                        details['is_relevant_for_target_year'] = True

        # Цветовые связи покупка-продажа (как в FFG)
        available_colors = ['#4FC3F7', '#FF9800', '#66BB6A', '#AB47BC', '#EF5350', '#FFEB3B', '#26C6DA', '#FF7043']
        color_index = 0
        pair_to_color = {}
        trade_id_to_colors = {}

        for symbol, events in instrument_events.items():
            for event in events:
                if event.get('display_type') != 'trade':
                    continue
                details = event.get('event_details') or {}
                if details.get('operation') == 'sell' and details.get('is_relevant_for_target_year'):
                    sell_id = details.get('trade_id')
                    used_buy_ids = details.get('used_buy_ids', [])
                    unique_buy_ids = []
                    seen = set()
                    for buy_id in used_buy_ids:
                        if buy_id not in seen:
                            seen.add(buy_id)
                            unique_buy_ids.append(buy_id)
                    for buy_id in unique_buy_ids:
                        if buy_id in used_buy_ids_for_target_year:
                            pair_key = (buy_id, sell_id)
                            if pair_key not in pair_to_color:
                                pair_to_color[pair_key] = available_colors[color_index % len(available_colors)]
                                color_index += 1

        for symbol, events in instrument_events.items():
            for event in events:
                if event.get('display_type') != 'trade':
                    continue
                details = event.get('event_details') or {}
                trade_id = details.get('trade_id')
                if not trade_id:
                    continue
                colors = []
                for (buy_id, sell_id), color in pair_to_color.items():
                    if trade_id in (buy_id, sell_id) and color not in colors:
                        colors.append(color)
                details['link_colors'] = colors

        # Пересчет total_sales_profit_rub после учета шортов/покрытий
        # Разделение по кодам дохода: 1530 (акции), 1532 (ПФИ/опционы)
        profit_by_income_code = {'1530': Decimal(0), '1532': Decimal(0)}
        for symbol, events in instrument_events.items():
            for event in events:
                if event.get('display_type') != 'trade':
                    continue
                details = event.get('event_details') or {}
                if details.get('operation') == 'sell':
                    dt_obj = event.get('datetime_obj')
                    if dt_obj and dt_obj.year == self.target_year:
                        # Используем summ, которая уже учитывает множитель для опционов
                        income_rub = (details.get('summ', Decimal(0)) * details.get('cbr_rate', Decimal(0))).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                        fifo_cost_val = details.get('fifo_cost_rub_decimal', Decimal(0)) or Decimal(0)
                        profit = income_rub - fifo_cost_val
                        total_sales_profit_rub += profit
                        # Добавляем в соответствующий код дохода
                        income_code = details.get('income_code', '1530')
                        profit_by_income_code[income_code] = profit_by_income_code.get(income_code, Decimal(0)) + profit

        # Собираем карту конвертаций: старый_символ -> новый_символ и новый_символ -> старый_символ
        conversion_map_old_to_new = {}
        conversion_map_new_to_old = {}
        processed_conversion_ids = set()

        # Собираем все конвертации из всех инструментов
        all_conversion_events = []
        for symbol, events in instrument_events.items():
            for event in events:
                if event.get('display_type') == 'conversion_info':
                    all_conversion_events.append(event)

        # Сортируем конвертации по дате
        all_conversion_events.sort(key=lambda x: x.get('datetime_obj') or datetime.min)

        # Строим карту конвертаций
        for event in all_conversion_events:
            details = event.get('event_details', {})
            conv_id = id(event)  # Используем id события как уникальный идентификатор
            if conv_id not in processed_conversion_ids:
                old_symbol = details.get('old_symbol') or details.get('old_ticker')
                new_symbol = details.get('new_symbol') or details.get('new_ticker')
                if old_symbol and new_symbol and old_symbol != new_symbol:
                    conversion_map_old_to_new[old_symbol] = new_symbol
                    conversion_map_new_to_old[new_symbol] = old_symbol
                    processed_conversion_ids.add(conv_id)

        # Определяем релевантные символы для отображения (по аналогии с FFG)
        # Начинаем с символов, у которых были продажи в целевом году
        relevant_symbols_for_display = set()
        for sold_symbol in symbols_with_sales_in_target_year:
            relevant_symbols_for_display.add(sold_symbol)

            # Идем назад по цепочке конвертаций
            # Для варрантов (WARRANT_XXX) также проверяем символ без префикса
            temp_symbol = sold_symbol
            if temp_symbol.startswith('WARRANT_') and temp_symbol not in conversion_map_new_to_old:
                base_symbol = temp_symbol[8:]  # Убираем 'WARRANT_'
                if base_symbol in conversion_map_new_to_old:
                    temp_symbol = base_symbol
                    relevant_symbols_for_display.add(temp_symbol)
            visited = {temp_symbol, sold_symbol}
            while temp_symbol in conversion_map_new_to_old:
                prev_symbol = conversion_map_new_to_old[temp_symbol]
                if prev_symbol == temp_symbol or prev_symbol in visited:
                    break
                relevant_symbols_for_display.add(prev_symbol)
                temp_symbol = prev_symbol
                visited.add(prev_symbol)

            # Идем вперед по цепочке конвертаций
            temp_symbol = sold_symbol
            visited = {temp_symbol}
            while temp_symbol in conversion_map_old_to_new:
                next_symbol = conversion_map_old_to_new[temp_symbol]
                if next_symbol == temp_symbol or next_symbol in visited:
                    break
                relevant_symbols_for_display.add(next_symbol)
                temp_symbol = next_symbol
                visited.add(next_symbol)

        # Добавляем символы с тем же ISIN (для случаев смены тикера без корп. действия)
        # Пример: LFC -> LFCHY (тот же ISIN US16939P1066)
        isin_to_symbols = defaultdict(set)
        for sym, isin in symbol_to_isin.items():
            if isin:
                isin_to_symbols[isin].add(sym)

        symbols_to_add = set()
        for rel_symbol in relevant_symbols_for_display:
            rel_isin = symbol_to_isin.get(rel_symbol)
            if rel_isin and rel_isin in isin_to_symbols:
                for same_isin_symbol in isin_to_symbols[rel_isin]:
                    if same_isin_symbol not in relevant_symbols_for_display:
                        symbols_to_add.add(same_isin_symbol)
        relevant_symbols_for_display.update(symbols_to_add)

        # Группируем события по "финальному" символу в цепочке конвертаций
        # Показываем всю историю с пометкой релевантности, но фильтруем старые нерелевантные
        filtered_history = defaultdict(list)

        # Граница даты для старых нерелевантных событий (3 года назад от целевого года)
        cutoff_year = self.target_year - 3

        for symbol, events in instrument_events.items():
            # Проверяем, является ли этот символ релевантным
            chain_symbols = {symbol}
            temp_prev = symbol
            visited_prev = {temp_prev}
            while temp_prev in conversion_map_new_to_old:
                prev = conversion_map_new_to_old[temp_prev]
                if prev == temp_prev or prev in visited_prev:
                    break
                chain_symbols.add(prev)
                temp_prev = prev
                visited_prev.add(prev)

            temp_next = symbol
            visited_next = {temp_next}
            while temp_next in conversion_map_old_to_new:
                next_sym = conversion_map_old_to_new[temp_next]
                if next_sym == temp_next or next_sym in visited_next:
                    break
                chain_symbols.add(next_sym)
                temp_next = next_sym
                visited_next.add(next_sym)

            # Если символ не релевантен, пропускаем все его события
            if relevant_symbols_for_display.isdisjoint(chain_symbols):
                continue

            for event in events:
                display_type = event.get('display_type')
                event_details = event.get('event_details', {})
                dt_obj = event.get('datetime_obj')

                # Определяем релевантность события
                is_relevant = False

                if display_type == 'trade':
                    if event_details.get('operation') == 'sell':
                        # Продажи в целевом году релевантны
                        if dt_obj and dt_obj.year == self.target_year:
                            is_relevant = True
                            event_details['is_relevant_for_target_year'] = True
                        else:
                            event_details['is_relevant_for_target_year'] = False
                    elif event_details.get('operation') == 'buy':
                        # Покупки, использованные для продаж в целевом году, релевантны
                        trade_id = event_details.get('trade_id')
                        if trade_id and trade_id in used_buy_ids_for_target_year:
                            is_relevant = True
                            event_details['is_relevant_for_target_year'] = True
                        else:
                            event_details['is_relevant_for_target_year'] = False

                elif display_type == 'conversion_info':
                    # Конвертации всегда считаем релевантными (они уже отфильтрованы выше)
                    event_details['is_relevant_for_target_year'] = True
                    is_relevant = True

                elif display_type == 'initial_holding':
                    # Начальные остатки релевантны, если они в цепочке релевантных символов
                    # Это значит, что они связаны с конвертацией или с продажами в целевом году
                    event_details['is_relevant_for_target_year'] = True
                    is_relevant = True
                elif display_type == 'acquisition_info':
                    # acquisitions релевантны, если символ в цепочке релевантных (как conversion_info)
                    # Это нужно для отображения подписок/выдачи прав, которые привели к продажам в целевом году
                    event_details['is_relevant_for_target_year'] = True
                    is_relevant = True

                # Фильтруем нерелевантные события старше 3 лет
                if not is_relevant and dt_obj and dt_obj.year < cutoff_year:
                    continue

                # Определяем ключ группировки - самый новый символ в цепочке
                if display_type == 'conversion_info':
                    event_symbol = event_details.get('new_symbol') or symbol
                else:
                    event_symbol = symbol

                grouping_key = event_symbol
                visited = {grouping_key}
                while grouping_key in conversion_map_old_to_new:
                    next_symbol = conversion_map_old_to_new[grouping_key]
                    if next_symbol == grouping_key or next_symbol in visited:
                        break
                    grouping_key = next_symbol
                    visited.add(next_symbol)

                # Если не нашли через conversion_map, ищем по ISIN
                # Пример: LFC имеет тот же ISIN что и LFCHY, но нет конвертации LFC->LFCHY
                if grouping_key == event_symbol:
                    event_isin = symbol_to_isin.get(event_symbol)
                    if event_isin:
                        # Ищем символ с тем же ISIN, который есть в conversion_map
                        for same_isin_sym in isin_to_symbols.get(event_isin, []):
                            if same_isin_sym != event_symbol and same_isin_sym in conversion_map_old_to_new:
                                # Идём по цепочке от этого символа
                                temp_key = same_isin_sym
                                temp_visited = {temp_key}
                                while temp_key in conversion_map_old_to_new:
                                    next_sym = conversion_map_old_to_new[temp_key]
                                    if next_sym == temp_key or next_sym in temp_visited:
                                        break
                                    temp_key = next_sym
                                    temp_visited.add(next_sym)
                                grouping_key = temp_key
                                break

                # Для acquisition_info: если тикер есть в symbols_with_sales_in_target_year,
                # используем его как ключ группировки (чтобы подписка была рядом с продажей)
                if display_type == 'acquisition_info':
                    acq_ticker = event_details.get('ticker', '')
                    if acq_ticker in symbols_with_sales_in_target_year:
                        grouping_key = acq_ticker
                    # Также проверяем варрантный префикс для тикера
                    elif f"WARRANT_{acq_ticker}" in symbols_with_sales_in_target_year:
                        grouping_key = f"WARRANT_{acq_ticker}"

                # Проверяем, есть ли группа с префиксом WARRANT_ для этого символа
                # (варранты группируются с префиксом WARRANT_)
                warrant_key = f"WARRANT_{grouping_key}"
                if warrant_key in symbols_with_sales_in_target_year:
                    grouping_key = warrant_key

                if display_type == 'acquisition_info':
                    print(f"[DEBUG] Adding acquisition to filtered_history: symbol={symbol}, grouping_key={grouping_key}, ticker={event_details.get('ticker')}, is_relevant={event_details.get('is_relevant_for_target_year')}")

                filtered_history[grouping_key].append(event)

        # Сортируем события в каждой группе по дате
        for symbol in filtered_history:
            filtered_history[symbol].sort(key=lambda x: x.get('datetime_obj') or datetime.min)

        # Определяем диапазон дат для PDF (от первой релевантной покупки до последней релевантной продажи)
        for symbol, events in filtered_history.items():
            min_relevant_date = None
            max_relevant_date = None

            # Находим границы диапазона релевантных событий
            for event in events:
                event_details = event.get('event_details', {})
                dt_obj = event.get('datetime_obj')
                if event_details.get('is_relevant_for_target_year') and dt_obj:
                    if min_relevant_date is None or dt_obj < min_relevant_date:
                        min_relevant_date = dt_obj
                    if max_relevant_date is None or dt_obj > max_relevant_date:
                        max_relevant_date = dt_obj

            # Устанавливаем флаг is_in_pdf_range для каждого события
            for event in events:
                event_details = event.get('event_details', {})
                dt_obj = event.get('datetime_obj')
                if min_relevant_date and max_relevant_date and dt_obj:
                    event_details['is_in_pdf_range'] = min_relevant_date <= dt_obj <= max_relevant_date
                else:
                    event_details['is_in_pdf_range'] = event_details.get('is_relevant_for_target_year', False)

        # Преобразуем defaultdict обратно в обычный dict
        filtered_history = dict(filtered_history)

        return filtered_history, total_sales_profit_rub, profit_by_income_code

    def _apply_conversion(self, conv, buy_lots, instrument_events, symbol_to_isin=None, symbol_to_name=None):
        if symbol_to_isin is None:
            symbol_to_isin = {}
        if symbol_to_name is None:
            symbol_to_name = {}

        old_ticker = conv['old_ticker']
        new_ticker = conv['new_ticker']
        old_qty_removed = conv['old_qty_removed']
        new_qty_received = conv['new_qty_received']

        # Определяем group_symbol с учётом класса актива
        asset_class_to = conv.get('asset_class_to', '')
        if asset_class_to in ('Варранты', 'Warrants'):
            new_symbol = f"WARRANT_{new_ticker}"
        elif asset_class_to in ('Опционы на акции и индексы', 'Stock Options'):
            new_symbol = f"OPTION_{new_ticker}"
        else:
            new_symbol = new_ticker

        asset_class_from = conv.get('asset_class_from', '')
        if asset_class_from in ('Варранты', 'Warrants'):
            old_symbol = f"WARRANT_{old_ticker}"
        elif asset_class_from in ('Опционы на акции и индексы', 'Stock Options'):
            old_symbol = f"OPTION_{old_ticker}"
        else:
            old_symbol = old_ticker

        # Рассчитываем соотношение конвертации (ratio)
        # Например: 1500 old -> 150 new, ratio = 150/1500 = 0.1 (10:1 reverse split)
        ratio = (new_qty_received / old_qty_removed) if old_qty_removed else Decimal(0)

        # IB в CSV использует текущий тикер для всех сделок (даже тех, что были до конвертации).
        # Если buy_lots[old_symbol] пустой, но buy_lots[new_symbol] не пустой -
        # значит сделки до конвертации записаны под new_symbol и их нужно перенести.
        if not buy_lots[old_symbol] and buy_lots[new_symbol]:
            # Переносим все лоты из new_symbol в old_symbol
            while buy_lots[new_symbol]:
                buy_lots[old_symbol].append(buy_lots[new_symbol].popleft())

        # Если buy_lots[old_symbol] всё ещё пустой, ищем по ISIN.
        # Бывает что тикер сменился (LFC -> LFCHY) без явного корп. действия в CSV.
        if not buy_lots[old_symbol]:
            old_isin = conv.get('old_isin', '')
            if old_isin:
                # Ищем символ с тем же ISIN
                for sym, isin in symbol_to_isin.items():
                    if isin != old_isin:
                        continue
                    if sym == old_ticker:
                        continue

                    candidate_key = sym
                    if old_symbol.startswith('WARRANT_') and not candidate_key.startswith('WARRANT_'):
                        candidate_key = f"WARRANT_{sym}"
                    elif old_symbol.startswith('OPTION_') and not candidate_key.startswith('OPTION_'):
                        candidate_key = f"OPTION_{sym}"

                    if buy_lots[candidate_key]:
                        # Нашли символ с тем же ISIN - переносим лоты
                        while buy_lots[candidate_key]:
                            buy_lots[old_symbol].append(buy_lots[candidate_key].popleft())
                        break

        total_qty_removed = Decimal(0)
        new_lots = []  # Собираем новые лоты - по одному на каждый исходный лот

        old_queue = buy_lots[old_symbol]
        while old_queue and total_qty_removed < old_qty_removed:
            lot = old_queue.popleft()
            remaining_to_remove = old_qty_removed - total_qty_removed

            # Определяем source_lot_ids для этого лота
            if lot.get('source_lot_ids'):
                lot_source_ids = list(lot['source_lot_ids'])  # Копируем список
            elif lot.get('lot_id'):
                lot_source_ids = [lot['lot_id']]
            else:
                lot_source_ids = []

            if lot['q_remaining'] > remaining_to_remove:
                # Частично используем этот лот
                qty_used = remaining_to_remove
                cost_used = remaining_to_remove * lot['cost_per_share_rub']
                lot['q_remaining'] -= remaining_to_remove
                total_qty_removed += remaining_to_remove
                old_queue.appendleft(lot)
            else:
                # Полностью используем этот лот
                qty_used = lot['q_remaining']
                cost_used = lot['q_remaining'] * lot['cost_per_share_rub']
                total_qty_removed += lot['q_remaining']

            # Создаём новый лот с пересчитанным количеством
            # Каждый старый лот становится отдельным новым лотом с сохранением source_lot_ids
            new_qty = qty_used * ratio
            if new_qty > 0:
                new_lots.append({
                    'q_remaining': new_qty,
                    'cost_per_share_rub': (cost_used / new_qty) if new_qty else Decimal(0),
                    'source_lot_ids': lot_source_ids,
                })

        # Добавляем все новые лоты в очередь в порядке FIFO
        for new_lot in new_lots:
            buy_lots[new_symbol].append(new_lot)

        # Если покупки были до периода отчёта (очередь была пуста или неполная),
        # создаём лот для оставшихся конвертированных акций с нулевой стоимостью
        if total_qty_removed < old_qty_removed and new_qty_received > 0:
            remaining_old = old_qty_removed - total_qty_removed
            remaining_new = remaining_old * ratio
            if remaining_new > 0:
                buy_lots[new_symbol].append({
                    'q_remaining': remaining_new,
                    'cost_per_share_rub': Decimal(0),  # Стоимость неизвестна (покупка до периода отчёта)
                    'source_lot_ids': [],  # Нет связи с покупками в отчёте
                })

        instrument_events[new_symbol].append({
            'display_type': 'conversion_info',
            'datetime_obj': conv['datetime_obj'],
            'event_details': {
                'corp_action_id': None,
                'old_symbol': old_symbol,
                'new_symbol': new_symbol,
                'old_ticker': old_ticker,
                'old_isin': conv['old_isin'],
                'old_instr_nm': symbol_to_name.get(old_ticker, old_ticker),
                'new_ticker': new_ticker,
                'new_isin': conv['new_isin'],
                'new_instr_nm': symbol_to_name.get(new_ticker, new_ticker),
                'old_quantity_removed': old_qty_removed,
                'new_quantity_received': new_qty_received,
                'ratio_comment': conv.get('comment', ''),
                'is_relevant_for_target_year': False,
                'asset_class_from': asset_class_from,
                'asset_class_to': asset_class_to,
            },
        })

    def _apply_acquisition(self, acq, buy_lots, instrument_events, symbol_to_isin=None, symbol_to_name=None):
        """Обрабатывает acquisition - получение инструмента через корп. действие.

        Создаёт лот для полученного инструмента и добавляет событие в историю.
        Типы acquisitions:
        - rights_issue: выдача прав (бесплатно)
        - spinoff: спин-офф (бесплатно)
        - subscription: подписка (оплачено)
        """
        if symbol_to_isin is None:
            symbol_to_isin = {}
        if symbol_to_name is None:
            symbol_to_name = {}

        ticker = acq.get('ticker', '')
        isin = acq.get('isin', '')
        quantity = acq.get('quantity', Decimal(0))
        currency = acq.get('currency', 'USD')
        cost = acq.get('cost', Decimal(0))  # Стоимость приобретения (0 для бесплатных)
        dt_obj = acq.get('datetime_obj')
        acq_type = acq.get('type', 'unknown')
        source_ticker = acq.get('source_ticker', '')
        asset_class = acq.get('asset_class', '')

        # Определяем group_symbol (для варрантов добавляем префикс)
        group_symbol = ticker
        if asset_class in ('Варранты', 'Warrants'):
            group_symbol = f"WARRANT_{ticker}"

        # Дублируем ISIN на group_symbol, чтобы внутренние связи/фильтры работали и для префиксных инструментов
        if isin and group_symbol and group_symbol != ticker and group_symbol not in symbol_to_isin:
            symbol_to_isin[group_symbol] = isin

        # Рассчитываем стоимость в рублях
        cbr_rate = self._get_cbr_rate(currency, dt_obj) or Decimal(0)
        cost_rub = (cost * cbr_rate).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) if cbr_rate else Decimal(0)
        cost_per_share_rub = (cost_rub / quantity) if quantity else Decimal(0)

        # Генерируем уникальный ID для лота
        lot_id = f"ACQ_{ticker}_{dt_obj.strftime('%Y%m%d%H%M%S') if dt_obj else 'unknown'}_{acq_type}"

        # Создаём лот
        buy_lots[group_symbol].append({
            'q_remaining': quantity,
            'cost_per_share_rub': cost_per_share_rub,
            'lot_id': lot_id,
            'source_lot_ids': [lot_id],
        })

        # Добавляем событие в историю для отображения
        instrument_events[group_symbol].append({
            'display_type': 'acquisition_info',
            'datetime_obj': dt_obj,
            'event_details': {
                'ticker': ticker,
                'isin': isin,
                'instr_nm': symbol_to_name.get(ticker, ticker),
                'quantity': quantity,
                'currency': currency,
                'cost': cost,
                'cost_rub': cost_rub,
                'cbr_rate': cbr_rate,
                'acquisition_type': acq_type,
                'source_ticker': source_ticker,
                'comment': acq.get('comment', ''),
                'is_relevant_for_target_year': False,
                'lot_id': lot_id,
            },
        })
