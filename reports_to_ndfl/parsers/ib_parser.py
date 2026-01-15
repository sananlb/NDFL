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
        conversions = self._parse_corporate_actions(sections, symbol_to_name)
        self._parse_interest(sections, other_commissions)
        self._parse_fees(sections, other_commissions, dividend_commissions)

        instrument_event_history, total_sales_profit, profit_by_income_code = self._build_fifo_history(trades, conversions, symbol_to_isin, symbol_to_name)

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
                if asset_class and asset_class not in ('Акции', 'Stocks', 'Опционы на акции и индексы', 'Stock Options'):
                    continue

                currency = self._get_value(row, header_map, ['Валюта', 'Currency']).upper()
                symbol = self._get_value(row, header_map, ['Символ', 'Symbol']).strip()
                group_symbol = symbol
                if asset_class in ('Опционы на акции и индексы', 'Stock Options'):
                    group_symbol = f"OPTION_{symbol}"
                datetime_raw = self._get_value(row, header_map, ['Дата/Время', 'Date/Time'])
                quantity_raw = self._get_value(row, header_map, ['Количество', 'Quantity'])
                price_raw = self._get_value(row, header_map, ['Цена транзакции', 'T. Price', 'Trade Price'])
                commission_raw = self._get_value(row, header_map, ['Комиссия/плата', 'Comm/Fee', 'Комиссия в USD'])
                proceeds_raw = self._get_value(row, header_map, ['Выручка', 'Proceeds'])

                quantity = self._parse_decimal(quantity_raw)
                if quantity == 0:
                    continue

                dt_obj = self._parse_datetime(datetime_raw)
                operation = 'buy' if quantity > 0 else 'sell'
                trade_id = f"IB_{symbol}_{dt_obj.strftime('%Y%m%d%H%M%S') if dt_obj else trade_index}_{trade_index}"
                cbr_rate = self._get_cbr_rate(currency, dt_obj) or Decimal(0)

                price = self._parse_decimal(price_raw)
                proceeds = self._parse_decimal(proceeds_raw)

                # Для опционов: используем множитель из секции "Информация о финансовом инструменте"
                # IB уже учитывает множитель в колонке Proceeds, но не в Price
                is_option = asset_class in ('Опционы на акции и индексы', 'Stock Options')
                if is_option:
                    # Для опционов пытаемся получить множитель из инструментальной информации
                    multiplier = symbol_to_multiplier.get(symbol)
                    if multiplier is None:
                        # Если не нашли, используем стандартное значение 100
                        multiplier = Decimal(100)
                else:
                    # Для акций множитель всегда 1
                    multiplier = Decimal(1)

                # Код дохода: 1530 для акций, 1532 для ПФИ (опционов)
                income_code = '1532' if is_option else '1530'

                # Округляем комиссию до сотых для соответствия с отчетом IB
                commission = abs(self._parse_decimal(commission_raw))
                commission = commission.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) if commission else Decimal(0)

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
                }
                trades.append(trade)
                trade_index += 1

        trades.sort(key=lambda x: x.get('datetime_obj') or datetime.min)
        return trades

    def _parse_dividends(self, sections):
        dividends = []
        dividends_blocks = sections.get('Дивиденды') or []
        tax_blocks = sections.get('Удерживаемый налог') or []

        tax_by_key = defaultdict(Decimal)
        for block in tax_blocks:
            header_map_tax = self._header_map(block.get('header', []))
            for row in block.get('data', []):
                date_raw = self._get_value(row, header_map_tax, ['Дата', 'Date'])
                desc = self._get_value(row, header_map_tax, ['Описание', 'Description'])
                currency = self._get_value(row, header_map_tax, ['Валюта', 'Currency']).upper()
                amount = self._parse_decimal(self._get_value(row, header_map_tax, ['Сумма', 'Amount']))
                ticker, _ = self._extract_symbol_isin(desc)
                dt_obj = self._parse_datetime(date_raw)
                if not dt_obj or not ticker:
                    continue
                key = (dt_obj.date(), ticker, currency)
                tax_by_key[key] += amount

        if not dividends_blocks:
            return dividends

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

                tax_amount = tax_by_key.get((dt_obj.date(), ticker, currency), Decimal(0))
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
                })

        dividends.sort(key=lambda x: (x.get('date') or date.min, x.get('ticker') or ''))
        return dividends

    def _parse_fees(self, sections, other_commissions, dividend_commissions):
        """Парсит секцию Сборы/комиссии.

        ADR fees и комиссии связанные с дивидендами идут в dividend_commissions.
        Остальные комиссии идут в other_commissions.
        """
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

                # Проверяем, связана ли комиссия с дивидендами
                # Примеры: "HSBK(...) Наличный дивиденд USD 2.258938 на акцию - FEE"
                desc_lower = (description or '').lower()
                is_dividend_related = 'дивиденд' in desc_lower or 'dividend' in desc_lower

                if is_dividend_related:
                    # Извлекаем тикер из описания для категории
                    ticker = self._extract_ticker_from_fee_description(description)
                    category = f"Комиссия по дивидендам ({ticker})" if ticker else "Комиссии по дивидендам"
                    self._add_dividend_commission(dividend_commissions, category, amount, currency, dt_obj, description)
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
        match = re.match(r'^([A-Z0-9.]+)\(([A-Z0-9]{12})\)', description.strip())
        if not match:
            return None, None
        return match.group(1), match.group(2)

    def _parse_corporate_actions(self, sections, symbol_to_name=None):
        if symbol_to_name is None:
            symbol_to_name = {}
        corp_blocks = sections.get('Корпоративные действия') or []
        if not corp_blocks:
            return []

        # Группируем все корпоративные действия по дате и первому тикеру
        temp_conversions = defaultdict(list)
        for block in corp_blocks:
            header_map = self._header_map(block.get('header', []))
            for row in block.get('data', []):
                description = self._get_value(row, header_map, ['Описание', 'Description'])
                quantity = self._parse_decimal(self._get_value(row, header_map, ['Количество', 'Quantity']))
                date_raw = self._get_value(row, header_map, ['Дата/Время', 'Date/Time'])
                dt_obj = self._parse_datetime(date_raw)
                if not dt_obj or quantity == 0:
                    continue
                # Ищем паттерны: TICKER(ISIN) и (TICKER, описание, ISIN)
                pairs = re.findall(r'([A-Z0-9\\.]+)\(([A-Z0-9]{12})\)', description or '')
                # Дополнительно ищем альтернативный формат: (TICKER, ..., ISIN)
                alt_pairs = re.findall(r'\(([A-Z0-9\\.]+),\s*[^,]+,\s*([A-Z0-9]{12})\)', description or '')
                pairs.extend(alt_pairs)
                if len(pairs) < 1:
                    continue

                # Первый тикер - это всегда старый инструмент
                old_ticker, old_isin = pairs[0]
                # Группируем по дате и старому тикеру
                key = (dt_obj.date(), old_ticker, old_isin)
                temp_conversions[key].append({
                    'description': description,
                    'quantity': quantity,
                    'pairs': pairs,
                    'dt_obj': dt_obj,
                })

        # Обрабатываем сгруппированные конвертации
        conversion_map = {}
        for key, items in temp_conversions.items():
            date, old_ticker, old_isin = key
            old_qty_removed = Decimal(0)
            new_ticker = None
            new_isin = None
            new_qty_received = Decimal(0)
            dt_obj = None
            comment = ''

            for item in items:
                dt_obj = item['dt_obj']
                quantity = item['quantity']
                pairs = item['pairs']
                comment = item['description']

                if quantity < 0:
                    # Списание старых акций
                    old_qty_removed += abs(quantity)
                else:
                    # Получение новых акций
                    new_qty_received += quantity
                    # Новый тикер берём из последней пары
                    if len(pairs) >= 2:
                        new_ticker, new_isin = pairs[-1]

            # Если не нашли новый тикер, пропускаем
            if not new_ticker or old_ticker == new_ticker:
                continue

            # Пропускаем технические смены тикеров (суффиксы .RTS8, .SUB8 и т.д.)
            # Проверяем несколько случаев:
            is_technical_rename = False

            # 1. Приоритетная проверка: если полное название инструмента не меняется И количество не меняется - это не конвертация
            old_company_name = symbol_to_name.get(old_ticker, '').strip()
            new_company_name = symbol_to_name.get(new_ticker, '').strip()
            if old_company_name and new_company_name and old_company_name == new_company_name:
                # Названия инструментов точно совпадают
                if old_qty_removed > 0 and new_qty_received > 0:
                    change_ratio = abs(1 - (new_qty_received / old_qty_removed))
                    if change_ratio < Decimal('0.01'):  # менее 1% изменения количества
                        is_technical_rename = True

            # 2. По паттерну тикеров: новый тикер = старый тикер + суффикс
            if not is_technical_rename and new_ticker.startswith(old_ticker + '.'):
                # Новый тикер начинается со старого + точка (например, ADC → ADC.RTS8)
                is_technical_rename = True

            # 3. По количеству: если количество не меняется
            if not is_technical_rename and old_qty_removed > 0 and new_qty_received > 0:
                ratio = abs(old_qty_removed - new_qty_received)
                if ratio < Decimal('0.01'):
                    is_technical_rename = True

            # 4. По сходству тикеров: один тикер содержится в другом (например, RAC → RACAU)
            if not is_technical_rename and (old_ticker in new_ticker or new_ticker in old_ticker):
                # Проверяем, что количество не сильно меняется
                if old_qty_removed > 0 and new_qty_received > 0:
                    change_ratio = abs(1 - (new_qty_received / old_qty_removed))
                    if change_ratio < Decimal('0.10'):  # менее 10% изменения
                        is_technical_rename = True

            # 5. Если нет данных о списании старых акций - это техническое переименование тикера
            if not is_technical_rename and old_qty_removed == 0:
                is_technical_rename = True

            # Пропускаем технические смены тикеров
            if is_technical_rename:
                continue

            conv_key = (date, old_ticker, new_ticker, old_isin, new_isin)
            conversion_map[conv_key] = {
                'datetime_obj': dt_obj,
                'old_ticker': old_ticker,
                'new_ticker': new_ticker,
                'old_isin': old_isin,
                'new_isin': new_isin,
                'old_qty_removed': old_qty_removed,
                'new_qty_received': new_qty_received,
                'comment': comment,
            }

        return list(conversion_map.values())

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

    def _add_dividend_commission(self, dividend_commissions, category, amount, currency, dt_obj, description):
        """Добавляет запись в dividend_commissions.

        Структура совместима с FFG: amount_by_currency, amount_rub, details.
        Сохраняем оригинальный знак: отрицательные = расходы.
        """
        cbr_rate = self._get_cbr_rate(currency, dt_obj) or Decimal(0)
        amount_rub = (amount * cbr_rate).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) if cbr_rate else Decimal(0)
        dividend_commissions[category]['amount_by_currency'][currency] += amount
        dividend_commissions[category]['amount_rub'] += amount_rub
        dividend_commissions[category]['details'].append({
            'date': dt_obj.strftime('%d.%m.%Y'),
            'amount': amount,
            'currency': currency,
            'amount_rub': amount_rub,
            'comment': description,
        })

    def _build_fifo_history(self, trades, conversions, symbol_to_isin=None, symbol_to_name=None):
        buy_lots = defaultdict(deque)
        short_sales = defaultdict(deque)
        instrument_events = defaultdict(list)
        trade_details_by_id = {}
        symbols_with_sales_in_target_year = set()
        used_buy_ids_for_target_year = set()
        total_sales_profit_rub = Decimal(0)

        if symbol_to_isin is None:
            symbol_to_isin = {}
        if symbol_to_name is None:
            symbol_to_name = {}

        conversions_by_date = sorted(conversions, key=lambda x: x.get('datetime_obj') or datetime.min)
        conversion_idx = 0

        for trade in trades:
            while conversion_idx < len(conversions_by_date):
                conv = conversions_by_date[conversion_idx]
                if trade.get('datetime_obj') and conv['datetime_obj'] > trade['datetime_obj']:
                    break
                self._apply_conversion(conv, buy_lots, instrument_events, symbol_to_isin, symbol_to_name)
                conversion_idx += 1

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

        while conversion_idx < len(conversions_by_date):
            conv = conversions_by_date[conversion_idx]
            self._apply_conversion(conv, buy_lots, instrument_events)
            conversion_idx += 1

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
                old_symbol = details.get('old_ticker')
                new_symbol = details.get('new_ticker')
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
            temp_symbol = sold_symbol
            visited = {temp_symbol}
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

                # Фильтруем нерелевантные события старше 3 лет
                if not is_relevant and dt_obj and dt_obj.year < cutoff_year:
                    continue

                # Определяем ключ группировки - самый новый символ в цепочке
                if display_type == 'conversion_info':
                    event_symbol = event_details.get('new_ticker')
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

                filtered_history[grouping_key].append(event)

        # Сортируем события в каждой группе по дате
        for symbol in filtered_history:
            filtered_history[symbol].sort(key=lambda x: x.get('datetime_obj') or datetime.min)

        # Преобразуем defaultdict обратно в обычный dict
        filtered_history = dict(filtered_history)

        return filtered_history, total_sales_profit_rub, profit_by_income_code

    def _apply_conversion(self, conv, buy_lots, instrument_events, symbol_to_isin=None, symbol_to_name=None):
        if symbol_to_isin is None:
            symbol_to_isin = {}
        if symbol_to_name is None:
            symbol_to_name = {}

        old_symbol = conv['old_ticker']
        new_symbol = conv['new_ticker']
        old_qty_removed = conv['old_qty_removed']
        new_qty_received = conv['new_qty_received']

        # Рассчитываем соотношение конвертации (ratio)
        # Например: 1500 old -> 150 new, ratio = 150/1500 = 0.1 (10:1 reverse split)
        ratio = (new_qty_received / old_qty_removed) if old_qty_removed else Decimal(0)

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

        instrument_events[new_symbol].append({
            'display_type': 'conversion_info',
            'datetime_obj': conv['datetime_obj'],
            'event_details': {
                'corp_action_id': None,
                'old_ticker': old_symbol,
                'old_isin': conv['old_isin'],
                'old_instr_nm': symbol_to_name.get(old_symbol, old_symbol),
                'new_ticker': new_symbol,
                'new_isin': conv['new_isin'],
                'new_instr_nm': symbol_to_name.get(new_symbol, new_symbol),
                'old_quantity_removed': old_qty_removed,
                'new_quantity_received': new_qty_received,
                'ratio_comment': conv.get('comment', ''),
                'is_relevant_for_target_year': False,
            },
        })
