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
            return {}, [], Decimal(0), Decimal(0), False, empty_commissions, empty_other, Decimal(0), [], Decimal(0)

        sections = {}
        for report in reports:
            report_sections = self._parse_csv_sections(report.report_file.path)
            for key, blocks in report_sections.items():
                sections.setdefault(key, [])
                sections[key].extend(blocks)

        dividend_commissions = defaultdict(lambda: {'amount_by_currency': defaultdict(Decimal), 'amount_rub': Decimal(0), 'details': []})
        other_commissions = defaultdict(lambda: {'currencies': defaultdict(Decimal), 'total_rub': Decimal(0), 'raw_events': []})
        total_other_commissions_rub = Decimal(0)

        trades = self._parse_trades(sections, other_commissions)
        dividends = self._parse_dividends(sections)
        conversions = self._parse_corporate_actions(sections)
        other_income = self._parse_interest(sections)
        self._parse_fees(sections, other_commissions)

        instrument_event_history, total_sales_profit = self._build_fifo_history(trades, conversions)

        total_other_commissions_rub = sum((data.get('total_rub', Decimal(0)) for data in other_commissions.values()), Decimal(0))
        total_dividends_rub = sum((d.get('amount_rub', Decimal(0)) for d in dividends), Decimal(0))
        total_other_income_rub = sum((i.get('amount_rub', Decimal(0)) for i in other_income), Decimal(0))

        return (
            instrument_event_history,
            dividends,
            total_dividends_rub,
            total_sales_profit,
            False,
            dividend_commissions,
            other_commissions,
            total_other_commissions_rub,
            other_income,
            total_other_income_rub,
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

    def _parse_trades(self, sections, other_commissions):
        trades = []
        trades_blocks = sections.get('Сделки') or []
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

                quantity = self._parse_decimal(quantity_raw)
                if quantity == 0:
                    continue

                dt_obj = self._parse_datetime(datetime_raw)
                operation = 'buy' if quantity > 0 else 'sell'
                trade_id = f"IB_{symbol}_{dt_obj.strftime('%Y%m%d%H%M%S') if dt_obj else trade_index}_{trade_index}"
                cbr_rate = self._get_cbr_rate(currency, dt_obj) or Decimal(0)

                trade = {
                    'trade_id': trade_id,
                    'datetime_obj': dt_obj,
                    'operation': operation,
                    'symbol': symbol,
                    'group_symbol': group_symbol,
                    'quantity': abs(quantity),
                    'price': self._parse_decimal(price_raw),
                    'commission': abs(self._parse_decimal(commission_raw)),
                    'currency': currency,
                    'cbr_rate': cbr_rate,
                    'instr_kind': asset_class,
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

    def _parse_fees(self, sections, other_commissions):
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
                if not dt_obj or dt_obj.year != self.target_year or amount == 0:
                    continue
                self._add_other_commission(other_commissions, subtitle, amount, currency, dt_obj, description)

    def _parse_interest(self, sections):
        interest_blocks = sections.get('Процент') or []
        if not interest_blocks:
            return []
        entries = []
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
                cbr_rate = self._get_cbr_rate(currency, dt_obj) or Decimal(0)
                amount_rub = (amount * cbr_rate).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) if cbr_rate else Decimal(0)
                entries.append({
                    'date': dt_obj.date(),
                    'description': description,
                    'amount': amount,
                    'currency': currency,
                    'cbr_rate_str': f"{cbr_rate:.4f}" if cbr_rate else '-',
                    'amount_rub': amount_rub,
                })
        entries.sort(key=lambda x: x.get('date') or date.min)
        return entries

    def _extract_symbol_isin(self, description):
        if not description:
            return None, None
        match = re.match(r'^([A-Z0-9.]+)\(([A-Z0-9]{12})\)', description.strip())
        if not match:
            return None, None
        return match.group(1), match.group(2)

    def _parse_corporate_actions(self, sections):
        corp_blocks = sections.get('Корпоративные действия') or []
        if not corp_blocks:
            return []

        conversion_map = {}
        for block in corp_blocks:
            header_map = self._header_map(block.get('header', []))
            for row in block.get('data', []):
                description = self._get_value(row, header_map, ['Описание', 'Description'])
                quantity = self._parse_decimal(self._get_value(row, header_map, ['Количество', 'Quantity']))
                date_raw = self._get_value(row, header_map, ['Дата/Время', 'Date/Time'])
                dt_obj = self._parse_datetime(date_raw)
                if not dt_obj or quantity == 0:
                    continue
                pairs = re.findall(r'([A-Z0-9\\.]+)\\(([A-Z0-9]{12})\\)', description or '')
                if len(pairs) < 2:
                    continue
                old_ticker, old_isin = pairs[0]
                new_ticker, new_isin = pairs[-1]
                key = (dt_obj.date(), old_ticker, new_ticker, old_isin, new_isin)
                item = conversion_map.setdefault(key, {
                    'datetime_obj': dt_obj,
                    'old_ticker': old_ticker,
                    'new_ticker': new_ticker,
                    'old_isin': old_isin,
                    'new_isin': new_isin,
                    'old_qty_removed': Decimal(0),
                    'new_qty_received': Decimal(0),
                    'comment': description,
                })
                if quantity < 0:
                    item['old_qty_removed'] += abs(quantity)
                else:
                    item['new_qty_received'] += quantity

        return list(conversion_map.values())

    def _record_commission_from_trade(self, row, header_map, other_commissions):
        currency = self._get_value(row, header_map, ['Валюта', 'Currency']).upper()
        datetime_raw = self._get_value(row, header_map, ['Дата/Время', 'Date/Time'])
        commission_raw = self._get_value(row, header_map, ['Комиссия/плата', 'Comm/Fee', 'Комиссия в USD'])
        amount = self._parse_decimal(commission_raw)
        dt_obj = self._parse_datetime(datetime_raw)
        if not dt_obj or dt_obj.year != self.target_year or amount == 0:
            return
        self._add_other_commission(other_commissions, 'FX комиссии', amount, currency, dt_obj, 'Forex trade commission')

    def _add_other_commission(self, other_commissions, category, amount, currency, dt_obj, description):
        normalized_amount = abs(amount)
        cbr_rate = self._get_cbr_rate(currency, dt_obj) or Decimal(0)
        amount_rub = (normalized_amount * cbr_rate).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP) if cbr_rate else Decimal(0)
        other_commissions[category]['currencies'][currency] += normalized_amount
        other_commissions[category]['total_rub'] += amount_rub
        other_commissions[category]['raw_events'].append({
            'date': dt_obj.date(),
            'amount': normalized_amount,
            'currency': currency,
            'amount_rub': amount_rub,
            'description': description,
        })

    def _build_fifo_history(self, trades, conversions):
        buy_lots = defaultdict(deque)
        short_sales = defaultdict(deque)
        instrument_events = defaultdict(list)
        trade_details_by_id = {}
        symbols_with_sales_in_target_year = set()
        used_buy_ids_for_target_year = set()
        total_sales_profit_rub = Decimal(0)

        conversions_by_date = sorted(conversions, key=lambda x: x.get('datetime_obj') or datetime.min)
        conversion_idx = 0

        for trade in trades:
            while conversion_idx < len(conversions_by_date):
                conv = conversions_by_date[conversion_idx]
                if trade.get('datetime_obj') and conv['datetime_obj'] > trade['datetime_obj']:
                    break
                self._apply_conversion(conv, buy_lots, instrument_events)
                conversion_idx += 1

            dt_obj = trade.get('datetime_obj')
            symbol = trade.get('group_symbol') or trade.get('symbol') or 'UNKNOWN'
            quantity = trade.get('quantity', Decimal(0))
            price = trade.get('price', Decimal(0))
            commission = trade.get('commission', Decimal(0))
            cbr_rate = trade.get('cbr_rate', Decimal(0))

            if trade['operation'] == 'buy':
                # Покрываем открытые шорты (если есть)
                remaining = quantity
                cost_shares_rub = (price * quantity * cbr_rate).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
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
                'instr_nm': trade.get('symbol') or symbol,
                'isin': symbol,
                'instr_kind': trade.get('instr_kind'),
                'p': price,
                'curr_c': trade.get('currency'),
                'cbr_rate': cbr_rate,
                'q': quantity,
                'summ': price * quantity,
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
        for symbol, events in instrument_events.items():
            for event in events:
                if event.get('display_type') != 'trade':
                    continue
                details = event.get('event_details') or {}
                if details.get('operation') == 'sell':
                    dt_obj = event.get('datetime_obj')
                    if dt_obj and dt_obj.year == self.target_year:
                        income_rub = (details.get('p', Decimal(0)) * details.get('q', Decimal(0)) * details.get('cbr_rate', Decimal(0))).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                        fifo_cost_val = details.get('fifo_cost_rub_decimal', Decimal(0)) or Decimal(0)
                        total_sales_profit_rub += (income_rub - fifo_cost_val)

        filtered_history = {}
        for symbol, events in instrument_events.items():
            if symbol in symbols_with_sales_in_target_year:
                events.sort(key=lambda x: x.get('datetime_obj') or datetime.min)
                filtered_history[symbol] = events

        return filtered_history, total_sales_profit_rub

    def _apply_conversion(self, conv, buy_lots, instrument_events):
        old_symbol = conv['old_ticker']
        new_symbol = conv['new_ticker']
        old_qty_removed = conv['old_qty_removed']
        new_qty_received = conv['new_qty_received']

        total_cost_basis = Decimal(0)
        total_qty_removed = Decimal(0)
        old_queue = buy_lots[old_symbol]
        while old_queue and total_qty_removed < old_qty_removed:
            lot = old_queue.popleft()
            remaining_to_remove = old_qty_removed - total_qty_removed
            if lot['q_remaining'] > remaining_to_remove:
                total_cost_basis += remaining_to_remove * lot['cost_per_share_rub']
                lot['q_remaining'] -= remaining_to_remove
                total_qty_removed += remaining_to_remove
                old_queue.appendleft(lot)
                break
            total_cost_basis += lot['q_remaining'] * lot['cost_per_share_rub']
            total_qty_removed += lot['q_remaining']

        cost_per_new_share = Decimal(0)
        if new_qty_received:
            cost_per_new_share = (total_cost_basis / new_qty_received) if new_qty_received else Decimal(0)
            buy_lots[new_symbol].append({
                'q_remaining': new_qty_received,
                'cost_per_share_rub': cost_per_new_share,
            })

        instrument_events[new_symbol].append({
            'display_type': 'conversion_info',
            'datetime_obj': conv['datetime_obj'],
            'event_details': {
                'corp_action_id': None,
                'old_ticker': old_symbol,
                'old_isin': conv['old_isin'],
                'new_ticker': new_symbol,
                'new_isin': conv['new_isin'],
                'old_quantity_removed': old_qty_removed,
                'new_quantity_received': new_qty_received,
                'ratio_comment': conv.get('comment', ''),
                'is_relevant_for_target_year': False,
            },
        })
