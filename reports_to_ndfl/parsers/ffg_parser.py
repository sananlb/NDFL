from decimal import Decimal

from .base import BaseBrokerParser
from ..FFG_ndfl import process_and_get_trade_data
from ..models import BrokerReport


class FFGParser(BaseBrokerParser):
    def process(self):
        files_queryset = BrokerReport.objects.filter(user=self.user, broker_type='ffg')
        result = process_and_get_trade_data(
            self.request,
            self.user,
            self.target_year,
            files_queryset=files_queryset,
        )
        # Нормализуем результат под общий контракт парсеров (как у IBParser):
        # (instrument_event_history, dividend_events, total_dividends_rub,
        #  total_sales_profit, parsing_error, dividend_commissions, other_commissions,
        #  total_other_commissions_rub, profit_by_income_code, profit_by_income_code_currencies)
        #
        # FFG не поддерживает опционы, поэтому всё идет в код 1530.
        if len(result) == 9:
            total_sales_profit = result[3] if len(result) > 3 else Decimal(0)
            profit_by_income_code = {'1530': total_sales_profit, '1532': Decimal(0)}
            profit_by_currency_1530 = result[8] if len(result) > 8 else {}
            profit_by_income_code_currencies = {'1530': profit_by_currency_1530, '1532': {}}
            return result[:8] + (profit_by_income_code, profit_by_income_code_currencies)

        if len(result) == 8:
            total_sales_profit = result[3] if len(result) > 3 else Decimal(0)
            profit_by_income_code = {'1530': total_sales_profit, '1532': Decimal(0)}
            profit_by_income_code_currencies = {'1530': {}, '1532': {}}
            return result + (profit_by_income_code, profit_by_income_code_currencies)

        return result
