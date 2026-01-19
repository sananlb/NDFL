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
        #  total_other_commissions_rub, profit_by_income_code, profit_by_income_code_currencies,
        #  dividends_by_currency, other_commissions_by_currency,
        #  income_by_income_code, income_by_income_code_currencies,
        #  cost_by_income_code, cost_by_income_code_currencies)
        #
        # FFG не поддерживает опционы, поэтому всё идет в код 1530.
        if len(result) == 15:
            # New format with income and cost by currencies
            total_sales_profit = result[3] if len(result) > 3 else Decimal(0)
            profit_by_income_code = {'1530': total_sales_profit, '1532': Decimal(0)}
            profit_by_currency_1530 = result[8] if len(result) > 8 else {}
            profit_by_income_code_currencies = {'1530': profit_by_currency_1530, '1532': {}}
            dividends_by_currency = result[9] if len(result) > 9 else {}
            other_commissions_by_currency = result[10] if len(result) > 10 else {}

            total_income_rub = result[11] if len(result) > 11 else Decimal(0)
            income_by_income_code = {'1530': total_income_rub, '1532': Decimal(0)}
            income_by_currency_1530 = result[12] if len(result) > 12 else {}
            income_by_income_code_currencies = {'1530': income_by_currency_1530, '1532': {}}

            total_cost_rub = result[13] if len(result) > 13 else Decimal(0)
            cost_by_income_code = {'1530': total_cost_rub, '1532': Decimal(0)}
            cost_by_currency_1530 = result[14] if len(result) > 14 else {}
            cost_by_income_code_currencies = {'1530': cost_by_currency_1530, '1532': {}}

            return result[:8] + (profit_by_income_code, profit_by_income_code_currencies,
                                dividends_by_currency, other_commissions_by_currency,
                                income_by_income_code, income_by_income_code_currencies,
                                cost_by_income_code, cost_by_income_code_currencies)

        if len(result) == 11:
            # Old format with dividends_by_currency and other_commissions_by_currency
            total_sales_profit = result[3] if len(result) > 3 else Decimal(0)
            profit_by_income_code = {'1530': total_sales_profit, '1532': Decimal(0)}
            profit_by_currency_1530 = result[8] if len(result) > 8 else {}
            profit_by_income_code_currencies = {'1530': profit_by_currency_1530, '1532': {}}
            dividends_by_currency = result[9] if len(result) > 9 else {}
            other_commissions_by_currency = result[10] if len(result) > 10 else {}
            # Добавляем пустые данные для income и cost
            income_by_income_code = {'1530': Decimal(0), '1532': Decimal(0)}
            income_by_income_code_currencies = {'1530': {}, '1532': {}}
            cost_by_income_code = {'1530': Decimal(0), '1532': Decimal(0)}
            cost_by_income_code_currencies = {'1530': {}, '1532': {}}
            return result[:8] + (profit_by_income_code, profit_by_income_code_currencies,
                                dividends_by_currency, other_commissions_by_currency,
                                income_by_income_code, income_by_income_code_currencies,
                                cost_by_income_code, cost_by_income_code_currencies)

        if len(result) == 9:
            total_sales_profit = result[3] if len(result) > 3 else Decimal(0)
            profit_by_income_code = {'1530': total_sales_profit, '1532': Decimal(0)}
            profit_by_currency_1530 = result[8] if len(result) > 8 else {}
            profit_by_income_code_currencies = {'1530': profit_by_currency_1530, '1532': {}}
            income_by_income_code = {'1530': Decimal(0), '1532': Decimal(0)}
            income_by_income_code_currencies = {'1530': {}, '1532': {}}
            cost_by_income_code = {'1530': Decimal(0), '1532': Decimal(0)}
            cost_by_income_code_currencies = {'1530': {}, '1532': {}}
            return result[:8] + (profit_by_income_code, profit_by_income_code_currencies, {}, {},
                                income_by_income_code, income_by_income_code_currencies,
                                cost_by_income_code, cost_by_income_code_currencies)

        if len(result) == 8:
            total_sales_profit = result[3] if len(result) > 3 else Decimal(0)
            profit_by_income_code = {'1530': total_sales_profit, '1532': Decimal(0)}
            profit_by_income_code_currencies = {'1530': {}, '1532': {}}
            income_by_income_code = {'1530': Decimal(0), '1532': Decimal(0)}
            income_by_income_code_currencies = {'1530': {}, '1532': {}}
            cost_by_income_code = {'1530': Decimal(0), '1532': Decimal(0)}
            cost_by_income_code_currencies = {'1530': {}, '1532': {}}
            return result + (profit_by_income_code, profit_by_income_code_currencies, {}, {},
                            income_by_income_code, income_by_income_code_currencies,
                            cost_by_income_code, cost_by_income_code_currencies)

        return result
