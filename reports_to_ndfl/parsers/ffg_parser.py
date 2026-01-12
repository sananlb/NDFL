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
        # FFG возвращает 8 элементов, добавляем 9-й для совместимости с IB
        # FFG не поддерживает опционы, поэтому всё идет в код 1530
        if len(result) == 8:
            total_sales_profit = result[3] if len(result) > 3 else Decimal(0)
            profit_by_income_code = {'1530': total_sales_profit, '1532': Decimal(0)}
            result = result + (profit_by_income_code,)
        return result
