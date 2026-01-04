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
        other_income = []
        total_other_income_rub = Decimal(0)
        return (*result, other_income, total_other_income_rub)
