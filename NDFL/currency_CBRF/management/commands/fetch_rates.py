from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone
from datetime import datetime, timedelta
from currency_CBRF.services import fetch_daily_rates, fetch_period_rates
from currency_CBRF.models import Currency, ExchangeRate
from decimal import Decimal

class Command(BaseCommand):
    help = 'Загружает курсы валют с сайта ЦБ РФ. Может загружать ежедневные курсы или курсы за период.'

    def add_arguments(self, parser):
        parser.add_argument(
            '--date',
            type=str,
            help='Дата для загрузки курсов в формате YYYY-MM-DD. Если не указана, загружаются последние доступные курсы.'
        )
        parser.add_argument(
            '--start-date',
            type=str,
            help='Начальная дата периода в формате YYYY-MM-DD для загрузки истории.'
        )
        parser.add_argument(
            '--end-date',
            type=str,
            help='Конечная дата периода в формате YYYY-MM-DD для загрузки истории.'
        )
        parser.add_argument(
            '--currencies',
            type=str,
            help='Список кодов валют (CharCode) через запятую для загрузки (например, "USD,EUR"). По умолчанию все из БД.'
        )

    def handle(self, *args, **options):
        target_date_str = options['date']
        start_date_str = options['start_date']
        end_date_str = options['end_date']
        currencies_str = options['currencies']

        target_currencies = None
        if currencies_str:
            char_codes = [code.strip().upper() for code in currencies_str.split(',')]
            target_currencies = Currency.objects.filter(char_code__in=char_codes)
            if not target_currencies.exists():
                self.stdout.write(self.style.WARNING(f"Указанные валюты ({currencies_str}) не найдены в базе данных. Обновление для них не будет произведено."))
        else:
            target_currencies = Currency.objects.all()


        if start_date_str and end_date_str:
            self._fetch_historical_rates(start_date_str, end_date_str, target_currencies)
        elif target_date_str:
            try:
                dt_obj = datetime.strptime(target_date_str, '%Y-%m-%d')
                cbr_date_str = dt_obj.strftime('%d/%m/%Y')
                self._fetch_and_save_daily_rates(cbr_date_str)
            except ValueError:
                raise CommandError(f"Неверный формат даты: {target_date_str}. Используйте YYYY-MM-DD.")
        else:
            self._fetch_and_save_daily_rates()

        self.stdout.write(self.style.SUCCESS('Загрузка курсов валют завершена.'))

    def _fetch_and_save_daily_rates(self, date_str_for_cbr=None):
        """Вспомогательный метод для загрузки и сохранения ежедневных курсов."""
        self.stdout.write(f"Запрос ежедневных курсов на дату: {date_str_for_cbr or 'последнюю доступную'}...")
        daily_data, rates_date_obj = fetch_daily_rates(date_str_for_cbr)

        if not daily_data or not rates_date_obj:
            self.stdout.write(self.style.ERROR(f"Не удалось получить ежедневные курсы на {date_str_for_cbr or 'последнюю доступную'}."))
            return

        self.stdout.write(self.style.SUCCESS(f"Получены курсы на {rates_date_obj.strftime('%Y-%m-%d')}."))

        saved_count = 0
        updated_count = 0
        new_currencies_count = 0
        for rate_data in daily_data:
            currency, created = Currency.objects.update_or_create(
                cbr_id=rate_data['cbr_id'],
                defaults={
                    'name': rate_data['name'],
                    'char_code': rate_data['char_code'],
                    'num_code': rate_data['num_code']
                }
            )
            if created:
                self.stdout.write(f"Добавлена новая валюта: {currency.char_code} ({currency.name}) с ID ЦБ РФ: {currency.cbr_id}")
                new_currencies_count +=1

            rate_obj, rate_created = ExchangeRate.objects.update_or_create(
                currency=currency,
                date=rates_date_obj,
                defaults={
                    'value': rate_data['value'],
                    'nominal': rate_data['nominal']
                }
            )
            if rate_created:
                saved_count += 1
            else:
                updated_count += 1
        
        if new_currencies_count > 0:
            self.stdout.write(f"Добавлено новых валют в справочник: {new_currencies_count}.")
        self.stdout.write(f"Сохранено новых курсов: {saved_count}. Обновлено существующих курсов: {updated_count}.")


    def _fetch_historical_rates(self, start_date_str, end_date_str, target_currencies_qs):
        """Вспомогательный метод для загрузки исторических данных."""
        try:
            start_dt = datetime.strptime(start_date_str, '%Y-%m-%d').date()
            end_dt = datetime.strptime(end_date_str, '%Y-%m-%d').date()
        except ValueError:
            raise CommandError("Неверный формат дат для периода. Используйте YYYY-MM-DD.")

        if start_dt > end_dt:
            raise CommandError("Начальная дата периода не может быть позже конечной даты.")

        cbr_start_date = start_dt.strftime('%d/%m/%Y')
        cbr_end_date = end_dt.strftime('%d/%m/%Y')

        if not target_currencies_qs or not target_currencies_qs.exists():
            self.stdout.write(self.style.WARNING("Нет валют в БД для загрузки исторических данных. Сначала заполните справочник валют (например, запустив команду без параметров даты)."))
            return

        total_saved_for_period = 0
        total_updated_for_period = 0

        for currency in target_currencies_qs:
            if not currency.cbr_id:
                self.stdout.write(self.style.WARNING(f"У валюты {currency.char_code} отсутствует ID ЦБ РФ. Пропуск загрузки истории."))
                continue

            self.stdout.write(f"Запрос истории для {currency.char_code} ({currency.cbr_id}) за период {start_date_str} - {end_date_str}...")
            period_data = fetch_period_rates(currency.cbr_id, cbr_start_date, cbr_end_date)

            if period_data is None: 
                self.stdout.write(self.style.ERROR(f"Ошибка при получении истории для {currency.char_code}."))
                continue

            if not period_data: 
                self.stdout.write(self.style.WARNING(f"Нет данных для {currency.char_code} за указанный период."))
                continue

            current_currency_saved = 0
            current_currency_updated = 0
            for rate_data in period_data:
                rate_obj, rate_created = ExchangeRate.objects.update_or_create(
                    currency=currency,
                    date=rate_data['date'],
                    defaults={
                        'value': rate_data['value'],
                        'nominal': rate_data['nominal']
                    }
                )
                if rate_created:
                    current_currency_saved += 1
                else:
                    current_currency_updated += 1
            
            self.stdout.write(f"Для {currency.char_code}: сохранено новых курсов {current_currency_saved}, обновлено существующих {current_currency_updated}.")
            total_saved_for_period += current_currency_saved
            total_updated_for_period += current_currency_updated
        
        self.stdout.write(f"Всего за период {start_date_str} - {end_date_str}: сохранено {total_saved_for_period} новых курсов, обновлено {total_updated_for_period} существующих курсов.")
