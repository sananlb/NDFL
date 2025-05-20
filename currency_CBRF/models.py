from django.db import models

class Currency(models.Model):
    name = models.CharField(max_length=100, verbose_name="Наименование валюты")
    char_code = models.CharField(max_length=3, unique=True, verbose_name="Символьный код ISO") # Например, USD
    num_code = models.CharField(max_length=3, unique=True, null=True, blank=True, verbose_name="Числовой код ISO") # Например, 840
    cbr_id = models.CharField(max_length=10, unique=True, help_text="Внутренний ID валюты ЦБ РФ (VAL_NM_RQ)", verbose_name="ID ЦБ РФ") # Например, R01235

    class Meta:
        verbose_name = "Валюта"
        verbose_name_plural = "Валюты"
        ordering = ['char_code']

    def __str__(self):
        return f"{self.char_code} ({self.name})"

class ExchangeRate(models.Model):
    currency = models.ForeignKey(Currency, on_delete=models.CASCADE, related_name='rates', verbose_name="Валюта")
    date = models.DateField(verbose_name="Дата курса")
    value = models.DecimalField(max_digits=10, decimal_places=4, help_text="Курс за единицу номинала", verbose_name="Курс")
    nominal = models.PositiveIntegerField(default=1, verbose_name="Номинал")

    class Meta:
        verbose_name = "Курс валюты"
        verbose_name_plural = "Курсы валют"
        constraints = [
            models.UniqueConstraint(fields=['currency', 'date'], name='unique_currency_date_rate')
        ]
        ordering = ['-date', 'currency__char_code']

    def __str__(self):
        return f"{self.currency.char_code} - {self.value} ({self.date.strftime('%Y-%m-%d')})"

    @property
    def unit_rate(self):
        """Курс за одну единицу валюты (с учетом номинала)."""
        if self.nominal == 0: # Предотвращение деления на ноль
            return self.value 
        return self.value / self.nominal
