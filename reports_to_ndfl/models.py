# reports_to_ndfl/models.py
from django.db import models
from django.contrib.auth.models import User

class UploadedXMLFile(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE, verbose_name="Пользователь")
    xml_file = models.FileField(upload_to='xml_files/', verbose_name="XML файл")
    year = models.IntegerField(
        help_text="Год отчета, извлеченный из тега <date_end> XML-файла",
        db_index=True,
        verbose_name="Год отчета (из XML)"
    )
    uploaded_at = models.DateTimeField(auto_now_add=True, verbose_name="Дата загрузки")
    original_filename = models.CharField(
        max_length=255,
        help_text="Оригинальное имя загруженного файла",
        verbose_name="Оригинальное имя файла"
    )

    def __str__(self):
        year_display = str(self.year) if self.year is not None and self.year != 0 else "Год не определен"
        user_display = self.user.username if self.user else "Пользователь не определен"
        return f"{self.original_filename} ({user_display} - Отчет за {year_display})"

    class Meta:
        verbose_name = "Загруженный XML файл"
        verbose_name_plural = "Загруженные XML файлы"
        # Уникальность для пользователя, оригинального имени файла и года из <date_end>
        unique_together = ('user', 'original_filename', 'year')
        ordering = ['-uploaded_at']


class BrokerReport(models.Model):
    BROKER_TYPES = [
        ('ffg', 'Freedom Finance Global'),
        ('ib', 'Interactive Brokers'),
    ]

    user = models.ForeignKey(User, on_delete=models.CASCADE, verbose_name="Пользователь")
    broker_type = models.CharField(
        max_length=10,
        choices=BROKER_TYPES,
        verbose_name="Тип брокера"
    )
    report_file = models.FileField(
        upload_to='broker_reports/%Y/%m/',
        verbose_name="Файл отчета"
    )
    year = models.IntegerField(
        help_text="Год отчета",
        db_index=True,
        verbose_name="Год отчета"
    )
    uploaded_at = models.DateTimeField(auto_now_add=True, verbose_name="Дата загрузки")
    original_filename = models.CharField(
        max_length=255,
        verbose_name="Оригинальное имя файла"
    )

    account_number = models.CharField(max_length=50, blank=True, verbose_name="Номер счета")
    base_currency = models.CharField(max_length=3, default='USD', verbose_name="Базовая валюта")

    class Meta:
        verbose_name = "Брокерский отчет"
        verbose_name_plural = "Брокерские отчеты"
        unique_together = ('user', 'broker_type', 'original_filename', 'year')
        ordering = ['-uploaded_at']

    def __str__(self):
        broker_display = self.get_broker_type_display()
        year_display = str(self.year) if self.year is not None and self.year != 0 else "Год не определен"
        return f"{broker_display} - {self.original_filename} ({year_display})"

    @property
    def file_extension(self):
        return 'xml' if self.broker_type == 'ffg' else 'csv'
