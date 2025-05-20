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