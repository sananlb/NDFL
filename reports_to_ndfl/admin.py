# reports_to_ndfl/admin.py
from django.contrib import admin
from .models import UploadedXMLFile, BrokerReport

@admin.register(UploadedXMLFile)
class UploadedXMLFileAdmin(admin.ModelAdmin):
    list_display = ('original_filename', 'user', 'year', 'uploaded_at')
    list_filter = ('year', 'user', 'uploaded_at')
    search_fields = ('original_filename', 'user__username', 'year')
    readonly_fields = ('uploaded_at',)
    date_hierarchy = 'uploaded_at'


@admin.register(BrokerReport)
class BrokerReportAdmin(admin.ModelAdmin):
    list_display = ('original_filename', 'broker_type', 'user', 'year', 'uploaded_at')
    list_filter = ('broker_type', 'year', 'user', 'uploaded_at')
    search_fields = ('original_filename', 'user__username', 'year')
    readonly_fields = ('uploaded_at',)
    date_hierarchy = 'uploaded_at'
