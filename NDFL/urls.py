# NDFL/urls.py (или ваш корневой urls.py)
from django.contrib import admin
from django.urls import path, include
from django.conf import settings # Для MEDIA_URL в DEBUG режиме
from django.conf.urls.static import static # Для MEDIA_URL в DEBUG режиме
from django.views.generic import RedirectView

urlpatterns = [
    path('', RedirectView.as_view(url='/reports/', permanent=False)),  # Редирект на главную
    path('admin/', admin.site.urls),
    path('reports/', include('reports_to_ndfl.urls')),
    path('currency/', include('currency_CBRF.urls')),
    path('accounts/', include('django.contrib.auth.urls')), # <--- ДОБАВИТЬ для стандартных URLов входа/выхода
]

# Для обслуживания медиафайлов в режиме разработки
if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT) # <--- ДОБАВИТЬ