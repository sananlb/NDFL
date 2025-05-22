# NDFL/urls.py (или ваш корневой urls.py)
from django.contrib import admin
from django.urls import path, include
from django.conf import settings
from django.conf.urls.static import static

# Импортируем auth_views
from django.contrib.auth import views as auth_views 

urlpatterns = [
    path('admin/', admin.site.urls),
    path('reports/', include('reports_to_ndfl.urls')),
    path('currency/', include('currency_CBRF.urls')),

    # Замените или закомментируйте ЭТУ СТРОКУ (если она была):
    # path('accounts/', include('django.contrib.auth.urls')),

    # И ДОБАВЬТЕ ЭТИ СТРОКИ для явного определения LoginView и LogoutView,
    # указывая правильный путь к вашему шаблону:
    path('accounts/login/', auth_views.LoginView.as_view(template_name='reports_to_ndfl/login.html'), name='login'), # <--- ИЗМЕНЕНИЕ ЗДЕСЬ
    path('accounts/logout/', auth_views.LogoutView.as_view(next_page='login'), name='logout'),

]

if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)