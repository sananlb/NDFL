from django.urls import path
from . import views

urlpatterns = [
    path('upload/', views.upload_xml_file, name='upload_xml_file'),
]
