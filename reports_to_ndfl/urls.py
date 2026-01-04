from django.urls import path
from django.views.generic import RedirectView
from . import views

urlpatterns = [
    path('', RedirectView.as_view(url='upload/', permanent=False)),
    path('upload/', views.upload_xml_file, name='upload_xml_file'),
    path('delete/<int:file_id>/', views.delete_xml_file, name='delete_xml_file'),
    path('download-pdf/', views.download_pdf, name='download_pdf'),
]
