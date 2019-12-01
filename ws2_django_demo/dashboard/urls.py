from django.urls import path
from . import views


urlpatterns = [
     path('homepage', views.homepage, name='homepage'),
     path('result', views.result, name='result')
]
