from django.urls import path

from .views import HomePageView, displayPageView

urlpatterns = [
    path('', HomePageView.as_view(), name='home'),
    path('display/', displayPageView, name='display'),
]