from django.urls import path

from . import views

urlpatterns = [
    path("schedule/", views.schedule, name="schedule"),
    path("style-test/", views.style_test, name="style-test"),
    path("refresh/", views.refresh, name="refresh"),
]
