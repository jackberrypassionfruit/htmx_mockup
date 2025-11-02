from django.urls import path

from . import views

urlpatterns = [
    path("schedule/", views.schedule, name="schedule"),
    path("schedule/style-test/", views.style_test, name="style-test"),
]
