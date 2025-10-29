from django.urls import path

from . import views

urlpatterns = [
    path("", views.index, name="index"),
    path("get-parts/<str:job_number>", views.get_parts, name="get_parts"),
]
