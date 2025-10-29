from django.urls import path

from . import views

urlpatterns = [
    path("", views.index, name="index"),
    path("get-jobs/", views.get_jobs, name="get-jobs"),
    path("get-parts/<str:job_number>", views.get_parts, name="get-parts"),
    path("move-job/", views.move_job, name="move-job"),
]
