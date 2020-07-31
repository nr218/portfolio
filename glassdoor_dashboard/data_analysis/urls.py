from django.urls import path
from . import views

urlpatterns = [  # pragma: no cover
    path("", views.index, name="index"),
]
