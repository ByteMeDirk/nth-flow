from django.contrib.auth.models import User
from django.db import models

class UserProfile(models.Model):
    """
    User profile model
    """
    user = models.OneToOneField(User, on_delete=models.CASCADE)
