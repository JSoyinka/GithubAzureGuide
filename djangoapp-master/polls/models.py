import datetime

from django.db import models
from django.utils import timezone
from postgres_copy import CopyManager

# Create your models here.
class Githubevent(models.Model):
    # id = models.AutoField()
    issue_id = models.CharField(max_length=50, blank=True, null=True)    
    issue_title = models.CharField(max_length=255, blank=True, null=True)
    action = models.CharField(max_length=50, blank=True, null=True)      
    repository = models.CharField(max_length=255, blank=True, null=True)
    repo_owner = models.CharField(max_length=50, blank=True, null=True)  
    time = models.DateTimeField(blank=True, null=True)

    # objects = CopyManager()

    class Meta:
        managed = False
        db_table = 'githubevent'

class Question(models.Model):
    question_text = models.CharField(max_length=200)
    pub_date = models.DateTimeField('date published')
    def __str__(self):
        return self.question_text
    def was_published_recently(self):
        now = timezone.now()
        return now - datetime.timedelta(days=1) <= self.pub_date <= now
    def was_published_recently(self):
        now = timezone.now()
        return now - datetime.timedelta(days=1) <= self.pub_date <= now
    was_published_recently.admin_order_field = 'pub_date'
    was_published_recently.boolean = True
    was_published_recently.short_description = 'Published recently?'

class Choice(models.Model):
    question = models.ForeignKey(Question, on_delete=models.CASCADE)
    choice_text = models.CharField(max_length=200)
    votes = models.IntegerField(default=0)
    def __str__(self):
        return self.choice_text

