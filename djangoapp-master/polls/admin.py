from django.contrib import admin
import os
from .models import Choice, Question, Githubevent
from django.contrib.auth.models import UserManager

class ChoiceInline(admin.TabularInline):
    model = Choice
    extra = 3

class QuestionAdmin(admin.ModelAdmin):
    fieldsets = [
        (None,               {'fields': ['question_text']}),
        ('Date information', {'fields': ['pub_date'], 'classes': ['collapse']}),
    ]
    inlines = [ChoiceInline]
    list_display = ('question_text', 'pub_date', 'was_published_recently')
    list_filter = ['pub_date']
    search_fields = ['question_text']

admin.site.register(Question, QuestionAdmin)

# print(5)
# if "data.csv" not in os.listdir():
#     print(1)
#     Githubevent.objects.to_csv(".\data.csv")

#     from .pandas_data import filter_in_admin

#     import logging, logging.config
#     import sys

#     LOGGING = {
#         'version': 1,
#         'handlers': {
#             'console': {
#                 'class': 'logging.StreamHandler',
#                 'stream': sys.stdout,
#             }
#         },
#         'root': {
#             'handlers': ['console'],
#             'level': 'INFO'
#         }
#     }   

#     logging.config.dictConfig(LOGGING)
#     logging.info(filter_in_admin())
# else:
#     print(2)
#     from .pandas_data import filter_in_admin

#     import logging, logging.config
#     import sys

#     LOGGING = {
#         'version': 1,
#         'handlers': {
#             'console': {
#                 'class': 'logging.StreamHandler',
#                 'stream': sys.stdout,
#             }
#         },
#         'root': {
#             'handlers': ['console'],
#             'level': 'INFO'
#         }
#     }   

#     logging.config.dictConfig(LOGGING)
#     logging.info(filter_in_admin())