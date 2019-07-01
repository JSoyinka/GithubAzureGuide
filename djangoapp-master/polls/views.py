from django.shortcuts import get_object_or_404, render
from django.http import HttpResponseRedirect
from django.urls import reverse
from django.views import generic
from django.utils import timezone

from .models import Choice, Question
import os
from .models import Choice, Question, Githubevent
from django.contrib.auth.models import UserManager

class IndexView(generic.ListView):
    template_name = 'polls/index.html'
    context_object_name = 'latest_question_list'

    def get_queryset(self):
        """
        Return the last five published questions (not including those set to be
        published in the future).
        """
        # return Question.objects.filter(
        #     pub_date__lte=timezone.now()
        # ).order_by('-pub_date')[:5]
        if "data.csv" not in os.listdir():
            print(1)
            Githubevent.objects.to_csv(".\data.csv")

            from .pandas_data import filter_in_admin

            import logging, logging.config
            import sys

            LOGGING = {
                'version': 1,
                'handlers': {
                    'console': {
                        'class': 'logging.StreamHandler',
                        'stream': sys.stdout,
                    }
                },
                'root': {
                    'handlers': ['console'],
                    'level': 'INFO'
                }
            }   

            logging.config.dictConfig(LOGGING)
            logging.info(filter_in_admin())
        else:
            print(2)
            from .pandas_data import filter_in_admin

            import logging, logging.config
            import sys

            LOGGING = {
                'version': 1,
                'handlers': {
                    'console': {
                        'class': 'logging.StreamHandler',
                        'stream': sys.stdout,
                    }
                },
                'root': {
                    'handlers': ['console'],
                    'level': 'INFO'
                }
            }   

            logging.config.dictConfig(LOGGING)
            answer = filter_in_admin()
            logging.info(answer)
            return answer

class DetailView(generic.DetailView):
    model = Question
    template_name = 'polls/detail.html'


class ResultsView(generic.DetailView):
    model = Question
    template_name = 'polls/results.html'
    def get_queryset(self):
        """
        Excludes any questions that aren't published yet.
        """
        return Question.objects.filter(pub_date__lte=timezone.now())

def vote(request, question_id):
    question = get_object_or_404(Question, pk=question_id)
    try:
        selected_choice = question.choice_set.get(pk=request.POST['choice'])
    except (KeyError, Choice.DoesNotExist):
        # Redisplay the question voting form.
        return render(request, 'polls/detail.html', {
            'question': question,
            'error_message': "You didn't select a choice.",
        })
    else:
        selected_choice.votes += 1
        selected_choice.save()
        # Always return an HttpResponseRedirect after successfully dealing
        # with POST data. This prevents data from being posted twice if a
        # user hits the Back button.
        return HttpResponseRedirect(reverse('polls:results', args=(question.id,)))

