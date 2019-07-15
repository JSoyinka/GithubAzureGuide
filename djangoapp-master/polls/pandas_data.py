import pandas
import os
from datetime import datetime
from datetime import timedelta

from .models import Githubevent


def calculate_elapsed(elapsed_array):
    if elapsed_array[0][1] == "closed":
        del elapsed_array[0]
    if len(elapsed_array) % 2 == 1:
        del elapsed_array[-1]
    times = [i[0] for i in elapsed_array]
    for i,k in zip(elapsed_array[0::2], temp_list[1::2]):
        temp_time += (k-i).total_seconds()
    return temp_time
    
    
def filter_in_admin():
    issue_df = pandas.DataFrame(list(Githubevent.objects.all().values()))
    # count_df = issue_df.groupby('issue_id').size().sort_values(ascending=False)
    repo = "microsoft/vscode" # Get name of repo
    repo_df = issue_df.loc[issue_df['repo'] == repo]
    # Get Items with more than one item in the array
    repo_df = repo_df[repo_df.iloc[:] > 1]

    total_times = []

    for id in list(repo_def.index):
        temp_df = repo_df.loc[repo_df['elapsed_array'] == id]
        total_time = calculate_elapsed(repo_df['elapsed_array'].sort())
        total_times.append(temp_time)

    return total_times

        
    # for id in list(count_df.index):
    #     temp_df = issue_df.loc[issue_df['issue_id'] == id]
    #     temp_df = temp_df.sort_values(by=['time'], ascending=True)
    #     temp_list = temp_df['time'].tolist()
    #     if temp_df['action'].iloc[0] == 'closed' and len(temp_df) % 2 == 0:  
    #         temp_list = temp_df['time'].tolist()
    #         del temp_list[0]
    #         # for i, time in enumerate(temp_list):
    #         #     temp_list[i] = datetime.strptime(time, '%Y-%m-%d %X')
    #         temp_list.append(datetime.combine(temp_list[-1], datetime.min.time()) + timedelta(days=1))
    #     elif temp_df['action'].iloc[0] == 'closed' and len(temp_df) % 2 == 1:  
    #         temp_list = temp_df['time'].tolist()
    #         del temp_list[0]
    #         # for i, time in enumerate(temp_list):
    #         #     temp_list[i] = datetime.strptime(time, '%Y-%m-%d %X')
    #         if len(temp_list) == 1:
    #             temp_list.append(datetime.combine(temp_list[-1], datetime.min.time()) + timedelta(days=1))
    #     elif len(temp_df) % 2 == 0:  
    #         temp_list = temp_df['time'].tolist()
    #         # for i, time in enumerate(temp_list):
    #         #     temp_list[i] = datetime.strptime(time, '%Y-%m-%d %X')
    #     else:  
    #         temp_list = temp_df['time'].tolist()
    #         # for i, time in enumerate(temp_list):
    #         #     temp_list[i] = datetime.strptime(time, '%Y-%m-%d %X')
    #         temp_list.append(datetime.combine(temp_list[-1], datetime.min.time()) + timedelta(days=1))    
    #     temp_time = 0
    #     for i,k in zip(temp_list[0::2], temp_list[1::2]):
    #         temp_time += (k-i).total_seconds()
    #     total_times.append(temp_time)
            
    # return(float(sum(total_times)) / len(total_times) / 60)


