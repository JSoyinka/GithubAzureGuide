import pandas
from datetime import datetime
from datetime import timedelta

issue_df = pandas.read_csv("first_three_days.csv")
count_df = issue_df.groupby('issue_id').size().sort_values(ascending=False)
count_df = count_df[count_df.iloc[:] > 1]

total_times = []

for id in list(count_df.index):
    temp_df = issue_df.loc[issue_df['issue_id'] == id]
    temp_df = temp_df.sort_values(by=['time'], ascending=True)
    temp_list = temp_df['time'].tolist()
    if temp_df['action'].iloc[0] == 'closed' and len(temp_df) % 2 == 0:  
        temp_list = temp_df['time'].tolist()
        del temp_list[0]
        for i, time in enumerate(temp_list):
            temp_list[i] = datetime.strptime(time, '%Y-%m-%dT%XZ')
        temp_list.append(datetime.combine(temp_list[-1], datetime.min.time()) + timedelta(days=1))
    elif temp_df['action'].iloc[0] == 'closed' and len(temp_df) % 2 == 1:  
        temp_list = temp_df['time'].tolist()
        del temp_list[0]
        for i, time in enumerate(temp_list):
            temp_list[i] = datetime.strptime(time, '%Y-%m-%dT%XZ')
        if len(temp_list) == 1:
            temp_list.append(datetime.combine(temp_list[-1], datetime.min.time()) + timedelta(days=1))
    elif len(temp_df) % 2 == 0:  
        temp_list = temp_df['time'].tolist()
        for i, time in enumerate(temp_list):
            temp_list[i] = datetime.strptime(time, '%Y-%m-%dT%XZ')
    else:  
        temp_list = temp_df['time'].tolist()
        for i, time in enumerate(temp_list):
            temp_list[i] = datetime.strptime(time, '%Y-%m-%dT%XZ')
        temp_list.append(datetime.combine(temp_list[-1], datetime.min.time()) + timedelta(days=1))    
    temp_time = 0
    for i,k in zip(temp_list[0::2], temp_list[1::2]):
        temp_time += (k-i).total_seconds()
    total_times.append(temp_time)
        
print(float(sum(total_times)) / len(total_times))
