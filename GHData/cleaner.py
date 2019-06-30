import ujson
import json
import pandas
import os

# input_dict = ujson.loads("sample.json")

json_file = []

def parse_issue_events(file_name):
    repository = []
    repo_owner = []
    time = []
    action = []
    issue_id = []
    issue_title = []
    with open(file_name) as f:
        try:
            json_objs = json.load(f)
        except:
            print "Error loading " + file_name
        for json_data in json_objs:
            if (json_data['type'] == 'IssuesEvent'):
                json_file.append(json_data)
                repo_name = json_data['repo']['name']
                action.append(json_data['payload']['action'])
                issue_id.append(json_data['payload']['issue']['id'])
                issue_title.append(json_data['payload']['issue']['title'])
                repository.append(repo_name)
                repo_owner.append(repo_name.split('/')[0])
                time.append(json_data['payload']['issue']['updated_at'])
        f.close()
    return({"issue_id": issue_id, "issue_title": issue_title, "action": action, "repository": repository, "repo_owner": repo_owner, "time": time})

 

# def main():
#     parse_issue_events()
#     with open("sample2.json", 'w') as fp:
#         json.dump(json_file, fp, indent=2)

years = [2016]
months = [1]
days = [1]
hours = list(range(0,24))
hours.remove(19)
url_path = "{y}-{m:02d}-{d:02d}-{h}.json"

for y in years:
    for m in months:
        for d in days:
            for h in hours:
                p = url_path.format(y=y, m=m, d=d, h=h)
                output = parse_issue_events(p)
                if "i" + p not in os.listdir():
                    with open("i" + p, 'w') as fp:
                        json.dump(json_file, fp, indent=2)
                all_data = {"issue_id": [], "issue_title": [], "action": [], "repository": [], "repo_owner": [], "time": []}
                all_data["issue_id"] = all_data["issue_id"] + output["issue_id"]
                all_data["issue_title"] = all_data["issue_title"] + output["issue_title"]
                all_data["action"] = all_data["action"] + output["action"]
                all_data["repository"] = all_data["repository"] + output["repository"]
                all_data["repo_owner"] = all_data["repo_owner"] + output["repo_owner"]
                all_data["time"] = all_data["time"] + output["time"]
                print(p)


all_df = pandas.DataFrame(all_data)
print(all_df)
pandas.DataFrame.to_csv(all_df, "first_three_days.csv")
