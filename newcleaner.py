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
        except json.JSONDecodeError as e:
            print("Error loading " + file_name)
            f.close()
            return(None)
        for json_data in json_objs:
            if (json_data['type'] == 'IssuesEvent'):
                # json_file.append(json_data)
                repo_name = json_data['repo']['name']
                action.append(json_data['payload']['action'])
                issue_id.append(json_data['payload']['issue']['id'])
                temp_title = json_data['payload']['issue']['title'].replace('\n','')
                temp_title.replace('\'','\"')
                issue_title.append(title[:250])
                repository.append(repo_name)
                repo_owner.append(repo_name.split('/')[0])
                time.append(datetime.strptime(json_data['payload']['issue']['updated_at'], '%Y-%m-%dT%XZ'))
        f.close()
    return({"issue_id": issue_id, "issue_title": issue_title, "action": action, "repository": repository, "repo_owner": repo_owner, "time": time})
