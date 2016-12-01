import csv
import json
import requests
import networkx as nx
import numpy as np
import  matplotlib.pyplot as plt

def generate_graph(filename):
    G=nx.Graph()
    with open('edge_list.csv','rb') as inf:
        inf_read =csv.reader(inf)
        for row in inf_read:
            G.add_edge(row[0],row[1],relation=row[2])
    return G

def draw(g):
    efollower=[(u,v) for (u,v,d) in g.edges(data=True) if d['relation'] == "follower"]
    efollowing=[(u,v) for (u,v,d) in g.edges(data=True) if d['relation'] == "following"]
    posi = nx.spring_layout(g)
    plt.axis("off")
    nx.draw_networkx_edges(g,posi,edgelist=efollower, width=3, edge_color='g')
    nx.draw_networkx_edges(g,posi,edgelist=efollowing,width=3,edge_color='b')
    #nx.draw_networkx(g,pos = posi, node_size = 50,label = None)
    nx.draw_networkx_nodes(g,posi,node_size=20, label = None)
    xlim = plt.gca().get_xlim()
    ylim = plt.gca().get_ylim()
    plt.show()

def find_followers(users,connections):
    bad_user_url = []
    connections = []
    for user in users:
        url = "https://api.github.com/users/"+user+"/followers?client_id=e1cd2b204e55b076618d&client_secret=34e05f2749d4b49bc411f3ea1cdd07479f9379b3"
        user_data = requests.get(url)
        #print url
        if user_data.status_code is not 200:
	    bad_user_url.append(url)
            #print bad_user_url
	    continue
	user_data_json = user_data.json()
        #print user_data_json
        for follower in user_data_json:
            if follower["login"] in users:
                tuple = (str(follower["login"]),user,"follower")
                connections = connections + [tuple]
        #break
    print "bad_user_url is :"
    print bad_user_url
    return connections


def find_following(users,connections):
    bad_user_url = []
    connections = []
    for user in users:
        url = "https://api.github.com/users/"+user+"/following?client_id=e1cd2b204e55b076618d&client_secret=34e05f2749d4b49bc411f3ea1cdd07479f9379b3"
        user_data = requests.get(url)
        #print url
        if user_data.status_code is not 200:
	    bad_user_url.append(url)
            #print bad_user_url
	    continue
	user_data_json = user_data.json()
        #print user_data_json
        for follower in user_data_json:
            if follower["login"] in users:
                tuple = (str(follower["login"]),user,"following")
                connections = connections + [tuple]
        #break
    print "bad_user_url is :"
    print bad_user_url
    return connections


def find_collaborators(users,connections):
    bad_user_url = []
    connections = []
    for user in users:
        url = "https://api.github.com/users/"+user+"/repos?client_id=e1cd2b204e55b076618d&client_secret=34e05f2749d4b49bc411f3ea1cdd07479f9379b3"
        user_data = requests.get(url)
        #print url
        if user_data.status_code is not 200:
	    bad_user_url.append(url)
            #print bad_user_url
	    continue
	user_data_json = user_data.json()
        #print user_data_json
        for repo in user_data_json:
            contributors = requests.get(str(repo["contributors_url"]))
            if contributors.status_code is not 200:
	        bad_user_url.append(url)
                #print bad_user_url
	        continue
            contributors = contributors.json()
	    for contributor in contributors:
                if str(contributor["login"]) in users and str(contributor["login"]) != user:
		    tuple = (str(contributor["login"]),user,"collaborator")
		    connections = connections + [tuple]
	    #break
        #break
    print "bad_user_url is :"
    print bad_user_url
    return connections

def generate_edge_list(connections):
    with open('edge_list.csv','a') as out:
        csv_out=csv.writer(out)
        #csv_out.writerow(['name','num'])
        for row in connections:
            csv_out.writerow(row)
        

def main():
    user_list = []
    with open('user_orig_data.csv', 'rb') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',')
        next(csvfile)
        for row in spamreader:
           user_list = user_list + [row[2]]
    #connections = []
    #connections = find_followers(user_list[2000:3000], connections)
    #connections = connections + find_following(user_list[2000:3000], connections)
    #connections = connections + find_collaborators(user_list[0:200], connections)
    #generate_edge_list(connections)
    G =  generate_graph("edge_list.edgelist")
    draw(G)


if __name__ == "__main__":
	main()
