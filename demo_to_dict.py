import csv
import json
from pymongo import MongoClient
import pandas as pd
import glob, os
from tqdm import tqdm
import pickle

def find_map(demo_info):
    for line in demo_info:
        if line.find(".bsp") != -1:
            match_map = line[line.find('maps/')+5:line.find('.bsp')]
            break
    return match_map


def player_info(demo_info):
    players = []
    for line in range(len(demo_info)):
        if demo_info[line].find("adding:player info:") != -1:
            player_dict = {}
            i = 1
            while demo_info[line+i] != "adding:player info:": # or demo_info[line+i].find(":") != -1
                if demo_info[line+i].find(":") == -1:
                    break
                player_dict[demo_info[line+i].split(':')[0].strip()] = demo_info[line+i].split(':')[1].strip()
                i += 1
            players.append(player_dict)
    return players


def event_org(event_string):
    '''
    Organize events by type
    '''
    sub_dict = {}
    if event_string.split(':')[0].strip() == 'position':
        sub_dict['x'] = event_string.split(':')[1].split(',')[0]
        sub_dict['y'] = event_string.split(':')[1].split(',')[1]
        sub_dict['z'] = event_string.split(':')[1].split(',')[2]
    elif event_string.split(':')[0].strip() == 'facing':
        sub_dict['pitch'] = event_string.split(':')[2].split(',')[0].strip()
        sub_dict['yaw'] =  event_string.split(':')[3].split(',')[0].strip()
    else:
        sub_dict[event_string.strip().split(':')[0].strip()] = event_string[len(event_string.split(':')[0])+2:].strip()
    return sub_dict


def parent_child(text_list):
    '''
    Determines how deep the item is in the dictionary
    For C++ class structure
    '''
    depth = 0
    sub_list = []
    event_list = []
    for line in text_list:
        previous_depth = depth
        if line == '{':
            depth += 1
        elif line == '}':
            depth -= 1
            if depth == 0:
                event_list.append(event_dict)
        elif depth == 0:
            event_dict = {'event_name': line}
        elif depth > 0:
            try: 
                event_dict.update(event_org(line))
            except:
                print line
                print type(line)
                print event_dict
                break
    return event_list

def events_to_dict(players, events, match_name, match_number, map_name):
    event_list = []
    round_number = 1
    event_number = 0
    round_prestart = 0
    round_end = 0
    for event in events:
        event_number += 1
        event['match'] = match_name
        event['match_number'] = match_number
        event['map_name'] = map_name
        if event['event_name'] in relevant_events:
            event['event_number'] = event_number
            if event['event_name'] == 'round_end':
                round_end = event_number
            if 'userid' in event.keys():
                if event['userid'] in [x['name'] + ' ' + '(id:' + x['userID'] + ')' for x in players]:
                    event['steamid'] = [x['xuid'] for x in players if x['name'] + ' ' + '(id:' + x['userID'] + ')' == event['userid']][0]
                #event['userid'] = event['userid'].split(' (id:')[0]
            if 'attacker' in event.keys():
                if event['attacker'] in [x['name'] + ' ' + '(id:' + x['userID'] + ')' for x in players]:
                    event['attacker_steamid'] = [x['xuid'] for x in players if x['name'] + ' ' + '(id:' + x['userID'] + ')' == event['attacker']][0]
                #event['attacker'] = event['attacker'].split(' (id:')[0]
            if event['event_name'] == 'player_spawn':
                if 'teamnum' in event.keys():
                    if event['teamnum'] == '2':
                        event['team'] = 'T'
                    elif event['teamnum'] == '3':
                        event['team'] = 'CT'
                    elif event['teamnum'] not in ['2', '3', 2, 3]:
                        continue
                if 'team' in event.keys():
                    if event['team'] == '2':
                        event['team'] = 'T'
                    elif event['team'] == '3':
                        event['team'] = 'CT'
                    elif event['team'] not in ['T', 'CT']:
                        continue
            event['round_number'] = round_number
            if event['event_name'] == 'round_prestart' and round_prestart < round_end:
                round_number += 1
            if event['event_name'] == 'round_prestart':
                round_prestart = event_number
            event_list.append(event)
    return event_list


def find_match_begin(events):
    event_number = 0
    warmup = 0
    round_prestart = 0
    begin_new_match = 0
    match_start = 0
    round_end = 0
    prestart_event = 0
    for event in events:
        event_number += 1
        if event['event_name'] =='round_announce_warmup':
            warmup = event_number
        if event['event_name'] =='round_prestart':
            round_prestart = event_number
        if event['event_name'] =='begin_new_match':
            begin_new_match = event_number
        if event['event_name'] =='round_announce_match_start':
            match_start = event_number
        if event['event_name'] =='round_end':
            round_end = event_number
        if warmup < round_prestart < begin_new_match < match_start < round_end:
            prestart_event = round_prestart
            break
    return prestart_event

client = MongoClient()
db = client.counter_strike
match_log = db.match_log

cwd = '/Users/johnwalters/ds/metis/projects/cs_go_stats'
os.chdir(cwd + "/demos_more")
demo_info_files = []
demo_event_files = []

for file in glob.glob("*_userinfo.txt"):
    demo_info_files.append(file)

for file in glob.glob("*_events.txt"):
    demo_event_files.append(file)
    
print len(demo_info_files)

match_name_number = [x[:-13] for x in demo_info_files]
match_name = [x[:-5] for x in match_name_number]


df_all = pd.DataFrame()
dict_all = []
relevant_events = ['player_death', 'weapon_fire', 'player_hurt', 'round_end', 'player_spawn', 'round_prestart', 'begin_new_match','round_announce_match_start', 'bomb_planted','bomb_defused','bomb_exploded']
for demo_file in tqdm(match_name_number):#tqdm(matches_test)
    match_name = demo_file[:-5]
    match_number = int(demo_file[-5:])
    #try:
    game_text = open(cwd +'/demos_more/' + demo_file + '_events.txt', 'r')
    game_info = open(cwd + '/demos_more/' + demo_file + '_userinfo.txt' , 'r').read().split("\n")
    map_name = find_map(game_info)
    steam_ids = player_info(game_info)
    game_data = game_text.read()
    game_data = game_data.split("\n")
    events = parent_child(game_data)
    start_event = find_match_begin(events)
    event_list = events_to_dict(steam_ids, events, match_name, match_number, map_name)
    dict_all = dict_all + event_list
    #except:
    #    failed_demos.append(demo_file)

#with open('all_demos_as_dict.pkl', 'w') as picklefile:
#            pickle.dump(dict_all, picklefile)

df_all = pd.DataFrame.from_dict(dict_all)

with open('all_demos_as_dict.pkl', 'w') as picklefile:
            pickle.dump(df_all, picklefile)