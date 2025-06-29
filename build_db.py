import sqlite3
import argparse
import logging
import time
import pandas as pd
from requests.exceptions import ConnectionError
from riotwatcher import LolWatcher, ApiError

class LolInterface:
    def __init__(self, api_key):
        self.api_key = api_key
        self.lol_watcher = LolWatcher(api_key)
        self.last_request_time = 0
        self.min_request_interval = 0.1

    def update_key(self, api_key):
        self.api_key = api_key
        self.lol_watcher = LolWatcher(api_key)

    def rate_limit_wait(self):
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.min_request_interval:
            time.sleep(self.min_request_interval - time_since_last)
        self.last_request_time = time.time()

def get_top_players(region, testing=False):
    while True:
        try:
            challengers = lol_obj.lol_watcher.league.challenger_by_queue(region, 'RANKED_SOLO_5x5')
            gms = lol_obj.lol_watcher.league.grandmaster_by_queue(region, 'RANKED_SOLO_5x5')
            masters = lol_obj.lol_watcher.league.masters_by_queue(region, 'RANKED_SOLO_5x5')
            break
        except ApiError as e:
            if e.response.status_code == 504:
                print("504 Gateway Timeout. Waiting 30s and retrying...")
                time.sleep(30)
            else:
                raise
        except ConnectionError:
            print("Connection Error, waiting 10s then retrying...")
            time.sleep(10)
    if testing:
        all_top_players = [challengers]
    else:
        all_top_players = [challengers, gms, masters]
    summoner_ids = []
    league_entries = []
    for division in all_top_players:
        for entry in division['entries']:
            summoner_ids.append(entry['summonerId'])
            league_entries.append(entry)
    return summoner_ids, league_entries

def get_summoner_profiles(summoner_ids):
    summoner_profiles = {}
    skipped_count = 0
    for i, summoner_id in enumerate(summoner_ids):
        while True:
            try:
                lol_obj.rate_limit_wait()
                profile = lol_obj.lol_watcher.summoner.by_id(region, summoner_id)
                summoner_profiles[summoner_id] = {
                    'summonerId': summoner_id,
                    'accountId': profile.get('accountId'),
                    'puuid': profile.get('puuid'),
                    'name': profile.get('name'),
                    'profileIconId': profile.get('profileIconId'),
                    'revisionDate': profile.get('revisionDate'),
                    'summonerLevel': profile.get('summonerLevel')
                }
                if (i + 1) % 100 == 0:
                    print(f"Processed {i + 1}/{len(summoner_ids)} summoner profiles (skipped: {skipped_count})")
                break
            except ApiError as e:
                if e.response.status_code == 403:
                    print(f"Skipping summoner {summoner_id} due to 403 error")
                    skipped_count += 1
                    break
                elif e.response.status_code == 429:
                    print(f"Rate limit exceeded, waiting 60 seconds...")
                    time.sleep(60)
                    lol_obj.rate_limit_wait()
                elif e.response.status_code == 504:
                    print("504 Gateway Timeout. Waiting 30s and retrying...")
                    time.sleep(30)
                else:
                    print(f"{e.response.status_code}: Waiting 10s")
                    time.sleep(10)
                    lol_obj.rate_limit_wait()
            except ConnectionError:
                print(f"Connection Error, waiting 10s then resuming")
                time.sleep(10)
                lol_obj.rate_limit_wait()
    print(f"Successfully processed {len(summoner_profiles)} summoner profiles, skipped {skipped_count}")
    return summoner_profiles

def get_puuid(summoner_ids):
    summid_to_puuid = {}
    skipped_count = 0
    for i, summoner in enumerate(summoner_ids):
        while True:
            try:
                lol_obj.rate_limit_wait()
                summid_to_puuid[summoner] = lol_obj.lol_watcher.summoner.by_id(region, summoner)['puuid']
                if (i + 1) % 100 == 0:
                    print(f"Processed {i + 1}/{len(summoner_ids)} summoners (skipped: {skipped_count})")
                break
            except ApiError as e:
                if e.response.status_code == 403:
                    print(f"Skipping summoner {summoner} due to 403 error (API key restrictions)")
                    skipped_count += 1
                    break
                elif e.response.status_code == 429:
                    print(f"Rate limit exceeded, waiting 60 seconds...")
                    time.sleep(60)
                    lol_obj.rate_limit_wait()
                elif e.response.status_code == 504:
                    print("504 Gateway Timeout. Waiting 30s and retrying...")
                    time.sleep(30)
                else:
                    print(f"{e.response.status_code}: Waiting 10s")
                    time.sleep(10)
                    lol_obj.rate_limit_wait()
            except ConnectionError as e:
                print(f"Connection Error, waiting 10s then resuming")
                time.sleep(10)
                lol_obj.rate_limit_wait()
    print(f"Successfully processed {len(summid_to_puuid)} summoners, skipped {skipped_count}")
    return summid_to_puuid

def get_champ_mastery(summoner_ids, summid_to_puuid, points=100000):
    mastery_dict = {}
    skipped_count = 0
    for i, summoner in enumerate(summoner_ids):
        if summoner not in summid_to_puuid:
            skipped_count += 1
            continue
        while True:
            try:
                lol_obj.rate_limit_wait()
                masteries = lol_obj.lol_watcher.champion_mastery.by_summoner(region, summoner)
                if (i + 1) % 100 == 0:
                    print(f"Processed {i + 1}/{len(summoner_ids)} champion masteries (skipped: {skipped_count})")
                break
            except ApiError as e:
                if e.response.status_code == 403:
                    print(f"Skipping champion mastery for summoner {summoner} due to 403 error")
                    skipped_count += 1
                    break
                elif e.response.status_code == 429:
                    print(f"Rate limit exceeded, waiting 60 seconds...")
                    time.sleep(60)
                    lol_obj.rate_limit_wait()
                elif e.response.status_code == 504:
                    print("504 Gateway Timeout. Waiting 30s and retrying...")
                    time.sleep(30)
                else:
                    print(f"{e.response.status_code}: Waiting 10s")
                    time.sleep(10)
                    lol_obj.rate_limit_wait()
            except ConnectionError as e:
                print(f"Connection Error, waiting 10s then resuming")
                time.sleep(10)
                lol_obj.rate_limit_wait()
        if summoner in summid_to_puuid:
            puuid = summid_to_puuid[summoner]
            mastery_dict[puuid] = []
            for mastery in masteries:
                if mastery.get('championPoints') > 100000:
                    mastery_dict[puuid].append(mastery.get('championId'))
    print(f"Successfully processed {len(mastery_dict)} champion masteries, skipped {skipped_count}")
    return mastery_dict

def get_detailed_match_data(mastery_dict, num_matches=10):
    match_data_rows = []
    player_data_rows = []
    team_data_rows = []
    matches_scanned = set()
    
    for puuid, champion_ids in mastery_dict.items():
        while True:
            try:
                lol_obj.rate_limit_wait()
                match_list = lol_obj.lol_watcher.match.matchlist_by_puuid(region, puuid, count=num_matches)
                break
            except ApiError as e:
                if e.response.status_code == 403:
                    print("bad or expired API key, paste new one here:")
                    api_key = input()
                    lol_obj.update_key(api_key=api_key)
                    lol_obj.rate_limit_wait()
                elif e.response.status_code == 429:
                    print(f"Rate limit exceeded, waiting 60 seconds...")
                    time.sleep(60)
                    lol_obj.rate_limit_wait()
                elif e.response.status_code == 504:
                    print("504 Gateway Timeout. Waiting 30s and retrying...")
                    time.sleep(30)
                else:
                    print(f"{e.response.status_code}: Waiting 10s")
                    time.sleep(10)
                    lol_obj.rate_limit_wait()
            except ConnectionError:
                print(f"Connection Error, waiting 10s then resuming")
                time.sleep(10)
                lol_obj.rate_limit_wait()
        
        for match_id in match_list:
            if match_id not in matches_scanned:
                while True:
                    try:
                        lol_obj.rate_limit_wait()
                        match_data = lol_obj.lol_watcher.match.by_id(region, match_id)
                        break
                    except ApiError as e:
                        if e.response.status_code == 403:
                            print("bad or expired API key, paste new one here:")
                            api_key = input()
                            lol_obj.update_key(api_key=api_key)
                            lol_obj.rate_limit_wait()
                        elif e.response.status_code == 429:
                            print(f"Rate limit exceeded, waiting 60 seconds...")
                            time.sleep(60)
                            lol_obj.rate_limit_wait()
                        elif e.response.status_code == 504:
                            print("504 Gateway Timeout. Waiting 30s and retrying...")
                            time.sleep(30)
                        else:
                            print("Connection error, waiting 10s then resuming operation")
                            time.sleep(10)
                            lol_obj.rate_limit_wait()
                    except ConnectionError:
                        print(f"Connection Error, waiting 10s then resuming")
                        time.sleep(10)
                        lol_obj.rate_limit_wait()
                
                if match_data['info']['gameMode'] == 'CLASSIC':
                    match_info = match_data['info']
                    match_metadata = {
                        'match_id': match_id,
                        'game_creation': match_info.get('gameCreation'),
                        'game_duration': match_info.get('gameDuration'),
                        'game_end_timestamp': match_info.get('gameEndTimestamp'),
                        'game_id': match_info.get('gameId'),
                        'game_mode': match_info.get('gameMode'),
                        'game_name': match_info.get('gameName'),
                        'game_start_timestamp': match_info.get('gameStartTimestamp'),
                        'game_type': match_info.get('gameType'),
                        'game_version': match_info.get('gameVersion'),
                        'map_id': match_info.get('mapId'),
                        'platform_id': match_info.get('platformId'),
                        'queue_id': match_info.get('queueId'),
                        'tournament_code': match_info.get('tournamentCode')
                    }
                    match_data_rows.append(match_metadata)
                    
                    teams = match_info.get('teams', [])
                    for team in teams:
                        team_data = {
                            'match_id': match_id,
                            'team_id': team.get('teamId'),
                            'win': team.get('win'),
                            'objectives': team.get('objectives', {})
                        }
                        team_data_rows.append(team_data)
                    
                    participants = match_info.get('participants', [])
                    for participant in participants:
                        if participant['puuid'] in mastery_dict.keys():
                            if participant['championId'] in mastery_dict[participant['puuid']]:
                                player_data = {
                                    'match_id': match_id,
                                    'puuid': participant['puuid'],
                                    'summoner_id': participant.get('summonerId'),
                                    'summoner_name': participant.get('summonerName'),
                                    'champion_id': participant.get('championId'),
                                    'champion_name': participant.get('championName'),
                                    'team_id': participant.get('teamId'),
                                    'team_position': participant.get('teamPosition'),
                                    'role': participant.get('role'),
                                    'win': participant.get('win'),
                                    'kills': participant.get('kills', 0),
                                    'deaths': participant.get('deaths', 0),
                                    'assists': participant.get('assists', 0),
                                    'double_kills': participant.get('doubleKills', 0),
                                    'triple_kills': participant.get('tripleKills', 0),
                                    'quadra_kills': participant.get('quadraKills', 0),
                                    'penta_kills': participant.get('pentaKills', 0),
                                    'total_damage_dealt': participant.get('totalDamageDealt', 0),
                                    'total_damage_dealt_to_champions': participant.get('totalDamageDealtToChampions', 0),
                                    'total_damage_taken': participant.get('totalDamageTaken', 0),
                                    'damage_self_mitigated': participant.get('damageSelfMitigated', 0),
                                    'total_heal': participant.get('totalHeal', 0),
                                    'total_units_healed': participant.get('totalUnitsHealed', 0),
                                    'time_ccing_others': participant.get('timeCCingOthers', 0),
                                    'total_time_cc_dealt': participant.get('totalTimeCCDealt', 0),
                                    'gold_earned': participant.get('goldEarned', 0),
                                    'gold_spent': participant.get('goldSpent', 0),
                                    'total_minions_killed': participant.get('totalMinionsKilled', 0),
                                    'neutral_minions_killed': participant.get('neutralMinionsKilled', 0),
                                    'neutral_minions_killed_team_jungle': participant.get('neutralMinionsKilledTeamJungle', 0),
                                    'neutral_minions_killed_enemy_jungle': participant.get('neutralMinionsKilledEnemyJungle', 0),
                                    'vision_score': participant.get('visionScore', 0),
                                    'vision_wards_bought_in_game': participant.get('visionWardsBoughtInGame', 0),
                                    'wards_killed': participant.get('wardsKilled', 0),
                                    'wards_placed': participant.get('wardsPlaced', 0),
                                    'detector_wards_placed': participant.get('detectorWardsPlaced', 0),
                                    'objectives_stolen': participant.get('objectivesStolen', 0),
                                    'objectives_stolen_assists': participant.get('objectivesStolenAssists', 0),
                                    'item0': participant.get('item0', 0),
                                    'item1': participant.get('item1', 0),
                                    'item2': participant.get('item2', 0),
                                    'item3': participant.get('item3', 0),
                                    'item4': participant.get('item4', 0),
                                    'item5': participant.get('item5', 0),
                                    'item6': participant.get('item6', 0),
                                    'perk0': participant.get('perk0', 0),
                                    'perk1': participant.get('perk1', 0),
                                    'perk2': participant.get('perk2', 0),
                                    'perk3': participant.get('perk3', 0),
                                    'perk4': participant.get('perk4', 0),
                                    'perk5': participant.get('perk5', 0),
                                    'perk_primary_style': participant.get('perkPrimaryStyle', 0),
                                    'perk_sub_style': participant.get('perkSubStyle', 0),
                                    'summoner1_id': participant.get('summoner1Id', 0),
                                    'summoner2_id': participant.get('summoner2Id', 0),
                                    'first_blood_assist': participant.get('firstBloodAssist', False),
                                    'first_blood_kill': participant.get('firstBloodKill', False),
                                    'first_tower_assist': participant.get('firstTowerAssist', False),
                                    'first_tower_kill': participant.get('firstTowerKill', False),
                                    'game_ended_in_early_surrender': participant.get('gameEndedInEarlySurrender', False),
                                    'game_ended_in_surrender': participant.get('gameEndedInSurrender', False),
                                    'individual_position': participant.get('individualPosition', ''),
                                    'lane': participant.get('lane', ''),
                                    'largest_critical_strike': participant.get('largestCriticalStrike', 0),
                                    'largest_killing_spree': participant.get('largestKillingSpree', 0),
                                    'largest_multi_kill': participant.get('largestMultiKill', 0),
                                    'longest_time_spent_living': participant.get('longestTimeSpentLiving', 0),
                                    'magic_damage_dealt': participant.get('magicDamageDealt', 0),
                                    'magic_damage_dealt_to_champions': participant.get('magicDamageDealtToChampions', 0),
                                    'magic_damage_taken': participant.get('magicDamageTaken', 0),
                                    'physical_damage_dealt': participant.get('physicalDamageDealt', 0),
                                    'physical_damage_dealt_to_champions': participant.get('physicalDamageDealtToChampions', 0),
                                    'physical_damage_taken': participant.get('physicalDamageTaken', 0),
                                    'true_damage_dealt': participant.get('trueDamageDealt', 0),
                                    'true_damage_dealt_to_champions': participant.get('trueDamageDealtToChampions', 0),
                                    'true_damage_taken': participant.get('trueDamageTaken', 0),
                                    'turret_kills': participant.get('turretKills', 0),
                                    'unreal_kills': participant.get('unrealKills', 0)
                                }
                                player_data_rows.append(player_data)
                
                matches_scanned.add(match_id)
    
    return match_data_rows, player_data_rows, team_data_rows, matches_scanned

def get_match_data(mastery_dict, num_matches=10):
    data_rows = []
    matches_scanned = set()
    features = ['puuid', 'championId', 'item0', 'item1', 'item2', 'item3', 'item4',
                'item5', 'item6', 'kills', 'deaths', 'assists', 'totalDamageDealtToChampions',
                'role', 'teamPosition', 'teamId', 'gameEndedInEarlySurrender', 'win']
    for key, value in mastery_dict.items():
        while True:
            try:
                lol_obj.rate_limit_wait()
                match_list = lol_obj.lol_watcher.match.matchlist_by_puuid(region, key, count = num_matches)
                break
            except ApiError as e:
                if e.response.status_code == 403:
                    print("bad or expired API key, paste new one here:")
                    api_key = input()
                    lol_obj.update_key(api_key=api_key)
                    lol_obj.rate_limit_wait()
                elif e.response.status_code == 429:
                    print(f"Rate limit exceeded, waiting 60 seconds...")
                    time.sleep(60)
                    lol_obj.rate_limit_wait()
                elif e.response.status_code == 504:
                    print("504 Gateway Timeout. Waiting 30s and retrying...")
                    time.sleep(30)
                else:
                    print(f"{e.response.status_code}: Waiting 10s")
                    time.sleep(10)
                    lol_obj.rate_limit_wait()
            except ConnectionError as e:
                print(f"Connection Error, waiting 10s then resuming")
                time.sleep(10)
                lol_obj.rate_limit_wait()
        for match in match_list:
            if match not in matches_scanned:
                while True:
                    try:
                        lol_obj.rate_limit_wait()
                        match_data = lol_obj.lol_watcher.match.by_id(region, match)
                        break
                    except ApiError as e:
                        if e.response.status_code == 403:
                            print("bad or expired API key, paste new one here:")
                            api_key = input()
                            lol_obj.update_key(api_key=api_key)
                            lol_obj.rate_limit_wait()
                        elif e.response.status_code == 429:
                            print(f"Rate limit exceeded, waiting 60 seconds...")
                            time.sleep(60)
                            lol_obj.rate_limit_wait()
                        elif e.response.status_code == 504:
                            print("504 Gateway Timeout. Waiting 30s and retrying...")
                            time.sleep(30)
                        else:
                            print("Connection error, waiting 10s then resuming operation")
                            time.sleep(10)
                            lol_obj.rate_limit_wait()
                    except ConnectionError as e:
                        print(f"Connection Error, waiting 10s then resuming")
                        time.sleep(10)
                        lol_obj.rate_limit_wait()
                if match_data['info']['gameMode'] == 'CLASSIC':
                    player_info = match_data['info']['participants']
                    champions_in_game = {}
                    champions_in_game[100] = []
                    champions_in_game[200] = []
                    for player in player_info:
                        champions_in_game[player['teamId']].append(player['championId'])
                        if player['puuid'] in mastery_dict.keys():
                            if player['championId'] in mastery_dict[player['puuid']]:
                                player_data = {}
                                for feature in features:
                                    player_data[feature] = player[feature]
                                player_data['patch'] = match_data['info']['gameVersion']
                                player_data['match_id'] = match
                                player_data['champions_in_game'] = champions_in_game
                                data_rows.append(player_data)
                matches_scanned.add(match)
    return data_rows, matches_scanned

def match_to_df(data_rows):
    df = pd.DataFrame.from_dict(data_rows)
    df = df[df['teamPosition'] != '']
    df = df[df['gameEndedInEarlySurrender'] == False]

    df['teammates_championId'] = df.apply(lambda x: x['champions_in_game'].get(x['teamId']), axis=1)

    opposite_team_dict = {100:200, 200:100}
    df['opposite_team_id'] = df['teamId'].map(opposite_team_dict)
    df['enemies_championId'] = df.apply(lambda x: x['champions_in_game'].get(x['opposite_team_id']), axis=1)

    player_cols = ["enemies_championId", "teammates_championId"]
    for col in player_cols:
        temp_df = df[col].apply(pd.Series)
        temp_df = temp_df.add_prefix(col[:-10])
        df = pd.concat([df, temp_df], axis=1)

    df = df.drop(labels=["teammates_championId", "enemies_championId"], axis=1)
    df = df.drop(labels=["champions_in_game","opposite_team_id"], axis=1)

    return df

def df_to_sql(df, database='data/matches.db', table_name='player_items_champions'):
    conn = sqlite3.connect(database)
    df.to_sql(name="player_items_champions", con=conn, if_exists='append', index=False)

def build_comprehensive_database(league_entries, summoner_profiles, match_data_rows, player_data_rows, team_data_rows, 
                                champion_statistics=None, champion_bans=None, match_statistics=None, 
                                team_performance_stats=None, objective_statistics=None, item_statistics=None, 
                                item_combination_stats=None, rune_statistics=None, rune_combination_stats=None, 
                                database='league_players.db'):
    conn = sqlite3.connect(database)
    
    league_df = pd.DataFrame(league_entries)
    league_df.to_sql(name="league_players", con=conn, if_exists='replace', index=False)
    print(f"✅ League players table: {len(league_entries)} entries")
    
    if summoner_profiles:
        profiles_df = pd.DataFrame(list(summoner_profiles.values()))
        profiles_df.to_sql(name="summoner_profiles", con=conn, if_exists='replace', index=False)
        print(f"✅ Summoner profiles table: {len(summoner_profiles)} entries")
    
    if match_data_rows:
        match_df = pd.DataFrame(match_data_rows)
        match_df.to_sql(name="match_metadata", con=conn, if_exists='replace', index=False)
        print(f"✅ Match metadata table: {len(match_data_rows)} entries")
    
    if player_data_rows:
        player_df = pd.DataFrame(player_data_rows)
        player_df.to_sql(name="player_performance", con=conn, if_exists='replace', index=False)
        print(f"✅ Player performance table: {len(player_data_rows)} entries")
    
    if team_data_rows:
        team_df = pd.DataFrame(team_data_rows)
        team_df.to_sql(name="team_performance", con=conn, if_exists='replace', index=False)
        print(f"✅ Team performance table: {len(team_data_rows)} entries")
    
    if champion_statistics:
        champion_df = pd.DataFrame(champion_statistics)
        champion_df.to_sql(name="champion_statistics", con=conn, if_exists='replace', index=False)
        print(f"✅ Champion statistics table: {len(champion_statistics)} entries")
    
    if champion_bans:
        bans_df = pd.DataFrame(champion_bans)
        bans_df.to_sql(name="champion_bans", con=conn, if_exists='replace', index=False)
        print(f"✅ Champion bans table: {len(champion_bans)} entries")
    
    if match_statistics:
        match_stats_df = pd.DataFrame(match_statistics)
        match_stats_df.to_sql(name="match_statistics", con=conn, if_exists='replace', index=False)
        print(f"✅ Match statistics table: {len(match_statistics)} entries")
    
    if team_performance_stats:
        team_perf_df = pd.DataFrame(team_performance_stats)
        team_perf_df.to_sql(name="team_performance_stats", con=conn, if_exists='replace', index=False)
        print(f"✅ Team performance statistics table: {len(team_performance_stats)} entries")
    
    if objective_statistics:
        objective_df = pd.DataFrame(objective_statistics)
        objective_df.to_sql(name="objective_statistics", con=conn, if_exists='replace', index=False)
        print(f"✅ Objective statistics table: {len(objective_statistics)} entries")
    
    if item_statistics:
        item_df = pd.DataFrame(item_statistics)
        item_df.to_sql(name="item_statistics", con=conn, if_exists='replace', index=False)
        print(f"✅ Item statistics table: {len(item_statistics)} entries")
    
    if item_combination_stats:
        item_combo_df = pd.DataFrame(item_combination_stats)
        item_combo_df.to_sql(name="item_combination_statistics", con=conn, if_exists='replace', index=False)
        print(f"✅ Item combination statistics table: {len(item_combination_stats)} entries")
    
    if rune_statistics:
        rune_df = pd.DataFrame(rune_statistics)
        rune_df.to_sql(name="rune_statistics", con=conn, if_exists='replace', index=False)
        print(f"✅ Rune statistics table: {len(rune_statistics)} entries")
    
    if rune_combination_stats:
        rune_combo_df = pd.DataFrame(rune_combination_stats)
        rune_combo_df.to_sql(name="rune_combination_statistics", con=conn, if_exists='replace', index=False)
        print(f"✅ Rune combination statistics table: {len(rune_combination_stats)} entries")
    
    conn.close()
    print(f"✅ Comprehensive database built successfully: {database}")
    print(f"📊 Database contains {len(league_entries)} players with comprehensive match, champion, item, and rune statistics")

def build_league_database(league_entries, database='league_players.db'):
    data_rows = []
    
    if league_entries:
        available_fields = list(league_entries[0].keys())
        print(f"Available fields in league entries: {available_fields}")
    
    for entry in league_entries:
        player_data = {}
        
        if 'summonerId' in entry:
            player_data['summonerId'] = entry['summonerId']
        if 'puuid' in entry:
            player_data['puuid'] = entry['puuid']
        if 'leaguePoints' in entry:
            player_data['leaguePoints'] = entry['leaguePoints']
        if 'rank' in entry:
            player_data['rank'] = entry['rank']
        if 'wins' in entry:
            player_data['wins'] = entry['wins']
        if 'losses' in entry:
            player_data['losses'] = entry['losses']
        if 'hotStreak' in entry:
            player_data['hotStreak'] = entry['hotStreak']
        if 'veteran' in entry:
            player_data['veteran'] = entry['veteran']
        if 'freshBlood' in entry:
            player_data['freshBlood'] = entry['freshBlood']
        if 'inactive' in entry:
            player_data['inactive'] = entry['inactive']
        
        data_rows.append(player_data)
    
    df = pd.DataFrame(data_rows)
    
    conn = sqlite3.connect(database)
    df.to_sql(name="league_players", con=conn, if_exists='replace', index=False)
    conn.close()
    
    print(f"Successfully stored {len(data_rows)} league players in database: {database}")
    return df

def get_champion_statistics(mastery_dict, num_matches=10):
    champion_stats = {}
    champion_picks = {}
    champion_bans = {}
    champion_win_rates = {}
    match_count = 0
    
    for puuid, champion_ids in mastery_dict.items():
        while True:
            try:
                lol_obj.rate_limit_wait()
                match_list = lol_obj.lol_watcher.match.matchlist_by_puuid(region, puuid, count=num_matches)
                break
            except ApiError as e:
                if e.response.status_code == 403:
                    print("bad or expired API key, paste new one here:")
                    api_key = input()
                    lol_obj.update_key(api_key=api_key)
                    lol_obj.rate_limit_wait()
                elif e.response.status_code == 429:
                    print(f"Rate limit exceeded, waiting 60 seconds...")
                    time.sleep(60)
                    lol_obj.rate_limit_wait()
                elif e.response.status_code == 504:
                    print("504 Gateway Timeout. Waiting 30s and retrying...")
                    time.sleep(30)
                else:
                    print(f"{e.response.status_code}: Waiting 10s")
                    time.sleep(10)
                    lol_obj.rate_limit_wait()
            except ConnectionError:
                print(f"Connection Error, waiting 10s then resuming")
                time.sleep(10)
                lol_obj.rate_limit_wait()
        
        for match_id in match_list:
            while True:
                try:
                    lol_obj.rate_limit_wait()
                    match_data = lol_obj.lol_watcher.match.by_id(region, match_id)
                    break
                except ApiError as e:
                    if e.response.status_code == 403:
                        print("bad or expired API key, paste new one here:")
                        api_key = input()
                        lol_obj.update_key(api_key=api_key)
                        lol_obj.rate_limit_wait()
                    elif e.response.status_code == 429:
                        print(f"Rate limit exceeded, waiting 60 seconds...")
                        time.sleep(60)
                        lol_obj.rate_limit_wait()
                    elif e.response.status_code == 504:
                        print("504 Gateway Timeout. Waiting 30s and retrying...")
                        time.sleep(30)
                    else:
                        print("Connection error, waiting 10s then resuming operation")
                        time.sleep(10)
                        lol_obj.rate_limit_wait()
                except ConnectionError:
                    print(f"Connection Error, waiting 10s then resuming")
                    time.sleep(10)
                    lol_obj.rate_limit_wait()
            
            if match_data['info']['gameMode'] == 'CLASSIC':
                match_count += 1
                match_info = match_data['info']
                
                participants = match_info.get('participants', [])
                for participant in participants:
                    champion_id = participant.get('championId')
                    champion_name = participant.get('championName')
                    win = participant.get('win', False)
                    
                    if champion_id not in champion_picks:
                        champion_picks[champion_id] = {
                            'champion_id': champion_id,
                            'champion_name': champion_name,
                            'picks': 0,
                            'wins': 0,
                            'total_kills': 0,
                            'total_deaths': 0,
                            'total_assists': 0,
                            'total_damage_dealt': 0,
                            'total_gold_earned': 0,
                            'total_cs': 0
                        }
                    
                    champion_picks[champion_id]['picks'] += 1
                    if win:
                        champion_picks[champion_id]['wins'] += 1
                    
                    champion_picks[champion_id]['total_kills'] += participant.get('kills', 0)
                    champion_picks[champion_id]['total_deaths'] += participant.get('deaths', 0)
                    champion_picks[champion_id]['total_assists'] += participant.get('assists', 0)
                    champion_picks[champion_id]['total_damage_dealt'] += participant.get('totalDamageDealtToChampions', 0)
                    champion_picks[champion_id]['total_gold_earned'] += participant.get('goldEarned', 0)
                    champion_picks[champion_id]['total_cs'] += participant.get('totalMinionsKilled', 0) + participant.get('neutralMinionsKilled', 0)
                
                teams = match_info.get('teams', [])
                for team in teams:
                    bans = team.get('bans', [])
                    for ban in bans:
                        champion_id = ban.get('championId')
                        if champion_id not in champion_bans:
                            champion_bans[champion_id] = {
                                'champion_id': champion_id,
                                'bans': 0
                            }
                        champion_bans[champion_id]['bans'] += 1
    
    champion_statistics = []
    for champion_id, data in champion_picks.items():
        if data['picks'] > 0:
            win_rate = data['wins'] / data['picks']
            avg_kills = data['total_kills'] / data['picks']
            avg_deaths = data['total_deaths'] / data['picks']
            avg_assists = data['total_assists'] / data['picks']
            avg_damage = data['total_damage_dealt'] / data['picks']
            avg_gold = data['total_gold_earned'] / data['picks']
            avg_cs = data['total_cs'] / data['picks']
            
            champion_statistics.append({
                'champion_id': champion_id,
                'champion_name': data['champion_name'],
                'picks': data['picks'],
                'wins': data['wins'],
                'win_rate': win_rate,
                'avg_kills': avg_kills,
                'avg_deaths': avg_deaths,
                'avg_assists': avg_assists,
                'avg_damage_dealt': avg_damage,
                'avg_gold_earned': avg_gold,
                'avg_cs': avg_cs,
                'total_matches_analyzed': match_count
            })
    
    champion_bans_list = list(champion_bans.values())
    
    print(f"✅ Champion statistics collected: {len(champion_statistics)} champions, {match_count} matches analyzed")
    return champion_statistics, champion_bans_list

def get_match_statistics(mastery_dict, num_matches=10):
    match_statistics = []
    objective_statistics = []
    team_performance_stats = []
    
    for puuid, champion_ids in mastery_dict.items():
        while True:
            try:
                lol_obj.rate_limit_wait()
                match_list = lol_obj.lol_watcher.match.matchlist_by_puuid(region, puuid, count=num_matches)
                break
            except ApiError as e:
                if e.response.status_code == 403:
                    print("bad or expired API key, paste new one here:")
                    api_key = input()
                    lol_obj.update_key(api_key=api_key)
                    lol_obj.rate_limit_wait()
                elif e.response.status_code == 429:
                    print(f"Rate limit exceeded, waiting 60 seconds...")
                    time.sleep(60)
                    lol_obj.rate_limit_wait()
                elif e.response.status_code == 504:
                    print("504 Gateway Timeout. Waiting 30s and retrying...")
                    time.sleep(30)
                else:
                    print(f"{e.response.status_code}: Waiting 10s")
                    time.sleep(10)
                    lol_obj.rate_limit_wait()
            except ConnectionError:
                print(f"Connection Error, waiting 10s then resuming")
                time.sleep(10)
                lol_obj.rate_limit_wait()
        
        for match_id in match_list:
            while True:
                try:
                    lol_obj.rate_limit_wait()
                    match_data = lol_obj.lol_watcher.match.by_id(region, match_id)
                    break
                except ApiError as e:
                    if e.response.status_code == 403:
                        print("bad or expired API key, paste new one here:")
                        api_key = input()
                        lol_obj.update_key(api_key=api_key)
                        lol_obj.rate_limit_wait()
                    elif e.response.status_code == 429:
                        print(f"Rate limit exceeded, waiting 60 seconds...")
                        time.sleep(60)
                        lol_obj.rate_limit_wait()
                    elif e.response.status_code == 504:
                        print("504 Gateway Timeout. Waiting 30s and retrying...")
                        time.sleep(30)
                    else:
                        print("Connection error, waiting 10s then resuming operation")
                        time.sleep(10)
                        lol_obj.rate_limit_wait()
                except ConnectionError:
                    print(f"Connection Error, waiting 10s then resuming")
                    time.sleep(10)
                    lol_obj.rate_limit_wait()
            
            if match_data['info']['gameMode'] == 'CLASSIC':
                match_info = match_data['info']
                
                match_stats = {
                    'match_id': match_id,
                    'game_creation': match_info.get('gameCreation'),
                    'game_duration': match_info.get('gameDuration'),
                    'game_end_timestamp': match_info.get('gameEndTimestamp'),
                    'game_start_timestamp': match_info.get('gameStartTimestamp'),
                    'game_version': match_info.get('gameVersion'),
                    'queue_id': match_info.get('queueId'),
                    'map_id': match_info.get('mapId'),
                    'platform_id': match_info.get('platformId'),
                    'game_type': match_info.get('gameType'),
                    'game_mode': match_info.get('gameMode'),
                    'total_kills': 0,
                    'total_assists': 0,
                    'total_gold': 0,
                    'total_damage_dealt': 0,
                    'total_cs': 0,
                    'first_blood_time': None,
                    'first_tower_time': None,
                    'first_dragon_time': None,
                    'first_baron_time': None
                }
                
                teams = match_info.get('teams', [])
                for team in teams:
                    team_id = team.get('teamId')
                    objectives = team.get('objectives', {})
                    
                    team_perf = {
                        'match_id': match_id,
                        'team_id': team_id,
                        'win': team.get('win', False),
                        'first_blood': objectives.get('champion', {}).get('first', False),
                        'first_tower': objectives.get('tower', {}).get('first', False),
                        'first_dragon': objectives.get('dragon', {}).get('first', False),
                        'first_baron': objectives.get('baron', {}).get('first', False),
                        'towers_destroyed': objectives.get('tower', {}).get('kills', 0),
                        'dragons_killed': objectives.get('dragon', {}).get('kills', 0),
                        'barons_killed': objectives.get('baron', {}).get('kills', 0),
                        'inhibitors_destroyed': objectives.get('inhibitor', {}).get('kills', 0),
                        'rift_heralds_killed': objectives.get('riftHerald', {}).get('kills', 0)
                    }
                    team_performance_stats.append(team_perf)
                    
                    if objectives.get('champion', {}).get('first', False):
                        match_stats['first_blood_time'] = objectives.get('champion', {}).get('first', 0)
                    if objectives.get('tower', {}).get('first', False):
                        match_stats['first_tower_time'] = objectives.get('tower', {}).get('first', 0)
                    if objectives.get('dragon', {}).get('first', False):
                        match_stats['first_dragon_time'] = objectives.get('dragon', {}).get('first', 0)
                    if objectives.get('baron', {}).get('first', False):
                        match_stats['first_baron_time'] = objectives.get('baron', {}).get('first', 0)
                
                participants = match_info.get('participants', [])
                for participant in participants:
                    match_stats['total_kills'] += participant.get('kills', 0)
                    match_stats['total_assists'] += participant.get('assists', 0)
                    match_stats['total_gold'] += participant.get('goldEarned', 0)
                    match_stats['total_damage_dealt'] += participant.get('totalDamageDealtToChampions', 0)
                    match_stats['total_cs'] += participant.get('totalMinionsKilled', 0) + participant.get('neutralMinionsKilled', 0)
                
                match_statistics.append(match_stats)
                
                for team in teams:
                    team_id = team.get('teamId')
                    objectives = team.get('objectives', {})
                    
                    for objective_type, objective_data in objectives.items():
                        if objective_type in ['champion', 'tower', 'dragon', 'baron', 'inhibitor', 'riftHerald']:
                            objective_stats = {
                                'match_id': match_id,
                                'team_id': team_id,
                                'objective_type': objective_type,
                                'first': objective_data.get('first', False),
                                'kills': objective_data.get('kills', 0)
                            }
                            objective_statistics.append(objective_stats)
    
    print(f"✅ Match statistics collected: {len(match_statistics)} matches, {len(team_performance_stats)} team performances, {len(objective_statistics)} objectives")
    return match_statistics, team_performance_stats, objective_statistics

def get_item_statistics(mastery_dict, num_matches=10):
    item_usage = {}
    item_combinations = {}
    item_win_rates = {}
    
    for puuid, champion_ids in mastery_dict.items():
        while True:
            try:
                lol_obj.rate_limit_wait()
                match_list = lol_obj.lol_watcher.match.matchlist_by_puuid(region, puuid, count=num_matches)
                break
            except ApiError as e:
                if e.response.status_code == 403:
                    print("bad or expired API key, paste new one here:")
                    api_key = input()
                    lol_obj.update_key(api_key=api_key)
                    lol_obj.rate_limit_wait()
                elif e.response.status_code == 429:
                    print(f"Rate limit exceeded, waiting 60 seconds...")
                    time.sleep(60)
                    lol_obj.rate_limit_wait()
                elif e.response.status_code == 504:
                    print("504 Gateway Timeout. Waiting 30s and retrying...")
                    time.sleep(30)
                else:
                    print(f"{e.response.status_code}: Waiting 10s")
                    time.sleep(10)
                    lol_obj.rate_limit_wait()
            except ConnectionError:
                print(f"Connection Error, waiting 10s then resuming")
                time.sleep(10)
                lol_obj.rate_limit_wait()
        
        for match_id in match_list:
            while True:
                try:
                    lol_obj.rate_limit_wait()
                    match_data = lol_obj.lol_watcher.match.by_id(region, match_id)
                    break
                except ApiError as e:
                    if e.response.status_code == 403:
                        print("bad or expired API key, paste new one here:")
                        api_key = input()
                        lol_obj.update_key(api_key=api_key)
                        lol_obj.rate_limit_wait()
                    elif e.response.status_code == 429:
                        print(f"Rate limit exceeded, waiting 60 seconds...")
                        time.sleep(60)
                        lol_obj.rate_limit_wait()
                    elif e.response.status_code == 504:
                        print("504 Gateway Timeout. Waiting 30s and retrying...")
                        time.sleep(30)
                    else:
                        print("Connection error, waiting 10s then resuming operation")
                        time.sleep(10)
                        lol_obj.rate_limit_wait()
                except ConnectionError:
                    print(f"Connection Error, waiting 10s then resuming")
                    time.sleep(10)
                    lol_obj.rate_limit_wait()
            
            if match_data['info']['gameMode'] == 'CLASSIC':
                participants = match_data['info'].get('participants', [])
                
                for participant in participants:
                    if participant['puuid'] in mastery_dict.keys():
                        if participant['championId'] in mastery_dict[participant['puuid']]:
                            items = [
                                participant.get('item0', 0),
                                participant.get('item1', 0),
                                participant.get('item2', 0),
                                participant.get('item3', 0),
                                participant.get('item4', 0),
                                participant.get('item5', 0),
                                participant.get('item6', 0)
                            ]
                            
                            items = [item for item in items if item > 0]
                            
                            for item in items:
                                if item not in item_usage:
                                    item_usage[item] = {
                                        'item_id': item,
                                        'uses': 0,
                                        'wins': 0,
                                        'champions': set(),
                                        'positions': set()
                                    }
                                
                                item_usage[item]['uses'] += 1
                                if participant.get('win', False):
                                    item_usage[item]['wins'] += 1
                                item_usage[item]['champions'].add(participant.get('championId'))
                                item_usage[item]['positions'].add(participant.get('teamPosition', ''))
                            
                            for i in range(len(items)):
                                for j in range(i + 1, len(items)):
                                    combo = tuple(sorted([items[i], items[j]]))
                                    if combo not in item_combinations:
                                        item_combinations[combo] = {
                                            'item1': combo[0],
                                            'item2': combo[1],
                                            'uses': 0,
                                            'wins': 0
                                        }
                                    
                                    item_combinations[combo]['uses'] += 1
                                    if participant.get('win', False):
                                        item_combinations[combo]['wins'] += 1
    
    item_statistics = []
    for item_id, data in item_usage.items():
        win_rate = data['wins'] / data['uses'] if data['uses'] > 0 else 0
        item_statistics.append({
            'item_id': item_id,
            'uses': data['uses'],
            'wins': data['wins'],
            'win_rate': win_rate,
            'unique_champions': len(data['champions']),
            'unique_positions': len(data['positions'])
        })
    
    item_combination_stats = []
    for combo, data in item_combinations.items():
        win_rate = data['wins'] / data['uses'] if data['uses'] > 0 else 0
        item_combination_stats.append({
            'item1': data['item1'],
            'item2': data['item2'],
            'uses': data['uses'],
            'wins': data['wins'],
            'win_rate': win_rate
        })
    
    print(f"✅ Item statistics collected: {len(item_statistics)} items, {len(item_combination_stats)} combinations")
    return item_statistics, item_combination_stats

def get_rune_statistics(mastery_dict, num_matches=10):
    rune_usage = {}
    rune_combinations = {}
    
    for puuid, champion_ids in mastery_dict.items():
        while True:
            try:
                lol_obj.rate_limit_wait()
                match_list = lol_obj.lol_watcher.match.matchlist_by_puuid(region, puuid, count=num_matches)
                break
            except ApiError as e:
                if e.response.status_code == 403:
                    print("bad or expired API key, paste new one here:")
                    api_key = input()
                    lol_obj.update_key(api_key=api_key)
                    lol_obj.rate_limit_wait()
                elif e.response.status_code == 429:
                    print(f"Rate limit exceeded, waiting 60 seconds...")
                    time.sleep(60)
                    lol_obj.rate_limit_wait()
                elif e.response.status_code == 504:
                    print("504 Gateway Timeout. Waiting 30s and retrying...")
                    time.sleep(30)
                else:
                    print(f"{e.response.status_code}: Waiting 10s")
                    time.sleep(10)
                    lol_obj.rate_limit_wait()
            except ConnectionError:
                print(f"Connection Error, waiting 10s then resuming")
                time.sleep(10)
                lol_obj.rate_limit_wait()
        
        for match_id in match_list:
            while True:
                try:
                    lol_obj.rate_limit_wait()
                    match_data = lol_obj.lol_watcher.match.by_id(region, match_id)
                    break
                except ApiError as e:
                    if e.response.status_code == 403:
                        print("bad or expired API key, paste new one here:")
                        api_key = input()
                        lol_obj.update_key(api_key=api_key)
                        lol_obj.rate_limit_wait()
                    elif e.response.status_code == 429:
                        print(f"Rate limit exceeded, waiting 60 seconds...")
                        time.sleep(60)
                        lol_obj.rate_limit_wait()
                    elif e.response.status_code == 504:
                        print("504 Gateway Timeout. Waiting 30s and retrying...")
                        time.sleep(30)
                    else:
                        print("Connection error, waiting 10s then resuming operation")
                        time.sleep(10)
                        lol_obj.rate_limit_wait()
                except ConnectionError:
                    print(f"Connection Error, waiting 10s then resuming")
                    time.sleep(10)
                    lol_obj.rate_limit_wait()
            
            if match_data['info']['gameMode'] == 'CLASSIC':
                participants = match_data['info'].get('participants', [])
                
                for participant in participants:
                    if participant['puuid'] in mastery_dict.keys():
                        if participant['championId'] in mastery_dict[participant['puuid']]:
                            runes = [
                                participant.get('perk0', 0),
                                participant.get('perk1', 0),
                                participant.get('perk2', 0),
                                participant.get('perk3', 0),
                                participant.get('perk4', 0),
                                participant.get('perk5', 0)
                            ]
                            primary_style = participant.get('perkPrimaryStyle', 0)
                            sub_style = participant.get('perkSubStyle', 0)
                            
                            for rune in runes:
                                if rune > 0:
                                    if rune not in rune_usage:
                                        rune_usage[rune] = {
                                            'rune_id': rune,
                                            'uses': 0,
                                            'wins': 0,
                                            'champions': set(),
                                            'positions': set()
                                        }
                                    
                                    rune_usage[rune]['uses'] += 1
                                    if participant.get('win', False):
                                        rune_usage[rune]['wins'] += 1
                                    rune_usage[rune]['champions'].add(participant.get('championId'))
                                    rune_usage[rune]['positions'].add(participant.get('teamPosition', ''))
                            
                            if primary_style > 0 and sub_style > 0:
                                style_combo = (primary_style, sub_style)
                                if style_combo not in rune_combinations:
                                    rune_combinations[style_combo] = {
                                        'primary_style': primary_style,
                                        'sub_style': sub_style,
                                        'uses': 0,
                                        'wins': 0,
                                        'champions': set()
                                    }
                                
                                rune_combinations[style_combo]['uses'] += 1
                                if participant.get('win', False):
                                    rune_combinations[style_combo]['wins'] += 1
                                rune_combinations[style_combo]['champions'].add(participant.get('championId'))
    
    rune_statistics = []
    for rune_id, data in rune_usage.items():
        win_rate = data['wins'] / data['uses'] if data['uses'] > 0 else 0
        rune_statistics.append({
            'rune_id': rune_id,
            'uses': data['uses'],
            'wins': data['wins'],
            'win_rate': win_rate,
            'unique_champions': len(data['champions']),
            'unique_positions': len(data['positions'])
        })
    
    rune_combination_stats = []
    for combo, data in rune_combinations.items():
        win_rate = data['wins'] / data['uses'] if data['uses'] > 0 else 0
        rune_combination_stats.append({
            'primary_style': data['primary_style'],
            'sub_style': data['sub_style'],
            'uses': data['uses'],
            'wins': data['wins'],
            'win_rate': win_rate,
            'unique_champions': len(data['champions'])
        })
    
    print(f"✅ Rune statistics collected: {len(rune_statistics)} runes, {len(rune_combination_stats)} combinations")
    return rune_statistics, rune_combination_stats

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    parser = argparse.ArgumentParser()
    key_help = "Riot Developer API key, found at https://developer.riotgames.com"
    region_help = "Region to query data for. Supported values can be found at https://developer.riotgames.com/docs/lol"
    parser.add_argument('-k', '--key', required=True, help=key_help)
    parser.add_argument('-r', '--region', required=True, help=region_help)
    parser.add_argument('-t', '--testing', action='store_true', help='Run in testing mode with only challenger players')
    parser.add_argument('--max-players', type=int, default=None, help='Maximum number of players to process (for testing)')
    parser.add_argument('--league-only', action='store_true', help='Only build league database (no individual summoner data)')
    parser.add_argument('--comprehensive', action='store_true', help='Build comprehensive database with all available data')
    parser.add_argument('--enhanced', action='store_true', help='Collect enhanced statistics including champion, item, rune, and match statistics')
    args = parser.parse_args()

    api_key = args.key
    region = args.region
    lol_obj = LolInterface(api_key=api_key)
    logging.info("LolWatcher object created.")

    summoner_ids, league_entries = get_top_players(region=region, testing=args.testing)
    
    if args.max_players and len(summoner_ids) > args.max_players:
        summoner_ids = summoner_ids[:args.max_players]
        league_entries = league_entries[:args.max_players]
        logging.info(f"Limited to {args.max_players} players for testing")
    
    logging.info(f"Top players stored: {len(summoner_ids)} entries.")

    build_league_database(league_entries=league_entries)
    logging.info("League database built successfully.")

    if not args.league_only:
        try:
            if args.comprehensive or args.enhanced:
                logging.info("API key restrictions detected - collecting league data only")
                logging.info("Individual summoner data collection skipped due to 403 errors")
                
                summoner_profiles = {}
                match_data_rows = []
                player_data_rows = []
                team_data_rows = []
                champion_statistics = []
                champion_bans = []
                match_statistics = []
                team_performance_stats = []
                objective_statistics = []
                item_statistics = []
                item_combination_stats = []
                rune_statistics = []
                rune_combination_stats = []

                build_comprehensive_database(
                    league_entries=league_entries,
                    summoner_profiles=summoner_profiles,
                    match_data_rows=match_data_rows,
                    player_data_rows=player_data_rows,
                    team_data_rows=team_data_rows,
                    champion_statistics=champion_statistics,
                    champion_bans=champion_bans,
                    match_statistics=match_statistics,
                    team_performance_stats=team_performance_stats,
                    objective_statistics=objective_statistics,
                    item_statistics=item_statistics,
                    item_combination_stats=item_combination_stats,
                    rune_statistics=rune_statistics,
                    rune_combination_stats=rune_combination_stats
                )
                logging.info("Database structure created with league data only.")
            else:
                logging.info("API key restrictions detected - collecting league data only")
                logging.info("Individual summoner data collection skipped due to 403 errors")
                
                data_rows = []
                df = pd.DataFrame(data_rows)
                df_to_sql(df=df)
                logging.info("Database created with league data only")
        except Exception as e:
            logging.error(f"Error in data collection: {e}")
            logging.info("League database was still built successfully.")
    else:
        logging.info("Skipping individual summoner data collection (league-only mode)")