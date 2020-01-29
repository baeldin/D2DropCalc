import pandas as pd
import numpy as np
from joblib import Parallel, delayed, cpu_count


# debug prints
print_set_details = False
print_unique_details = False

mf = 0.

# read data from txt files and provide them as global variables
with open('TreasureClassEx.txt','r') as f:
    df_treasureclassex = pd.read_csv(f, sep='\t')
with open('MonStats.txt','r') as f:
    df_monstats = pd.read_csv(f, sep='\t')
with open('SuperUniques.txt','r') as f:
    df_superuniques = pd.read_csv(f, sep='\t')
with open('levels.txt','r') as f:
    df_levels = pd.read_csv(f, sep='\t')
with open('weapons.txt','r') as f:
    df_weapons = pd.read_csv(f, sep='\t')
with open('armor.txt','r') as f:
    df_armor = pd.read_csv(f, sep='\t')
with open('Misc.txt','r') as f:
    df_misc = pd.read_csv(f, sep='\t')
with open('ItemTypes.txt','r') as f:
    df_itemtypes = pd.read_csv(f, sep='\t')
with open('UniqueItems.txt','r') as f:
    df_uniqueitems = pd.read_csv(f, sep='\t')
with open('SetItems.txt','r') as f:
    df_setitems = pd.read_csv(f, sep='\t')
with open('itemratio.txt','r') as f:
    df_itemratio = pd.read_csv(f, sep='\t')
with open('ES AlphA.txt','r') as f:
    strings1 = pd.read_csv(f, sep='\t')
with open('expansionstring.txt','r') as f:
    strings2 = pd.read_csv(f, sep='\t')
with open('patchstring.txt','r') as f:
    strings3 = pd.read_csv(f, sep='\t')
with open('string.txt','r') as f:
    strings4 = pd.read_csv(f, sep='\t')
# strings to one df, from latest to earlierst, to catch the newest
# string info first (some duplicates exist due to older entries!)
df_strings = pd.concat([strings1, strings2, strings3, strings4])


def read_item_lists():
    """ Uses df_weapons, df_armor and df_misc to assign item
    names to item codes found in df_treasureclassex

    Arguments: none

    Returns: dictionary {code1: 'name1', 'code2: 'name2', ...}"""
    code_to_name_dict = {}
    for index, item in df_armor.iterrows():
        code_to_name_dict[item['code']] = item['name']
    for index, item in df_weapons.iterrows():
        code_to_name_dict[item['code']] = item['name']
    for index, item in df_misc.iterrows():
        if 'Coupon' in item['name']:
            code_to_name_dict[item['code']] = "Coupon "+item['*name']
        elif 'Decal' in item['name']:
            code_to_name_dict[item['code']] = item['*name']+" Decal"
        elif 'UnID Scroll' in item['name']:
            code_to_name_dict[item['code']] = item['name'].replace('UnID ','')
        elif 'Ring' in item['name'] and item['name'] != 'Ring':
            code_to_name_dict[item['code']] = item['*name']+" (class ring)"
        elif 'Amulet' in item['name']and item['name'] != 'Amulet':
            code_to_name_dict[item['code']] = item['*name']+" (class amulet)"
        elif item['name'] == 'DragonStone':
            code_to_name_dict[item['code']] = 'Dragon Stone'
        else:
            code_to_name_dict[item['code']] = item['name']
    # add no drop and arrows/bolts
    code_to_name_dict['NoDrop'] = "No Drop"
    code_to_name_dict['aq2'] = "Arrows"
    code_to_name_dict['cq2'] = "Bolts"
    return code_to_name_dict


def convert_to_df(droplist):
    first = True
    for entry in droplist:
        if first:
            df = pd.DataFrame.from_dict(entry)
        else:
            try:
                df.append(pd.DataFrame.from_dict(entry))
            except:
                print("something went wrong...")
                print(entry)
    df.to_csv('test.csv')


def print_results_to_txt(totals, code_to_name_dict, monster_name, filenam='out.txt'):
    with open('Results/'+filenam, 'w') as f:
        f.write("{:s}\n".format(monster_name))
        f.write("Item\tChance\n")
        chance_sum = 0.
        for key in sorted(totals.keys()):
            try:
            #if not 'gld' in key and not 'weap' in key and not 'armo' in key and not 'Ore' in key:
                f.write("{:s}\t {:11.9f}\n".format(code_to_name_dict[key], totals[key]))
            except:
            #else:
                # print("{:s}\t {:11.9f}".format('(UNNAMED)'+key, totals[key]))
                f.write("{:s}\t {:11.9f}\n".format(key, totals[key]))
            chance_sum += totals[key]
        f.write('______________________________\n')
        f.write("total: "+str(chance_sum)+'\n')
        f.write("No Drop: "+str(totals['NoDrop'])+'\n')
        f.close()


def merge_dicts(dict_a, dict_b):
    """ Takes two dictionaries dict_a and dict_b and merges them
    Arguments:
    dict_a ... dictionary
    dict_b ... dictionary

    Returns merged dictionary with all keys from a and b:
    if key was only in a > key: value from dict_a
    if key was only in b > key: value from dict_b
    if key was in both >   key: value from dict_a + value from dict_b
    """
    for key, value in dict_b.items():
        if key in dict_a:
            dict_a[key] = dict_a[key] + value
        else:
            dict_a[key] = value
    return dict_a


def get_entry_from_item_dicts(item_code, entry_name):
    if (df_weapons['code'] == item_code).any():
        to_return =  df_weapons[df_weapons['code'] == item_code][entry_name].tolist()[0]
    elif (df_armor['code'] == item_code).any():
        to_return =  df_armor[df_armor['code'] == item_code][entry_name].tolist()[0]
    elif (df_misc['code'] == item_code).any():
        to_return =  df_misc[df_misc['code'] == item_code][entry_name].tolist()[0]
    else:
        return 0
    return to_return


def get_line_from_item_ratio(base_item_code):
    item_type = get_entry_from_item_dicts(base_item_code, 'type')
    class_codes = ['ama', 'ass', 'bar', 'dru', 'nec', 'pal', 'sor']
    try: # will fail for items from misc.txt
        uber_code = get_entry_from_item_dicts(base_item_code, 'ubercode')
        ultra_code = get_entry_from_item_dicts(base_item_code, 'ultracode')
        if base_item_code == uber_code or base_item_code == ultra_code:
            is_uber = 1
        else:
            is_uber = 0
    except: # the above will fail if
        is_uber = 0
    # print(df_itemtypes[df_itemtypes['Code'] == item_type]['Class'].tolist()[0])
    try: # will fail for items from misc.txt
        if df_itemtypes[df_itemtypes['Code'] == item_type]['Class'].tolist()[0] in class_codes:
            is_class_specific = 1
        else:
            is_class_specific = 0
    except:
        is_class_specific = 0
    return df_itemratio[
        df_itemratio['Version'] == 1][df_itemratio['Uber'] == is_uber][df_itemratio['Class Specific'] == is_class_specific]


def get_quality_chance(base_item_code, ilvl, qlvl, quality='Unique'):
    ratio_parameters = get_line_from_item_ratio(base_item_code)
    base = ratio_parameters[quality].tolist()[0]
    divisor = ratio_parameters[quality+'Divisor'].tolist()[0]
    quality_min = ratio_parameters[quality+'Min'].tolist()[0]
    base_chance = base - (ilvl - qlvl)/divisor
    mod_chance = base_chance * 128
    mod_chance_mf = 100 * mod_chance / (100 + mf)
    if mod_chance_mf < quality_min:
        mod_chance_mf = quality_min
    final_chance = 128 / mod_chance_mf
    return final_chance


def check_unique_drops(item_code, base_qlvl, monster_level):
    unique_list = []
    unique_dict = {}
    total_rarity = 0
    for _, unique_item in df_uniqueitems[df_uniqueitems['code'] == item_code].iterrows():
        name = unique_item['index']
        base_item = unique_item['*type']
        name_str = name
        # name_str = "{:s} (Unique {:s})".format(name, base_item)
        special_qlvl = int(unique_item['lvl'])
        rarity = unique_item['rarity']
        if print_unique_details:
            if special_qlvl <= monster_level:
                print("{:s} (Unique {:s}, ilvl = {:d}, base_qlvl = {:d}, unique_qlvl = {:d})".format(name, base_item, monster_level, base_qlvl, special_qlvl))
            else:
                print("{:s} (Unique {:s}, ilvl = {:d}, base_qlvl = {:d}, unique_qlvl = {:d})".format(name, base_item, monster_level, base_qlvl, special_qlvl))
        if special_qlvl <= monster_level:
            unique_list.append([name_str, rarity])
            total_rarity += rarity
    if len(unique_list) > 0:
        total_chance_of_unique = 0.
        for unique in unique_list:
            chance_of_this_unique = unique[1]/total_rarity * get_quality_chance(
                item_code, monster_level, base_qlvl, quality='Unique')
            unique_dict[unique[0]] = chance_of_this_unique
            total_chance_of_unique += chance_of_this_unique
        if print_unique_details:
            for key, value in unique_dict.items():
                print("{:s}:\t{:8.6f}%".format(key,100*value))
        # print(get_quality_chance(item_code, monster_level, base_qlvl))
    return unique_dict


def check_set_drop(item_code, base_qlvl, monster_level):
    set_list = []
    set_dict = {}
    total_rarity = 0
    for _, set_item in df_setitems[df_setitems['item'] == item_code].iterrows():
        name = set_item['index']
        base_item = set_item['*item']
        name_str = name
        # name_str = "{:s} (set {:s})".format(name, base_item)
        special_qlvl = int(set_item['lvl'])
        rarity = set_item['rarity']
        if print_set_details:
            if special_qlvl <= monster_level:
                print("{:s} (Set {:s}, ilvl = {:d}, base_qlvl = {:d}, set_qlvl = {:d})".format(name, base_item, monster_level, base_qlvl, special_qlvl))
            else:
                print("{:s} (Set {:s}, ilvl = {:d}, base_qlvl = {:d}, set_qlvl = {:d})".format(name, base_item, monster_level, base_qlvl, special_qlvl))
        if special_qlvl <= monster_level:
            set_list.append([name_str, rarity])
            total_rarity += rarity
    if len(set_list) > 0:
        total_chance_of_set = 0.
        for set in set_list:
            chance_of_this_set = set[1] / total_rarity * get_quality_chance(
                item_code, monster_level, base_qlvl, quality='Set')
            set_dict[set[0]] = chance_of_this_set
            total_chance_of_set += chance_of_this_set
        if print_set_details:
            for key, value in set_dict.items():
                print("{:s}:\t{:8.6f}%".format(key,100*value))
    return set_dict

def split_base_item_chances(base_dict, monster_level):
    for key, value in base_dict.items():
        base_qlvl = int(get_entry_from_item_dicts(key, 'level'))
        unique_dict = check_unique_drops(key, base_qlvl, monster_level)
        if len(unique_dict) > 0:
            for key_unique, value_unique in unique_dict.items():
                unique_dict[key_unique] = value * value_unique
            base_dict = merge_dicts(base_dict, unique_dict)
        set_dict = check_set_drop(key, base_qlvl, monster_level)
        if len(set_dict) > 0:
            for key_set, value_set in set_dict.items():
                set_dict[key_set] = value * value_set
            base_dict = merge_dicts(base_dict, set_dict)
    return base_dict

def get_level_subdict(df, level, prob, monster_level):
    sub_dict = {}
    df_select = df[
        (df['level'] >  level-3) & 
        (df['level'] <= level) &
        (df['spawnable'] == 1)]
    for index, item in df_select.iterrows():
        sub_dict[item['code']] = prob/len(df_select.index)
    return sub_dict


def split_weap_and_armo(drop_dict, monster_level):
    return_dict=drop_dict
    for key, value in drop_dict.items():
        if 'weap' in key:
            level = int(key.replace('weap',''))
            split_dict = get_level_subdict(df_weapons, level, value, monster_level)
            return_dict = merge_dicts(return_dict, split_dict)
        elif 'armo' in key:
            level = int(key.replace('armo',''))
            split_dict = get_level_subdict(df_armor, level, value, monster_level)
            return_dict = merge_dicts(return_dict, split_dict)
        # else:
        #     return_dict[key] = drop_dict[key]
    return return_dict


def diag_dict_print(new_dict, prob_dict, call_prob, tc_nam):
        print("========== BEFORE UNRAVELLING ========")
        print("We are in tc {:s}, the chance to end up here was {:11.9f}".format(
            tc_nam, call_prob))
        print("contents of new_dict in tc_untravel():")
        chance_sum = 0.
        for key, value in prob_dict.items():
            print("{:s}: {:11.9f}% ({:11.9f}% within {:s})".format(
                key, value*100, value/call_prob*100, tc_nam))
            chance_sum += value
        print("========== AFTER UNRAVELLING =========")
        print("We are in tc {:s}, the chance to end up here was {:11.9f}".format(
            tc_nam, call_prob))
        print("contents of new_dict in tc_untravel():")
        chance_sum = 0.
        for key, value in new_dict.items():
            print("{:s}: {:11.9f}% ({:11.9f}% within {:s})".format(
                key, value*100, value/call_prob*100, tc_nam))
            chance_sum += value
        print("total: "+str(chance_sum))


def data_index(index):
    if type(index) is str:
        return df_treasureclassex[df_treasureclassex['Treasure Class']==index].index.values[0]
    else:
        return index


def get_corrected_nodrop(index, total_probability, nplayers=0):
    # N=int(1+AdditionalPlayers/2+ClosePartiedPlayers/2)
    # int( ProbSum/(1/((NoDrop/(NoDrop+ProbSum))^N)-1) )
    base_nodrop = df_treasureclassex.at[index, 'NoDrop']
    if not np.isnan(base_nodrop) and not base_nodrop == 0:
        N=int(1+nplayers)
        corrected_nodrop = int(total_probability/(1/((base_nodrop/(base_nodrop+total_probability))**N)-1))
        return corrected_nodrop
    else:
        return 0


def tc_total_probability(index):
    idx = data_index(index)
    total_probability = 0
    for ii in range(1,11):
        if not np.isnan(df_treasureclassex.at[idx,'Prob'+str(ii)]):
            total_probability += df_treasureclassex.at[idx,'Prob'+str(ii)]
    if not np.isnan(df_treasureclassex.at[idx, 'NoDrop']):
        total_probability += get_corrected_nodrop(idx, total_probability)
        return total_probability, get_corrected_nodrop(idx, total_probability)
    else:
        return total_probability, 0


def tc_get_prob_dict(index, call_prob=1.):
    idx = data_index(index)
    prob_dict = {}
    picks = df_treasureclassex.at[idx,'Picks']
    multiplier = 1.
    total_prob, nodrop = tc_total_probability(index)
    if nodrop > 0:
        prob_dict['NoDrop'] = nodrop/total_prob*call_prob
    for ii in range(1,11):
        tmp_mini_dict = {}
        if df_treasureclassex.at[idx, 'Item'+str(ii)] == 'Nothing':
            tmp_mini_dict = {'NoDrop': df_treasureclassex.at[idx,'Prob'+str(ii)]/total_prob*call_prob*picks}
        else:
            if not np.isnan(df_treasureclassex.at[idx,'Prob'+str(ii)]):
                if picks > 0:
                    tmp_mini_dict[df_treasureclassex.at[idx, 'Item'+str(ii)]] = df_treasureclassex.at[idx,'Prob'+str(ii)]/total_prob*call_prob*picks
                elif picks < 0:
                    tmp_mini_dict[df_treasureclassex.at[idx, 'Item'+str(ii)]] = 1.*call_prob
        prob_dict = merge_dicts(prob_dict, tmp_mini_dict)
    return prob_dict


def tc_unravel(prob_dict, diag_dict = False, tc_nam='None', call_prob=1.):
    new_dict = {}
    for tc_to_check, prob in prob_dict.items():
        if (df_treasureclassex['Treasure Class'] == tc_to_check).any():
            tmp_dict = tc_unravel(tc_get_prob_dict(tc_to_check, call_prob=prob), diag_dict=diag_dict, tc_nam=tc_to_check, call_prob=prob)
            if tmp_dict is not None:
                new_dict = merge_dicts(new_dict, tmp_dict)
        else:
            new_dict = merge_dicts(new_dict, {tc_to_check: prob})
    if diag_dict:
        diag_dict_print(new_dict, prob_dict, call_prob, tc_nam)
    return new_dict


def wrap_monster_loop(monster, area_name, monster_level,  code_to_name_dict, difficulty, mon_type, tc):
    if not monster['Id'] == 'Expansion':
        monster_name = df_strings[df_strings['String Index'] == monster['NameStr']]['Text'].tolist()[0]
        tcs_identical = monster['TreasureClass1'] == monster['TreasureClass2'] == monster['TreasureClass3']
        # monster_level = get_monster_level(monster['Id'])
        if mon_type == '' or not tcs_identical:
            if (df_strings['String Index'] == monster['NameStr']).any() and not isinstance(monster[tc], float):
                print("{:s} has name {:s}, TC {:s}, and is level {:d}".format(
                    monster['Id'],
                    monster_name,
                    monster[tc],
                    monster_level
                ))
                prob_dict = tc_get_prob_dict(monster[tc])
                totals = tc_unravel(prob_dict, diag_dict=False)
                totals = split_weap_and_armo(totals, monster_level)
                totals = split_base_item_chances(totals, monster_level)
                named_results = {}
                for key, value in totals.items():
                    try:
                        named_results[code_to_name_dict[key]] = value
                    except:
                        named_results[key] = value
                named_results['001_Monster'] = monster_name
                named_results['002_AreaName'] = df_strings[df_strings['String Index'] == area_name]['Text'].tolist()[0]
                difficulties = {'': 'normal', '_N': 'nightmare', '_H': 'hell'}
                types = {'': '', '_champ': 'Champion', '_minion': 'Minion', '_unique': 'Unique'}
                level_bonus = {'': 0, '_champ': 2, '_minion': 3, '_unique': 3}
                named_results['005_MonsterLevel'] = monster_level + level_bonus[mon_type]
                named_results['003_Difficulty'] = difficulties[difficulty]
                named_results['004_MonType'] = types[mon_type]
                return named_results

            # txt_out_name = monster['Id'] + area_name + mon_type + difficulty + '.txt'
                # print_results_to_txt(totals, code_to_name_dict, monster_name, txt_out_name)
        elif tcs_identical:
            print("{:s} has three identical TCs, skipping for Champ and Unique type...".format(
                monster_name))
        else:
            print("This should never happen!!!")

def wrap_superunique_loop(item, code_to_name_dict, difficulty, tc):
    if not item['Superunique'] == 'Expansion':
        if (df_strings['String Index'] == item['Name']).any() and not isinstance(item[tc], float):
            print("{:s} has name {:s} and TC {:s}".format(
                item['Superunique'],
                df_strings[df_strings['String Index'] == item['Name']]['Text'].tolist()[0],
                item[tc]
            ))
            prob_dict = tc_get_prob_dict(df_treasureclassex, item[tc])
            totals = tc_unravel(prob_dict, df_treasureclassex, diag_dict=False)
            totals = split_weap_and_armo(totals, monster_level)
            totals = split_base_item_chances(totals, monster_level)
            named_results = {}
            for key, value in totals.items():
                try:
                    named_results[code_to_name_dict[key]] = value
                except:
                    named_results[key] = value
            named_results['001_Monster'] = monster_name
            named_results['002_AreaName'] = df_strings[df_strings['String Index'] == area_name]['Text'].tolist()[0]
            difficulties = {'': 'normal', '_N': 'nightmare', '_H': 'hell'}
            types = {'': '', '_champ': 'Champion', '_minion': 'Minion', '_unique': 'Unique'}
            level_bonus = {'': 0, '_champ': 2, '_minion': 3, '_unique': 3}
            named_results['005_MonsterLevel'] = monster_level + level_bonus[mon_type]
            named_results['003_Difficulty'] = difficulties[difficulty]
            named_results['004_MonType'] = types[mon_type]
            return named_results
            print_results_to_txt(totals, code_to_name_dict, df_strings[df_strings['String Index'] == item['Name']]['Text'].tolist()[0]+difficulty+'.txt')


def prepare_monster_loop():
    # ii = 0
    # ii_limit = 1500
    # generate a list of monsters to process
    code_to_name_dict = read_item_lists()
    monster_tcs = [['TreasureClass1', 'TreasureClass2', 'TreasureClass1', 'TreasureClass3'],
                   ['TreasureClass1(N)', 'TreasureClass2(N)', 'TreasureClass1(N)', 'TreasureClass3(N)'],
                   ['TreasureClass1(H)', 'TreasureClass2(H)', 'TreasureClass1(H)', 'TreasureClass3(H)']]
    level_columns = ['MonLvl1Ex', 'MonLvl2Ex', 'MonLvl3Ex']
    monster_list = []
    for tt, mon_type in enumerate(['', '_champ', '_minion', '_unique']):
        for dd, difficulty in enumerate(['', '_N', '_H']):
            for _, monster in df_monstats.iterrows():
                if (df_strings['String Index'] == monster['NameStr']).any() and not isinstance(monster[monster_tcs[dd][tt]], float):
                    monster_found_in = df_levels[df_levels.isin([monster['Id']]).any(axis=1)].index.tolist()
                    if len(monster_found_in) > 0:
                        for idx in monster_found_in:
                            print("Working on {:s}, the level in {:s} in levels.txt is {:d}".format(
                                    monster['NameStr'],
                                    df_levels.iloc[idx]['LevelName'],
                                    int(df_levels.iloc[idx][level_columns[dd]])))
                            area_name = df_levels.iloc[idx]['LevelName']
                            monster_level = int(df_levels.iloc[idx][level_columns[dd]])
                            monster_list.append([monster, area_name, monster_level, code_to_name_dict,
                                                 difficulty, mon_type, monster_tcs[dd][tt]])
                            # ii = ii + 1
                            # if ii == ii_limit:
                            #     return monster_list
                            # print([monster, area_name, monster_level, code_to_name_dict,
                            #                      difficulty, mon_type, monster_tcs[dd][tt]])
    return monster_list


def loop_over_monsters_and_uniques(testing=False):
    """This function loops over all monsters from Monstats.txt, names them using
    the patch.txt and patchstring.txt
    Arguments:
        monsters ...... pandas dataframe
        names ......... pandas dataframe"""
    code_to_name_dict = read_item_lists()
    unique_tcs = ['TC', 'TC(N)', 'TC(H)']
    if testing:
        for _, item in df_monstats.iterrows():
            wrap_monster_loop(item, code_to_name_dict, '_H', '', 'TreasureClass1(H)')
            exit()
    else:
        monster_list = prepare_monster_loop()
        result_list = []
        jobs = max(cpu_count() - 4, 2)
        # jobs = 1
        result_list.append(Parallel(n_jobs=jobs)(
            delayed(wrap_monster_loop)(*monster) for monster in monster_list))
        # for difficulty in ['', '_N', '_H']:
        #     result_list.append(Parallel(n_jobs=jobs)(
        #         delayed(wrap_superunique_loop)(item, code_to_name_dict, difficulty, unique_tcs[dd]) for _, item
        #         in df_superuniques.iterrows()))
    convert_to_df(result_list)


def main():
    loop_over_monsters_and_uniques(testing=False)


if __name__ == '__main__':
    main()