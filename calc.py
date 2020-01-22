import pandas as pd
import numpy as np
import multiprocessing as mp
from joblib import Parallel, delayed


# read data from txt files and provide them as global variables
with open('TreasureClassEx.txt','r') as f:
    df_treasureclassex = pd.read_csv(f, sep='\t')
with open('MonStats.txt','r') as f:
    df_monstats = pd.read_csv(f, sep='\t')
with open('SuperUniques.txt','r') as f:
    df_superuniques = pd.read_csv(f, sep='\t')
with open('weapons.txt','r') as f:
    df_weapons = pd.read_csv(f, sep='\t')
with open('armor.txt','r') as f:
    df_armor = pd.read_csv(f, sep='\t')
with open('Misc.txt','r') as f:
    df_misc = pd.read_csv(f, sep='\t')
with open('UniqueItems.txt','r') as f:
    df_uniqueitems = pd.read_csv(f, sep='\t')
with open('SetItems.txt','r') as f:
    df_setitems = pd.read_csv(f, sep='\t')
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


def split_base_item_chances(base_dict):
    unique_and_set_dict = {}
    for key, value in base_dict.items():
        if (df_uniqueitems['code'] == key).any():
            name = df_uniqueitems[df_uniqueitems['code'] == key]['index'].tolist()[0]
            base_item = df_uniqueitems[df_uniqueitems['code'] == key]['*type'].tolist()[0]
            uni_key "{:s} (Unique {:s})".format(name, base_item)
        if (df_setitems['item'] == key).any():
            name = df_setitems[df_setitems['item'] == key]['index'].tolist()[0]
            base_item = df_setitems[df_setitems['item'] == key]['*item'].tolist()[0]
            set_key "{:s} (Unique {:s})".format(name, base_item)


def get_level_subdict(df, level, prob):
    sub_dict = {}
    df_select = df[
        (df['level'] >  level-3) & 
        (df['level'] <= level) &
        (df['spawnable'] == 1)]
    for index, item in df_select.iterrows():
        sub_dict[item['code']] = prob/len(df_select.index)
    split_base_item_chances(sub_dict)
    return sub_dict


def split_weap_and_armo(drop_dict):
    return_dict={}
    for key, value in drop_dict.items():
        if 'weap' in key:
            level = int(key.replace('weap',''))
            split_dict = get_level_subdict(df_weapons, level, value)
            return_dict = merge_dicts(return_dict, split_dict)
        elif 'armo' in key:
            level = int(key.replace('armo',''))
            split_dict = get_level_subdict(df_armor, level, value)
            return_dict = merge_dicts(return_dict, split_dict)
        else:
            return_dict[key] = drop_dict[key]
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


def wrap_monster_loop(item, code_to_name_dict, difficulty, mon_type, tc):
    if not item['Id'] == 'Expansion':
        monster_name = df_strings[df_strings['String Index'] == item['NameStr']]['Text'].tolist()[0]
        monster_id = item['Id']
        tcs_identical = item['TreasureClass1'] == item['TreasureClass2'] == item['TreasureClass3']
        if mon_type == '' or not tcs_identical:
            if (df_strings['String Index'] == item['NameStr']).any() and not isinstance(item[tc], float):
                print("{:s} has name {:s} and TC {:s}".format(
                    item['Id'],
                    monster_name,
                    item[tc]
                ))
                prob_dict = tc_get_prob_dict(item[tc])
                totals = tc_unravel(prob_dict, diag_dict=False)
                totals = split_weap_and_armo(totals)
                print_results_to_txt(totals, code_to_name_dict, monster_name, monster_id + mon_type + difficulty + '.txt')
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
            totals = split_weap_and_armo(totals)
            print_results_to_txt(totals, code_to_name_dict, df_strings[df_strings['String Index'] == item['Name']]['Text'].tolist()[0]+difficulty+'.txt')

def loop_over_monsters_and_uniques():
    """This function loops over all monsters from Monstats.txt, names them using
    the patch.txt and patchstring.txt
    Arguments:
        monsters ...... pandas dataframe
        names ......... pandas dataframe"""
    code_to_name_dict = read_item_lists()
    monster_tcs = [['TreasureClass1',    'TreasureClass2',    'TreasureClass3'],
                   ['TreasureClass1(N)', 'TreasureClass2(N)', 'TreasureClass3(N)'],
                   ['TreasureClass1(H)', 'TreasureClass2(H)', 'TreasureClass3(H)']]
    unique_tcs = ['TC', 'TC(N)', 'TC(H)']
    for dd, difficulty in enumerate(['','_N','_H']):
        for tt, mon_type in enumerate(['', '_champ', '_unique']):
            Parallel(n_jobs=1)(delayed(wrap_monster_loop)(item, code_to_name_dict, difficulty, mon_type, monster_tcs[dd][tt]) for _, item in df_monstats.iterrows())
        if mon_type == '_unique':
            Parallel(n_jobs=1)(delayed(wrap_superunique_loop)(item, code_to_name_dict, difficulty, unique_tcs[dd]) for _, item in df_superuniques.iterrows())


def main():
    for _, item in df_monstats.iterrows():
        if item['TreasureClass1'] == item['TreasureClass2']:
            print(item['Id'], item['NameStr'])
    loop_over_monsters_and_uniques()


if __name__ == '__main__':
    main()