import pandas as pd
import numpy as np
import multiprocessing as mp
from joblib import Parallel, delayed

def read_TreasurClassEx():
    with open('TreasureClassEx.txt','r') as f:
        df = pd.read_csv(f, sep='\t')
    return df


def read_monstats():
    with open('Monstats.txt') as f:
        df = pd.read_csv(f, sep='\t')
    return df

def read_monster_names():
    with open('string.txt') as f1:
        df1 = pd.read_csv(f1, sep='\t')
    with open('patchstring.txt') as f2:
        df2 = pd.read_csv(f2, sep='\t')
    with open('expansionstring.txt') as f3:
        df3 = pd.read_csv(f3, sep='\t')
    with open('ES AlphA.txt') as f4:
        df4 = pd.read_csv(f4, sep='\t')
    return pd.concat([df4,df3,df2,df1])


def read_unique_monsters():
    with open('SuperUniques.txt') as f:
        df = pd.read_csv(f, sep='\t')
    return df


def read_item_lists():
    with open('Misc.txt','r') as f:
        df_misc = pd.read_csv(f, sep='\t')
    with open('armor.txt','r') as f:
        df_armor = pd.read_csv(f, sep='\t')
    with open('weapons.txt','r') as f:
        df_weapons = pd.read_csv(f, sep='\t')
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



def print_results_to_txt(totals, code_to_name_dict, filenam='out.txt'):
    with open('Results/'+filenam, 'w') as f:
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
    for key, value in dict_b.items():
        if key in dict_a:
            dict_a[key] = dict_a[key] + value
        else:
            dict_a[key] = value
    return dict_a


def get_level_subdict(df, level, prob):
    sub_dict = {}
    # print(df)
    df_select = df[
        (df['level'] >  level-3) & 
        (df['level'] <= level) &
        (df['spawnable'] == 1)]
    for index, item in df_select.iterrows():
        sub_dict[item['code']] = prob/len(df_select.index)
        # print(sub_dict)
    return sub_dict


def split_weap_and_armo(drop_dict):
    return_dict={}
    with open('weapons.txt','r') as f:
        df_weapons = pd.read_csv(f, sep='\t')
    with open('armor.txt','r') as f:
        df_armor = pd.read_csv(f, sep='\t')
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
    # for key, value in return_dict.items():
    #     print("{:s}: {:11.9f}".format(key, value))
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


def data_index(data, index):
    if type(index) is str:
        return data[data['Treasure Class']==index].index.values[0]
    else:
        return index


def tc_total_probability(data, index):
    idx = data_index(data, index)
    total_probability = 0
    for ii in range(1,11):
        if not np.isnan(data.at[idx,'Prob'+str(ii)]):
            total_probability += data.at[idx,'Prob'+str(ii)]
    if not np.isnan(data.at[idx, 'NoDrop']):
        total_probability += data.at[idx, 'NoDrop']
    return total_probability


def tc_get_prob_dict(data, index, call_prob=1.):
    idx = data_index(data, index)
    prob_dict = {}
    picks = data.at[idx,'Picks']
    multiplier = 1.
    total_prob = tc_total_probability(data, index)
    if not np.isnan(data.at[idx, 'NoDrop']) and picks > 0:
        prob_dict['NoDrop'] = data.at[idx, 'NoDrop']/total_prob*call_prob
    for ii in range(1,11):
        tmp_mini_dict = {}
        if data.at[idx, 'Item'+str(ii)] == 'Nothing':
            tmp_mini_dict = {'NoDrop': data.at[idx,'Prob'+str(ii)]/total_prob*call_prob*picks}
        else:
            if not np.isnan(data.at[idx,'Prob'+str(ii)]):
                if picks > 0:
                    tmp_mini_dict[data.at[idx, 'Item'+str(ii)]] = data.at[idx,'Prob'+str(ii)]/total_prob*call_prob*picks
                elif picks < 0:
                    tmp_mini_dict[data.at[idx, 'Item'+str(ii)]] = 1.*call_prob
                elif picks == 0:
                    return {'NoDrop': 1.}
        prob_dict = merge_dicts(prob_dict, tmp_mini_dict)
    return prob_dict


def tc_unravel(prob_dict, data, diag_dict = False, tc_nam='None', call_prob=1.):
    # diag_dict = True
    new_dict = {}
    for tc_to_check, prob in prob_dict.items():
        # print("checking "+tc_to_check)
        if (data['Treasure Class'] == tc_to_check).any():
            # print("yes")
            tmp_dict = tc_unravel(tc_get_prob_dict(data, tc_to_check, call_prob=prob), data, diag_dict=diag_dict, tc_nam=tc_to_check, call_prob=prob)
            # print(tmp_dict)
            if tmp_dict is not None:
                new_dict = merge_dicts(new_dict, tmp_dict)
        else:
            new_dict = merge_dicts(new_dict, {tc_to_check: prob})
    if diag_dict:
        diag_dict_print(new_dict, prob_dict, call_prob, tc_nam)
    return new_dict


def wrap_monster_loop(item, data, names, code_to_name_dict, difficulty, type, tc):
    if not item['Id'] == 'Expansion':
        if (names['String Index'] == item['NameStr']).any() and not isinstance(item[tc], float):
            try:
                print("{:s} has name {:s} and TC {:s}".format(
                    item['Id'],
                    names[names['String Index'] == item['NameStr']]['Text'].tolist()[0],
                    item[tc]
                ))
                prob_dict = tc_get_prob_dict(data, item[tc])
                totals = tc_unravel(prob_dict, data, diag_dict=False)
                totals = split_weap_and_armo(totals)
                print_results_to_txt(totals, code_to_name_dict, names[names['String Index'] == item['NameStr']]['Text'].tolist()[0] + type + difficulty + '.txt')
            except:
                print(item['Id'])
                print(names[names['String Index'] == item['NameStr']]['Text'].tolist()[0])
                print(item['TreasureClass1'])
                exit()


def wrap_superunique_loop(item, data, names, code_to_name_dict, difficulty, tc):
    #for _, item in uniques.iterrows():
    if not item['Superunique'] == 'Expansion':
        if (names['String Index'] == item['Name']).any() and not isinstance(item[tc], float):
            print("{:s} has name {:s} and TC {:s}".format(
                item['Superunique'],
                names[names['String Index'] == item['Name']]['Text'].tolist()[0],
                item[tc]
            ))
            prob_dict = tc_get_prob_dict(data, item[tc])
            totals = tc_unravel(prob_dict, data, diag_dict=False)
            totals = split_weap_and_armo(totals)
            print_results_to_txt(totals, code_to_name_dict, names[names['String Index'] == item['Name']]['Text'].tolist()[0]+difficulty+'.txt')

def loop_over_monsters_and_uniques(data, monsters, uniques, names):
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
        for tt, type in enumerate(['', '_champ', '_unique']):
            Parallel(n_jobs=16)(delayed(wrap_monster_loop)(item, data, names, code_to_name_dict, difficulty, type, monster_tcs[dd][tt]) for _, item in monsters.iterrows())
        if type == '_unique':
            Parallel(n_jobs=16)(delayed(wrap_superunique_loop)(item, data, names, code_to_name_dict, difficulty, unique_tcs[dd]) for _, item in uniques.iterrows())


def main():
    monsters = read_monstats()
    uniques = read_unique_monsters()
    names = read_monster_names()
    data = read_TreasurClassEx()
    loop_over_monsters_and_uniques(data, monsters, uniques, names)


f = open('hello.txt','w')
f.write('hello')
f.close()
if __name__ == '__main__':
    main()