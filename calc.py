import pandas as pd
import numpy as np
from collections import Counter 

def read_TreasurClassEx():
	with open('TreasureClassEx.txt','r') as f:
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
		elif 'Ring' in item['name']:
			code_to_name_dict[item['code']] = item['*name']+" (class ring)"
		elif 'Amulet' in item['name']:
			code_to_name_dict[item['code']] = item['*name']+" (class amulet)"
		else:
			code_to_name_dict[item['code']] = item['name']
	# add no drop
	code_to_name_dict['NoDrop'] = "No Drop"
	code_to_name_dict['aq2'] = "Arrows"
	code_to_name_dict['cq2'] = "Bolts"
	return code_to_name_dict


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
			return_dict = Counter(return_dict) + Counter(split_dict)
		elif 'armo' in key:
			level = int(key.replace('armo',''))
			split_dict = get_level_subdict(df_armor, level, value)
			return_dict = Counter(return_dict) + Counter(split_dict)
		else:
			return_dict[key] = drop_dict[key]
	# for key, value in return_dict.items():
	# 	print("{:s}: {:11.9f}".format(key, value))
	return return_dict


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
	if not np.isnan(data.at[idx, 'NoDrop']):
		prob_dict['NoDrop'] = data.at[idx, 'NoDrop']/total_prob*call_prob
	for ii in range(1,11):
		if not np.isnan(data.at[idx,'Prob'+str(ii)]):
			if picks > 0:
				prob_dict[data.at[idx, 'Item'+str(ii)]] = data.at[idx,'Prob'+str(ii)]/total_prob*call_prob*picks
			elif picks < 0:
				prob_dict[data.at[idx, 'Item'+str(ii)]] = 1.*call_prob
			elif picks == 0:
				return {}
	return prob_dict


def tc_unravel(prob_dict, data, diag_dict = False, tc_nam='None', call_prob=1.):
	# diag_dict = True
	new_dict = {}
	for tc_to_check, prob in prob_dict.items():
		# print("checking "+tc_to_check)
		if (data['Treasure Class'] == tc_to_check).any():
			# print("yes")
			tmp_dict = tc_unravel(tc_get_prob_dict(data, tc_to_check, call_prob=prob), data, tc_nam=tc_to_check, call_prob=prob)
			# print(tmp_dict)
			if tmp_dict is not None:
				new_dict = Counter(new_dict) + Counter(tmp_dict)
				# for key in tmp_dict:
				# 	new_dict[key] = tmp_dict[key]
		else:
			# print("no")
			new_dict[tc_to_check] = prob
	if diag_dict:

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

	return new_dict


def main():
	data = read_TreasurClassEx()
	blah = data[data['Treasure Class']=='Potion 7']
	code_to_name_dict = read_item_lists()
	prob_dict = tc_get_prob_dict(data, 'A5C Book (H)')
	# print(prob_dict)
	totals = tc_unravel(prob_dict, data)
	totals = split_weap_and_armo(totals)
	chance_sum = 0.
	# print("========== FINAL RESULT ===========")
	for key in sorted(totals.keys()):
		try:
		#if not 'gld' in key and not 'weap' in key and not 'armo' in key and not 'Ore' in key:
			print("{:s}\t {:11.9f}".format(code_to_name_dict[key], totals[key]))
		except:
		#else:
			# print("{:s}\t {:11.9f}".format('(UNNAMED)'+key, totals[key]))
			print("{:s}\t {:11.9f}".format(key, totals[key]))
		chance_sum += totals[key]
	print("total: "+str(chance_sum))


if __name__ == '__main__':
	main()