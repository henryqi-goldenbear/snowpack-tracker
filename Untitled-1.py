'''

'''

def dotPairs(array):
    xy_pairs = []
    entry_to_index = {}
    order = 1
    for index in range(len(array)):
        if array[index] == '.':
            pass
        else:
            # check if earlier compliment is there
            if array[index] in entry_to_index:
                entry_to_index[array[index]].append((index, order))
            else:
                entry_to_index[array[index]]=[(index, order)]
            if str(10-int(array[index])) in entry_to_index and entry_to_index[str(10-int(array[index]))]:
                for (i, o) in entry_to_index[str(10-int(array[index]))]:
                    diff1 = index-i
                    diff2 = order-o
                    xy_pairs.append(str(10-int(array[index]))+'-'+array[index]+':'+str(diff1-diff2))
                entry_to_index[str(10-int(array[index]))] = [] 
            order+=1
    return xy_pairs

print(dotPairs("1...2.9.8...2"))
print(dotPairs( "5..5"))