input_list = ["hello","HELLO","I","AM","AM","sumit","sumit","I","hello"]
input_set = set(input_list)
print(input_set)
result_list = []
for word in input_set:
    result_list.append((word,input_list.count(word)))
print(result_list )
