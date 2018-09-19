import numpy as np
import pandas as pd 
from sklearn.ensemble import RandomForestClassifier
#数据处理
data = pd.read_csv('20160421_MLdata_v4.csv')
data_np = np.array(data)
data_list = data_np.tolist()
data_list_copy = data_list[:]
train = []
while len(train)<160:
    index = randrange(len(data_list_copy))
    train.append(data_list_copy.pop(index))
    test = data_list_copy
#训练测试
max_depth =10
min_size = 1
n_trees = 15
n_features = int(sqrt(60))
test_score,oob_score = RandomForest(train,test,max_depth,min_size,n_trees,n_features)
print('test score:',test_score)
print('oob score:',oob_score)