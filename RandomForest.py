from random import randrange  
from  math import sqrt, log2  

############################################################  
########################一些数据处理函数的实现：#################  

def subsample(dataset):  
    '''''样本有放回随机采样,输入数据集，返回采样子集'''  
    sample=list()  
    n_sample=len(dataset)  
    while len(sample)<n_sample:  
        index=randrange(len(dataset))  
        sample.append(dataset[index])  
    return sample  

################################  
#######**评价函数的实现**#########  
#对基尼指数（基尼不纯度）定义的实现：  
def giniIndex(rows):  
    '''''rows：输入样本合; 
    返回值为基尼指数'''  
    gini=0  
    total=len(rows)  #集合总样本数量  
    results=uniquecounts(rows) #dict：表示集合中各类别及其统计量  
    for k in results:  
        pk=float(results[k])/total  #集合中每类样本数量/总数量  
        gini+=pk*pk  

    gini=1-gini   #基尼指数  
    return gini  

#对熵定义的实现：  
def entropy(rows):  
    '''''rows：rows：输入样本合; 
    返回值为熵'''  
    entropy=0  
    total=len(rows)  #集合总样本数  
    results=uniquecounts(rows) ##dict：表示集合中各类别及其统计量  
    #print(results)  
    for k in results.keys():  
        pk=float(results[k])/total #集合中每类样本的概率  
        entropy+=pk*log2(pk)       #log以2为底的熵单位为bit  
    entropy=-entropy  
    return entropy  

#方差的实现（处理数值型数据）  
def variance(rows):  
    if len(rows)==0:  
        return 0  
    data=[float(row[-1]) for row in rows]  
    mean=sum(data)/len(data)  
    variance=sum([(d-mean)**2 for d in data])/len(data)  
    return variance  

#####################################################  
#####################决策树的实现######################  

class decisionNode(object):  
    '''''建立决策树结点类'''  
    def __init__(self,col=-1,value=None,results=None,tb=None,fb=None,samples=None):  
        '''''col:待检验的列索引值;value:为了使结果为true,当前列必须匹配的值 
        tb,fb:下一层结点,result:dict,叶结点处,该分支的结果,其它结点处为None'''  
        self.col=col  
        self.value=value  
        self.results=results  
        self.tb=tb  
        self.fb=fb  
        self.samples=samples  

#依据某一列（特征）对数据集合进行拆分，能够处理数值型数据或名词型数据  
def divideset(rows,column,value):  
    #定义一个函数，令其告诉我们数据行属于第一组（返回值为 True）还是属于第二组（返回值为False）  

    split_fun=None  
    if isinstance(value,int) or isinstance(value,float):  
        split_fun=lambda row:row[column]>=value  
    else:  
        split_fun = lambda row: row[column] == value  

    #将数据集拆分成两个集合并返回  
    set1=[row for row in rows if split_fun(row)]  
    set2=[row for row in rows if not split_fun(row)]  
    return (set1,set2)  

#对各种可能的结果进行计数（每一行数据的最后一列（标签）记录了这一结果）  
def uniquecounts(rows):  
    '''''rows：输入样本集合; 
    返回值dict'''  
    results={}  
    #print(rows)  
    for row in rows:  
        rs=row[-1]  
        #print(rs)  
        if not rs in results.keys():  
            results[rs]=0  
        results[rs]+=1  
    return results  
#决策树的构造过程：决策树构造过程中需要注意特征抽样.  
def buildDTree(rows,scoref,n_features,min_size,max_depth,depth): #函数是对每个结点的建立  
    '''''rows:数据集 
    scoref：评价函数 
    n_features:抽样特征数 
    min_size:结点停止继续分叉的样本数 
    max_depth：树的最大深度'''  
    if len(rows)<=min_size:  
        return decisionNode(results=uniquecounts(rows)) #当结点样本数量<=min_size时,停止分叉，并返回该结点的类别  
    if depth>=max_depth:  
        return decisionNode(results=uniquecounts(rows)) #当树深度>=max_depth时，停止分叉，并返回该结点的类别  
    current_score=scoref(rows)  

    #定义一些变量以记录最佳拆分条件  
    best_gain=0  
    best_criteria=None      #最佳拆分点:（特征，特征取值）  
    best_sets=None          #最佳拆分点产生的子集  

    #特征抽样：  
    rows_copy=rows[:]  
    features_index=[]  
    while len(features_index)<n_features:  
        index=randrange(len(rows_copy[0])-1)  
        if index not in features_index:  
            features_index.append(index)  

    # 根据抽取的特征对数据集进行拆分  
    for col in features_index:  
        # 记录某个特征的取值种类  
        column_values={}  
        for row in rows:  
            column_values[row[col]]=1  
        #根据这个特征对数据集进行拆分  
        values=[]  
        for key in column_values.keys():     # 这个特征下的属性值种类  
            values.append(key)  

        if None in values:  #当属性值缺失时： #（本来想用C4.5的策略,但是最后没有实现）  
            func = lambda x : None not in x  
            data = [row for row in rows if func(row)]  #无缺失值的数据  
            p = float(len(data))/len(rows)    #无缺失值的数据占总数据的比例  
            values.remove(None)    #剔除None  
            if len(values) > 0:  
                for value in values:  
                    (set1, set2) = divideset(data, col, value)  
                    # 计算拆分下的信息增益：  
                    p_set1 = float(len(set1) / len(data))  
                    gain_sub = current_score - p_set1 * scoref(set1) - (1 - p_set1) * scoref(set2)  
                    gain=p*gain_sub  
                    if gain > best_gain and len(set1) > 0 and len(set2) > 0:  
                        best_gain = gain  
                        best_criteria = (col, value)  
                        best_sets = (set1, set2)  

        else:                  #属性值未缺失时  
            for value in values:  
                (set1,set2)=divideset(rows,col,value)  

                #计算拆分下的信息增益：  
                p_set1=float(len(set1)/len(rows))  
                gain=current_score-p_set1*scoref(set1)-(1-p_set1)*scoref(set2)  
                if gain>best_gain and len(set1)>0 and len(set2)>0:  
                    best_gain=gain  
                    best_criteria=(col,value)  
                    best_sets=(set1,set2)  

    #创建分支：  
    if best_gain>0:  
        TrueBranch=buildDTree(best_sets[0],scoref,n_features,min_size,max_depth,depth+1)  
        FalseBranch=buildDTree(best_sets[1],scoref,n_features,min_size,max_depth,depth+1)  
        return decisionNode(col=best_criteria[0],value=best_criteria[1],tb=TrueBranch,fb=FalseBranch)  
    else:  
        return decisionNode(results=uniquecounts(rows))  




# 预测函数：  
def predict_results(observation,tree):  
    if tree.results != None:  
        return tree.results  
    else:  
        v = observation[tree.col]  
        if v == None:  
            tr, fr = predict_results(observation,tree.tb), predict_results(observation,tree.fb)  
            tcount=sum(tr.values())  
            fcount=sum(fr.values())  
            tw=float(tcount)/(tcount+fcount)  
            fw=float(fcount)/(tcount+fcount)  
            result={}  
            for k,v in tr.items():  
                result[k]=v*tw  
            for k,v in fr.items():  
                if k not in result:result[k]=0  
                result[k] += v*fw  
            return result  
        else:  
            Branch = None  
            if isinstance(v, int) or isinstance(v, float):  
                if v >= tree.value:  
                    Branch = tree.tb  
                else:  
                    Branch = tree.fb  
            else:  
                if v == tree.value:  
                    Branch = tree.tb  
                else:  
                    Branch = tree.fb  
            return predict_results(observation, Branch)  

def predict(observation, tree):  
    results=predict_results(observation,tree)  
    label = None  
    b_count = 0  
    for key in results.keys():  
        if results[key] > b_count:  
            b_count = results[key]  
            label = key  
    return label  
##############################################################  
############***精度评价函数***###########################  
def accuracy(actual,predicted):  
    '''''输入：实际值,预测值'''  
    correct=0  
    for i in  range(len(actual)):  
        if actual[i]==predicted[i]:  
            correct+=1  
    accuracy=(float(correct)/len(actual))*100  
    return accuracy  

##############################################################  
##############***Out-of-Bag***################################  
#进行袋外估计等相关函数的实现,需要注意并不是每个样本都可能出现在随机森林的袋外数据中  
#因此进行oob估计时需要注意估计样本的数量  
#Breiman 的论文说明 oob估计的error近似于测试集和训练集样本大小相同时的测试error  
def OOB(oobdata,train,trees):  
    '''''输入为：袋外数据dict,训练集,tree_list 
    return oob准确率'''  
    n_rows=[]  
    count=0  
    n_trees=len(trees)   #森林中树的棵树  

    for key,item in oobdata.items():  
        n_rows.append(item)  

    print(len(n_rows))   #所有trees中的oob数据的合集  

    n_rows_list=sum(n_rows,[])  

    unique_list=[]  
    for l1 in n_rows_list:     #从oob合集中计算独立样本数量  
        if l1 not in unique_list:  
            unique_list.append(l1)  

    n=len(unique_list)  
    print(n)  

    #对训练集中的每个数据，进行遍历，寻找其作为oob数据时的所有trees,并进行多数投票  
    for row in train:  
        pre = []  
        for i in range(n_trees):  
            if row not in oobdata[i]:  
                pre.append(predict(row,trees[i]))  
        if len(pre)>0:  
            label=max(set(pre),key=pre.count)  
            if label==row[-1]:  
                count+=1  

    return (float(count)/n)*100  

##############################################################  
############***RandomForest的实现***###########################  
#综合多颗树的预测结果，给出预测结果  
def bagging_predict(trees,row):  
    predictions=[predict(row,tree) for tree in trees]  
    #print(predictions)  
    return max(set(predictions),key=predictions.count)  

def RandomForest(train,test,max_depth,min_size,n_trees,n_features,scoref=giniIndex):  
    trees=[]  #产生的单颗树存于列表中  
    oobs={}  
    for i in range(n_trees):  
        subset=subsample(train)  
        oobs[i]=subset  
        tree=buildDTree(subset,scoref,n_features,min_size,max_depth,0)  
        trees.append(tree)  
        #drawtree(tree,jpeg='%d'%i)  
    oob_score=OOB(oobs,train,trees)   #oob准确率  
    predictions=[bagging_predict(trees,row) for row in test]  
    actual = [row[-1] for row in test]  
    test_score=accuracy(actual, predictions)   #测试集准确率  
    return  test_score ,oob_score 