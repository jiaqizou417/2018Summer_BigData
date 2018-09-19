from random import randrange  
from  math import sqrt, log2  

############################################################  
########################һЩ���ݴ�������ʵ�֣�#################  

def subsample(dataset):  
    '''''�����зŻ��������,�������ݼ������ز����Ӽ�'''  
    sample=list()  
    n_sample=len(dataset)  
    while len(sample)<n_sample:  
        index=randrange(len(dataset))  
        sample.append(dataset[index])  
    return sample  

################################  
#######**���ۺ�����ʵ��**#########  
#�Ի���ָ�������᲻���ȣ������ʵ�֣�  
def giniIndex(rows):  
    '''''rows������������; 
    ����ֵΪ����ָ��'''  
    gini=0  
    total=len(rows)  #��������������  
    results=uniquecounts(rows) #dict����ʾ�����и������ͳ����  
    for k in results:  
        pk=float(results[k])/total  #������ÿ����������/������  
        gini+=pk*pk  

    gini=1-gini   #����ָ��  
    return gini  

#���ض����ʵ�֣�  
def entropy(rows):  
    '''''rows��rows������������; 
    ����ֵΪ��'''  
    entropy=0  
    total=len(rows)  #������������  
    results=uniquecounts(rows) ##dict����ʾ�����и������ͳ����  
    #print(results)  
    for k in results.keys():  
        pk=float(results[k])/total #������ÿ�������ĸ���  
        entropy+=pk*log2(pk)       #log��2Ϊ�׵��ص�λΪbit  
    entropy=-entropy  
    return entropy  

#�����ʵ�֣�������ֵ�����ݣ�  
def variance(rows):  
    if len(rows)==0:  
        return 0  
    data=[float(row[-1]) for row in rows]  
    mean=sum(data)/len(data)  
    variance=sum([(d-mean)**2 for d in data])/len(data)  
    return variance  

#####################################################  
#####################��������ʵ��######################  

class decisionNode(object):  
    '''''���������������'''  
    def __init__(self,col=-1,value=None,results=None,tb=None,fb=None,samples=None):  
        '''''col:�������������ֵ;value:Ϊ��ʹ���Ϊtrue,��ǰ�б���ƥ���ֵ 
        tb,fb:��һ����,result:dict,Ҷ��㴦,�÷�֧�Ľ��,������㴦ΪNone'''  
        self.col=col  
        self.value=value  
        self.results=results  
        self.tb=tb  
        self.fb=fb  
        self.samples=samples  

#����ĳһ�У������������ݼ��Ͻ��в�֣��ܹ�������ֵ�����ݻ�����������  
def divideset(rows,column,value):  
    #����һ����������������������������ڵ�һ�飨����ֵΪ True���������ڵڶ��飨����ֵΪFalse��  

    split_fun=None  
    if isinstance(value,int) or isinstance(value,float):  
        split_fun=lambda row:row[column]>=value  
    else:  
        split_fun = lambda row: row[column] == value  

    #�����ݼ���ֳ��������ϲ�����  
    set1=[row for row in rows if split_fun(row)]  
    set2=[row for row in rows if not split_fun(row)]  
    return (set1,set2)  

#�Ը��ֿ��ܵĽ�����м�����ÿһ�����ݵ����һ�У���ǩ����¼����һ�����  
def uniquecounts(rows):  
    '''''rows��������������; 
    ����ֵdict'''  
    results={}  
    #print(rows)  
    for row in rows:  
        rs=row[-1]  
        #print(rs)  
        if not rs in results.keys():  
            results[rs]=0  
        results[rs]+=1  
    return results  
#�������Ĺ�����̣������������������Ҫע����������.  
def buildDTree(rows,scoref,n_features,min_size,max_depth,depth): #�����Ƕ�ÿ�����Ľ���  
    '''''rows:���ݼ� 
    scoref�����ۺ��� 
    n_features:���������� 
    min_size:���ֹͣ�����ֲ�������� 
    max_depth������������'''  
    if len(rows)<=min_size:  
        return decisionNode(results=uniquecounts(rows)) #�������������<=min_sizeʱ,ֹͣ�ֲ棬�����ظý������  
    if depth>=max_depth:  
        return decisionNode(results=uniquecounts(rows)) #�������>=max_depthʱ��ֹͣ�ֲ棬�����ظý������  
    current_score=scoref(rows)  

    #����һЩ�����Լ�¼��Ѳ������  
    best_gain=0  
    best_criteria=None      #��Ѳ�ֵ�:������������ȡֵ��  
    best_sets=None          #��Ѳ�ֵ�������Ӽ�  

    #����������  
    rows_copy=rows[:]  
    features_index=[]  
    while len(features_index)<n_features:  
        index=randrange(len(rows_copy[0])-1)  
        if index not in features_index:  
            features_index.append(index)  

    # ���ݳ�ȡ�����������ݼ����в��  
    for col in features_index:  
        # ��¼ĳ��������ȡֵ����  
        column_values={}  
        for row in rows:  
            column_values[row[col]]=1  
        #����������������ݼ����в��  
        values=[]  
        for key in column_values.keys():     # ��������µ�����ֵ����  
            values.append(key)  

        if None in values:  #������ֵȱʧʱ�� #����������C4.5�Ĳ���,�������û��ʵ�֣�  
            func = lambda x : None not in x  
            data = [row for row in rows if func(row)]  #��ȱʧֵ������  
            p = float(len(data))/len(rows)    #��ȱʧֵ������ռ�����ݵı���  
            values.remove(None)    #�޳�None  
            if len(values) > 0:  
                for value in values:  
                    (set1, set2) = divideset(data, col, value)  
                    # �������µ���Ϣ���棺  
                    p_set1 = float(len(set1) / len(data))  
                    gain_sub = current_score - p_set1 * scoref(set1) - (1 - p_set1) * scoref(set2)  
                    gain=p*gain_sub  
                    if gain > best_gain and len(set1) > 0 and len(set2) > 0:  
                        best_gain = gain  
                        best_criteria = (col, value)  
                        best_sets = (set1, set2)  

        else:                  #����ֵδȱʧʱ  
            for value in values:  
                (set1,set2)=divideset(rows,col,value)  

                #�������µ���Ϣ���棺  
                p_set1=float(len(set1)/len(rows))  
                gain=current_score-p_set1*scoref(set1)-(1-p_set1)*scoref(set2)  
                if gain>best_gain and len(set1)>0 and len(set2)>0:  
                    best_gain=gain  
                    best_criteria=(col,value)  
                    best_sets=(set1,set2)  

    #������֧��  
    if best_gain>0:  
        TrueBranch=buildDTree(best_sets[0],scoref,n_features,min_size,max_depth,depth+1)  
        FalseBranch=buildDTree(best_sets[1],scoref,n_features,min_size,max_depth,depth+1)  
        return decisionNode(col=best_criteria[0],value=best_criteria[1],tb=TrueBranch,fb=FalseBranch)  
    else:  
        return decisionNode(results=uniquecounts(rows))  




# Ԥ�⺯����  
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
############***�������ۺ���***###########################  
def accuracy(actual,predicted):  
    '''''���룺ʵ��ֵ,Ԥ��ֵ'''  
    correct=0  
    for i in  range(len(actual)):  
        if actual[i]==predicted[i]:  
            correct+=1  
    accuracy=(float(correct)/len(actual))*100  
    return accuracy  

##############################################################  
##############***Out-of-Bag***################################  
#���д�����Ƶ���غ�����ʵ��,��Ҫע�Ⲣ����ÿ�����������ܳ��������ɭ�ֵĴ���������  
#��˽���oob����ʱ��Ҫע���������������  
#Breiman ������˵�� oob���Ƶ�error�����ڲ��Լ���ѵ����������С��ͬʱ�Ĳ���error  
def OOB(oobdata,train,trees):  
    '''''����Ϊ����������dict,ѵ����,tree_list 
    return oob׼ȷ��'''  
    n_rows=[]  
    count=0  
    n_trees=len(trees)   #ɭ�������Ŀ���  

    for key,item in oobdata.items():  
        n_rows.append(item)  

    print(len(n_rows))   #����trees�е�oob���ݵĺϼ�  

    n_rows_list=sum(n_rows,[])  

    unique_list=[]  
    for l1 in n_rows_list:     #��oob�ϼ��м��������������  
        if l1 not in unique_list:  
            unique_list.append(l1)  

    n=len(unique_list)  
    print(n)  

    #��ѵ�����е�ÿ�����ݣ����б�����Ѱ������Ϊoob����ʱ������trees,�����ж���ͶƱ  
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
############***RandomForest��ʵ��***###########################  
#�ۺ϶������Ԥ����������Ԥ����  
def bagging_predict(trees,row):  
    predictions=[predict(row,tree) for tree in trees]  
    #print(predictions)  
    return max(set(predictions),key=predictions.count)  

def RandomForest(train,test,max_depth,min_size,n_trees,n_features,scoref=giniIndex):  
    trees=[]  #�����ĵ����������б���  
    oobs={}  
    for i in range(n_trees):  
        subset=subsample(train)  
        oobs[i]=subset  
        tree=buildDTree(subset,scoref,n_features,min_size,max_depth,0)  
        trees.append(tree)  
        #drawtree(tree,jpeg='%d'%i)  
    oob_score=OOB(oobs,train,trees)   #oob׼ȷ��  
    predictions=[bagging_predict(trees,row) for row in test]  
    actual = [row[-1] for row in test]  
    test_score=accuracy(actual, predictions)   #���Լ�׼ȷ��  
    return  test_score ,oob_score 