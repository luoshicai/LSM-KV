#include "memtable.h"


void memTable::compaction(vector<searchNode *> &intersection, uint32_t level1, uint32_t level2, searchNode *parent){
    searchNode* newSearchList=nullptr;     //新生成的缓存链表
    searchNode* tail=nullptr;
    uint64_t current_size=0;      //大于MAXSIZE=2086880时切断sstable
    uint64_t current_numOfNode=0;   //归并链表中的节点数目
    uint64_t current_timeLabel=0;  //归并集中最大的timeLabel
    char BF[10240];
    string sourcePath=".\\data\\level";    //作为源在访问数据时用
    string direction=".\\data\\level" + to_string(level2);   //加上文件扩展名即为新的sstable目录
    uint32_t setSize = intersection.size();    //有多少个sstable需要归并
    uint64_t setNumber = 0;   //归并集里面的总结点数
    ForCompaction* MergeSet = new ForCompaction[setSize];
    for (uint32_t i=0; i<setSize; ++i){
        MergeSet[i].tableName = intersection[i]->tableName;
        MergeSet[i].path = sourcePath+to_string(intersection[i]->tableLevel) +"\\"+intersection[i]->tableName;
        MergeSet[i].nextKey = intersection[i]->index[0].key;
        MergeSet[i].offset = intersection[i]->index[0].offset;
        MergeSet[i].numOfNode = intersection[i]->numOfNode;
        MergeSet[i].fin.open(MergeSet[i].path,ios::binary);
        MergeSet[i].timeLabel = intersection[i]->timeLabel;
        setNumber+=intersection[i]->numOfNode;
        if (current_timeLabel<intersection[i]->timeLabel){
            current_timeLabel=intersection[i]->timeLabel;
        }
    }
    //准备工作
    MergeNode* MergeList=nullptr;
    MergeNode* MergeParent=nullptr;
    //开始归并
    for (uint64_t i=0; i<setNumber; ++i){
        //找到当前最小的key值,同样大的key值取最大的timeLabel的
        uint64_t current_minKey=~0;
        uint32_t current_offset=0;
        uint32_t length;
        string data;
        uint32_t tmp;
        for (uint32_t m=0; m<setSize; ++m){
            if (MergeSet[m].surplus&&MergeSet[m].nextKey<current_minKey){
                current_minKey=MergeSet[m].nextKey;
                current_offset=MergeSet[m].offset;
                tmp=m;
            }
            else if (MergeSet[m].surplus&&MergeSet[m].nextKey==current_minKey){
                if (MergeSet[m].timeLabel>MergeSet[tmp].timeLabel){
                    //永远不会再用时间戳小的那一个故可以把它更新掉
                    ++i;
                    MergeSet[tmp].indexTmp++;
                    if (MergeSet[tmp].indexTmp<MergeSet[tmp].numOfNode){
                        uint32_t p=MergeSet[tmp].indexTmp;
                        indexPair point;
                        memcpy(&point,(char*)intersection[tmp]->index+12*p,sizeof (indexPair));
                        MergeSet[tmp].nextKey=point.key;
                        MergeSet[tmp].offset=point.offset;
                    }
                    else {
                        MergeSet[tmp].surplus=false;
                    }
                    tmp=m;
                    current_offset=MergeSet[tmp].offset;
                }
                else{
                    //如果旧的时间戳比新的时间戳大，则也应该将新的时间戳滤掉
                    ++i;
                    MergeSet[m].indexTmp++;
                    if (MergeSet[m].indexTmp<MergeSet[m].numOfNode){
                        uint32_t p = MergeSet[m].indexTmp;
                        indexPair point;
                        memcpy(&point,(char*)intersection[m]->index+12*p,sizeof (indexPair));
                        MergeSet[m].nextKey=point.key;
                        MergeSet[m].offset=point.offset;
                    }
                    else{
                        MergeSet[m].surplus=false;
                    }
                }
            }
        }
        //更新被选中的那个
        MergeSet[tmp].indexTmp++;
        if (MergeSet[tmp].indexTmp<MergeSet[tmp].numOfNode){
            uint32_t p=MergeSet[tmp].indexTmp;
            indexPair point;
            memcpy(&point,(char*)intersection[tmp]->index+12*p,sizeof (indexPair));
            MergeSet[tmp].nextKey=point.key;
            MergeSet[tmp].offset=point.offset;
        }
        else {
            MergeSet[tmp].surplus=false;
        }
        //读取数据
        if (MergeSet[tmp].surplus){
            length = MergeSet[tmp].offset - current_offset;
            char *current_data = new char[length+1];
            MergeSet[tmp].fin.seekg(current_offset,ios::beg);
            MergeSet[tmp].fin.read(current_data,length);
            current_data[length]='\0';
            data = current_data;
            delete[] current_data;
        }
        else {
            length=70000;
            char *current_data = new char[length];
            MergeSet[tmp].fin.seekg(current_offset,ios::beg);
            MergeSet[tmp].fin.read(current_data,length);
            int count=MergeSet[tmp].fin.gcount();
            current_data[count]='\0';
            data = current_data;
            delete[] current_data;
        }
        //判断表的容量是否超过2MB
        if (current_size+baseSize+data.length()>=MAXSIZE){
            ++timeLabel;
            string tablePath = direction + "\\" + to_string(timeLabel) + ".sst";
            Merge_sstable(MergeList,tablePath,current_numOfNode,MergeList->key,MergeParent->key,current_timeLabel,BF);
            //更新缓存
            searchNode* newSearchNode = new searchNode;
            newSearchNode->tableName=to_string(timeLabel)+".sst";
            newSearchNode->timeLabel=current_timeLabel;
            newSearchNode->tableLevel=level2;
            newSearchNode->numOfNode=current_numOfNode;
            newSearchNode->minKey=MergeList->key;
            newSearchNode->maxKey=MergeParent->key;
            newSearchNode->index=new indexPair[current_numOfNode];
            ifstream fin(tablePath,ios::binary);
            fin.seekg(32,ios::beg);
            fin.read((char*)newSearchNode->BF,10240);
            fin.seekg(10272,ios::beg);
            fin.read((char*)newSearchNode->index,sizeof (char)*12*current_numOfNode);
            //后生成的放在链表的最前面
            newSearchNode->next=newSearchList;
            newSearchList=newSearchNode;
            //跟缓存中链表相关数据清空
            clearMergeList(MergeList);
            MergeList=nullptr;
            current_size=0;
            current_numOfNode=0;
            for (int n=0;n<10240;++n){
                BF[n]=0;
            }
        }
        //放入表中
        MergeNode* NewMergeNode = new MergeNode;
        NewMergeNode->key = current_minKey;
        NewMergeNode->data = data;
        NewMergeNode->next = nullptr;
        if (MergeList==nullptr){
            MergeList = NewMergeNode;
            MergeParent = NewMergeNode;
        }
        else {
            MergeParent->next = NewMergeNode;
            MergeParent = NewMergeNode;
        }
        //更新容量和表中的结点数量
        current_size=current_size+baseSize+data.length();
        current_numOfNode++;
        //更新布隆过滤器
        uint32_t hash[4]={0};
        MurmurHash3_x64_128(&current_minKey,sizeof (current_minKey),1,hash);
        for (int m=0; m<4; ++m){
            hash[m]=hash[m]%10240;
            BF[hash[m]]=1;
        }

        //对于归并结束后没有满2MB的缓存也应该创建sstable
        if (i+1==setNumber){
            ++timeLabel;
            string tablePath = direction + "\\" + to_string(timeLabel) + ".sst";
            Merge_sstable(MergeList,tablePath,current_numOfNode,MergeList->key,MergeParent->key,current_timeLabel,BF);
            //更新缓存
            ifstream current_fin(tablePath,ios::binary);
            searchNode* newSearchNode = new searchNode;
            newSearchNode->tableName=to_string(timeLabel)+".sst";
            newSearchNode->timeLabel=current_timeLabel;
            newSearchNode->tableLevel=level2;
            newSearchNode->numOfNode=current_numOfNode;
            newSearchNode->minKey=MergeList->key;
            newSearchNode->maxKey=MergeParent->key;
            newSearchNode->index = new indexPair[current_numOfNode];
            current_fin.seekg(32,ios::beg);
            current_fin.read((char*)newSearchNode->BF,10240);
            current_fin.seekg(10272,ios::beg);
            current_fin.read((char*)newSearchNode->index,sizeof (char)*12*current_numOfNode);
            current_fin.close();
            //后生成的放在链表的最前面
            newSearchNode->next=newSearchList;
            newSearchList=newSearchNode;
            //跟缓存中链表相关数据清空
            clearMergeList(MergeList);
            MergeList=nullptr;
            current_size=0;
            current_numOfNode=0;
            for (int n=0;n<10240;++n){
                BF[n]=0;
            }
        }
    }
    //归并结束，删除之前的sstable,更新缓存
    for (uint32_t m=0; m<setSize; ++m){
        MergeSet[m].fin.close();
    }
    //更新缓存
    searchNode* searchNodeTmp;
    searchNode* searchNodeParent;
    if (parent==nullptr){
       searchNodeTmp=searchList;
       searchNodeParent = nullptr;
    }
    else {
        searchNodeTmp=parent->next;
        searchNodeParent=parent;
    }
    uint32_t deleteTmp=0;
    //删除原表中在归并集中的结点
    while (searchNodeTmp!=nullptr&&deleteTmp<setSize){
        if (searchNodeTmp->tableName==intersection[deleteTmp]->tableName){
            deleteTmp++;
            if (searchNodeTmp==searchList){
               searchList = searchNodeTmp->next;
               delete searchNodeTmp;
               searchNodeTmp = searchList;
            }
            else {
                searchNodeParent->next=searchNodeTmp->next;
                delete searchNodeTmp;
                searchNodeTmp = searchNodeParent->next;
            }
        }
        else {
            searchNodeParent=searchNodeTmp;
            searchNodeTmp=searchNodeTmp->next;
        }
    }
    //添加新结点，按照结点范围的大小在level2中再做归并排序
    tail=newSearchList;
    while (tail->next!=nullptr){
        tail=tail->next;
    }
    if (parent==nullptr){
        if (searchList==nullptr){
            searchList=newSearchList;
        }
        else {
            parent = searchList;
        }
    }
    if (parent!=nullptr){
        while (parent->next!=nullptr&&parent->next->tableLevel<level2) {
            parent=parent->next;
        }
        while (parent->next!=nullptr&&parent->next->maxKey>newSearchList->maxKey&&parent->next->tableLevel==level2) {
            parent=parent->next;
        }
        if (parent==searchList){
            tail->next=searchList;
            searchList = newSearchList;
        }
        else{
            tail->next=parent->next;
            parent->next=newSearchList;
        }
    }
    //删除之前的sstable
    for (uint32_t m=0; m<setSize; ++m){
        utils::rmfile(MergeSet[m].path.c_str());
    }
    //释放new出的空间
    delete [] MergeSet;
}


void memTable::Merge_sstable(MergeNode *MerList, string tablePath,
                             uint64_t current_numOfNode,uint64_t current_minKey,
                             uint64_t current_maxKey, uint64_t current_timeLabel,
                             char BF[])
{
     //创建新表
     ofstream fout(tablePath,ios::binary);
     //写入表头
     fout.write((char*)&current_timeLabel,sizeof (uint64_t));
     fout.write((char*)&current_numOfNode,sizeof (uint64_t));
     fout.write((char*)&current_minKey,sizeof (uint64_t));
     fout.write((char*)&current_maxKey,sizeof (uint64_t));
     //写入BF
     fout.write(BF,sizeof (char)*10240);
     //写入数据
     uint32_t itmp=10272;
     uint32_t dtmp=10272+current_numOfNode*12;
     MergeNode* tmp=MerList;
     uint64_t key;
     string data;
     for (uint64_t i=0; i<current_numOfNode; ++i){
         key=tmp->key;
         data=tmp->data;
         fout.seekp(itmp,ios::beg);
         fout.write((char*)&key,sizeof (uint64_t));
         fout.write((char*)&dtmp,sizeof (uint32_t));
         itmp+=12;
         fout.seekp(dtmp,ios::beg);
         fout.write(data.c_str(),sizeof (char)*data.size());
         dtmp+=sizeof (char)*data.size();
         tmp=tmp->next;
     }
}

void memTable::clearMergeList(MergeNode *MergeList){
    MergeNode* tmp;
    MergeNode* HEAD=MergeList->next;
    int i=0;
    while (HEAD!=nullptr) {
        tmp=HEAD;
        HEAD=HEAD->next;
        delete tmp;
        ++i;
    }
    delete MergeList;
}


void memTable::Compaction(){
    std::vector<searchNode*> intersection;
    std::vector<std::string> ret;
    uint64_t LevelMinKey=~0;
    uint64_t LevelMaxKey=0;
    string catalogue=".\\data\\level";
    string current_dir;
    uint32_t current_Level=1;
    uint64_t current_dirCapacity=4;   //Level和dirCapacity应该同时增加
    uint64_t current_size;
    uint32_t next_Level=2;
    string next_dir;
    current_dir = catalogue + to_string(current_Level);
    next_dir = catalogue + to_string(next_Level);
    while ((current_size=utils::scanDir(current_dir,ret))>current_dirCapacity){
        //如果这一级满了，则一定会用到下一级，可以判断下一级是否存在，如果不存在则创建下一级
        if (!utils::dirExists(next_dir)){
            utils::mkdir(next_dir.c_str());
        }
        searchNode* searchNode_tmp=searchList;
        searchNode* searchNode_parent=searchList;
        uint32_t num=0;   //将链表中第capacity+1个开始的结点放入intersection中
        //跳过current_Level中前capacity个结点
        while (searchNode_tmp!=nullptr&&num<current_dirCapacity) {
            if (searchNode_tmp->tableLevel==current_Level){
                num++;
            }
            searchNode_parent=searchNode_tmp;
            searchNode_tmp=searchNode_tmp->next;
        }
        //记录current_Level中超出的结点
        while (searchNode_tmp!=nullptr&&searchNode_tmp->tableLevel==current_Level) {
            if (LevelMinKey>searchNode_tmp->minKey){
                LevelMinKey=searchNode_tmp->minKey;
            }
            if (LevelMaxKey<searchNode_tmp->maxKey){
                LevelMaxKey=searchNode_tmp->maxKey;
            }
            intersection.push_back(searchNode_tmp);
            searchNode_tmp=searchNode_tmp->next;
        }
        //找到next_Level中有交集的结点
        while (searchNode_tmp!=nullptr&&searchNode_tmp->tableLevel==next_Level){
            if ((searchNode_tmp->minKey<LevelMinKey&&searchNode_tmp->maxKey>LevelMinKey)||
                (searchNode_tmp->minKey<LevelMaxKey&&searchNode_tmp->maxKey>LevelMaxKey))
            {
               intersection.push_back(searchNode_tmp);
            }
            searchNode_tmp=searchNode_tmp->next;
        }
        compaction(intersection,current_Level,next_Level,searchNode_parent);
        //更新循环变量
        ret.clear();
        intersection.clear();
        LevelMinKey=~0;
        LevelMaxKey=0;
        current_Level++;
        next_Level++;
        current_dirCapacity*=2;
        current_dir = catalogue + to_string(current_Level);
        next_dir = catalogue + to_string(next_Level);
    }
}
