#include "memtable.h"

void memTable::createSSTable(){
    //创建目录
    if (!utils::dirExists(".\\data\\level0")){
        utils::_mkdir(".\\data\\level0");
    }
    //sstable的创建，name为扩展名
    ++timeLabel;
    stringstream ss;
    ss << timeLabel;
    string name;
    ss >> name;
    name+=".sst";
    ofstream fout(".\\data\\level0\\"+name,ios::binary);
    //header数据的输入,包括时间戳，结点数量，最小键值，最大键值
    fout.write((char*)&timeLabel,sizeof (uint64_t));
    fout.write((char*)&numOfNode,sizeof (uint64_t));
    fout.write((char*)&minKey,sizeof (uint64_t));
    fout.write((char*)&maxKey,sizeof (uint64_t));
    //BF的输入
    char BF[10240]={0};
    KVNode* pointer=head;
    uint64_t poiKey;
    for (uint64_t i=0; i<numOfNode; ++i){
        pointer=pointer->next[0];
        poiKey=pointer->key;
        uint32_t hash[4]={0};
        MurmurHash3_x64_128(&poiKey,sizeof (poiKey),1,hash);
        for (int m=0; m<4; ++m){
            hash[m]=hash[m]%10240;
            BF[hash[m]]=1;
        }
    }
    fout.write(BF,sizeof (char)*10240);
    //索引区与数据区的输入
    uint32_t itmp=10272;  //itmp记录下一个索引写入的位置
    uint32_t dtmp=10272+numOfNode*12; //dtmp记录下一个数据写入的位置
    KVNode* tmp=head;
    uint64_t key;
    string data;
    for (uint64_t i=0;i<numOfNode;++i){
        tmp=tmp->next[0];
        key=tmp->key;
        data=tmp->data;
        fout.seekp(itmp,ios::beg);
        fout.write((char*)&key,sizeof (uint64_t));
        fout.write((char*)&dtmp,sizeof (uint32_t));
        itmp+=12;
        fout.seekp(dtmp,ios::beg);
        fout.write(data.c_str(),sizeof (char)*data.size());
        dtmp+=sizeof (char)*data.size();
    }
    //非数据区放入缓存
    this->appendSearchNode(".\\data\\level0\\"+name);
    //内存的清空
    this->clearMemTable();
    fout.close();



    //5.7
    //commpation
    std::vector<std::string> ret;   //commpation上目录下的所有文件
    if (utils::scanDir(".\\data\\level0",ret)>2){
        if (!utils::dirExists(".\\data\\level1")){
            utils::_mkdir(".\\data\\level1");
        }
        uint64_t LevelMinKey=~0;
        uint64_t LevelMaxKey=0;
        searchNode* tmp1=searchList;
        std::vector<searchNode*> intersection;
        for (int i=0; i<3; ++i){
            intersection.push_back(tmp1);
            if (tmp1->maxKey>LevelMaxKey){
                LevelMaxKey=tmp1->maxKey;
            }
            if (tmp1->minKey<LevelMinKey){
                LevelMinKey=tmp1->minKey;
            }
            tmp1=tmp1->next;
        }
        while (tmp1!=nullptr&&tmp1->tableLevel==1){
            if ((LevelMinKey>=tmp1->minKey&&LevelMinKey<=tmp1->maxKey)||(LevelMaxKey>=tmp1->minKey&&LevelMaxKey<=tmp1->maxKey)){
               intersection.push_back(tmp1);
            }
            tmp1 = tmp1->next;
        }
        compaction(intersection,0,1,nullptr);
        Compaction();
    }
    return;
}


//非数据区放入缓存
void memTable::appendSearchNode(string path){
    searchNode* newSearchNode = new searchNode;
    stringstream ss;
    ss << timeLabel;
    string name;
    ss >> name;
    name+=".sst";
    newSearchNode->tableName=name;
    newSearchNode->tableLevel=0;
    newSearchNode->timeLabel=timeLabel;
    newSearchNode->numOfNode=numOfNode;
    newSearchNode->minKey=minKey;
    newSearchNode->maxKey=maxKey;
    newSearchNode->index=new indexPair[numOfNode];
    ifstream fin(path,ios::binary);
    fin.seekg(32,ios::beg);
    fin.read((char*)newSearchNode->BF,10240);
    fin.seekg(10272,ios::beg);
    fin.read((char*)newSearchNode->index,sizeof (char)*12*numOfNode);
    //后生成的插入链表最前面
    if (searchList==nullptr){
        searchList=newSearchNode;
    }
    else{
        newSearchNode->next=searchList;
        searchList=newSearchNode;
    }
    return;
}


//采用二分查找查找该sstable中是否有key,如果有,返回其data(原样返回，包括"~DELETED~"),如果没有,返回空
string memTable::findSearchNode(searchNode* sstable,uint64_t key){
    uint64_t numOfNode=sstable->numOfNode;
    uint64_t tmp;
    uint64_t left=0,right=numOfNode;
    while (left <= right) {
        tmp=(left+right)/2;
        indexPair point;
        memcpy(&point,(char*)sstable->index+12*tmp,sizeof (indexPair));
        if (point.key==key){
            uint32_t length;
            indexPair start;
            indexPair end;
            if (key==sstable->maxKey){   //处理文件结尾，length=70000限定了最长的数据不会超过70000个byte
                memcpy(&start,(char*)sstable->index+12*tmp,sizeof (indexPair));
                length=70000;
            }
            else{
                memcpy(&start,(char*)sstable->index+12*tmp,sizeof (indexPair));
                memcpy(&end,(char*)sstable->index+12*(tmp+1),sizeof (indexPair));
                length=end.offset-start.offset;
            }
//            uint32_t length=sstable->index[tmp+1].offset-sstable->index[tmp].offset;
            return this->fetchData(sstable->tableName,sstable->tableLevel,start.offset,length);
        }
        else if (point.key<key){
            left=tmp+1;
            continue;
        }
        else if (point.key>key){
            right=tmp-1;
            continue;
        }
    }
    return "";

}

//根据提供的时间戳和offset在对应的sstable中取回数据
string memTable::fetchData(string tableName, uint32_t tableLevel, uint32_t offset,uint32_t length){
   string path=".\\data\\level"+ to_string(tableLevel) + "\\" +tableName;
   ifstream fin(path,ios::binary);
   fin.seekg(offset,ios::beg);
   char* data=new char[sizeof (char)*length+1];
   fin.read(data,sizeof (char)*length);
   if (length<70000){
       data[sizeof(char)*length]='\0';
   }
   else {
       int count=fin.gcount();
       data[count]='\0';
   }
   string m=data;
   delete []data;
   return m;
}

void memTable::scanSearchNode(searchNode* sstable,uint64_t key1, uint64_t key2,
                              std::list<std::pair<uint64_t, std::string> > &list,
                              vector<uint64_t> &ExcludedList){
    uint64_t numOfNode=sstable->numOfNode;
    uint64_t tmpForKey1;
    //key1不在sstable的区间
    if (sstable->minKey>=key1){
        tmpForKey1=0;
    }
    else{
        //key1一定在sstable所在的区间内，采用二分查找查找第一个大于key1的值所在的数组中的位置
        uint64_t left=0,right=numOfNode-1;
        while (left<=right) {
            tmpForKey1=(left+right)/2;
            indexPair point;
            memcpy(&point,(char*)sstable->index+12*tmpForKey1,sizeof (indexPair));
            if (key1<point.key){
                right=tmpForKey1-1;
            }
            else if (key1>=point.key){
                left=tmpForKey1+1;
            }
        }
        tmpForKey1=left;  //tmpForkey1为第一个大于key1的元素在数组中的下标
    }
    string path=".\\data\\level" + to_string(sstable->tableLevel) + "\\" +sstable->tableName;
    ifstream fin(path,ios::binary);
    uint64_t numForCopy;  //在该sstable中符合条件的结点个数
    uint64_t copyLength;  //一共copy了多少字节
    indexPair start,end;  //开始结点与结尾结点
    char *copyResult;     //copy之后的结果
    //比较key2与maxKey,如果key2大于等于maxKey,则从tmpForKey1开始直接copy，反之则找tmpForKey2
    if (key2>=sstable->maxKey){
        numForCopy=numOfNode-tmpForKey1;
        memcpy(&start,(char*)sstable->index+12*tmpForKey1,sizeof (indexPair));
        memcpy(&end,(char*)sstable->index+12*(numOfNode-1),sizeof (indexPair));
        copyLength=end.offset-start.offset+70000;  //最后70000是预留给最后一个结点数据的
        copyResult=new char[sizeof (char)*copyLength];
        fin.seekg(start.offset,ios::beg);
        fin.read(copyResult,sizeof (char)*copyLength);
        copyLength=fin.gcount();
        copyResult[copyLength]='\0';
    }
    else{
        uint64_t tmpForKey2;
        indexPair point;
        uint64_t left=0,right=numOfNode-1;
        while (left<=right) {
            tmpForKey2=(left+right)/2;
            memcpy(&point,(char*)sstable->index+12*tmpForKey2,sizeof (indexPair));
            if (key2<point.key){
                right=tmpForKey2-1;
            }
            else if (key2>=point.key){
                left=tmpForKey2+1;
            }
        }
        tmpForKey2=left;   //tmpForkey2是第一个大于等于key2的位置
        memcpy(&end,(char*)sstable->index+12*tmpForKey2,sizeof (indexPair));
        if (end.key>key2){
            --tmpForKey2;
            memcpy(&end,(char*)sstable->index+12*tmpForKey2,sizeof (indexPair));
        }
        //tmpForKey2是第一个小于等于key2的位置
        numForCopy=tmpForKey2-tmpForKey1+1;
        memcpy(&start,(char*)sstable->index+12*tmpForKey1,sizeof (indexPair));
        memcpy(&end,(char*)sstable->index+12*(tmpForKey2+1),sizeof (indexPair));
        copyLength=end.offset-start.offset;
        copyResult=new char[sizeof (char)*copyLength+1];
        fin.seekg(start.offset,ios::beg);
        fin.read(copyResult,sizeof (char)*copyLength);
        copyResult[copyLength]='\0';
    }
    //将数据解读出来放入list中
    indexPair pointForKey1;
    memcpy(&pointForKey1,(char*)sstable->index+12*tmpForKey1,sizeof (indexPair));
    for (uint64_t i=1;i<numForCopy;++i){
        indexPair pointForKey2;
        uint64_t singleLength;
        memcpy(&pointForKey2,(char*)sstable->index+12*(tmpForKey1+i),sizeof (indexPair));
        singleLength=pointForKey2.offset-pointForKey1.offset;
        uint64_t realKey=pointForKey1.key;
        char *data=new char[singleLength+1];
        memcpy(data,copyResult+12*(i-1),singleLength);
        data[singleLength]='\0';
        string realData=data;
        delete[] data;
        if (whetherAppend(realKey,ExcludedList)){
            if (realData=="~DELETED~"){
                ExcludedList.push_back(realKey);
            }
            else{
                list.push_back(make_pair(realKey,realData));
                ExcludedList.push_back(realKey);
            }
        }
        pointForKey1=pointForKey2;
        copyLength-=singleLength;
    }
    char* data=new char[copyLength+1];
    memcpy(data,copyResult+12*(numForCopy-1),copyLength);
    data[copyLength]='\0';
    string realData=data;
    delete [] data;
    delete [] copyResult;
    uint64_t realKey=pointForKey1.key;
    if (whetherAppend(realKey,ExcludedList)){
        if (realData=="~DELETED~"){
            ExcludedList.push_back(realKey);
        }
        else{
            list.push_back(make_pair(realKey,realData));
            ExcludedList.push_back(realKey);
        }
    }
}

//用布隆过滤器判断键值是否在表中存在
bool memTable::BFJudge(uint64_t key, char *BF){
    uint32_t hash[4];
    MurmurHash3_x64_128(&key,sizeof (key),1,hash);
    for (int i=0; i<4; ++i){
        hash[i]=hash[i]%10240;
        if (BF[hash[i]]!=1){
            return false;
        }
    }
    return true;
}
