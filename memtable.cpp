#include "memtable.h"


bool sortByTimeLabel(searchNode *Node1,searchNode *Node2);
bool sortByMinKey(searchNode *Node1,searchNode *Node2);

memTable::memTable()
{
    head =new KVNode(0,"head",HEAD);
    nil =new KVNode(~0,"nil",NIL);
    for (int i=0;i<MAXLEVEL;++i){
        head->next[i]=nil;
    }
    //读取内存
    if (utils::dirExists(".\\data\\initial")){
        uint64_t read_numOfNode = 0;
        ifstream fin(".\\data\\initial\\initialize.sst");
        fin.read((char*)&timeLabel, sizeof (uint64_t));
        fin.read((char*)&read_numOfNode, sizeof (uint64_t));
        indexPair* index = new indexPair[read_numOfNode];
        fin.read((char*)index,12*read_numOfNode);
        indexPair index_start;
        indexPair index_end;
        memcpy(&index_start,(char*)index,sizeof (indexPair));
        for (uint64_t i=1; i<read_numOfNode; ++i){
            //读取内存
            memcpy(&index_end,(char*)index+12*i,sizeof (indexPair));
            uint32_t length=index_end.offset-index_start.offset;
            char* read_data= new char[length+1];
            fin.read(read_data,length);
            read_data[length]='\0';
            string data = read_data;
            delete []read_data;
            put(index->key,data);
            index_start.key=index_end.key;
            index_start.offset=index_end.offset;
        }
        uint32_t length = 70000;
        char* read_data = new char[length];
        fin.read(read_data,length);
        uint64_t account=fin.gcount();
        read_data[account]='\0';
        string data = read_data;
        delete []read_data;
        put(index_start.key,data);
        fin.close();
        //删除initial目录下的文件及目录本身
        utils::rmfile(".\\data\\initial\\initialize.sst");
        utils::rmdir(".\\data\\initial");
    }
    //将已有的sstable读入内存
    uint32_t read_level = 0;
    string sourcePath=".\\data\\level";
    string dir_path;
    string file_path;
    uint32_t dir_size;
    dir_path = sourcePath + to_string(read_level);
    while (utils::dirExists(dir_path)) {
        vector<string> dir_file;
        vector<searchNode*> file_set;
        dir_size = utils::scanDir(dir_path,dir_file);
        for (uint32_t i=0; i<dir_size; ++i){
            searchNode* dir_searchNode = new searchNode;
            file_path = dir_path + "\\" + dir_file[i];
            ifstream fin(file_path,ios::binary);
            fin.read((char*)&dir_searchNode->timeLabel,sizeof (uint64_t));
            fin.read((char*)&dir_searchNode->numOfNode,sizeof (uint64_t));
            fin.read((char*)&dir_searchNode->minKey,sizeof (uint64_t));
            fin.read((char*)&dir_searchNode->maxKey,sizeof (uint64_t));
            fin.read((char*)dir_searchNode->BF,10240);
            dir_searchNode->index = new indexPair[dir_searchNode->numOfNode];
            fin.read((char*)dir_searchNode->index,12*dir_searchNode->numOfNode);
            dir_searchNode->tableName=dir_file[i];
            dir_searchNode->tableLevel=read_level;
            file_set.push_back(dir_searchNode);
        }
        //在同一级中排序，大的在前，小的在后
            //level0 中按照timeLabel排，因为可能又交叉
        if (read_level==0){
           sort(file_set.begin(),file_set.end(),sortByTimeLabel);
           for (uint32_t n=0; n<dir_size; ++n){
               file_set[n]->next=searchList;
               searchList=file_set[n];
           }
        }
        else {
            searchNode* level_head = nullptr;
            searchNode* read_tail=searchList;
            sort(file_set.begin(),file_set.end(),sortByTimeLabel);
            for (uint32_t n = 0; n<dir_size; ++n){
                file_set[n]->next = level_head;
                level_head = file_set[n];
            }
            //接入主链表
            if (searchList==nullptr){
                searchList = level_head;
            }
            else{
                while (read_tail->next!=nullptr) {
                    read_tail=read_tail->next;
                }
                read_tail->next = level_head;
            }
        }
        //更新level
        read_level++;
        dir_path = sourcePath + to_string(read_level);
        dir_file.clear();
    }

}

double memTable::my_rand(){
    s = (16807 * s) % 2147483647ULL;
    return (s + 0.0) / 2147483647ULL;
}

int memTable::randomLevel(){
    int result = 1;
    while (result < MAXLEVEL && my_rand() < 0.5)
    {
        ++result;
    }
    return result;
}

void memTable::put(uint64_t key, const std::string &s){
    //检查是否超过2MB，如果超过，则生成sstable
    if (size+baseSize+s.length()>=MAXSIZE){
        this->createSSTable();
    }
    //正常插入
    KVNode* update[MAXLEVEL];
    KVNode* tmp=head;
    for (int i=MAXLEVEL-1;i>=0;--i){
        while(key>tmp->next[i]->key){
            tmp=tmp->next[i];
        }
        if (key==tmp->next[i]->key){
            size-=tmp->next[i]->data.length();
            size+=s.length();
            tmp=tmp->next[i];
            tmp->data=s;
            return;
        }
        if (key<tmp->next[i]->key){
            update[i]=tmp;
        }
    }
    //插入新结点
    int level=randomLevel();
    KVNode* newNode = new KVNode(key,s,KVNodeType::NORMAL);
    for (int i=0;i<level;++i){
        newNode->next[i]=update[i]->next[i];
        update[i]->next[i]=newNode;
    }
    size=size+baseSize+s.length();
    if (minKey>key)
        minKey=key;
    if (maxKey<key)
        maxKey=key;
    numOfNode++;
    return;
}


string memTable::get(uint64_t key){
    //在内存中查找
    KVNode* tmp=head;
    for (int i=MAXLEVEL-1;i>=0;--i){
        while (key>tmp->next[i]->key) {
            tmp=tmp->next[i];
        }
        if (key==tmp->next[i]->key){
            if (tmp->next[i]->data!="~DELETED~")
                return tmp->next[i]->data;
            else
                return "";
        }
    }
    //在ssTable中查找
    searchNode* searchTmp=searchList;
    while (searchTmp!=nullptr){
        if (searchTmp->minKey<=key&&searchTmp->maxKey>=key&&BFJudge(key,searchTmp->BF)){
            string result=this->findSearchNode(searchTmp,key);
            if (result=="~DELETED~"){
                return "";
            }
            else if (result!=""){
                return result;
            }
        }
        searchTmp=searchTmp->next;
    }
    return "";
}

bool memTable::del(uint64_t key){
    //在内存中查找
    KVNode* tmp=head;
    for (int i=MAXLEVEL-1;i>=0;--i){
        while (key>tmp->next[i]->key) {
            tmp=tmp->next[i];
        }
        if (key==tmp->next[i]->key){
            if (tmp->next[i]->data!="~DELETED~"){
                size-=tmp->next[i]->data.length();
                size+=9;
                tmp->next[i]->data="~DELETED~";
                return true;
            }
            else{
                return false;
            }
        }
    }
    //在sstable中查找
    searchNode* searchTmp=searchList;
    while (searchTmp!=nullptr){
        if (searchTmp->minKey<=key&&searchTmp->maxKey>=key&&BFJudge(key,searchTmp->BF)){
            string result=this->findSearchNode(searchTmp,key);
            if (result=="~DELETED~"){
                return false;
            }
            else if (result!=""){
                this->put(key,"~DELETED~");
                return true;
            }
        }
        searchTmp=searchTmp->next;
    }
    return false;
}

//pair排序用
bool operator<(pair<uint64_t,string> a,pair<uint64_t,string> b){
    return a.first<b.first;
}

void memTable::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list){
    vector<uint64_t> ExcludedList;
    //在内存中查找
    KVNode* tmp=head;
    uint64_t maxNum=0;
    uint64_t minNum=~0;
    for (int i=MAXLEVEL-1;i>=0;--i){
        while (key1>tmp->next[i]->key) {
            tmp=tmp->next[i];
        }
    }
    while (tmp->next[0]->key<=key2) {
       uint64_t a = tmp->next[0]->key;
       string b = tmp->next[0]->data;
       if (b=="~DELETED~"){
           ExcludedList.push_back(a);
       }
       else if (this->whetherAppend(a,ExcludedList)){
           list.push_back(make_pair(a,b));
           ExcludedList.push_back(a);
           if (a>maxNum){
               maxNum=a;
           }
           else if (a<minNum){
               minNum=a;
           }
       }
       tmp=tmp->next[0];
    }
    //在sstable中查找
    searchNode* searchTmp=searchList;
    while (searchTmp!=nullptr){
        if (key2>=searchTmp->minKey&&key1<=searchTmp->maxKey){
            this->scanSearchNode(searchTmp,key1,key2,list,ExcludedList);
        }
        searchTmp=searchTmp->next;
    }

    //排序
    list.sort();
    return;
}

void memTable::clearMemTable(){
    while (head->next[0]->type!=KVNodeType::NIL) {
        KVNode* tmp = head->next[0];
        head->next[0]=tmp->next[0];
        delete tmp;
    }
    for (int i=0;i<MAXLEVEL;++i){
        head->next[i]=nil;
    }
    numOfNode=0;
    minKey=~0;
    maxKey=0;
    size=0;
    return;
}

//因为是按照后生成的ssTable到先生成的ssTable看的，所以如果先插入再删除再插入不会二次计算。
bool memTable::whetherAppend(uint64_t key, vector<uint64_t> &ExcludedList){
      for (uint64_t i=0;i<ExcludedList.size();++i){
          if (ExcludedList[i]==key){
              return false;
          }
      }
      return true;
}

void memTable::reset(){
    uint32_t current_Level=0;
    string source_path=".\\data\\level";
    string current_dir=source_path + to_string(current_Level);
    string filePath;
    searchNode* search_tmp=searchList;
    //删除所有的sstable
    while (search_tmp!=nullptr) {
        filePath = source_path + to_string(search_tmp->tableLevel) + "\\" + search_tmp->tableName;
        utils::rmfile(filePath.c_str());
        search_tmp=search_tmp->next;
    }
    //删除所有的目录
    while (utils::dirExists(current_dir)) {
        utils::rmdir(current_dir.c_str());
        //更新参数
        current_Level++;
        current_dir=source_path + to_string(current_Level);
    }
    //删除缓存
    clearMemTable();
    timeLabel=0;
}


/*
 * 生成initial目录，在data目录下，其下有一个特殊的sstable名字叫initialize.sst
 * 它有一个40个byte的header，分别是 size,timeLabel,numOfNode,minKey,maxKey
 * 然后是索引区和数据区，存放关闭前存放在内存中的数据
 */
memTable::~memTable(){
    utils::_mkdir(".\\data\\initial");
    ofstream fout(".\\data\\initial\\initialize.sst",ios::binary);
    //写入header
    fout.write((char*)&timeLabel, sizeof (uint64_t));
    fout.write((char*)&numOfNode, sizeof (uint64_t));
    //写入index和数据
    uint32_t itmp=16;  //itmp记录下一个索引写入的位置
    uint32_t dtmp=16+numOfNode*12; //dtmp记录下一个数据写入的位置
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
    fout.close();
    clearMemTable();
}

bool sortByTimeLabel(searchNode *Node1, searchNode *Node2){
    return Node1->timeLabel<Node2->timeLabel;
}

bool sortByMinKey(searchNode *Node1, searchNode *Node2){
    return (Node1->minKey<Node2->minKey);
}
