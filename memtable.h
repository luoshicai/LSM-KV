#ifndef MEMTABLE_H
#define MEMTABLE_H
#include <cstdint>
#include <string>
#include <list>
#include <sstream>
#include <fstream>
#include <iostream>
#include <vector>
#include <algorithm>
#include "MurmurHash3.h"
#include "utils.h"
using namespace std;

/* 4.6
  1.persistence尚未实现，需要写析构函数和reset函数
  2.sstable文件尾读取方法待改进
  3.因为在这次写代码时预想的level0是无限大，所以生成目录上也需要改进*/
#define MAXLEVEL 20
#define MAXSIZE  2086880 //2097152-32-10240
enum KVNodeType{
    HEAD=1,
    NORMAL,
    NIL
};

struct KVNode{
    uint64_t key;
    string data;
    KVNodeType type;
    KVNode* next[MAXLEVEL];
    KVNode(int _key,string _s,KVNodeType _type)
        :key(_key),data(_s),type(_type){
        for (int i=0;i<MAXLEVEL;++i){
            next[i]=nullptr;
        }
    }
};

struct indexPair{
    uint64_t key;
    uint32_t offset;
};

struct searchNode{
    uint64_t timeLabel=0;
    uint64_t numOfNode=0;
    uint64_t minKey=~0;
    uint64_t maxKey=0;
    char BF[10240]={0};
    indexPair* index;
    searchNode* next=nullptr;
    string tableName;
    uint32_t tableLevel;
};

//为了归并时的方便创建了两个数据结构，一个存放原归并集的信息，
//一个是先将归并的数据放在一个链表中后统一生成ssTable
struct ForCompaction{
    string path;
    uint32_t indexTmp=0;
    uint64_t nextKey;
    uint64_t numOfNode;
    ifstream fin;
    uint64_t timeLabel;
    uint32_t offset;
    bool surplus=true;
    string tableName;
};

struct MergeNode{
    uint64_t key;
    string data;
    MergeNode* next=nullptr;
};

class memTable
{
private:
    KVNode* head;
    KVNode* nil;
    uint64_t size=0;
    uint64_t baseSize=12; //uint64_t8个byte,offset4个byte
    uint64_t timeLabel=0;
    uint64_t numOfNode=0;
    uint64_t minKey=~0;
    uint64_t maxKey=0;
    searchNode* searchList=nullptr;
    unsigned long long s = 1;
    double my_rand();
    int randomLevel();
public:
    memTable();
    ~memTable();
    void put(uint64_t key, const string &s);  //增
    string get(uint64_t key); //查
    bool del(uint64_t key);   //删
    void reset();
    void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list);
    void createSSTable();  //当跳表中的数据超过2MB时生成sstable
    void clearMemTable();  //清空memTable
    void appendSearchNode(string path);  //写入磁盘后为便于查找，将非数据区放入缓存（一条后生成的sstable在最前面的链表）
    string findSearchNode(searchNode* node,uint64_t key); //二分查找该ssTable中是否含key
    string fetchData(string tableName,uint32_t tableLevel,uint32_t offset,uint32_t length); //取data
    void scanSearchNode(searchNode* sstable,uint64_t key1, uint64_t key2,
                        std::list<std::pair<uint64_t, std::string> > &list,
                        vector<uint64_t> &ExcludedKey); //区间查找ssTable中的数据
    bool whetherAppend(uint64_t key, vector<uint64_t> &ExcludedKey);
    bool BFJudge(uint64_t key,char* BF);
    void compaction(vector<searchNode*> &intersection, uint32_t level1, uint32_t level2, searchNode* parent);
    void Merge_sstable(MergeNode* MerList, string tablePath,
                       uint64_t current_numOfNode, uint64_t current_minKey,
                       uint64_t current_maxKey, uint64_t current_timeLabel,
                       char BF[]);
    void Merge_appendSearchNode(string path);
    void clearMergeList(MergeNode* MergeList);
    void Compaction();   //对于从level1开始的反复确认是否需要compaction
};

#endif // MEMTABLE_H
