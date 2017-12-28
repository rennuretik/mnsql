#ifndef __Minisql__IndexManager__
#define __Minisql__IndexManager__

#include <stdio.h>
#include <map>
#include <string>
#include <sstream>
#include "Attribute.h"
#include "BPlusTree.h"

class API;

class IndexManager{
private:
    typedef map<string,BPlusTree<int> *> intMap;
    typedef map<string,BPlusTree<string> *> stringMap;
    typedef map<string,BPlusTree<float> *> floatMap;

    int static const TYPE_FLOAT = Attribute::TYPE_FLOAT;
    int static const TYPE_INT = Attribute::TYPE_INT;
    
    API *api;
    
    intMap indexIntMap;
    stringMap indexStringMap;
    floatMap indexFloatMap;
    struct keyTmp{
        int intTmp;
        float floatTmp;
        string stringTmp;
    };      // help to convert to specfied type
    struct keyTmp kt;
    
    int get_degree(int type);

    int get_key_size(int type);
   
    void set_key(int type,string key);
    
    
public:
    IndexManager(API *api);
    ~IndexManager();

    void create_index(string filePath,int type);
    
    void drop_index(string filePath,int type);
    
    offset_num search_index(string filePath,string key,int type);

    void insert_index(string filePath,string key,offset_num blockOffset,int type);
    
    void delete_index_by_key(string filePath,string key,int type);
};



#endif
