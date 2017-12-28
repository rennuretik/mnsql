#ifndef CATALOGMANAGER_H
#define CATALOGMANAGER_H

#include <string>
#include <vector>
#include "Attribute.h"
#include "BufferManager.h"
#include "IndexInfo.h"
using namespace std;

class CatalogManager
{
public:
    BufferManager bm;
    CatalogManager();
    virtual ~CatalogManager();
    
    int add_index(string indexName,string tableName,string attributeName,int type);
    int revoke_index_on_attr(string tableName,string AttributeName,string indexName);
    int find_table(string tableName);
    int find_index(string indexName);
    int drop_table(string tableName);
    int drop_index(string index);
    
    int delete_value(string tableName, int deleteNum);      // delete the number of record
    int insert_record(string tableName, int recordNum);  
    int get_record_num(string tableName);
    
    int get_index_name_list(string tableName, vector<string>* indexNameVector);
    int get_index_all(vector<index_info> * indexs);
    int set_index_on_attr(string tableName,string AttributeName,string indexName);
    int add_table(string tableName, vector<Attribute>* attributeVector, string primaryKeyName ,int primaryKeyLocation );
    int get_index_type(string indexName);
    int get_attribute(string tableName, vector<Attribute>* attributeVector);
    int cal_length(string tableName);
    int cal_length2(int type);
    void get_record_string(string tableName, vector<string>* recordContent, char* recordResult);
};




#endif