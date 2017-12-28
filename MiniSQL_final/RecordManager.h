#ifndef RECORDMANAGER_H
#define RECORDMANAGER_H
#include "Condition.h"
#include "Attribute.h"
#include "RecordManager.h"
#include "BufferManager.h"
#include "Minisql.h"
#include <string>
#include <vector>
using namespace std;
class API;
class RecordManager{
public:
	RecordManager(){}
    BufferManager bm;
    API *api;
    
    int create_table(string tableName);
    int drop_table(string tableName);
    
    int drop_index(string indexName);
    int create_index(string indexName);
    
    int insert_record(string tableName, char* record, int recordSize);
    
    int show_all_records(string tableName, vector<string>* attributeNameVector, vector<Condition>* conditionVector);
    int show_block_record(string tableName, vector<string>* attributeNameVector, vector<Condition>* conditionVector, int blockOffset);
    
    int find_all_record(string tableName, vector<Condition>* conditionVector);
    
    int delete_all_record(string tableName, vector<Condition>* conditionVector);
    int delete_block_record(string tableName,  vector<Condition>* conditionVector, int blockOffset);
    
    int all_index_records_already_insert(string tableName,string indexName);
    
    string get_file_name_table(string tableName);
    string get_file_name_index(string indexName);
private:
    int show_block_record(string tableName, vector<string>* attributeNameVector, vector<Condition>* conditionVector, blockNode* block);
    int find_block_record(string tableName, vector<Condition>* conditionVector, blockNode* block);
    int delete_block_record(string tableName,  vector<Condition>* conditionVector, blockNode* block);
    int index_block_record_already_insert(string tableName,string indexName, blockNode* block);
    
    bool fit_record_condition(char* recordBegin,int recordSize, vector<Attribute>* attributeVector,vector<Condition>* conditionVector);
    void print_record(char* recordBegin, int recordSize, vector<Attribute>* attributeVector, vector<string> *attributeNameVector);
    bool fit_content_condition(char* content, int type, Condition* condition);
    void print_content(char * content, int type);
};
#endif