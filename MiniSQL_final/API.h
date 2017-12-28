#ifndef API_H
#define API_H

#include "Attribute.h"
#include "Condition.h"
#include "Minisql.h"
#include "IndexInfo.h"
#include <string>
#include <cstring>
#include <vector>
#include <stdio.h>

class CatalogManager;
class RecordManager;
class IndexManager;
class API{
public:
    RecordManager *rm;
    CatalogManager *cm;
    IndexManager *im;
	API(){}
	~API(){}
    
    
    void drop_table(string tableName);
    void create_table(string tableName, vector<Attribute>* attributeVector, string primaryKeyName,int primaryKeyLocation);
    
    void drop_index(string indexName);
	void create_index(string indexName, string tableName, string attributeName);
  
    void show_record(string tableName, vector<string>* attributeNameVector = NULL);
	void show_record(string tableName,  vector<string>* attributeNameVector, vector<Condition>* conditionVector);
	void insert_record(string tableName,vector<string>* recordContent);

	void delete_record(string tableName);
    void delete_record(string tableName, vector<Condition>* conditionVector);
	int get_records_number(string tableName);
	int get_records_size(string tableName);

	int get_type_size(int type);
    void get_all_index_addr_info(vector<index_info> *indexNameVector);
    
    int get_attr_name(string tableName, vector<string>* attributeNameVector);
	int get_attr_type(string tableName, vector<string>* attributeTypeVector);
    int get_attribute(string tableName, vector<Attribute>* attributeVector);

    void insert_index_value(string indexName, string value, int blockOffset);
    void insert_index(string indexName, char* value, int type, int blockOffset);
    void delete_record_index(char* recordBegin,int recordSize, vector<Attribute>* attributeVector, int blockOffset);
    void insert_record_index(char* recordBegin,int recordSize, vector<Attribute>* attributeVector, int blockOffset);
    
private:
    int table_exist_f(string tableName);
    int get_index_name_list(string tableName, vector<string>* indexNameVector);
    void print_table_attr(vector<string>* name);
};

struct int_t{
	int value;
};

struct float_t{
	float value;
};
#endif