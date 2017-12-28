#include "API.h"
#include "RecordManager.h"
#include "CatalogManager.h"
#include "IndexManager.h"

#define UNKNOWN_FILE 8
#define TABLE_FILE 9
#define INDEX_FILE 10

CatalogManager *cm;
IndexManager* im;

void API::drop_table(string tableName)
{
    if (!table_exist_f(tableName)) return;
    
    vector<string> indexNameVector;
    
    //get all index in the table, and drop them all
    get_index_name_list(tableName, &indexNameVector);
    for (int i = 0; i < indexNameVector.size(); i++)
    {
        printf("%s", indexNameVector[i].c_str());
        
        drop_index(indexNameVector[i]);
    }
    
    //delete a table file
    if(rm->drop_table(tableName))
    {
        //delete a table information
        cm->drop_table(tableName);
        printf("Drop table %s successfully\n", tableName.c_str());
    }
}


void API::drop_index(string indexName)
{
    if (cm->find_index(indexName) != INDEX_FILE)
    {
        printf("There is no index %s \n", indexName.c_str());
        return;
    }
    
    //delete a index file
    if (rm->drop_index(indexName))
    {
        
        //get type of index
        int indexType = cm->get_index_type(indexName);
        if (indexType == -2)
        {
            printf("error\n");
            return;
        }
        
        //delete a index information
        cm->drop_index(indexName);
        
        //delete a index tree
        im->drop_index(rm->get_file_name_index(indexName), indexType);
        printf("Drop index %s successfully\n", indexName.c_str());
    }
}


void API::create_index(string indexName, string tableName, string attributeName)
{
    if (cm->find_index(indexName) == INDEX_FILE)
    {
        cout << "There is index " << indexName << " already" << endl;
        return;
    }
    
    if (!table_exist_f(tableName)) return;
    
    vector<Attribute> attributeVector;
    cm->get_attribute(tableName, &attributeVector);
    int i;
    int type = 0;
    for (i = 0; i < attributeVector.size(); i++)
    {
        if (attributeName == attributeVector[i].name)
        {
            if (!attributeVector[i].if_unique)
            {
                cout << "the attribute is not unique" << endl;
                
                return;
            }
            type = attributeVector[i].type;
            break;
        }
    }
    
    if (i == attributeVector.size())
    {
        cout << "there is not this attribute in the table" << endl;
        return;
    }
    
     //RecordManager to create a index file
    if (rm->create_index(indexName))
    {
        //CatalogManager to add a index information
        cm->add_index(indexName, tableName, attributeName, type);
        
        //get type of index
        int indexType = cm->get_index_type(indexName);
        if (indexType == -2)
        {
            cout << "error";
            return;
        }
        
        //indexManager to create a index tress
        im->create_index(rm->get_file_name_index(indexName), indexType);
        
        //recordManager insert already record to index
        rm->all_index_records_already_insert(tableName, indexName);
        printf("Create index %s successfully\n", indexName.c_str());
    }
    else
    {
        cout << "Create index " << indexName << " failed." << endl;
    }
}


void API::create_table(string tableName, vector<Attribute>* attributeVector, string primaryKeyName,int primaryKeyLocation)
{

    
    if(cm->find_table(tableName) == TABLE_FILE)
    {
        cout << "There is a table " << tableName << " already" << endl;
        return;
    }
    
    //RecordManager to create a table file
    if(rm->create_table(tableName))
    {
        //CatalogManager to create a table information
        cm->add_table(tableName, attributeVector, primaryKeyName, primaryKeyLocation);
   
        printf("Create table %s successfully\n", tableName.c_str());
    }
    
    if (primaryKeyName != "")
    {
        //get a primary key
        string indexName = "primary_" + primaryKeyName;
        create_index(indexName, tableName, primaryKeyName);
    }
}


void API::show_record(string tableName, vector<string>* attributeNameVector)
{
    vector<Condition> conditionVector;
    show_record(tableName, attributeNameVector, &conditionVector);
}


void API::show_record(string tableName, vector<string>* attributeNameVector, vector<Condition>* conditionVector)
{
    if (cm->find_table(tableName) == TABLE_FILE)
    {
        int num = 0;
        vector<Attribute> attributeVector;
        get_attribute(tableName, &attributeVector);
        
        vector<string> allAttributeName;
        if (attributeNameVector == NULL) {
            for (Attribute attribute : attributeVector)
            {
                allAttributeName.insert(allAttributeName.end(), attribute.name);
            }
            
            attributeNameVector = &allAttributeName;
        }
        
        //print attribute name you want to show
        print_table_attr(attributeNameVector);
        
        for (string name : (*attributeNameVector))
        {
            int i = 0;
            for (i = 0; i < attributeVector.size(); i++)
            {
                if (attributeVector[i].name == name)
                {
                    break;
                }
            }
            
            if (i == attributeVector.size())
            {
                cout << "the attribute which you want to print is not exist in the table" << endl;
                return;
            }
        }
        
        int blockOffset = -1;
        if (conditionVector != NULL)
        {
            for (Condition condition : *conditionVector)
            {
                int i = 0;
                for (i = 0; i < attributeVector.size(); i++)
                {
                    if (attributeVector[i].name == condition.attributeName)
                    {
                        if (condition.operate == Condition::OPERATOR_EQUAL && attributeVector[i].index != "")
                        {
                                blockOffset = im->search_index(rm->get_file_name_index(attributeVector[i].index), condition.value, attributeVector[i].type);
                        }
                        break;
                    }
                }
                
                if (i == attributeVector.size())
                {
                    cout << "the attribute is not exist in the table" << endl;
                    return;
                }
            }
        }
        
        if (blockOffset == -1)
        {
            num = rm->show_all_records(tableName, attributeNameVector,conditionVector);
        }
        else
        {
            num = rm->show_block_record(tableName, attributeNameVector, conditionVector, blockOffset);
        }
        
        printf("%d records selected\n", num);
    }
    else
    {
        cout << "There is no table " << tableName << endl;
    }
}


void API::insert_record(string tableName, vector<string>* recordContent)
{
    if (!table_exist_f(tableName)) return;
    
    string indexName;
    
    //deal if the record could be insert (if index is exist)
    vector<Attribute> attributeVector;
    
    vector<Condition> conditionVector;
    
    get_attribute(tableName, &attributeVector);
    for (int i = 0 ; i < attributeVector.size(); i++)   //这个table所有的attributes
    {
        indexName = attributeVector[i].get_index_name();
        if (indexName != "")
        {
            //if the attribute has a index
            int blockoffest = im->search_index(rm->get_file_name_index(indexName), (*recordContent)[i], attributeVector[i].type);
            
            if (blockoffest != -1)
            {
                //if the value has exist in index tree then fail to insert the record
                cout << "Insert failed because index value exists" << endl;
                return;
            }
        }
        else if (attributeVector[i].if_unique)
        {
            //if the attribute is unique but not index
            Condition condition(attributeVector[i].name, (*recordContent)[i], Condition::OPERATOR_EQUAL);
            conditionVector.insert(conditionVector.end(), condition);
        }
    }
    
    if (conditionVector.size() > 0)
    {
        for (int i = 0; i < conditionVector.size(); i++) {
            vector<Condition> conditionTmp;
            conditionTmp.insert(conditionTmp.begin(), conditionVector[i]);
            
            int recordConflictNum =  rm->find_all_record(tableName, &conditionTmp);
            if (recordConflictNum > 0) {
                cout << "Insert failed because unique value exist" << endl;
                return;
            }

        }
    }
    
    char recordString[2000];
    memset(recordString, 0, 2000);
    
    //CatalogManager to get the record string
    cm->get_record_string(tableName, recordContent, recordString);
    
    //RecordManager to insert the record into file; and get the position of block being insert
    int recordSize = cm->cal_length(tableName);
    int blockOffset = rm->insert_record(tableName, recordString, recordSize);
    
    if(blockOffset >= 0)
    {
        insert_record_index(recordString, recordSize, &attributeVector, blockOffset);
        cm->insert_record(tableName, 1);
        printf("insert record into table %s successfully\n", tableName.c_str());
    }
    else
    {
        cout << "insert record into table " << tableName << " fail" << endl;
    }
}


void API::delete_record(string tableName)
{
    vector<Condition> conditionVector;
    delete_record(tableName, &conditionVector);
}


void API::delete_record(string tableName, vector<Condition>* conditionVector)
{
    if (!table_exist_f(tableName)) return;
    
    int num = 0;
    vector<Attribute> attributeVector;
    get_attribute(tableName, &attributeVector);

    int blockOffset = -1;
    if (conditionVector != NULL)
    {
        for (Condition condition : *conditionVector)
        {
            if (condition.operate == Condition::OPERATOR_EQUAL)
            {
                for (Attribute attribute : attributeVector)
                {
                    if (attribute.index != "" && attribute.name == condition.attributeName)
                    {
                        blockOffset = im->search_index(rm->get_file_name_index(attribute.index), condition.value, attribute.type);
                    }
                }
            }
        }
    }

    
    if (blockOffset == -1)
    {
        //if we con't find the block by index,we need to find all block
        num = rm->delete_all_record(tableName, conditionVector);
    }
    else
    {
        //find the block bty index,search in the block
        num = rm->delete_block_record(tableName, conditionVector, blockOffset);
    }
    
    //delete the number of record in in the table
    cm->delete_value(tableName, num);
    printf("delete %d record in table %s\n", num, tableName.c_str());
}


int API::get_records_number(string tableName)
{
    if (!table_exist_f(tableName)) return 0;
    
    return cm->get_record_num(tableName);
}


int API::get_records_size(string tableName)
{
    if (!table_exist_f(tableName)) return 0;
    
    return cm->cal_length(tableName);
}


int API::get_type_size(int type)
{
    return cm->cal_length2(type);
}


int API::get_index_name_list(string tableName, vector<string>* indexNameVector)
{
    if (!table_exist_f(tableName)) {
        return 0;
    }
    return cm->get_index_name_list(tableName, indexNameVector);
}


void API::get_all_index_addr_info(vector<index_info> *indexNameVector)
{
    cm->get_index_all(indexNameVector);
    for (int i = 0; i < (*indexNameVector).size(); i++)
    {
        (*indexNameVector)[i].index_name = rm->get_file_name_index((*indexNameVector)[i].index_name);
    }
}


int API::get_attribute(string tableName, vector<Attribute>* attributeVector)
{
    if (!table_exist_f(tableName)) {
        return 0;
    }
    return cm->get_attribute(tableName, attributeVector);
}


void API::insert_record_index(char* recordBegin,int recordSize, vector<Attribute>* attributeVector,  int blockOffset)
{
    char* contentBegin = recordBegin;
    for (int i = 0; i < (*attributeVector).size() ; i++)
    {
        int type = (*attributeVector)[i].type;
        int typeSize = get_type_size(type);
        if ((*attributeVector)[i].index != "")
        {
            insert_index((*attributeVector)[i].index, contentBegin, type, blockOffset);
        }
        
        contentBegin += typeSize;
    }
}


void API::insert_index(string indexName, char* contentBegin, int type, int blockOffset)
{
    string content= "";
    stringstream tmp;
    //if the attribute has index
    ///这里传*attributeVector)[i].index这个index的名字, blockOffset,还有值
    if (type == Attribute::TYPE_INT)
    {
        int value = *((int*)contentBegin);
        tmp << value;
    }
    else if (type == Attribute::TYPE_FLOAT)
    {
        float value = *((float* )contentBegin);
        tmp << value;
    }
    else
    {
        char value[255];
        memset(value, 0, 255);
        memcpy(value, contentBegin, sizeof(type));
        string stringTmp = value;
        tmp << stringTmp;
    }
    tmp >> content;
    im->insert_index(rm->get_file_name_index(indexName), content, blockOffset, type);
}


void API::delete_record_index(char* recordBegin,int recordSize, vector<Attribute>* attributeVector, int blockOffset)
{
    char* contentBegin = recordBegin;
    for (int i = 0; i < (*attributeVector).size() ; i++)
    {
        int type = (*attributeVector)[i].type;
        int typeSize = get_type_size(type);
        
        string content= "";
        stringstream tmp;
        
        if ((*attributeVector)[i].index != "")
        {
            //if the attribute has index
            ///这里传*attributeVector)[i].index这个index的名字, blockOffset,还有值
            if (type == Attribute::TYPE_INT)
            {
                int value = *((int*)contentBegin);
                tmp << value;
            }
            else if (type == Attribute::TYPE_FLOAT)
            {
                float value = *((float* )contentBegin);
                tmp << value;
            }
            else
            {
                char value[255];
                memset(value, 0, 255);
                memcpy(value, contentBegin, sizeof(type));
                string stringTmp = value;
                tmp << stringTmp;
            }
            
            tmp >> content;
            im->delete_index_by_key(rm->get_file_name_index((*attributeVector)[i].index), content, type);

        }
        contentBegin += typeSize;
    }

}


int API::table_exist_f(string tableName)
{
    if (cm->find_table(tableName) != TABLE_FILE)
    {
        cout << "There is no table " << tableName << endl;
        return 0;
    }
    else
    {
        return 1;
    }
}


void API::print_table_attr(vector<string>* attributeNameVector)
{
    int i = 0;
    for ( i = 0; i < (*attributeNameVector).size(); i++)
    {
        printf("%s ", (*attributeNameVector)[i].c_str());
    }
    if (i != 0)
        printf("\n");
}

