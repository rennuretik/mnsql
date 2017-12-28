#ifndef minisql_IndexInfo_h
#define minisql_IndexInfo_h

#include <string>
using namespace std;

class index_info
{
public:
	index_info(string i,string t,string a,int ty)
    {index_name = i;table_name = t;Attribute = a;type = ty;}
    string index_name;
    string table_name;
    string Attribute;
    int type;
};

#endif