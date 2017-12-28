#ifndef minisql_Attribute_h
#define minisql_Attribute_h

#include <string>
#include <iostream>
using namespace std;

class Attribute
{
public:
    string name;
    int type;                  //the type of the attribute, -1: float, 0: int,
    bool if_unique;
    string index;         // default: ""
    Attribute(string n, int t, bool i);
    
public:
    int static const TYPE_FLOAT = -1;
    int static const TYPE_INT = 0;
    string get_index_name(){return index;}
    
    
};

#endif
