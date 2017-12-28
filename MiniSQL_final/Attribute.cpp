#include "Attribute.h"

Attribute::Attribute(string n, int t, bool i) {
    name = n;
    type = t;
    if_unique = i;
    index = "";
}
