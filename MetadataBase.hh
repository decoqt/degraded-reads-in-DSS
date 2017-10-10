#ifndef _METADATA_BASE_HH_
#define _METADATA_BASE_HH_

#include <fstream>
#include <iostream>
#include <map>
#include <string>
#include <streambuf>
#include <vector>
#include <unordered_map>

#include <dirent.h>

#include "Config.hh"
#include "RSUtil.hh"
#include "Util/hiredis.h"

#define METADATA_BASE_DEBUG true

using namespace std;

class MetadataBase {
  protected: 
    Config* _conf;
    RSUtil* _rsUtil;
  public:
    MetadataBase(Config* conf, RSUtil* rsu) :
      _conf(conf),
      _rsUtil(rsu){};
    ~MetadataBase(){};
    // <ip, blk> pair
    virtual vector<pair<unsigned int, string>> getStripeBlks(const string& blkName, unsigned int requestorIP) = 0;
    virtual vector<pair<unsigned int, string>> getRRStripeBlks(const string& blkName, unsigned int requestorIP) = 0;
    virtual map<string, int> getCoefficient(const string& blkName) = 0;
};

#endif

