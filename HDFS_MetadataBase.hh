#ifndef _HDFS_METADATA_BASE_HH_
#define _HDFS_METADATA_BASE_HH_

#include <set>
#include "MetadataBase.hh"

using namespace std;

class HDFS_MetadataBase : public MetadataBase{
  private: 
    map<string, set<pair<unsigned int, string>>> _blk2Stripe;
    map<string, unsigned int> _blkIdInStripe;
    map<string, map<string, int>> _coefficient;
  public:
    HDFS_MetadataBase(Config* conf, RSUtil* rsu);
    // <ip, blk> pair
    vector<pair<unsigned int, string>> getStripeBlks(const string& blkName, unsigned int requestorIP);
    map<string, int> getCoefficient(const string& blkName);
};

#endif

