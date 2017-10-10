#ifndef _QFS_METADATA_BASE_HH_
#define _QFS_METADATA_BASE_HH_

#include <arpa/inet.h>
#include <condition_variable>
#include <thread>
#include <set>
#include <list>
#include "MetadataBase.hh"

using namespace std;

class QFS_MetadataBase : public MetadataBase{
  private:
    map<string, list<pair<unsigned int, string>>> _blk2Stripe; //list
    map<string, unsigned int> _blkIdInStripe;
    map<string, map<string, int>> _coefficient;

    condition_variable _blkLock;
    mutex _blkLock_m;
    thread _metadataThread;
    unsigned int _locIP;
    void ProcessMetadata();
  public:
    QFS_MetadataBase(Config* conf, RSUtil* rsu);
    ~QFS_MetadataBase(){};
    // <ip, blk> pair
    vector<pair<unsigned int, string>> getStripeBlks(const string& blkName, unsigned int requestorIP);
    vector<pair<unsigned int, string>> getRRStripeBlks(const string& blkName, unsigned int requestorIP);
    map<string, int> getCoefficient(const string& blkName);
};

#endif

