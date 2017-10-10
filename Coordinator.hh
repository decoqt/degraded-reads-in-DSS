#ifndef _COORDINATOR_HH_
#define _COORDINATOR_HH_

#include <algorithm>
#include <fstream>
#include <iostream>
#include <map>
#include <thread>
#include <unordered_map>

#include <arpa/inet.h>
#include <hiredis/hiredis.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include "Config.hh"
#include "MetadataBase.hh"
#include "HDFS_MetadataBase.hh"
#include "PathSelection.hh"
#include "QFS_MetadataBase.hh"
#include "RSUtil.hh"

#define COORDINATOR_DEBUG true
#define COMMAND_MAX_LENGTH 256
#define FILENAME_MAX_LENGTH 256

using namespace std;

/**
 * ONE COORDINATOR to rule them ALL!!!
 */
class Coordinator {
  protected:
    Config* _conf;
    RSUtil* _rsUtil;
    size_t _handlerThreadNum;
    size_t _distributorThreadNum;
    vector<thread> _distThrds;
    vector<thread> _handlerThrds;

    int _slaveCnt;
    int _ecK;
    int _ecN;
    int _ecM;
    int _coefficient;
    int* _rsEncMat;

    string _rsConfigFile;

    MetadataBase* _metadataBase;
    
    /** 
     * mutex and conditional variables for intercommunication between handler and distributor
     */
    // <ip, <redisList of cmdDistributor, redisContext>>
    vector<pair<unsigned int, pair<string, redisContext*>>> _ip2Ctx;
    redisContext* _selfCtx;
   
    map<unsigned int, int> _ip2idx;
    vector<vector<double>> _linkWeight;
    PathSelection* _pathSelection;
 
    void init();
    int searchCtx(vector<pair<unsigned int, pair<string, redisContext*>>>&, unsigned int, size_t, size_t);
    redisContext* initCtx(unsigned int);

    void cmdDistributor(int id, int total);

    virtual void requestHandler();
    string ip2Str(unsigned int) const;

    int isRequestorHolder(vector<pair<unsigned int, string>>&, unsigned int) const;

  public:
    // just init redis contexts
    Coordinator(Config*);
    void doProcess();
};

#endif //_DR_COORDINATOR_HH_

