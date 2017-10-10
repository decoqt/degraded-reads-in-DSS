#ifndef _PPR_PULL_DR_WORKER_HH_
#define _PPR_PULL_DR_WORKER_HH_

#include <map>

#include "DRWorker.hh"

class PPRPullDRWorker : public DRWorker {
    void requestorCompletion(string&, vector<unsigned int>&);
    void nonRequestorCompletion(string&, redisContext*, vector<unsigned int>&);
    void readWorker(const string&);
    void sendWorker(const string&, redisContext* rc1);

    vector<unsigned int> getChildrenIndices(int, unsigned int);
    unsigned int getID(int k) const;
    unsigned int PPRnextIP(int id, unsigned int ecK) const;

    /* OLD */
    void nonRequestorCompletion(string&, redisContext*, redisContext*);
  public : 
    void doProcess();
    PPRPullDRWorker(Config* conf) : DRWorker(conf){;};
};

#endif //_PPR_PULL_DR_WORKER_HH_

