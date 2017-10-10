#ifndef _PPR_DR_WORKER_HH_
#define _PPR_DR_WORKER_HH_

#include "DRWorker.hh"

class PPRDRWorker : public DRWorker {
    void requestorCompletion(string&, vector<unsigned int>&);
    void nonRequestorCompletion(string&, redisContext*, vector<unsigned int>&);
    void readWorker(const string&);
    void sendWorker(const string&, redisContext* rc1);

    vector<unsigned int> getChildrenIndices(int, unsigned int);
    unsigned int getID(int k) const;

    /* OLD */
    void nonRequestorCompletion(string&, redisContext*, redisContext*);
  public : 
    void doProcess();
    PPRDRWorker(Config* conf) : DRWorker(conf){;};
};

#endif //_PPR_DR_WORKER_HH_

