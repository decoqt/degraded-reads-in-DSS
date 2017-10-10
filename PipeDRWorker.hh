#ifndef _PIPE_DRWORKER_HH_ 
#define _PIPE_DRWORKER_HH_ 

#include "DRWorker.hh"

class PipeDRWorker : public DRWorker {

    void readWorker2(const string&);
    void sendWorker2(const char*, redisContext* rc1);

    void requestorCompletion2(string&);
    void nonRequestorCompletion2(string&, redisContext*);
    void pullRequestorCompletion(string&, redisContext*);
  public :
    void doProcess();
    PipeDRWorker(Config* conf) : DRWorker(conf) {;};
};

#endif //_PIPE_DRWORKER_HH_ 
