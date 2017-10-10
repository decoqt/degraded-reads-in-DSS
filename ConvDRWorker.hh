#ifndef _CONV_DRWORKER_HH_ 
#define _CONV_DRWORKER_HH_ 

#include "DRWorker.hh"

class ConvDRWorker : public DRWorker {
    void readWorker(const string&);
    void sendWorker(const char*, redisContext* rc1);

    void requestorCompletion(string&, unsigned int[]);
    void nonRequestorCompletion(string&, redisContext*);
  public :
    void doProcess();
    ConvDRWorker(Config* conf) : DRWorker(conf) {;};
};

#endif //_CONV_DRWORKER_HH_ 
