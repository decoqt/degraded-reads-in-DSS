#ifndef _RR_DR_WORKER_HH_
#define _RR_DR_WORKER_HH_

#include <queue>

#include "DRWorker.hh"

using namespace std;

class RRDRWorker : public DRWorker {
    int _toReqIdx;
    mutex _toReqMtx;
    condition_variable _toReqCV;

    int _readCnt;
    mutex _toPullerMtx;
    condition_variable _toPullerCV;

    int _toSendCnt;
    mutex _toSenderMtx;
    condition_variable _toSenderCV;

    void requestorCompletion(string&, redisContext*);
    void nonRequestorCompletion(string&);

    // pull style interface
    void requestorCompletionPull(string&, redisContext*);
    void nonRequestorCompletionPull(string&, redisContext*);

    void readWorker(const string&);
    void sendWorker(const string&, redisContext* rc1, redisContext* rc2, bool isReq, bool reqHolder);
    void sendWorkerPull(const string&, redisContext* rc1, redisContext* rc2, bool isReq, bool reqHolder);
    void toReqSender(const string&, redisContext* rc2, bool isReq, bool reqHolder);

    // new pipeline logic
    void sendWorker(const string&, redisContext* rc1, bool isReq, bool reqHolder);
    void nonRequestorCompletion(string&, redisContext*);

    // new interface
    void puller(const string&, redisContext* rc);
    void sender(const string&, redisContext* rc);
    void reader(const string&, redisContext* rc);

    /* OLD */
    void nonRequestorCompletion(string&, redisContext*, redisContext*);

  public : 
    void doProcess();
    RRDRWorker(Config* conf) : DRWorker(conf){;};
};

#endif //_CYCL_DR_WORKER_HH_
