#ifndef _CYCL_COORDINATOR_HH_
#define _CYCL_COORDINATOR_HH_

#include "Coordinator.hh"

using namespace std;

/**
 * Coordinator implementation for RRic ECPipe (extended version)
 */
class RRCoordinator : public Coordinator {
    
    int*** _preComputedMat; 
    // override
    void requestHandler() {;};
  public:
    // init redis contexts and pre-compute the recovery matrix
    RRCoordinator(Config* c);
};

#endif //_CYCL_COORDINATOR_HH_

