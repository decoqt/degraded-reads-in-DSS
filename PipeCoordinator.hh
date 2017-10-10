#ifndef _PIPE_COORDINATOR_HH_
#define _PIPE_COORDINATOR_HH_

#include "Coordinator.hh"

using namespace std;

/**
 * Coordinator implementation for Pipe
 */
class PipeCoordinator : public Coordinator {
    // override
    void requestHandler();
  public:
    // just init redis contexts
    PipeCoordinator(Config* c) : Coordinator(c){;};
};

#endif //_PIPE_COORDINATOR_HH_

