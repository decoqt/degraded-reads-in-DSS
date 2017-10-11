#include <iostream>
#include <thread>
#include <vector>

#include "PipeCoordinator.hh"
#include "ConvCoordinator.hh"
#include "CyclCoordinator.hh"
#include "RRCoordinator.hh"
#include "PPRCoordinator.hh"
#include "MetadataBase.hh"

using namespace std;

/**
 * The main function of DRcoordinator
 */

int main(int argc, char** argv) {
  Config* conf = new Config("conf/config.xml");
  Coordinator *coord;
  if (conf -> _DRPolicy == "ppr") {
    cout << "ECCoordinator: starting PPR coordinator" << endl;
    coord = new PPRCoordinator(conf);
  } else if (conf -> _DRPolicy == "conv") {
    cout << "ECCoordinator: starting conventional coordinator" << endl;
    coord = new ConvCoordinator(conf);
  } else if (conf -> _DRPolicy == "ecpipe") {
    cout << "ECCoordinator: starting ECPipe coordinator" << conf->_ECPipePolicy<< endl;
    coord = new PipeCoordinator(conf);
    cout << "PipeCoordinator: starting Pipe coordinator" << endl;
  } 
  coord -> doProcess();
  return 0;
}

