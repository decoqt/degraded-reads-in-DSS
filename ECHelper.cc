#include <iostream>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/fcntl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "BlockReporter.hh"
#include "Config.hh"
#include "ConvDRWorker.hh"
#include "CyclDRWorker.hh"
#include "DRWorker.hh"
#include "RRDRWorker.hh"
#include "PPRDRWorker.hh"
#include "PPRPullDRWorker.hh"
#include "PipeDRWorker.hh"

using namespace std;

int main(int argc, char** argv) {
  Config* conf = new Config("conf/config.xml");
  BlockReporter::report(conf -> _coordinatorIP,
      conf -> _blkDir.c_str(), 
      conf -> _localIP);

  int workerThreadNum = conf -> _agWorkerThreadNum;
  thread thrds[workerThreadNum];
  DRWorker** drWorkers = (DRWorker**)calloc(sizeof(DRWorker*), workerThreadNum);
  for (int i = 0; i < workerThreadNum; i ++) {
    if (conf -> _DRPolicy == "ecpipe") {
      cout << "ECHelper: starting ECPipe helper" << endl;
      if (conf -> _ECPipePolicy == "cyclic") drWorkers[i] = new CyclDRWorker(conf);
      if (conf -> _ECPipePolicy == "basic") drWorkers[i] = new PipeDRWorker(conf);
      if (conf -> _ECPipePolicy == "crr") drWorkers[i] = new RRDRWorker(conf);
    } else if (conf -> _DRPolicy == "ppr") {
      drWorkers[i] = new PPRPullDRWorker(conf);
    } else if (conf -> _DRPolicy == "conv") {
      drWorkers[i] = new ConvDRWorker(conf);
    }

    thrds[i] = thread([=]{drWorkers[i] -> doProcess();});
  }
  for (int i = 0; i < workerThreadNum; i ++) {
    thrds[i].join();
  }
  return 0;
}

