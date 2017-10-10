#include <iostream>
#include <string>

#include <hiredis/hiredis.h>

#include "ECPipeInputStream.hh"
#include "Config.hh"

using namespace std;

int main(int argc, char** argv) {
  if (argc < 2) {
    cout << "Usage: " << argv[0] << " [lost file name]" << endl;
    return 0;
  }
  Config* conf = new Config("conf/config.xml");
  
  string filename(argv[1]);

  ECPipeInputStream cip(conf, 
      conf->_packetCnt, 
      conf->_packetSize, 
      conf->_DRPolicy, 
      conf->_ECPipePolicy, 
      conf->_coordinatorIP, 
      conf->_localIP, 
      filename);

  cip.output2File("testfileOut");
  return 0;
}

