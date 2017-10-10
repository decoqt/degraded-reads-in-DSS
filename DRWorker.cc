#include "DRWorker.hh"

using namespace std;

DRWorker::DRWorker(Config* conf) : _conf(conf) { 
  _packetCnt = _conf -> _packetCnt; 
  _packetSize = _conf -> _packetSize; 
  init(_conf -> _coordinatorIP, _conf -> _localIP, _conf -> _helpersIPs);
}

void DRWorker::cleanup() {
  fill_n(_diskFlag, _packetCnt, false);
  _waitingToSend = 0;
  for (int i = 0; i < _packetCnt; i ++) memset(_diskPkts[i], 0, _packetSize);
  cout << "DRWorker() cleanup finished" << endl;
}

/**
 * Init the data structures
 */
void DRWorker::init(unsigned int cIP, unsigned int sIP, vector<unsigned int>& slaveIP) {
  cout << "_packetCnt " << _packetCnt << endl;
  _diskPkts = (char**)malloc(_packetCnt * sizeof(char*));
  _toSend = (char**)malloc(_packetCnt * sizeof(char*));
  //_networkPkts = vector<char*>(_packetCnt, NULL);
  _diskFlag = (bool*)calloc(_packetCnt, sizeof(bool));
  //_networkFlag = vector<bool>(_packetCnt, false);
  _diskMtx = vector<mutex>(_packetCnt);
  //_networkMtx = vector<mutex>(_packetCnt);
  _waitingToSend = 0;

  for (int i = 0; i < _packetCnt; i ++) _diskPkts[i] = (char*)calloc(sizeof(char), _packetSize);


  // TODO: create contexts to other agenets, self and coordinator
  //_coordinatorCtx = initCtx(cIP);
  _selfCtx = initCtx(0);
  for (auto &it : slaveIP) {
    _slavesCtx.push_back({it, initCtx(it)});
  }
  sort(_slavesCtx.begin(), _slavesCtx.end());
}

string DRWorker::ip2Str(unsigned int ip) {
  string retVal;
  retVal += to_string(ip & 0xff);
  retVal += '.';
  retVal += to_string((ip >> 8) & 0xff);
  retVal += '.';
  retVal += to_string((ip >> 16) & 0xff);
  retVal += '.';
  retVal += to_string((ip >> 24) & 0xff);
  return retVal;
}

redisContext* DRWorker::initCtx(unsigned int redisIP) {
  cout << "initing cotex to " << ip2Str(redisIP) << endl;
  struct timeval timeout = { 1, 500000 }; // 1.5 seconds
  redisContext* rContext = redisConnectWithTimeout(ip2Str(redisIP).c_str(), 6379, timeout);
  if (rContext == NULL || rContext -> err) {
    if (rContext) {
      cerr << "Connection error: " << rContext -> errstr << endl;
      redisFree(rContext);
    } else {
      cerr << "Connection error: can't allocate redis context at IP: " << redisIP << endl;
    }
  }
  return rContext;
}

redisContext* DRWorker::findCtx(unsigned int ip) {
  int sid = 0, eid = _slavesCtx.size() - 1, mid;
  while (sid < eid) {
    mid = (sid + eid) / 2;
    if (_slavesCtx[mid].first == ip) { 
      return _slavesCtx[mid].second;
    } else if (_slavesCtx[mid].first < ip) {
      sid = mid + 1;
    } else {
      eid = mid - 1;
    }
  }
  return _slavesCtx[sid].first == ip ? _slavesCtx[sid].second : NULL;
}




