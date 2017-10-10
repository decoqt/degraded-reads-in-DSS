#include "Coordinator.hh"

Coordinator::Coordinator(Config* conf) {
  _conf = conf;
  cerr <<"Construct coordinator"<<endl;
  init();

  _handlerThrds = vector<thread>(_handlerThreadNum);
  _distThrds = vector<thread>(_distributorThreadNum);
  cerr <<"Construct coordinator end"<<endl;
}

/**
 * return index in the stripe if requestor itself is a holder of corresponding
 * blocks
 * if not exisit, return -1
 */
int Coordinator::isRequestorHolder(vector<pair<unsigned int, string>>& stripe, unsigned int requestorIP) const {
  int sid = 0, eid = stripe.size() - 1, mid;
  while (sid < eid) {
    mid = (sid + eid) / 2;
    if (stripe[mid].first == requestorIP) return mid;
    else if (stripe[mid].first > requestorIP) eid = mid - 1;
    else sid = mid + 1;
  }
  return stripe[sid].first == requestorIP ? sid : -1;
}

void Coordinator::init() {
  cerr <<"Construct coordinator init"<<endl;
  _handlerThreadNum = _conf -> _coCmdReqHandlerThreadNum;
  _distributorThreadNum = _conf -> _coCmdDistThreadNum;
  _ecK = _conf -> _ecK;
  _ecN = _conf -> _ecN;
  _ecM = _ecN - _ecK;
  _slaveCnt = _conf -> _helpersIPs.size();
  
  if (COORDINATOR_DEBUG) cout << "# of slaves: " << _slaveCnt << endl;
  
  /**
   * Queue of cmd distributor: "disQ:{i}", i is the index of the distributor
   */
  vector<int> rightBounder;

  if (_slaveCnt % _distributorThreadNum == 0) rightBounder.push_back(_slaveCnt / _distributorThreadNum);
  else rightBounder.push_back(_slaveCnt / _distributorThreadNum + 1);

  int remaining = _slaveCnt % _distributorThreadNum, idx, rBounderId;
  for (int i = 1; i < _distributorThreadNum; i ++) { 
    if (i >= remaining) rightBounder.push_back(rightBounder.back() + _slaveCnt / _distributorThreadNum);
    else rightBounder.push_back(rightBounder.back() + _slaveCnt / _distributorThreadNum + 1);
  }

  idx = 0; 
  rBounderId = 0;
  string prefix("disQ:");
  for (auto& it : _conf -> _helpersIPs) {
    if (++ idx > rightBounder[rBounderId]) rBounderId ++;
    _ip2Ctx.push_back({it, {prefix + to_string(rBounderId), initCtx(it)}});
  }
  _selfCtx = initCtx(_conf -> _localIP);

  _rsConfigFile = _conf -> _rsConfigFile;
  ifstream ifs(_rsConfigFile);
  _rsEncMat = (int*)malloc(sizeof(int) * _ecM * _ecK);
  for (int i = 0; i < _ecM; i ++) {
    for (int j = 0; j < _ecK; j ++) {
      ifs >> _rsEncMat[_ecK * i + j];
    }
  }
  ifs.close();

  puts("initializing rs utility");
  _rsUtil = new RSUtil(_conf, _rsEncMat);

  if (_conf -> _pathSelectionEnabled) {
    // init matrix
    _linkWeight = vector<vector<double>>(_slaveCnt, vector<double>(_slaveCnt, 0));

    // read in data
    ifstream ifsLink(_conf -> _linkWeightConfigFile);
    for (int i = 0; i < _slaveCnt; i ++) {
      for (int j = 0; j < _slaveCnt; j ++) {
        ifsLink >> _linkWeight[i][j];
      }
    }
    ifsLink.close();

    cout << "_linkWeight: " << endl;
    for (int i = 0; i < _slaveCnt; i ++) {
      for (int j = 0; j < _slaveCnt; j ++) {
          cout << _linkWeight[i][j] << " ";
      }
      cout << endl;
    }

    // init _ip2idx
    for (int i = 0; i < _slaveCnt; i ++) {
      _ip2idx[_conf -> _helpersIPs[i]] = i;
    }
    cout << "_ip2idx: " << endl;
    for (int i = 0; i < _slaveCnt; i ++) {
      cout << _conf -> _helpersIPs[i] << " " << _ip2idx[_conf -> _helpersIPs[i]] << endl;
    }

    // init path selection
    _pathSelection = new PathSelection();
  }

  // start metadatabase
  if (_conf -> _fileSysType == "HDFS") {
    if (COORDINATOR_DEBUG) cout << "creating metadata base for HDFS " << endl;
    _metadataBase = new HDFS_MetadataBase(_conf, _rsUtil);
  }else if(_conf -> _fileSysType == "QFS"){
    if (COORDINATOR_DEBUG) cout << "creating metadata base for QFS " << endl;
    _metadataBase = new QFS_MetadataBase(_conf, _rsUtil);
  }
  cerr <<"Construct coordinator init end"<<endl;

  //sort(_ip2Ctx.begin(), _ip2Ctx.end());
}

redisContext* Coordinator::initCtx(unsigned int redisIP) {
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

void Coordinator::doProcess() {
  cerr << "doprocess1 " << endl;
  // starts the threads
  for (int i = 0; i < _distributorThreadNum; i ++) {
    _distThrds[i] = thread([=]{this -> cmdDistributor(i, _distributorThreadNum);});
  }
  cerr << "doprocess2 " << endl;
  for (int i = 0; i < _handlerThreadNum; i ++) {
    _handlerThrds[i] = thread([=]{this -> requestHandler();});
  }

  // should not reach here
  for (int i = 0; i < _distributorThreadNum; i ++) {
    _distThrds[i].join();
  }
  for (int i = 0; i < _handlerThreadNum; i ++) {
    _handlerThrds[i].join();
  }
}

/**
 * Since the _ip2Ctx is sorted, we just do a binary search,
 * which should be faster than unordered_map with a modest number of slaves
 *
 * TODO: We current assume that IP must be able to be found.  Should add error
 * handling afterwards
 */
int Coordinator::searchCtx(
    vector<pair<unsigned int, pair<string, redisContext*>>>& arr,
    unsigned int target,
    size_t sId,
    size_t eId) {
  int mid;
  while (sId < eId) {
    mid = (sId + eId) / 2;
    if (arr[mid].first < target) sId = mid + 1;
    else if (arr[mid].first > target) eId = mid - 1;
    else return mid;
  }
  return sId;
}

void Coordinator::cmdDistributor(int idx, int total) {
  //return;
  //size_t sId, eId, i, j;
  int i, j, currIdx;
  struct timeval timeout = {1, 500000}; // 1.5 seconds
  string disQKey("disQ:");
  disQKey += to_string(idx);


  char* cmdBase;
  bool reqBlkHolder; // requestor itself is a block holder
  redisReply* rReply, *rReply2;

  // local filename of operator and requested block
  char locFN[FILENAME_MAX_LENGTH], dstFN[FILENAME_MAX_LENGTH]; 

  //if (COORDINATOR_DEBUG) cout << " cmdDistributor idx: " << idx 
  //  << " starting id: " << sId << " ending id: " << eId 
  //  << " charged ips: " << cIPstr << endl;

  // TODO: trying to switch communication to conditional variable
  redisContext* locCtx = redisConnectWithTimeout("127.0.0.1", 6379, timeout),
    *opCtx;
  if (locCtx == NULL || locCtx -> err) {
    if (locCtx) {
      cerr << "Connection error: " << locCtx -> errstr << endl;
    } else {
      cerr << "Connection error: can't allocate redis context" << endl;
    }
    redisFree(locCtx);
    return;
  }

  while (1) {
    /* Redis command: BLPOP (LISTNAME1) [LISTNAME2 ...] TIMEOUT */
    rReply = (redisReply*)redisCommand(locCtx, 
        "BLPOP %s 100", disQKey.c_str());
	cout<<"disQkey "<<disQKey.c_str() <<endl;
    if (rReply -> type == REDIS_REPLY_NIL) {
      cerr << "Coordinator::CmdDistributor() empty queue " << endl;
      freeReplyObject(rReply);
    } else if (rReply -> type == REDIS_REPLY_ERROR) {
      cerr << "Coordinator::CmdDistributor() ERROR happens " << endl;
      freeReplyObject(rReply);
    } else {
      struct timeval tv1, tv2;
      gettimeofday(&tv1, NULL);
      memcpy((char*)&currIdx, rReply -> element[1] -> str, 4);
      opCtx = _ip2Ctx[currIdx].second.second;

      cerr << "Coordinator::push dr cmds " <<rReply->element[1]->str<< endl;
      // TODO: try use asynchronous style
      rReply2 = (redisReply*)redisCommand(
          opCtx, 
          "RPUSH dr_cmds %b", 
          rReply -> element[1] -> str + 4,
          rReply -> element[1] -> len - 4);

      freeReplyObject(rReply2);
      freeReplyObject(rReply);
      gettimeofday(&tv2, NULL);
      string logMsg = "deliver from " + to_string((tv1.tv_sec * 1000000 + tv1.tv_usec) * 1.0 / 1000000) + " " +to_string((tv2.tv_sec * 1000000 + tv2.tv_usec) * 1.0 / 1000000) + "\n"; 
      puts(logMsg.c_str());
    }
  }
}


string Coordinator::ip2Str(unsigned int ip) const {
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

void Coordinator::requestHandler() {
  // now overrided by inherited classes
  // TODO: add ECPipe and PPR flag in Config.hh
  //requestHandlerECPipe();
  //;
    cout << "why here? " << endl;
}



