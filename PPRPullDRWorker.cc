#include "PPRPullDRWorker.hh"

unsigned int PPRPullDRWorker::PPRnextIP(int id, unsigned int ecK) const {
  id ++;
  return id + (id & (- id)) - 1;
}

vector<unsigned int> PPRPullDRWorker::getChildrenIndices(int idx, unsigned int ecK) {
  vector<unsigned int> retVal;
  idx ++;
  int lastOne = 0;
  while ((idx >> lastOne) % 2 == 0) lastOne ++;

  int id = ((idx >> lastOne) - 1 << lastOne);
  for (-- lastOne; lastOne >= 0; lastOne --) {
    id += (1 << lastOne);
    if (id <= ecK) retVal.push_back(id - 1); 
    else {
      vector<unsigned int> temp = getChildrenIndices(id - 1, ecK);
      retVal.insert(retVal.end(), temp.begin(), temp.end());
    }
  }
  //reverse(retVal.begin(), retVal.end());
  return retVal;
}

unsigned int PPRPullDRWorker::getID(int k) const {
  k ++;
  while ((k & (k - 1)) != 0) k += (k & (-k));
  return -- k;
}

/**
 * string: lostblock name
 */
void PPRPullDRWorker::sendWorker(const string& filename, redisContext* rc1) {
  redisReply* rReply;
  const char* redisKey = filename.c_str();
  int ecK = _ecK;
  
  for (int i = 0; i < _packetCnt; i ++) {
    //cout << i << endl;
    if (DR_WORKER_DEBUG) cout << "PPRPullDRWorker::sendWorker() before sending " << i << endl;
    while (i >= _waitingToSend) {
      unique_lock<mutex> lck(_mainSenderMtx);
      _mainSenderCondVar.wait(lck);
    } 

    // TODO: error handling
    if (_id == _ecK) {
      rReply = (redisReply*)redisCommand(rc1, "RPUSH %s %b",
          redisKey, _toSend[i], _packetSize);
    } else {
      rReply = (redisReply*)redisCommand(rc1, "RPUSH tmp:%s:%d %b",
          redisKey, _id, _toSend[i], _packetSize);
    }
    if (DR_WORKER_DEBUG) cout << "PPRPullDRWorker::sendWorker() after sending " << i << endl;

    free(_toSend[i]);
    freeReplyObject(rReply);
  }
  if (DR_WORKER_DEBUG) cout << "PPRPullDRWorker::sendWorker()" << endl;
}

void PPRPullDRWorker::doProcess() {
  string lostBlkName, localBlkName;
  int idInStripe, start, pos, *subOrder;
  int subId, i, subBase, pktId, ecK;
  unsigned int prevIP, nextIP, requestorIP;
  const char* cmd;
  redisContext *nextCtx, *requestorCtx;
  bool reqHolder, isRequestor;
  unsigned int lostBlkNameLen, localBlkNameLen;

  while (true) {
    // loop FOREVER
    redisReply* rReply = (redisReply*)redisCommand(_selfCtx, "blpop dr_cmds 100");
    if (rReply -> type == REDIS_REPLY_NIL) {
      if (DR_WORKER_DEBUG) cout << "PPRPullDRWorker::doProcess(): empty list" << endl;
      freeReplyObject(rReply);
    } else if (rReply -> type == REDIS_REPLY_ERROR) {
      if (DR_WORKER_DEBUG) cout << "PPRPullDRWorker::doProcess(): error happens" << endl;
      freeReplyObject(rReply);
    } else {
      /** 
       * Parsing Cmd
       *
       * Cmd format: 
       * [a(4Byte)][b(4Byte)][c(4Byte)][d(4Byte)][e(4Byte)][f(?Byte)][g(?Byte)]
       * a: ecK pos: 0 // if ((ecK & 0xff00) >> 1) == 1), requestor is a holder
       * b: requestor ip start pos: 4
       * c: prev ip start pos 8
       * d: next ip start pos 12
       * e: id pos 16
       * f: lost file name (4Byte lenght + length) start pos 20, 24
       * g: corresponding filename in local start pos ?, ? + 4
       */
      cmd = rReply -> element[1] -> str;
      memcpy((char*)&_ecK, cmd, 4);
      memcpy((char*)&_id, cmd + 4, 4);
      unsigned int holderIps[_ecK + 1];

      memcpy((char*)holderIps, cmd + 8, 4 * (_ecK + 1));

      // get file names
      memcpy((char*)&lostBlkNameLen, cmd + 8 + 4 * (_ecK + 1), 4);
      _coefficient = (lostBlkNameLen >> 16);
      lostBlkNameLen = (lostBlkNameLen & 0xffff);
      lostBlkName = string(cmd + 12 + 4 * (_ecK + 1), lostBlkNameLen);
      if (_ecK != _id) {
        memcpy((char*)&localBlkNameLen, cmd + 12 + 4 * (_ecK + 1) + lostBlkNameLen, 4);
        localBlkName = string(cmd + 16 + 4 * (_ecK + 1) + lostBlkNameLen, localBlkNameLen);
      }
      freeReplyObject(rReply);

      if (DR_WORKER_DEBUG) {
        cout << "lostBlkName: " << lostBlkName << endl
          << " localBlkName: " << localBlkName << endl
          << " id: " << _id  << endl
          << " ecK: " << _ecK << endl;
        for (int j = 0; j < _ecK; j ++) cout << " ip of node " << j << " is " << ip2Str(holderIps[j]) << endl;
      }

      thread diskThread([=]{readWorker(localBlkName);});
      //thread sendThread([=]{sendWorker(lostBlkName, findCtx(nextIP));});
      redisContext* wrtCtx = PPRnextIP(_id, _ecK) >= _ecK ? findCtx(holderIps[_ecK]) : _selfCtx;

      vector<pair<unsigned int, redisContext*>> ctxes;
      
      vector<unsigned int> children;
      if (_id < _ecK) {
        children = getChildrenIndices(_id, _ecK);
      } else {
        children = getChildrenIndices(getID(_id), _ecK);
      }

      //cout << "children: ";
      if (DR_WORKER_DEBUG)
        for (auto it : children) cout << "children: " << it << " " << ip2Str(holderIps[it]) << endl;

      for (auto it : children) {
        if (_id != _ecK) ctxes.push_back({it, findCtx(holderIps[it])});
        else ctxes.push_back({it, _selfCtx});
      }

      for (int i = 0; i < _packetCnt; i ++) {
        while (!_diskFlag[i]) {
          unique_lock<mutex> lck(_diskMtx[i]);
          _diskCv.wait(lck);
        }
        if (DR_WORKER_DEBUG) cout << "before writing pkt " << i << endl;
        for (auto it : ctxes) {
          if (DR_WORKER_DEBUG) cout << "fetching from children " << it.first << endl;
          rReply = (redisReply*)redisCommand(it.second,
              "BLPOP tmp:%s:%d 10000",
              lostBlkName.c_str(), it.first);
          Computation::XORBuffers(_diskPkts[i], 
              rReply -> element[1] -> str, _packetSize);
          freeReplyObject(rReply);
        }
        if (_id == _ecK) {
          rReply = (redisReply*)redisCommand(wrtCtx,
              "RPUSH %s %b",
              lostBlkName.c_str(), _diskPkts[i], _packetSize);
        } else {
          rReply = (redisReply*)redisCommand(wrtCtx,
              "RPUSH tmp:%s:%d %b",
              lostBlkName.c_str(), _id, _diskPkts[i], _packetSize);
        }
        freeReplyObject(rReply);
        cout << "write pkt " << i << endl;
      }

      diskThread.join();
      // lazy garbage collection
      cleanup();
    }
  }
}

void PPRPullDRWorker::nonRequestorCompletion(string& lostBlkName, redisContext* nCtx, vector<unsigned int>& children) {
  // tmp:{lostBlkName}:i -> tmp:{lostBlkName}:i
  redisReply* rReply, *rr;

  int round = 0, idxInRound = 0, ecK = (_ecK & 0xff);
  for (int i = 0; i < _packetCnt; i ++) {
    while (!_diskFlag[i]) {
      unique_lock<mutex> lck(_diskMtx[i]);
      _diskCv.wait(lck);
    }
    if (DR_WORKER_DEBUG) cout << "PPRDRWorker::nonRequestorCompletion(): before processing packet " << i << endl;

    for (auto it : children) {
      rReply = (redisReply*)redisCommand(_selfCtx,
          "BLPOP tmp:%s:%d 10000",
          lostBlkName.c_str(), it);
      Computation::XORBuffers(_diskPkts[i], 
          rReply -> element[1] -> str, _packetSize);
    }
    _toSend[i] = _diskPkts[i];

    _waitingToSend ++;
    if (DR_WORKER_DEBUG) cout << "PPRDRWorker::nonRequestorCompletion(): packet " << i << endl;
    unique_lock<mutex> lck(_mainSenderMtx);
    _mainSenderCondVar.notify_one();
  }
  if (DR_WORKER_DEBUG) cout << "PPRDRWorker::nonRequestorCompletion() completed" << endl;
}

void PPRPullDRWorker::requestorCompletion(string& lostBlkName, vector<unsigned int>& children) {
  // tmp:{lostBlkName}:i -> {lostBlkName}:i
  redisReply* rReply, *rr;

  for (int i = 0; i < _packetCnt; i ++) {
    while (!_diskFlag[i]) {
      unique_lock<mutex> lck(_diskMtx[i]);
      _diskCv.wait(lck);
    }
    for (auto it : children) {
      rReply = (redisReply*)redisCommand(_selfCtx,
          "BLPOP tmp:%s:%d 10000",
          lostBlkName.c_str(), it);

      Computation::XORBuffers(_diskPkts[i], 
          rReply -> element[1] -> str, _packetSize);
      freeReplyObject(rReply);
    }
    if (DR_WORKER_DEBUG) cout << "PPRDRWorker::requestorCompletion(): i is " << i << " _waitingToSend = " << _waitingToSend << endl;
    _toSend[i] = _diskPkts[i];
    _waitingToSend ++;
    unique_lock<mutex> lck(_mainSenderMtx);
    _mainSenderCondVar.notify_one();
  }
}

void PPRPullDRWorker::readWorker(const string& fileName) {
  if (fileName == "") {
    for (int i = 0; i < _packetCnt; i ++) {
      //_diskPkts[i] = (char*)calloc(_packetSize, sizeof(char));

      _diskFlag[i] = true;
      unique_lock<mutex> lck(_diskMtx[i]);
      _diskCv.notify_one();

      if (DR_WORKER_DEBUG) cout << "PPRDRWorker::readWorker() processing pkt " << i << endl;
    }
    return;
  }

  string fullName = _conf -> _blkDir + '/' + fileName;
  int fd = open(fullName.c_str(), O_RDONLY);
  int subId = 0, subBase = 0, pktId;
  size_t readLen, readl, base = _conf->_packetSkipSize;

  for (pktId = 0; pktId < _packetCnt; pktId ++) {
    //_diskPkts[pktId] = (char*)malloc(_packetSize * sizeof(char));
    readLen = 0;
    while (readLen < _packetSize) {
      if ((readl = pread(fd, 
              _diskPkts[pktId] + readLen, 
              _packetSize - readLen, 
              base + pktId * _packetSize + readLen)) < 0) {
        cerr << "ERROR During disk read" << endl;
      } else {
        readLen += readl;
      }
    }
    RSUtil::multiply(_diskPkts[pktId], _coefficient, _packetSize);

    if (DR_WORKER_DEBUG) cout << "PPRDRWorker::readWorker() processing pkt " << pktId << endl;

    // notify through conditional variable
    _diskFlag[pktId] = true;
    unique_lock<mutex> lck(_diskMtx[pktId]);
    _diskCv.notify_one();
  }
  close(fd);
}



