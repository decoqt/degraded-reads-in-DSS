#include "ConvDRWorker.hh"

#include "RSUtil.hh"

void ConvDRWorker::sendWorker(const char* redisKey, redisContext* rc) {
  // TODO: thinking about a lazy rubbish collecion...

  if (rc == NULL) cout << "ERROR: null context" << endl;
  redisReply* rReply;
  for (int i = 0; i < _packetCnt; i ++) {
    cout << "sendWorker(): " << i << endl;
    while (i >= _waitingToSend) {
      unique_lock<mutex> lck(_mainSenderMtx);
      _mainSenderCondVar.wait(lck);
    } 

    // TODO: error handling
    rReply = (redisReply*)redisCommand(rc, "RPUSH %s %b",
        redisKey, _diskPkts[i], _packetSize);
    if (DR_WORKER_DEBUG) cout << "ConvDRWorker::sendWorker() send out packet" << i << endl;

    
    //free(_diskPkts[i]);
    freeReplyObject(rReply);
  }
}

void ConvDRWorker::readWorker(const string& fileName) {
  string fullName = _conf -> _blkDir + '/' + fileName;
  cout << "readWorker(): local filename: " << fullName << endl;
  int fd = open(fullName.c_str(), O_RDONLY);
  cout << "readWorker(): fd: " << fd << endl;
  int subId = 0, i, subBase = 0, pktId;
  int readLen, readl, base = _conf->_packetSkipSize;

  for (i = 0; i < _packetCnt; i ++) {
    if (_id != _ecK) {
      //_diskPkts[i] = (char*)malloc(_packetSize * sizeof(char));
      readLen = 0;
      while (readLen < _packetSize) {
        if ((readl = pread(fd, 
                _diskPkts[i] + readLen, 
                _packetSize - readLen, 
                base + readLen)) < 0) {
          cerr << "ERROR During disk read" << endl;
        } else {
          readLen += readl;
        }
      }
      // puts("after read");
      // puts("readWorker() before computing");
      if (DR_WORKER_DEBUG) cout << "_coefficient: " << _coefficient << endl;
      RSUtil::multiply(_diskPkts[i], _coefficient, _packetSize);
      //puts("readWorker() after computing");
    } else _diskPkts[i] = (char*)calloc(sizeof(char), _packetSize);

    // notify through conditional variable
    {
      unique_lock<mutex> lck(_diskMtx[i]);
      _diskFlag[i] = true;
      _diskCv.notify_one();
    }
    //lck.unlock();
    base += _packetSize;
    if (DR_WORKER_DEBUG) cout << "ConvDRWorker::readWorker() read packet " << i << endl;
  }
  close(fd);
}

void ConvDRWorker::requestorCompletion(
    string& lostBlkName, unsigned int ips[]) {
  int ecK = _ecK, pktId;
  redisReply* rReply;

  redisContext* rC[_ecK];
  if (_id == _ecK) {
    for (int i = 0; i < _ecK; i ++) {
      rC[i] = findCtx(_conf -> _localIP);
    }
  }

  for (pktId = 0; pktId < _packetCnt; pktId ++) {
    if (_id != _ecK) {
      while (!_diskFlag[pktId]) {
        unique_lock<mutex> lck(_diskMtx[pktId]);
        _diskCv.wait(lck);
      }
    }
    // recv and compute
    if (_id == _ecK) {
      for (int i = 0; i < _ecK; i ++) {
        if (DR_WORKER_DEBUG) cout << "waiting for blk " << i << " packet " << pktId << endl;
        rReply = (redisReply*)redisCommand(rC[i], 
            "BLPOP tmp:%s:%d 100", lostBlkName.c_str(), i);
        //cout << _id << " computing " << (int)(rReply -> element[1] -> str[0]) 
        //  << " " << (int)(_diskPkts[pktId][0]) << endl;
        Computation::XORBuffers(_diskPkts[pktId], 
            rReply -> element[1] -> str,
            _packetSize);
        freeReplyObject(rReply);
      }
    }

    {
      _waitingToSend ++;
      unique_lock<mutex> lck(_mainSenderMtx);
      _mainSenderCondVar.notify_one();
    }
  }
}

void ConvDRWorker::doProcess() {
  string lostBlkName, localBlkName;
  int subId, i, subBase, pktId, ecK;
  unsigned int nextIP, requestorIP;
  redisContext *nextCtx, *requestorCtx;
  bool reqHolder, isRequestor;
  unsigned int lostBlkNameLen, localBlkNameLen;
  const char* cmd;
  redisReply* rReply;
  timeval tv1, tv2;
  thread sendThread;

  while (true) {
    // loop FOREVER
    rReply = (redisReply*)redisCommand(_selfCtx, "blpop dr_cmds 100");
    gettimeofday(&tv1, NULL);

    if (rReply -> type == REDIS_REPLY_NIL) {
      if (DR_WORKER_DEBUG) cout << "PipeDRWorker::doProcess(): empty list" << endl;
      //freeReplyObject(rReply);
    } else if (rReply -> type == REDIS_REPLY_ERROR) {
      if (DR_WORKER_DEBUG) cout << "PipeDRWorker::doProcess(): error happens" << endl;
      //freeReplyObject(rReply);
    } else {
      if (DR_WORKER_DEBUG) cout << "ConvDRWorker::doProcess(): cmd recv'd" << endl;
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
      cout << "cmd length: " << rReply -> element[1] -> len << endl;
      for (int i = 0; i < rReply -> element[1] -> len; i ++) cout << i << ": " << (int)(rReply -> element[1] -> str[i]) << endl;
      char ccmd[rReply -> element[1] -> len];
      unsigned int id;
      memcpy(ccmd, rReply -> element[1] -> str, rReply -> element[1] -> len);
      cmd = rReply -> element[1] -> str;
      memcpy((char*)&_ecK, ccmd, 4);
      cout << "_ecK: " << _ecK << endl;
      memcpy((char*)&requestorIP, ccmd + 4, 4);
      cout << "requestorIP: " << requestorIP << endl;
      memcpy((char*)&_coefficient, ccmd + 8, 4);
      cout << "_coefficient: " << _coefficient << endl;
      memcpy((char*)&nextIP, ccmd + 12, 4);
      cout << "nextIP: " << nextIP << endl;
      memcpy((char*)&id, ccmd + 16, 4);
      //memcpy((char*)&_id, ccmd + 16, 4);
      _id = id;
      cout << "_id: " << _id << endl;

      unsigned int* ips;
      // get file names
      if (_ecK != _id) {
        memcpy((char*)&lostBlkNameLen, ccmd + 20, 4);
        lostBlkName = string(ccmd + 24, lostBlkNameLen);
        memcpy((char*)&localBlkNameLen, ccmd + 24 + lostBlkNameLen, 4);
        localBlkName = string(ccmd + 28 + lostBlkNameLen, localBlkNameLen);
      } else { 
        ips = (unsigned int*)calloc(sizeof(unsigned int), _ecK);
        memcpy((char*)ips, ccmd + 20, 4 * _ecK);
        memcpy((char*)&lostBlkNameLen, ccmd + 20 + 4 * _ecK, 4);
        lostBlkName = string(ccmd + 24 + 4 * _ecK, lostBlkNameLen);
      }

      if (DR_WORKER_DEBUG) {
        cout << " lostBlkName: " << lostBlkName << endl
          << " localBlkName: " << localBlkName << endl
          << " id: " << _id  << endl
          << " ecK: " << _ecK << endl
          << " requestorIP: " << ip2Str(requestorIP) << endl
          << " nextIP: " << ip2Str(nextIP) << endl;
      }

      cout << "starting readworker" << endl;
      // start thread reading from disks
      //puts("starting readWorker");
      thread diskThread([=]{readWorker(localBlkName);});
      _waitingToSend = 0;
      //redisContext* nC = findCtx(requestorIP);
      cout << "_localIP: " << ip2Str(_conf -> _localIP) << endl;
      //redisContext* nC = _selfCtx;
      redisContext* nC = findCtx(requestorIP);
      string sendKey;
      if (_ecK == _id) {
        sendKey = lostBlkName;
      } else {
        sendKey = "tmp:" + lostBlkName + ":" + to_string(_id);
      }
      if (DR_WORKER_DEBUG) cout << "starting sendWorker" << endl;
      if (_id == _ecK) sendThread = thread([=]{sendWorker(sendKey.c_str(), _selfCtx);});
      else sendThread = thread([=]{sendWorker(sendKey.c_str(), nC);});

      requestorCompletion(lostBlkName, ips);


      diskThread.join();
      sendThread.join();
      gettimeofday(&tv2, NULL);
      if (DR_WORKER_DEBUG)
        cout << "request starts at " << tv1.tv_sec << "." << tv1.tv_usec
          << "ends at " << tv2.tv_sec << "." << tv2.tv_usec << endl;
      cleanup();
      // lazy clean up
      //freeReplyObject(rReply);
    }
  }
}

