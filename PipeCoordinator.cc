#include "PipeCoordinator.hh"

void PipeCoordinator::requestHandler() {
  struct timeval timeout = {1, 500000}; // 1.5 seconds
  redisContext* rContext = redisConnectWithTimeout("127.0.0.1", 6379, timeout);
  if (rContext == NULL || rContext -> err) {
    if (rContext) {
      cerr << "Connection error: " << rContext -> errstr << endl;
      redisFree(rContext);
    } else {
      cerr << "Connection error: can't allocate redis context" << endl;
      redisFree(rContext);
    }
    return;
  }

  redisReply* rReply;
  unsigned int requestorIP;
  char* reqStr;
  size_t requestedFileNameLen, localFileBase, localFileNameLen;
  vector<pair<string, pair<char*, size_t>>> cmds;

  struct timeval tv1, tv2;

  while (true) {
    if (COORDINATOR_DEBUG); 
    cout << "waiting for requests ..." << endl;
    /* Redis command: BLPOP (LISTNAME1) [LISTNAME2 ...] TIMEOUT */
    rReply = (redisReply*)redisCommand(rContext, "blpop dr_requests 100");
    if (rReply -> type == REDIS_REPLY_NIL) {
      cerr << "PipeCoordinator::requestHandler() empty queue " << endl;
      freeReplyObject(rReply);
      continue;
    } else if (rReply -> type == REDIS_REPLY_ERROR) {
      cerr << "PipeCoordinator::requestHandler() ERROR happens " << endl;
      freeReplyObject(rReply);
      continue;
    } else {
      gettimeofday(&tv1, NULL);
      string input(rReply -> element[1] -> str);
      /**
       * Command format:
       * |<---Requestor IP (4Byte)--->|<---Lost filename (?Byte)--->|
       */
      reqStr = rReply -> element[1] -> str;
      memcpy((char*)&requestorIP, reqStr, 4);
      string filename(reqStr + 4);
      freeReplyObject(rReply);
      requestedFileNameLen = filename.length();

      if (COORDINATOR_DEBUG) cout << "request recv'd: ip: " << requestorIP
        << "requested file name: " << filename << endl;

      // do process
      /**
       * Cmd format: 
       * [idx(4Byte)][a(4Byte)][b(4Byte)][c(4Byte)][d(4Byte)][e(4Byte)][f(?Byte)][g(?Byte)]
       * idx: idx in _ip2Ctx pos 0 // this will not be sent to ECHelper
       * a: ecK pos: 0 // if ((ecK & 0xff00) >> 8) == 1), requestor is a holder
       * b: requestor ip start pos: 4
       * c: prev ip start pos 8
       * d: next ip start pos 12
       * e: id pos 16
       * f: lost file name (4Byte lenght + length) start pos 20, 24
       * g: corresponding filename in local start pos ?, ? + 4
       */
      vector<pair<unsigned int, string>> stripe;
      if (_conf -> _ECPipePolicy == "crr") {
	stripe = _metadataBase -> getRRStripeBlks(filename, requestorIP);
	}else {
      	stripe = _metadataBase -> getStripeBlks(filename, requestorIP);
	}

      if (_conf -> _pathSelectionEnabled) {
          cout << "checkpoint 1" << endl;
        vector<int> IPids;
        vector<pair<unsigned int, string>> temp;
        IPids.push_back(_ip2idx[requestorIP]);
        for (int i = 0; i < _ecK; i ++) {
          IPids.push_back(_ip2idx[stripe[i].first]);
        }
        for (auto it : IPids) cout << it << " ";
        cout << endl;
          cout << "checkpoint 2" << endl;

        // get the matrix
        vector<vector<double>> lWeightMat = vector<vector<double>>(_ecK + 1, vector<double>(_ecK + 1, 0));
        for (int i = 0; i < _ecK + 1; i ++) {
          for (int j = 0; j < _ecK + 1; j ++) {
            lWeightMat[i][j] = _linkWeight[IPids[i]][IPids[j]];
          }
        }

          cout << "checkpoint 3" << endl;
          cout << "lWeightMat: " << endl;
        for (int i = 0; i < _ecK + 1; i ++) {
          for (int j = 0; j < _ecK + 1; j ++) {
            cout << lWeightMat[i][j] << " ";
          }
          cout << endl;
        }

        _pathSelection -> intelligentSearch(lWeightMat, _ecK);
        vector<int> selectedPath = _pathSelection -> getPath();

        cout << "checkpoint 4 selected path:" << endl;
        for (auto it : selectedPath) cout << it << " ";
        cout << endl;

        for (int i = 0; i < _ecK; i ++) temp.push_back(stripe[selectedPath[_ecK - i] - 1]);
        swap(temp, stripe);

        cout << "checkpoint 5 blocks in order:" << endl;
        for (auto it : stripe) cout << it.first << " " << it.second << endl;
      }

      map<string, int> coef = _metadataBase -> getCoefficient(filename);
      int rHolder = isRequestorHolder(stripe, requestorIP), ecK;

      if (COORDINATOR_DEBUG) cout << "rHolder: " << rHolder << endl;

      char* drCmd;
      //pair<unsigned int, pair<char*, size_t>> cmdStruct;
      if (rHolder!= -1) {
        drCmd = (char*)calloc(sizeof(char), COMMAND_MAX_LENGTH);

        /* pack idx, begin */
        int idx = searchCtx(_ip2Ctx, requestorIP, 0, _slaveCnt - 1);
        memcpy(drCmd, (char*)&idx, 4);
        drCmd = drCmd + 4;
        /* pack idx, end */

        ecK = 0x010000;
        memcpy(drCmd, (char*)&ecK, 4);
        //cout << "ecK: " << ecK << endl;
        //cout << "first four bytes: ";
        //for (int i = 0; i < 4; i ++) cout << (int)drCmd[i] << " ";
        memcpy(drCmd + 4, (char*)&requestorIP, 4);
        memcpy(drCmd + 16, (char*)&ecK, 4);
        //cout << "requestorIP: " << requestorIP << endl;
        unsigned int coefficient = coef[stripe[rHolder].second];
        coefficient = ((coefficient << 16) | requestedFileNameLen);
        memcpy(drCmd + 20, (char*)&coefficient, 4);
        //memcpy(drCmd + 20, (char*)&requestedFileNameLen, 4);
        //cout << "requestedFileNameLen: " << requestedFileNameLen << endl;
        memcpy(drCmd + 24, filename.c_str(), requestedFileNameLen);
        localFileBase = 24 + requestedFileNameLen;
        localFileNameLen = stripe[rHolder].second.length();
        memcpy(drCmd + localFileBase, (char*)&localFileNameLen, 4);
        memcpy(drCmd + localFileBase + 4, stripe[rHolder].second.c_str(), localFileNameLen);


        stripe.erase(stripe.begin() + rHolder);
        memcpy(drCmd + 8, (char*)&requestorIP, 4);
        /* restore drCmd pointer, begin */
        drCmd = drCmd - 4;
        /* restore drCmd pointer, end */

        cmds.push_back({_ip2Ctx[idx].second.first, 
            {drCmd, localFileBase + localFileNameLen + 8}});
        ecK = (stripe.size() | 0x0100);
      } else {
        ecK = stripe.size();
      }


      for (int i = 0; i < (ecK & 0xff); i ++) {
        if (COORDINATOR_DEBUG) cout << "i: " << i << "rHolder: " 
          << rHolder << " ecK: " << ecK << endl;
        drCmd = (char*)calloc(sizeof(char), COMMAND_MAX_LENGTH);

        /* pack idx, begin */
        int idx = searchCtx(_ip2Ctx, stripe[i].first, 0, _slaveCnt - 1);
        memcpy(drCmd, (char*)&idx, 4);
        drCmd = drCmd + 4;
        /* pack idx, end */

        memcpy(drCmd, (char*)&ecK, 4);
        memcpy(drCmd + 4, (char*)&requestorIP, 4);
        memcpy(drCmd + 8, (char*)&(i == 0 ? stripe[ecK - 1].first : stripe[i - 1].first), 4);
        memcpy(drCmd + 12, (char*)&(i == (ecK & 0xff) - 1 ? stripe[0].first : stripe[i + 1].first), 4);
        memcpy(drCmd + 16, (char*)&i, 4);
        unsigned int coefficient = coef[stripe[i].second];
        coefficient = ((coefficient << 16) | requestedFileNameLen);
        memcpy(drCmd + 20, (char*)&coefficient, 4);
        //memcpy(drCmd + 20, (char*)&requestedFileNameLen, 4);
        memcpy(drCmd + 24, filename.c_str(), requestedFileNameLen);
        localFileBase = 24 + requestedFileNameLen;
        localFileNameLen = stripe[i].second.length();
        memcpy(drCmd + localFileBase, (char*)&localFileNameLen, 4);
        memcpy(drCmd + localFileBase + 4, stripe[i].second.c_str(), localFileNameLen);

        /* restore drCmd pointer, begin */
        drCmd = drCmd - 4;
        /* restore drCmd pointer, end */

        cmds.push_back({_ip2Ctx[idx].second.first, 
            {drCmd, localFileBase + localFileNameLen + 8}});
        //cmds.push_back({ip2Str(stripe[i].first), 
        //    {drCmd, localFileBase + localFileNameLen + 4}});
        if (COORDINATOR_DEBUG) cout << "rHolder: " << rHolder << endl;

      }
      puts("checkpoint2");

      // pipeline commands
      redisAppendCommand(rContext, "MULTI");
      for (auto& it : cmds) {
	cout<<"pipicordinator: "<<it.first.c_str()<<endl;
        redisAppendCommand(rContext, "RPUSH %s %b", 
            it.first.c_str(), 
            it.second.first, it.second.second);
      }
      redisAppendCommand(rContext, "EXEC");

      // execute commands
      redisGetReply(rContext, (void **)&rReply);
      freeReplyObject(rReply);
      for (auto& it : cmds) {
        redisGetReply(rContext, (void **)&rReply);
        freeReplyObject(rReply);
        free(it.second.first);
      }
      redisGetReply(rContext, (void **)&rReply);
      freeReplyObject(rReply);
      cmds.clear();

      if (_conf -> _ECPipePolicy == "basic") {
        rReply = (redisReply*)redisCommand(_selfCtx, "RPUSH %s %b", filename.c_str(), &(stripe.back().first), 4);
//        freeReplyObject(rReply);
      } else if (_conf -> _ECPipePolicy == "crr") {
	cout<<"RR to do "<<endl;
        for (int i = 0; i < stripe.size() - 1; i ++) {
          redisAppendCommand(_selfCtx, "RPUSH %s %b", filename.c_str(), &(stripe[i].first), 4);
        }
        for (int i = 0; i < stripe.size() - 1; i ++) {
          redisGetReply(_selfCtx, (void**)&rReply);
//          freeReplyObject(rReply);
        }
      } else {
        for (int i = 0; i < stripe.size() - 1; i ++) {
          redisAppendCommand(_selfCtx, "RPUSH %s %b", filename.c_str(), &(stripe[i].first), 4);
        }
        for (int i = 0; i < stripe.size() - 1; i ++) {
          redisGetReply(_selfCtx, (void**)&rReply);
//          freeReplyObject(rReply);
        }
      }

      gettimeofday(&tv2, NULL);
      string logMsg = "start at " + to_string((tv1.tv_sec * 1000000 + tv1.tv_usec) * 1.0 / 1000000);
      logMsg += " end at " + to_string((tv1.tv_sec * 1000000 + tv1.tv_usec) * 1.0 / 1000000) + "\n";
      cout << logMsg;
    }
  }
  // should never end ...
}


