#include "PPRCoordinator.hh"

/**
 * the return value can ba larger than ecK.  
 * If the return value is larger than ecK, it is the requestor.
 */
unsigned int PPRCoordinator::PPRnextIP(int id, unsigned int ecK) const {
  id ++;
  return id + (id & (- id)) - 1;
}

void PPRCoordinator::requestHandler() {
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

  while (true) {
    if (COORDINATOR_DEBUG) cout << "waiting for requests ..." << endl;
    /* Redis command: BLPOP (LISTNAME1) [LISTNAME2 ...] TIMEOUT */
    rReply = (redisReply*)redisCommand(rContext, "blpop dr_requests 100");
    if (rReply -> type == REDIS_REPLY_NIL) {
      cerr << "CmdExtractor::worker() empty queue " << endl;
      freeReplyObject(rReply);
      continue;
    } else if (rReply -> type == REDIS_REPLY_ERROR) {
      cerr << "CmdExtractor::worker() ERROR happens " << endl;
      freeReplyObject(rReply);
      continue;
    } else {
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
       * a: ecK pos: 0 // if ((ecK & 0xff00) >> 1) == 1), requestor is a holder
       * b: id pos 4
       * c: holder ips (ecK * 4 Byte)
       * f: lost file name (4Byte lenght + length) start pos 20, 24
       * g: corresponding filename in local start pos ?, ? + 4
       */
      vector<pair<unsigned int, string>> stripe = 
        _metadataBase -> getStripeBlks(filename, requestorIP);
      map<string, int> coef = _metadataBase -> getCoefficient(filename);

      char* drCmd;
      int ecK = stripe.size();
      unsigned int holderIps[ecK + 1];
      for (int i = 0; i < ecK; i ++) {
        holderIps[i] = stripe[i].first;
        cout << "ip of blk " << i << " is " << ip2Str(holderIps[i]) << endl;
      }
      holderIps[ecK] = requestorIP;

      for (int i = 0; i < ecK; i ++) {
        if (COORDINATOR_DEBUG) cout << "i: " << i << " ecK: " << ecK << endl;
        drCmd = (char*)calloc(sizeof(char), COMMAND_MAX_LENGTH);

        /* pack idx, begin */
        int idx = searchCtx(_ip2Ctx, stripe[i].first, 0, _slaveCnt - 1);
        memcpy(drCmd, (char*)&idx, 4);
        drCmd = drCmd + 4;
        /* pack idx, end */

        memcpy(drCmd, (char*)&ecK, 4);
        memcpy(drCmd + 4, (char*)&i, 4);
        memcpy(drCmd + 8, (char*)holderIps, 4 * (ecK + 1));

        unsigned int coefficient = coef[stripe[i].second];
        coefficient = ((coefficient << 16) | requestedFileNameLen);
        memcpy(drCmd + 4 * (ecK + 1) + 8, (char*)&coefficient, 4);
        //memcpy(drCmd + 4 * (ecK + 1) + 8, (char*)&requestedFileNameLen, 4);
        memcpy(drCmd + 4 * (ecK + 1) + 12, filename.c_str(), requestedFileNameLen);
        localFileBase = 12 + 4 * (ecK + 1) + requestedFileNameLen;
        localFileNameLen = stripe[i].second.length();
        memcpy(drCmd + localFileBase, (char*)&localFileNameLen, 4);
        memcpy(drCmd + localFileBase + 4, stripe[i].second.c_str(), localFileNameLen);

        /* restore drCmd pointer, begin */
        drCmd = drCmd - 4;
        /* restore drCmd pointer, end */

        cmds.push_back({_ip2Ctx[idx].second.first, 
            {drCmd, localFileBase + localFileNameLen + 8}});
      }

      /**
       * Adding cmd in requestor begins
       */
      drCmd = (char*)calloc(sizeof(char), COMMAND_MAX_LENGTH);
      /* pack idx, begin */
      int idx = searchCtx(_ip2Ctx, requestorIP, 0, _slaveCnt - 1);
      memcpy(drCmd, (char*)&idx, 4);
      drCmd = drCmd + 4;
      /* pack idx, end */


      memcpy(drCmd, (char*)&ecK, 4);
      memcpy(drCmd + 4, (char*)&ecK, 4);
      memcpy(drCmd + 8, (char*)holderIps, 4 * (ecK + 1));
      memcpy(drCmd + 4 * (ecK + 1) + 8, (char*)&requestedFileNameLen, 4);
      memcpy(drCmd + 4 * (ecK + 1) + 12, filename.c_str(), requestedFileNameLen);
      localFileBase = 12 + 4 * (ecK + 1) + requestedFileNameLen;

      /* restore drCmd pointer, begin */
      drCmd = drCmd - 4;
      /* restore drCmd pointer, end */

      cmds.push_back({_ip2Ctx[idx].second.first, {drCmd, localFileBase + 4}});
      /**
       * Adding cmd in requestor ends
       */

      // pipeline commands
      redisAppendCommand(rContext, "MULTI");
      for (auto& it : cmds) {
        cout << "command recver" << it.first << endl;
        for (int i = 0; i < it.second.second; i ++) printf("%2x", it.second.first[i]);
        cout << endl;
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

      rReply = (redisReply *)redisCommand(_selfCtx, "RPUSH %s %b", filename.c_str(), &requestorIP, 4);
    }
  }
  // should never end ...
}



