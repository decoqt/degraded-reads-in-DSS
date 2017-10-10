#include "HDFS_MetadataBase.hh"

HDFS_MetadataBase::HDFS_MetadataBase(Config* conf, RSUtil* rsu) :
  MetadataBase(conf, rsu)
{
  // TODO: read from stripe store
  DIR* dir;
  FILE* file;
  struct dirent* ent;
  int start, pos;
  unsigned int idxInStripe;
  string fileName, blkName, bName, rawStripe, stripeStore = _conf -> _stripeStore; 
  set<string> blks;
  map<string, set<string>> blk2Stripe;
  map<string, vector<string>> recoveree;
  
  map<string, set<string>> RRblk2Stripe;
  map<string, vector<string>> RRrecoveree;

  _blk2Stripe.clear();
  _RRblk2Stripe.clear();

  if ((dir = opendir(stripeStore.c_str())) != NULL) {
    while ((ent = readdir(dir)) != NULL) {
      if (METADATA_BASE_DEBUG) cout << "filename: " << ent -> d_name << endl;
      if (strcmp(ent -> d_name, ".") == 0 ||
          strcmp(ent -> d_name, "..") == 0) {
        continue;
      }

      // open and read in the stripe metadata
      fileName = string(ent -> d_name);
      blkName = fileName.substr(fileName.find(':') + 1);
      blkName = blkName.substr(0, blkName.find_last_of('_'));
      // TODO: remove duplicates
      blks.insert(blkName);
      ifstream ifs(stripeStore + "/" + string(ent -> d_name));
      rawStripe = string(istreambuf_iterator<char>(ifs), istreambuf_iterator<char>());
      if (METADATA_BASE_DEBUG) cout << rawStripe << endl;
      ifs.close();

      // parse the stripe content
      start = 0;
      idxInStripe = 0;
      // remove \newline
      if (rawStripe.back() == 10) rawStripe.pop_back();

      // blks used for recovery
      vector<string> recoveredBlks;
      vector<string> RRrecoveredBlks;
      int bidInStripe;
      while (true) {
        pos = rawStripe.find(':', start);
        if (pos == string::npos) bName = rawStripe.substr(start);
        else bName = rawStripe.substr(start, pos - start);
        bName = bName.substr(0, bName.find_last_of('_'));
        if (METADATA_BASE_DEBUG) cout << "blkName: " << blkName 
          << " bName: " << bName << endl;

        if (bName != blkName) {
	    
            RRblk2Stripe[blkName].insert(bName);
            RRrecoveredBlks.push_back(bName);
            cout << "here3" << endl;
            if (RRrecoveree.find(bName) == RRrecoveree.end()) 
              RRrecoveree[bName] = vector<string>();
            cout << "here3.5 " << recoveree[bName].size() << endl;
            RRrecoveree[bName].push_back(blkName);
          cout << "here" << endl;
          if (recoveredBlks.size() < _conf -> _ecK) {
            blk2Stripe[blkName].insert(bName);
            recoveredBlks.push_back(bName);
            cout << "herae3" << endl;
            if (recoveree.find(bName) == recoveree.end()) 
              recoveree[bName] = vector<string>();
        //    cout << "here3.5 " << recoveree[bName].size() << endl;
            recoveree[bName].push_back(blkName);
        //    cout << "here4" << endl;
          }
          idxInStripe ++;
        } else {
        //  cout << " here" << endl;
          //bidInStripe = _blkIdInStripe[blkName] = idxInStripe ++;
          bidInStripe = idxInStripe ++;
        }

        if (pos == string::npos) break;
        start = pos + 1;
      }

      int* coef = _rsUtil -> getCoefficient(bidInStripe);
      for (int i = 0; i < recoveredBlks.size(); i ++) {
        _coefficient[blkName].insert({recoveredBlks[i], coef[i]});
      }
      for (int i = 0; i < RRrecoveredBlks.size(); i ++) {
        _RRcoefficient[blkName].insert({RRrecoveredBlks[i], 1});
      }
      if (METADATA_BASE_DEBUG) {
        for (auto it : RRblk2Stripe) {
          cout << "add" << it.first << ": ";
          for (auto i : it.second) cout << " " << i;
          cout << endl;
        }

        for (auto it : blk2Stripe) {
          cout << it.first << ": ";
          for (auto i : it.second) cout << " " << i;
          cout << endl;
        }
      }
    }
    closedir(dir);
  } else {
    // TODO: error handling
    ;
  }

  // TODO: wait for infomation from redis
  struct timeval timeout = {1, 500000}; // 1.5 seconds
  redisContext* rContext = redisConnectWithTimeout("127.0.0.1", 6379, timeout);
  if (rContext == NULL || rContext -> err) {
    if (rContext) {
      cerr << "Connection error: " << rContext -> errstr << endl;
      redisFree(rContext);
    } else {
      cerr << "Connection error: can't allocate redis context" << endl;
    }
    return;
  }

  redisReply* rReply;
  unsigned int holderIP;
  while (true) {
    rReply = (redisReply*)redisCommand(rContext, "BLPOP blk_init 20");
    if (rReply -> type == REDIS_REPLY_NIL) {
      cerr << "HDFS_MetadataBase::HDFSinit() empty queue " << endl;
      freeReplyObject(rReply);
      continue;
//	break;
    } else if (rReply -> type == REDIS_REPLY_ERROR) {
      cerr << "HDFS_MetadataBase::HDFSinit() ERROR happens " << endl;
      freeReplyObject(rReply);
      continue;
    } else {
      /** 
       * cmd format:
       * |<----IP (4 bytes)---->|<-----file name (?Byte)----->|
       */
      memcpy((char*)&holderIP, rReply -> element[1] -> str, 4);
      blkName = string(rReply -> element[1] -> str + 4);
      if (METADATA_BASE_DEBUG) {
        cout << "HDFS_MetadataBase::HDFSinit() getting blk: " << blkName << endl;
      }
      freeReplyObject(rReply);
      if (blks.find(blkName) == blks.end()) {
	cout <<"HDFS_MetadataBase: find end"<<endl;
	continue;
	}

	cout <<"HDFS_MetadataBase: find no end"<<endl;
      //for (auto &it : blk2Stripe[blkName]) {
      for (auto &it : recoveree[blkName]) {
        cout << "getting blkName: " << blkName << " it: " << it << endl;
        _blk2Stripe[it].insert({holderIP, blkName});
      }
      for (auto &it : RRrecoveree[blkName]) {
        cout << "getting blkName: " << blkName << " it: " << it << endl;
        _RRblk2Stripe[it].insert({holderIP, blkName});
      }
      blks.erase(blkName);
	cout <<"HDFS_MetadataBase: erase"<<endl;
      if (blks.empty()) break;
    }
  }
}

vector<pair<unsigned int, string>> HDFS_MetadataBase::getRRStripeBlks(const string& blkName, unsigned int requestorIP) {
  cout << "HDFS_MetadataBase::getRRStripeBlks()" << blkName << endl;
  set<pair<unsigned int, string>> retVal = _RRblk2Stripe[blkName];
  vector<pair<unsigned int, string>> ret = vector<pair<unsigned int, string>>(retVal.begin(), retVal.end());
  for(int i=0; i<ret.size(); i++) {
          cout<<"RR ip = "<<ret[i].first<<" ";
          cout<<"block = "<<ret[i].second<<endl;
  }
  return ret;
}

vector<pair<unsigned int, string>> HDFS_MetadataBase::getStripeBlks(const string& blkName, unsigned int requestorIP) {
  cout << "HDFS_MetadataBase::getStripeBlks()" << blkName << endl;
  cout << "shuffle = " << _conf -> _shuffle << endl;
  set<pair<unsigned int, string>> retVal = _blk2Stripe[blkName];

  vector<pair<unsigned int, string>> ret = vector<pair<unsigned int, string>>(retVal.begin(), retVal.end());
  map<unsigned int, string> rackInfos = _conf->_rackInfos;
  string reqrack = rackInfos[requestorIP];
  cout << "request rack = " <<reqrack<<endl;

  map<string, vector<pair<unsigned int, string>>> stripe2Rack;

  for(int i=0; i<ret.size(); i++) {
          unsigned int tempip = ret[i].first;
          string temprack = rackInfos[tempip];
          if(stripe2Rack.find(temprack) == stripe2Rack.end()) {
                  vector<pair<unsigned int, string>> tempv;
                  tempv.push_back(ret[i]);
                  stripe2Rack[temprack] = tempv;
          } else {
                  stripe2Rack[temprack].push_back(ret[i]);
          }
  }

  map<string, vector<pair<unsigned int, string>>>::iterator it = stripe2Rack.begin();
  int idx=0;
  for(; it!=stripe2Rack.end(); ++it) {
          string temprname = it->first;
          vector<pair<unsigned int, string>> tempv = it->second;
          srand(unsigned(time(NULL))+idx);
          idx++;
          std::random_shuffle((it->second).begin(), (it->second).end());
  }

  vector<pair<unsigned int, string>> newret;
  vector<pair<unsigned int, string>> mine;

  if (_conf -> _shuffle) {
          std::cout<<"shuffle == true"<<std::endl;
          srand(unsigned(time(NULL)));
          std::random_shuffle(ret.begin(), ret.end());
          newret = ret;
  } else {
        for (it = stripe2Rack.begin(); it!=stripe2Rack.end(); ++it) {
                string temprname = it->first;
                if(!temprname.compare(reqrack)) {
                        vector<pair<unsigned int, string>> tempv = it->second;
                        for(int i=0; i<tempv.size(); i++) {
                                mine.push_back(tempv[i]);
                        }
                } else {
                        vector<pair<unsigned int, string>> tempv = it->second;
                        for(int i=0; i<tempv.size(); i++) {
                                newret.push_back(tempv[i]);
                        }
                }
        }

    for(int i=0; i<mine.size(); i++) {
            newret.push_back(mine[i]);
    }
  }

  for(int i=0; i<newret.size(); i++) {
          cout<<"ip = "<<newret[i].first<<" ";
          cout<<"block = "<<newret[i].second<<" ";
          cout<<"rack = "<<rackInfos[newret[i].first]<<endl;
  }
  cout<<"stripe size is "<<newret.size()<<endl;
  return newret;
}

map<string, int> HDFS_MetadataBase::getCoefficient(const string& blkName){
  return _coefficient[blkName];
}
