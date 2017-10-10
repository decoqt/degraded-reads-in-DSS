#include "QFS_MetadataBase.hh"

QFS_MetadataBase::QFS_MetadataBase(Config* conf, RSUtil* rsu) :
  MetadataBase(conf, rsu),
  _locIP(conf->_localIP)
{
  _metadataThread = thread([=] {ProcessMetadata();});
}
void QFS_MetadataBase::ProcessMetadata(){
  struct timeval timeout = {1, 500000}; // 1.5 seconds
  if(METADATA_BASE_DEBUG) cout << "Starting Metadata thread" << endl;
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
  if(METADATA_BASE_DEBUG) cout << "Done." << endl;
  redisReply* rReply;
  unsigned int holderIP;
  int idxInStripe;

  set<string> blks;
  list<string> recoveredBlks;
  map<string, vector<string>> recoveree;

  // temp variable
  string bName, ipStr;
  unsigned int ip;
  map<unsigned int, bool> ipAssigned;
  int start, bidInStripe;
  int *coef;
  string handleStr, blkName, blkNameNoVers, bNameNoVers;
  while (true) {
    if(METADATA_BASE_DEBUG) cout << "Waiting Metadata..." << endl;
    rReply = (redisReply*)redisCommand(rContext, "BLPOP dr_meta 0");
    if (rReply -> type == REDIS_REPLY_NIL) {
      if(METADATA_BASE_DEBUG) cerr << "QFS_MetadataBase::HDFSinit() empty queue " << endl;
    } else if (rReply -> type == REDIS_REPLY_ERROR) {
      cerr << "QFS_MetadataBase::HDFSinit() ERROR happens " << endl;
    } else {
      lock_guard<mutex> _lock(_blkLock_m);
      if(METADATA_BASE_DEBUG) cout << "meta recv: " << rReply -> element[1] -> str << endl;
      
      // reset temp variable
      start = 0;
      bidInStripe = -1;
      idxInStripe = 0;
      ipAssigned.clear();
      _blk2Stripe[blkName].clear();
      _coefficient[blkName].clear();
      recoveredBlks.clear();

      // get request meta string
      // format: need_recover_blkName:need_recover_IP_str:index_0_blkName:index_0_IP_str:index_1_blkName:index_1_IP_str:...:index_n_blkName:index_n_IP_str
      handleStr = rReply -> element[1] -> str;

      // start process request
      blkName = handleStr.substr( 0, start = handleStr.find(':') ); handleStr = handleStr.substr( start + 1 );
      ipStr = handleStr.substr( 0, start = handleStr.find(':') );   handleStr = handleStr.substr( start + 1 );

      blkNameNoVers = blkName.substr(0, blkName.find_last_of('.') );

      inet_pton(AF_INET, ipStr.c_str(), &ip);
      ipAssigned[ip] = true; // prevent client ip

      if(METADATA_BASE_DEBUG) cout << "Recovering: " << blkName << ", requesting from IP= " << ipStr << endl;

      while(
        handleStr != "" &&
        (recoveredBlks.size() < _conf -> _ecK || bidInStripe < 0)
      ){
        bName = handleStr.substr( 0, start = handleStr.find(':') ); handleStr = handleStr.substr( start + 1 );
        ipStr = handleStr.substr( 0, start = handleStr.find(':') ); handleStr = handleStr.substr( start + 1 );

        bNameNoVers = bName.substr(0, bName.find_last_of('.'));

        if(ipStr.size() && recoveredBlks.size() < _conf -> _ecK){
          inet_pton(AF_INET, ipStr.c_str(), &ip);
          if(ipAssigned.find(ip) == ipAssigned.end()){ // prevent a host working with 2 chunk
            ipAssigned[ip] = true;

            _blk2Stripe[blkName].push_back({ip, bName});
            recoveredBlks.push_back(bName);

            if(METADATA_BASE_DEBUG) cout << "Using: index=" << idxInStripe << ", bName=" << bName << ", from IP=" << ipStr << endl;
          }else{
            // there are more than 1 chunks lost in first K+1 chunks
            break;
          }
        }else if(!ipStr.size() && blkNameNoVers != bNameNoVers){
          // there are more than 1 chunks lost in first K+1 chunks
          break;
        }
        if(blkNameNoVers == bNameNoVers)
          if(bidInStripe < 0) bidInStripe /*= _blkIdInStripe[blkName]*/ = idxInStripe % 9;
          else break;
        ++idxInStripe;
      }

      if(METADATA_BASE_DEBUG) cout << "Recovering index=" << bidInStripe << endl;

      // setup coefficient
      // return status
      bool canRecover;
      // checking is chunk can recovery
      if(canRecover = (recoveredBlks.size() == _conf -> _ecK && bidInStripe >= 0)){
        // chunk can be recovery
        coef = _rsUtil -> getCoefficient(bidInStripe);
        int i=0;
        for(list<string>::iterator it=recoveredBlks.begin(); it != recoveredBlks.end(); ++it, ++i){
          cout << coef[i] << endl;
          _coefficient[blkName].insert({*it, coef[i]});
        }
        _blkLock.notify_all();
      }else{
        _blk2Stripe.erase(blkName);
        _coefficient.erase(blkName);
      }

      if(METADATA_BASE_DEBUG) cout << "Can block " << blkName << " recover using ECPipe? " << ( canRecover ? "True": "False" ) << endl;
      // send signal to redis for status returning
      freeReplyObject(
        redisCommand(
          rContext, 
          "RPUSH dr_meta_ret:%s %u", 
          blkName.c_str(), 
          canRecover
        )
      );
    }
    freeReplyObject(rReply);
  }
}

vector<pair<unsigned int, string>> QFS_MetadataBase::getStripeBlks(const string& blkName, unsigned int requestorIP) {
  // TODO: currently, we assume that all metadata are collected during
  // initialization..  However, we should know that this may not be 100%
  // percent sure, we need to add a side path to retrieve data from HDFS
  // Namenode.
  unique_lock<mutex> _lock(_blkLock_m);
  _blkLock.wait(_lock, [this, blkName]{return _blk2Stripe.find(blkName) != _blk2Stripe.end();});

  list<pair<unsigned int, string>> retVal = _blk2Stripe[blkName];
  return vector<pair<unsigned int, string>>(retVal.begin(), retVal.end());
}


map<string, int> QFS_MetadataBase::getCoefficient(const string& blkName){
  unique_lock<mutex> _lock(_blkLock_m);
  _blkLock.wait(_lock, [this, blkName]{return _coefficient.find(blkName) != _coefficient.end();});
  return _coefficient[blkName];
}
