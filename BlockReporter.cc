#include "BlockReporter.hh"

void BlockReporter::report(unsigned int coIP, 
    const char* blkDir, 
    unsigned int localIP) {
  DIR* dir;
  FILE* file;
  struct dirent* ent;

  string rServer;
  rServer += to_string(coIP & 0xff);
  rServer += ".";
  rServer += to_string((coIP >> 8) & 0xff);
  rServer += ".";
  rServer += to_string((coIP >> 16) & 0xff);
  rServer += ".";
  rServer += to_string((coIP >> 24) & 0xff);

  cout << "coordinator server: " << rServer << endl;
  struct timeval timeout = {1, 500000}; // 1.5 seconds
  redisContext* rContext = redisConnectWithTimeout(rServer.c_str(), 6379, timeout);
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

  char info[256];
  if ((dir = opendir(blkDir)) != NULL) {
    while ((ent = readdir(dir)) != NULL) {
      if (strcmp(ent -> d_name, ".") == 0 ||
          strcmp(ent -> d_name, "..") == 0) {
        continue;
      }
      memcpy(info, (char*)&localIP, 4);
      memcpy(info + 4, ent -> d_name, strlen(ent -> d_name));
      rReply = (redisReply*)redisCommand(rContext,
          "RPUSH blk_init %b", info, 4 + strlen(ent -> d_name));
      freeReplyObject(rReply);
    }
    closedir(dir);
  } else {
    // TODO: error handling
    cerr << "BlockReporter::report() opening directory error" << endl;
  }


  redisFree(rContext);
}
