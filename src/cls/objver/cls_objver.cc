/*
 * This is an example RADOS object class built using only the Ceph SDK interface.
 */
#include "include/rados/objclass.h"
#include "config.h"
#include "helper.cc"
#include <stack>
#include <vector>

using namespace std;

CLS_VER(1,0)
CLS_NAME(objver)

cls_handle_t h_class;
cls_method_handle_t h_put;
cls_method_handle_t h_get;
cls_method_handle_t h_lsver;

string read_curr_ver(cls_method_context_t hctx)
{
    string curr_ver = map_get(hctx, "VER");
    string data = map_get(hctx, "VER:" + curr_ver);
    ver_obj vo;
    decode(data, &vo);
    return vo.data;
}

vector<string> compute_delta(vector<string> &dirt_data, int start);

static int get(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  if (in->length() == 0) {
    out->append(read_curr_ver(hctx));
    return 0;
  }

  // get a prev version
  string data = map_get(hctx, "VER:" + in->to_str());
  ver_obj vo;
  decode(data, &vo);
  if (vo.type == T_FULL) {
    out->append(vo.data);
    return 0;
  }

  stack<string> delta_s;
  while (vo.type == T_DELTA) {
    delta_s.push(vo.data);
    data = map_get(hctx, "VER:" + to_string(vo.base));
    decode(data, &vo);
  }
  string output = vo.data;
  while (!delta_s.empty()) {
    string d = delta_s.top();
    output = do_patch(d, output);
    delta_s.pop();
  }
  out->append(output);
  return 0;
}

static int put(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  // create the object
  int ret = cls_cxx_create(hctx, false);
  if (ret < 0)  return ret;

  bufferlist bl;
  ret = cls_cxx_map_get_val(hctx, "VER", &bl);
  if (ret < 0) {
    // new object
    // set version number
    ver_obj vo;
    map_set(hctx, "VER", "0");
    vo.type = T_FULL;
    vo.data = in->to_str();
    map_set(hctx, "VER:0", encode(&vo));
    map_set(hctx, "DIRT_NUM", "0");
  } else {
    // currently has object
    ver_obj vo;
    int curr_ver = stoi(bl.to_str(), nullptr);
    int dirt_num = stoi(map_get(hctx, "DIRT_NUM"), nullptr);
    dirt_num += 1;
    if (dirt_num < MAX_DIRT_NUM) {
      map_set(hctx, "DIRT_NUM", to_string(dirt_num));
    } else {
      // compute deltas in batch
      vector<string> dirt_data;
      for (int i = 0; i < dirt_num; i++) {
        decode(map_get(hctx, "VER:" + to_string(curr_ver - dirt_num + i + 1)), &vo);
        dirt_data.push_back(vo.data);
      }
      vector<string> snapshots = compute_delta(dirt_data, curr_ver - dirt_num + 1);
      for (int i = 0; i < dirt_num; i++) {
        map_set(hctx, "VER:" + to_string(curr_ver - dirt_num + i + 1), snapshots[i]);
      }
      map_set(hctx, "DIRT_NUM", "0");
    }
    
    // update version number
    map_set(hctx, "VER", to_string(curr_ver + 1));
    
    // write to the current version
    vo.type = T_FULL;
    vo.data = in->to_str();
    map_set(hctx, "VER:" + to_string(curr_ver + 1), encode(&vo));
  }

  return 0;
}

int minKey(int key[], bool mstSet[]) {
  int min = INT_MAX;
  int minIdx;
  for (int v = 0; v < MAX_DIRT_NUM; v++) {
    if (mstSet[v] == false && key[v] < min) {
      min = key[v];
      minIdx = v;
    }
  }
  return minIdx;
}

vector<string> compute_delta(vector<string> &dirt_data, int start) {
  vector<string> ret;
  ret.resize(dirt_data.size());

  int graph[MAX_DIRT_NUM][MAX_DIRT_NUM];
  int parent[MAX_DIRT_NUM];
  int key[MAX_DIRT_NUM];
  bool mstSet[MAX_DIRT_NUM];
  // construct graph
  for (int i = 0; i < MAX_DIRT_NUM; i++) {
    for (int j = 0; j < MAX_DIRT_NUM; j++) {
      if (i == j) {
        graph[i][j] = dirt_data[i].length();
      } else if (i < j) {
        string delta = do_diff(dirt_data[i], dirt_data[j]);
        graph[i][j] = delta.length();
      } else {
        graph[i][j] = graph[j][i];
      }
    }
  }

  int minLen = INT_MAX;
  int minIdx;

  for (int i = 0; i < MAX_DIRT_NUM; i++) {
    key[i] = INT_MAX;
    mstSet[i] = false;
    if (dirt_data[i].length() < minLen) {
      minIdx = i;
    }
  }

  key[minIdx] = 0;
  parent[minIdx] = -1;

  for (int count = 0; count < MAX_DIRT_NUM - 1; count++) {
    int u = minKey(key, mstSet);
    mstSet[u] = true;
    for (int v = 0; v < MAX_DIRT_NUM; v++) {
      if (graph[u][v] < key[v] && mstSet[v] == false) {
        parent[v] = u;
        key[v] = graph[u][v];
      }
    }
  }

  ver_obj vo;
  for (int i = 0; i < MAX_DIRT_NUM; i++) {
    if (parent[i] == -1) {
      vo.type = T_FULL;
      vo.data = dirt_data[i];
      ret[i] = encode(&vo);
    } else {
      string delta = do_diff(dirt_data[parent[i]], dirt_data[i]);
      if (delta.length() < dirt_data[i].length()) {
        vo.type = T_DELTA;
        vo.base = parent[i] + start;
        vo.data = delta;
      } else {
        vo.type = T_FULL;
        vo.data = dirt_data[i];
      }
      ret[i] = encode(&vo);
    }
  }

  return ret;
}

static int lsver(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  bufferlist bl;
  int ret = cls_cxx_map_get_val(hctx, "VER", &bl);
  if (ret < 0) return ret;

  int curr_ver = stoi(bl.to_str(), nullptr);
  for (int i = 0; i <= curr_ver; i++) {
    out->append(to_string(i) + "\n");
  }

  return 0;
}

void __cls_init()
{
  CLS_LOG(0, "loading cls_objver");

  cls_register("objver", &h_class);

  cls_register_cxx_method(h_class, "put",
      CLS_METHOD_RD|CLS_METHOD_WR,
      put, &h_put);
  
  cls_register_cxx_method(h_class, "get",
      CLS_METHOD_RD,
      get, &h_get);

  cls_register_cxx_method(h_class, "lsver",
      CLS_METHOD_RD,
      lsver, &h_lsver);
}
