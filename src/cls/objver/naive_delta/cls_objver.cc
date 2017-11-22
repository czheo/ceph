/*
 * This is an example RADOS object class built using only the Ceph SDK interface.
 */
#include "include/rados/objclass.h"
#include "helper.cc"
#include <stack>

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
  } else {
    // currently has object
    // 1. read current version
    int curr_ver = stoi(bl.to_str(), nullptr);

    // 2. read current obj
    string curr_s = read_curr_ver(hctx);
    
    // 3. write new obj in version
    string new_s = in->to_str();
    string diff = do_diff(new_s, curr_s);
    ver_obj vo;
    if (diff.length() < curr_s.length()) {
      // store delta
      vo.type = T_DELTA;
      vo.base = curr_ver + 1;
      vo.data = diff;
    } else {
      // store full
      vo.type = T_FULL;
      vo.data = curr_s;
    }

    map_set(hctx, "VER:" + to_string(curr_ver), encode(&vo));
    
    // 4. update version number
    map_set(hctx, "VER", to_string(curr_ver + 1));
    
    // 5. write to the current version
    vo.type = T_FULL;
    vo.data = in->to_str();
    map_set(hctx, "VER:" + to_string(curr_ver + 1), encode(&vo));
  }

  return 0;
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
