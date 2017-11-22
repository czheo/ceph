#include "lib/diff_match_patch.h"
#define T_FULL 0
#define T_DELTA 1
#include <string>
using namespace std;

typedef int ver_type;
typedef struct {
  ver_type type;
  int base;
  string data;
} ver_obj;

void map_set(cls_method_context_t hctx, string key, string data) {
    int ret;
    bufferlist bl;
    bl.append(data);
    ret = cls_cxx_map_set_val(hctx, key, &bl);
    if (ret < 0) throw;
}

string map_get(cls_method_context_t hctx, string key) {
  int ret;
  bufferlist bl;
  ret = cls_cxx_map_get_val(hctx, key, &bl);
  if (ret < 0) throw;
  return bl.to_str();
}

// bufferlist to ver_obj
void decode(string data, ver_obj *obj) {
  size_t found = data.find("\n");
  // read meta
  string meta = data.substr(0, found);
  if (data[0] == 'F') {
    obj->type = T_FULL;
  } else if (meta[0] == 'D') {
    obj->type = T_DELTA;
    obj->base = stoi(meta.substr(1), nullptr);
  } else {
    throw;
  }
  obj->data = data.substr(found + 1);
}

// ver_obj to bufferlist
string encode(ver_obj *obj) {
  string ret;
  if (obj->type == T_FULL) {
    ret.append("F");
  } else if (obj->type == T_DELTA) {
    ret.append("D");
    ret.append(to_string(obj->base));
  } else {
    throw;
  }
  ret.append("\n");
  ret.append(obj->data);
  return ret;
}

string do_diff(string &s1, string &s2) {
  diff_match_patch<string> dmp;
  return dmp.patch_toText(dmp.patch_make(s1, s2));
}

string do_patch(string &patch, string &s) {
  diff_match_patch<string> dmp;
  pair<string, vector<bool> > out
           = dmp.patch_apply(dmp.patch_fromText(patch), s);
  return out.first;
}
