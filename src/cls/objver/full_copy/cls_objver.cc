/*
 * This is an example RADOS object class built using only the Ceph SDK interface.
 */
#include "objclass/objclass.h"

using namespace std;

CLS_VER(1,0)
CLS_NAME(objver)

cls_handle_t h_class;
cls_method_handle_t h_put;
cls_method_handle_t h_get;
cls_method_handle_t h_lsver;

static int get(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  int ret;
  if (in->length() == 0) {
    // get current version
    // get size
    uint64_t size;
    ret = cls_cxx_stat(hctx, &size, NULL);
    if (ret < 0) return ret;

    // read obj
    ret = cls_cxx_read(hctx, 0, size, out);
    if (ret < 0) return ret;

    return 0;
  }

  ret = cls_cxx_map_get_val(hctx, "VER:" + in->to_str(), out);
  if (ret < 0) return ret;
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
    bl.append("0");
    ret = cls_cxx_map_set_val(hctx, "VER", &bl);
    if (ret < 0) return ret;
  } else {
    // existing object
    // 1. read current version
    int curr_ver = stoi(bl.to_str(), nullptr);
    bl.clear();

    // 2. read current obj
    // get size
    uint64_t size;
    int ret = cls_cxx_stat(hctx, &size, NULL);
    if (ret < 0) return ret;

    // read obj
    ret = cls_cxx_read(hctx, 0, size, &bl);
    if (ret < 0) return ret;
    
    // 3. write current obj in version
    ret = cls_cxx_map_set_val(hctx, "VER:" + to_string(curr_ver), &bl);
    if (ret < 0) return ret;

    // 4. update version number
    bl.clear();
    bl.append(to_string(curr_ver + 1));
    ret = cls_cxx_map_set_val(hctx, "VER", &bl);
    if (ret < 0) return ret;
  }

  // write to the object
  ret = cls_cxx_write(hctx, 0, in->length(), in);
  if (ret < 0) return ret;

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
