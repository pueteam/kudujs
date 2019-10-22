/* cppsrc/main.cpp */
#include <napi.h>
#include "kudunode.h"
#include "kudujs.h"

Napi::Object InitAll(Napi::Env env, Napi::Object exports) {
  kudujs::Init(env, exports);
  return KuduJS::Init(env, exports);
}

NODE_API_MODULE(NODE_GYP_MODULE_NAME, InitAll)