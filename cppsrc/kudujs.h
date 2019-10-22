#include <napi.h>
#include "kuduclass.h"

class KuduJS : public Napi::ObjectWrap<KuduJS> {
 public:
  static Napi::Object Init(Napi::Env env, Napi::Object exports); //Init function for setting the export key to JS
  KuduJS(const Napi::CallbackInfo& info); //Constructor to initialise

 private:
  static Napi::FunctionReference constructor; //reference to store the class definition that needs to be exported to JS
  Napi::Value CreateTable(const Napi::CallbackInfo& info);
  Napi::Value DeleteTable(const Napi::CallbackInfo& info);
  Napi::Value InsertRow(const Napi::CallbackInfo& info);
  Napi::Value UpdateRow(const Napi::CallbackInfo& info);
  Napi::Value UpsertRow(const Napi::CallbackInfo& info);
  Napi::Value InsertRows(const Napi::CallbackInfo& info);
  Napi::Value ScanRow(const Napi::CallbackInfo& info);
  KuduClass *actualClass_; //internal instance of actualclass used to perform actual operations.
};