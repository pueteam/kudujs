#include "kudujs.h"

using std::string;

Napi::FunctionReference KuduJS::constructor;

Napi::Object KuduJS::Init(Napi::Env env, Napi::Object exports) {
  Napi::HandleScope scope(env);

  Napi::Function func = DefineClass(env, "KuduJS", {
    InstanceMethod("createTable", &KuduJS::CreateTable),
    InstanceMethod("deleteTable", &KuduJS::DeleteTable),
    InstanceMethod("insertRow", &KuduJS::InsertRow),
    InstanceMethod("updateRow", &KuduJS::UpdateRow),
    InstanceMethod("upsertRow", &KuduJS::UpsertRow),
    InstanceMethod("insertRows", &KuduJS::InsertRows),
    InstanceMethod("scanRow", &KuduJS::ScanRow),
  });

  constructor = Napi::Persistent(func);
  constructor.SuppressDestruct();

  exports.Set("KuduJS", func);
  return exports;
}

KuduJS::KuduJS(const Napi::CallbackInfo& info) : Napi::ObjectWrap<KuduJS>(info)  {
  Napi::Env env = info.Env();
  Napi::HandleScope scope(env);

  int length = info.Length();
  if (length != 1 || !info[0].IsArray()) {
    Napi::TypeError::New(env, "Array expected").ThrowAsJavaScriptException();
  }

  Napi::Array value = info[0].As<Napi::Array>();
  vector<string> master_addrs;
  for (unsigned int i = 0; i < value.Length(); i++) {
    master_addrs.push_back(value.Get(i).ToString());
  }
  
  this->actualClass_ = new KuduClass(master_addrs);
}

Napi::Value KuduJS::CreateTable(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  Napi::HandleScope scope(env);

  if (  info.Length() != 3 || !info[0].IsString()) {
    Napi::TypeError::New(env, "Arguments missing").ThrowAsJavaScriptException();
    return Napi::Number::New(info.Env(), -1);
  }

  Napi::String tableName = info[0].As<Napi::String>();
  Napi::Array schema = info[1].As<Napi::Array>();
  Napi::Number numTablets = info[2].As<Napi::Number>();
  vector<KSchema> sc;
  for (unsigned int i = 0; i < schema.Length(); i++) {
    Napi::Object value = schema.Get(i).ToObject();
    KSchema tmp = KSchema(value.Get("key").ToString(), value.Get("type").ToNumber(), value.Get("primaryKey").ToBoolean(), value.Get("notNull").ToBoolean());
    sc.push_back(tmp);
  }
  this->actualClass_->CreateTable(tableName.ToString(), sc, numTablets.Int32Value());

  return Napi::Number::New(info.Env(), 0);
}

Napi::Value KuduJS::DeleteTable(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  Napi::HandleScope scope(env);

  if (  info.Length() != 1 || !info[0].IsString()) {
    Napi::TypeError::New(env, "Table name is missing").ThrowAsJavaScriptException();
    return Napi::Number::New(info.Env(), -1);
  }

  Napi::String tableName = info[0].As<Napi::String>();
  this->actualClass_->DeleteTable(tableName.ToString());

  return Napi::Number::New(info.Env(), 0);
}

Napi::Value KuduJS::InsertRow(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  Napi::HandleScope scope(env);

  if (  info.Length() != 2 || !info[0].IsString()) {
    Napi::TypeError::New(env, "Arguments missing").ThrowAsJavaScriptException();
    return Napi::Number::New(info.Env(), -1);
  }

  Napi::String tableName = info[0].As<Napi::String>();
  Napi::Object row = info[1].As<Napi::Object>();
  this->actualClass_->InsertRow(tableName.ToString(), row);

  return Napi::Number::New(info.Env(), 0);
}

Napi::Value KuduJS::UpdateRow(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  Napi::HandleScope scope(env);

  if (  info.Length() != 2 || !info[0].IsString()) {
    Napi::TypeError::New(env, "Arguments missing").ThrowAsJavaScriptException();
    return Napi::Number::New(info.Env(), -1);
  }

  Napi::String tableName = info[0].As<Napi::String>();
  Napi::Object row = info[1].As<Napi::Object>();
  this->actualClass_->UpdateRow(tableName.ToString(), row);

  return Napi::Number::New(info.Env(), 0);
}

Napi::Value KuduJS::UpsertRow(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  Napi::HandleScope scope(env);

  if (  info.Length() != 2 || !info[0].IsString()) {
    Napi::TypeError::New(env, "Arguments missing").ThrowAsJavaScriptException();
    return Napi::Number::New(info.Env(), -1);
  }

  Napi::String tableName = info[0].As<Napi::String>();
  Napi::Object row = info[1].As<Napi::Object>();
  this->actualClass_->UpsertRow(tableName.ToString(), row);

  return Napi::Number::New(info.Env(), 0);
}

Napi::Value KuduJS::InsertRows(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  Napi::HandleScope scope(env);

  if (  info.Length() != 2 || !info[0].IsString()) {
    Napi::TypeError::New(env, "Arguments missing").ThrowAsJavaScriptException();
    return Napi::Number::New(info.Env(), -1);
  }

  Napi::String tableName = info[0].As<Napi::String>();
  Napi::Array rows = info[1].As<Napi::Array>();
  this->actualClass_->InsertRows(tableName.ToString(), rows);

  return Napi::Number::New(info.Env(), 0);
}

Napi::Value KuduJS::ScanRow(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  Napi::HandleScope scope(env);

  if (  info.Length() != 2 || !info[0].IsString()) {
    Napi::TypeError::New(env, "Arguments missing").ThrowAsJavaScriptException();
    return Napi::Number::New(info.Env(), -1);
  }

  Napi::String tableName = info[0].As<Napi::String>();
  Napi::Array result = this->actualClass_->ScanRow(tableName.ToString(), info);

  // return Napi::Array::New(info.Env(), 0);
  //return result;
  // return Napi::Array::New(info.Env(), result);
  return result;
}
