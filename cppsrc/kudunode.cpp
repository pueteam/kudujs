#include "kudunode.h"


/*
 * NodeJS methods
 */

Napi::Object kudujs::Init(Napi::Env env, Napi::Object exports) {
    Napi::Object DataType = Napi::Object::New(env);
    DataType.Set("INT8", 0);
    DataType.Set("INT16", 1);
    DataType.Set("INT32", 2);
    DataType.Set("INT64", 3);
    DataType.Set("STRING", 4);
    DataType.Set("BOOL", 5);
    DataType.Set("FLOAT", 6);
    DataType.Set("DOUBLE", 7);
    DataType.Set("BINARY", 8);
    DataType.Set("UNIXTIME_MICROS", 9);
    DataType.Set("DECIMAL", 10);
    DataType.Set("TIMESTAMP", 9);
    exports.Set("DataType", DataType);
    Napi::Object ComparisonOp = Napi::Object::New(env);
    ComparisonOp.Set("LESS_EQUAL", 0);
    ComparisonOp.Set("GREATER_EQUAL", 1);
    ComparisonOp.Set("EQUAL", 2);
    ComparisonOp.Set("LESS", 3);
    ComparisonOp.Set("GREATER", 4);
    exports.Set("ComparisonOp", ComparisonOp);
    return exports;
}

