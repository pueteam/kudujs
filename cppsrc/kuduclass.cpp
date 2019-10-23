#include "kuduclass.h"
#include <kudu/client/callbacks.h>
#include <kudu/client/client.h>
#include <kudu/client/row_result.h>
#include <kudu/client/stubs.h>
#include <kudu/client/value.h>
#include <kudu/common/partial_row.h>

#include <ctime>
#include <iostream>
#include <sstream>

using kudu::client::KuduClient;
using kudu::client::KuduClientBuilder;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduError;
using kudu::client::KuduInsert;
using kudu::client::KuduUpdate;
using kudu::client::KuduUpsert;
using kudu::client::KuduPredicate;
using kudu::client::KuduScanBatch;
using kudu::client::KuduRowResult;
using kudu::client::KuduScanner;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduSession;
using kudu::client::KuduStatusFunctionCallback;
using kudu::client::KuduTable;
using kudu::client::KuduTableAlterer;
using kudu::client::KuduTableCreator;
using kudu::client::KuduValue;
using kudu::client::sp::shared_ptr;
using kudu::KuduPartialRow;
using kudu::MonoDelta;
using kudu::Status;
using kudu::Slice;

using std::ostringstream;
using std::string;
using std::vector;

KSchema::KSchema(string key, int type, bool primaryKey, bool notNull) {
  this->key_ = key;
  this->type_ = type;
  this->primaryKey_ = primaryKey;
  this->notNull_ = notNull;
}

string KSchema::GetKey() const {
  return this->key_;
}

int KSchema::GetType() const {
  return this->type_;
}

bool KSchema::IsPrimaryKey() const {
  return this->primaryKey_;
}

bool KSchema::IsNotNull() const {
  return this->notNull_;
}

KuduClass::KuduClass(vector<string> masters){
  this->masters_ = masters;


  kudu::client::SetVerboseLogLevel(2);
  KUDU_LOG(INFO) << "Running with Kudu client version: " <<
      kudu::client::GetShortVersionString();
  KUDU_LOG(INFO) << "Long version info: " <<
      kudu::client::GetAllVersionInfo();

  // This is to install and automatically un-install custom logging callback.
  // LogCallbackHelper log_cb_helper;
  // Enable verbose debugging for the client library.
  
  KUDU_LOG(INFO) << "Created a client connection: " << masters[0];
  KUDU_CHECK_OK(CreateClient(this->masters_, &this->client_));
  KUDU_LOG(INFO) << "Created a client connection";

  // Disable the verbose logging.
  kudu::client::SetVerboseLogLevel(0);
}

string KuduClass::getValue()
{
  return this->value_;
}

string KuduClass::add(string toAdd)
{
  this->value_ += toAdd;
  return this->value_;
}

void KuduClass::CreateTable(string tableName, vector<KSchema> schema, int numTablets, int partitioning, vector<string>& columns) {
  KUDU_LOG(INFO) << "Creating a schema";
  KuduSchema sc(CreateSchema(schema));
  KUDU_LOG(INFO) << "Created a schema";
  // Create a table with that schema.
  bool exists = false;
  KUDU_CHECK_OK(DoesTableExist(this->client_, tableName, &exists));
  if (exists) {
    this->client_->DeleteTable(tableName);
    KUDU_LOG(INFO) << "Deleting old table before creating new one";
  }
  KUDU_CHECK_OK(CreateKuduTable(this->client_, tableName, sc, numTablets, partitioning, columns));
  KUDU_LOG(INFO) << "Created a table " + tableName;
}

void KuduClass::DeleteTable(string tableName) {
  // Delete the table.
  KUDU_CHECK_OK(this->client_->DeleteTable(tableName));
  KUDU_LOG(INFO) << "Deleted a table " + tableName;
}

/*
* Kudu methods
*/

Status KuduClass::CreateClient(const vector<string>& master_addrs,
                          shared_ptr<KuduClient>* client) {
  return KuduClientBuilder()
      .master_server_addrs(master_addrs)
      .default_admin_operation_timeout(MonoDelta::FromSeconds(20))
      .Build(client);
}

KuduSchema KuduClass::CreateSchema(const vector<KSchema> schema) {
  KuduSchema sc;
  KuduSchemaBuilder b;

  for (unsigned int i = 0; i < schema.size(); i++) {
    // std::cout << "Evaluating: " << schema[i].GetKey() << ":" << schema[i].IsPrimaryKey() << std::endl;
    if (schema[i].IsPrimaryKey()) {
      b.AddColumn(schema[i].GetKey())->Type(static_cast<kudu::client::KuduColumnSchema::DataType>(schema[i].GetType()))->NotNull()->PrimaryKey();
    } else if (schema[i].IsNotNull()) {
      b.AddColumn(schema[i].GetKey())->Type(static_cast<kudu::client::KuduColumnSchema::DataType>(schema[i].GetType()))->NotNull();
    } else {
      b.AddColumn(schema[i].GetKey())->Type(static_cast<kudu::client::KuduColumnSchema::DataType>(schema[i].GetType()));
    }
  }
  KUDU_CHECK_OK(b.Build(&sc));
  return sc;
}

Status KuduClass::DoesTableExist(const shared_ptr<KuduClient>& client,
                            const string& table_name,
                            bool *exists) {
  shared_ptr<KuduTable> table;
  Status s = client->OpenTable(table_name, &table);
  if (s.ok()) {
    *exists = true;
  } else if (s.IsNotFound()) {
    *exists = false;
    s = Status::OK();
  }
  return s;
}

Status KuduClass::CreateKuduTable(const shared_ptr<KuduClient>& client,
                          const string& table_name,
                          const KuduSchema& schema,
                          int num_tablets,
                          int partitioning,
                          vector<string>& column_names) {

  // Set the schema and range partition columns.
  KuduTableCreator* table_creator = client->NewTableCreator();
  
  table_creator->table_name(table_name)
      .schema(&schema);
  if (partitioning == 0) {
    table_creator->set_range_partition_columns(column_names);
    // Generate and add the range partition splits for the table.
    int32_t increment = 1000 / num_tablets;
    for (int32_t i = 1; i < num_tablets; i++) {
      KuduPartialRow* row = schema.NewRow();
      KUDU_CHECK_OK(row->SetInt32(0, i * increment));
      table_creator->add_range_partition_split(row);
    }
  }

  if (partitioning == 1) {
    table_creator->add_hash_partitions(column_names, num_tablets);
  }

  Status s = table_creator->Create();
  delete table_creator;
  return s;
}

static Status AlterTable(const shared_ptr<KuduClient>& client,
                        const string& table_name) {
  KuduTableAlterer* table_alterer = client->NewTableAlterer(table_name);
  table_alterer->AlterColumn("int_val")->RenameTo("integer_val");
  table_alterer->AddColumn("another_val")->Type(KuduColumnSchema::BOOL);
  table_alterer->DropColumn("string_val");
  Status s = table_alterer->Alter();
  delete table_alterer;
  return s;
}

static void StatusCB(void* unused, const Status& status) {
  KUDU_LOG(INFO) << "Asynchronous flush finished with status: "
                      << status.ToString();
}

Status KuduClass::InsertRow(const string tableName, const Napi::Object value) {
  KUDU_LOG(INFO) << "Inserting a single record in " << tableName;
  // Insert a row into the table.
  shared_ptr<KuduTable> table;
  KUDU_CHECK_OK(this->client_->OpenTable(tableName, &table));

  shared_ptr<KuduSession> session = table->client()->NewSession();
  KUDU_RETURN_NOT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  session->SetTimeoutMillis(5000);

  KuduSchema schema = table->schema();

  KuduInsert* insert = table->NewInsert();
  KuduPartialRow* row = insert->mutable_row();

  Napi::Array props = value.GetPropertyNames();

  for(int i = 0, l = props.Length(); i < l; i++) {
    string prop = props.Get(i).ToString();

    for (int i = 0, l = schema.num_columns(); i < l; i++) {
      KuduColumnSchema col = schema.Column(i);

      if (col.name().compare(prop) == 0) {
        switch (col.type())
        {
        case KuduColumnSchema::INT8:
          KUDU_CHECK_OK(row->SetInt8(prop, (int)value.Get(prop).ToNumber()));
          break;
        case KuduColumnSchema::INT16:
          KUDU_CHECK_OK(row->SetInt16(prop, (int)value.Get(prop).ToNumber()));
          break;
        case KuduColumnSchema::INT32:
          KUDU_CHECK_OK(row->SetInt32(prop, value.Get(prop).ToNumber()));
          break;
        case KuduColumnSchema::INT64:
          KUDU_CHECK_OK(row->SetInt64(prop, value.Get(prop).ToNumber()));
          break;
        case KuduColumnSchema::STRING:
          KUDU_CHECK_OK(row->SetString(prop, value.Get(prop).ToString().Utf8Value()));
          break;
        case KuduColumnSchema::BOOL:
          KUDU_CHECK_OK(row->SetBool(prop, value.Get(prop).ToBoolean()));
          break;
        case KuduColumnSchema::FLOAT:
          KUDU_CHECK_OK(row->SetFloat(prop, value.Get(prop).ToNumber()));
          break;
        case KuduColumnSchema::DOUBLE:
          KUDU_CHECK_OK(row->SetDouble(prop, value.Get(prop).ToNumber()));
          break;
        case KuduColumnSchema::BINARY:
          KUDU_CHECK_OK(row->SetBinary(prop, value.Get(prop).ToString().Utf8Value()));
          break;
        case KuduColumnSchema::UNIXTIME_MICROS:
          KUDU_CHECK_OK(row->SetUnixTimeMicros(prop, value.Get(prop).ToNumber()));
          break;
        
        default:
          break;
        }
      }
    }
  }
  KUDU_CHECK_OK(session->Apply(insert));

  Status s = session->Flush();
  if (s.ok()) {
    return s;
  }

  // Test asynchronous flush.
  KuduStatusFunctionCallback<void*> status_cb(&StatusCB, NULL);
  session->FlushAsync(&status_cb);

  // Look at the session's errors.
  vector<KuduError*> errors;
  bool overflow;
  session->GetPendingErrors(&errors, &overflow);
  if (!errors.empty()) {
    s = overflow ? Status::IOError("Overflowed pending errors in session") :
        errors.front()->status();
    while (!errors.empty()) {
      delete errors.back();
      errors.pop_back();
    }
  }
  KUDU_RETURN_NOT_OK(s);

  // Close the session.
  return session->Close();
}

Status KuduClass::UpdateRow(const string tableName, const Napi::Object value) {
  KUDU_LOG(INFO) << "Updating a single record in " << tableName;
  // Insert a row into the table.
  shared_ptr<KuduTable> table;
  KUDU_CHECK_OK(this->client_->OpenTable(tableName, &table));

  shared_ptr<KuduSession> session = table->client()->NewSession();
  KUDU_RETURN_NOT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  session->SetTimeoutMillis(5000);

  KuduSchema schema = table->schema();

  KuduUpdate* update = table->NewUpdate();
  KuduPartialRow* row = update->mutable_row();

  Napi::Array props = value.GetPropertyNames();

  for(int i = 0, l = props.Length(); i < l; i++) {
    string prop = props.Get(i).ToString();

    for (int i = 0, l = schema.num_columns(); i < l; i++) {
      KuduColumnSchema col = schema.Column(i);

      if (col.name().compare(prop) == 0) {
        switch (col.type())
        {
        case KuduColumnSchema::INT8:
          KUDU_CHECK_OK(row->SetInt8(prop, (int)value.Get(prop).ToNumber()));
          break;
        case KuduColumnSchema::INT16:
          KUDU_CHECK_OK(row->SetInt16(prop, (int)value.Get(prop).ToNumber()));
          break;
        case KuduColumnSchema::INT32:
          KUDU_CHECK_OK(row->SetInt32(prop, value.Get(prop).ToNumber()));
          break;
        case KuduColumnSchema::INT64:
          KUDU_CHECK_OK(row->SetInt64(prop, value.Get(prop).ToNumber()));
          break;
        case KuduColumnSchema::STRING:
          KUDU_CHECK_OK(row->SetString(prop, value.Get(prop).ToString().Utf8Value()));
          break;
        case KuduColumnSchema::BOOL:
          KUDU_CHECK_OK(row->SetBool(prop, value.Get(prop).ToBoolean()));
          break;
        case KuduColumnSchema::FLOAT:
          KUDU_CHECK_OK(row->SetFloat(prop, value.Get(prop).ToNumber()));
          break;
        case KuduColumnSchema::DOUBLE:
          KUDU_CHECK_OK(row->SetDouble(prop, value.Get(prop).ToNumber()));
          break;
        case KuduColumnSchema::BINARY:
          KUDU_CHECK_OK(row->SetBinary(prop, value.Get(prop).ToString().Utf8Value()));
          break;
        case KuduColumnSchema::UNIXTIME_MICROS:
          KUDU_CHECK_OK(row->SetUnixTimeMicros(prop, value.Get(prop).ToNumber()));
          break;
        
        default:
          break;
        }
      }
    }
  }
  KUDU_CHECK_OK(session->Apply(update));

  Status s = session->Flush();
  if (s.ok()) {
    return s;
  }

  // Test asynchronous flush.
  KuduStatusFunctionCallback<void*> status_cb(&StatusCB, NULL);
  session->FlushAsync(&status_cb);

  // Look at the session's errors.
  vector<KuduError*> errors;
  bool overflow;
  session->GetPendingErrors(&errors, &overflow);
  if (!errors.empty()) {
    s = overflow ? Status::IOError("Overflowed pending errors in session") :
        errors.front()->status();
    while (!errors.empty()) {
      delete errors.back();
      errors.pop_back();
    }
  }
  KUDU_RETURN_NOT_OK(s);

  // Close the session.
  return session->Close();
}

Status KuduClass::UpsertRow(const string tableName, const Napi::Object value) {
  KUDU_LOG(INFO) << "Upserting a single record in " << tableName;
  // Insert a row into the table.
  shared_ptr<KuduTable> table;
  KUDU_CHECK_OK(this->client_->OpenTable(tableName, &table));

  shared_ptr<KuduSession> session = table->client()->NewSession();
  KUDU_RETURN_NOT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  session->SetTimeoutMillis(5000);

  KuduSchema schema = table->schema();

  KuduUpsert* upsert = table->NewUpsert();
  KuduPartialRow* row = upsert->mutable_row();

  Napi::Array props = value.GetPropertyNames();

  for(int i = 0, l = props.Length(); i < l; i++) {
    string prop = props.Get(i).ToString();

    for (int i = 0, l = schema.num_columns(); i < l; i++) {
      KuduColumnSchema col = schema.Column(i);

      if (col.name().compare(prop) == 0) {
        switch (col.type())
        {
        case KuduColumnSchema::INT8:
          KUDU_CHECK_OK(row->SetInt8(prop, (int)value.Get(prop).ToNumber()));
          break;
        case KuduColumnSchema::INT16:
          KUDU_CHECK_OK(row->SetInt16(prop, (int)value.Get(prop).ToNumber()));
          break;
        case KuduColumnSchema::INT32:
          KUDU_CHECK_OK(row->SetInt32(prop, value.Get(prop).ToNumber()));
          break;
        case KuduColumnSchema::INT64:
          KUDU_CHECK_OK(row->SetInt64(prop, value.Get(prop).ToNumber()));
          break;
        case KuduColumnSchema::STRING:
          KUDU_CHECK_OK(row->SetString(prop, value.Get(prop).ToString().Utf8Value()));
          break;
        case KuduColumnSchema::BOOL:
          KUDU_CHECK_OK(row->SetBool(prop, value.Get(prop).ToBoolean()));
          break;
        case KuduColumnSchema::FLOAT:
          KUDU_CHECK_OK(row->SetFloat(prop, value.Get(prop).ToNumber()));
          break;
        case KuduColumnSchema::DOUBLE:
          KUDU_CHECK_OK(row->SetDouble(prop, value.Get(prop).ToNumber()));
          break;
        case KuduColumnSchema::BINARY:
          KUDU_CHECK_OK(row->SetBinary(prop, value.Get(prop).ToString().Utf8Value()));
          break;
        case KuduColumnSchema::UNIXTIME_MICROS:
          KUDU_CHECK_OK(row->SetUnixTimeMicros(prop, value.Get(prop).ToNumber()));
          break;
        
        default:
          break;
        }
      }
    }
  }
  KUDU_CHECK_OK(session->Apply(upsert));

  Status s = session->Flush();
  if (s.ok()) {
    return s;
  }

  // Test asynchronous flush.
  KuduStatusFunctionCallback<void*> status_cb(&StatusCB, NULL);
  session->FlushAsync(&status_cb);

  // Look at the session's errors.
  vector<KuduError*> errors;
  bool overflow;
  session->GetPendingErrors(&errors, &overflow);
  if (!errors.empty()) {
    s = overflow ? Status::IOError("Overflowed pending errors in session") :
        errors.front()->status();
    while (!errors.empty()) {
      delete errors.back();
      errors.pop_back();
    }
  }
  KUDU_RETURN_NOT_OK(s);

  // Close the session.
  return session->Close();
}

Status KuduClass::InsertRows(const string tableName, const Napi::Array rows) {
  KUDU_LOG(INFO) << "Inserting multiple records in " << tableName;
  // Insert a row into the table.
  shared_ptr<KuduTable> table;
  KUDU_CHECK_OK(this->client_->OpenTable(tableName, &table));

  shared_ptr<KuduSession> session = table->client()->NewSession();
  KUDU_RETURN_NOT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  session->SetTimeoutMillis(5000);

  KuduSchema schema = table->schema();

  for (unsigned int i = 0; i < rows.Length(); i++) {
    Napi::Object value = rows.Get(i).ToObject();
    KuduInsert* insert = table->NewInsert();
    KuduPartialRow* row = insert->mutable_row();

    Napi::Array props = value.GetPropertyNames();

    for(int i = 0, l = props.Length(); i < l; i++) {
      string prop = props.Get(i).ToString();

      for (int i = 0, l = schema.num_columns(); i < l; i++) {
        KuduColumnSchema col = schema.Column(i);

        if (col.name().compare(prop) == 0) {
          switch (col.type())
          {
          case KuduColumnSchema::INT8:
            KUDU_CHECK_OK(row->SetInt8(prop, (int)value.Get(prop).ToNumber()));
            break;
          case KuduColumnSchema::INT16:
            KUDU_CHECK_OK(row->SetInt16(prop, (int)value.Get(prop).ToNumber()));
            break;
          case KuduColumnSchema::INT32:
            KUDU_CHECK_OK(row->SetInt32(prop, value.Get(prop).ToNumber()));
            break;
          case KuduColumnSchema::INT64:
            KUDU_CHECK_OK(row->SetInt64(prop, value.Get(prop).ToNumber()));
            break;
          case KuduColumnSchema::STRING:
            KUDU_CHECK_OK(row->SetString(prop, value.Get(prop).ToString().Utf8Value()));
            break;
          case KuduColumnSchema::BOOL:
            KUDU_CHECK_OK(row->SetBool(prop, value.Get(prop).ToBoolean()));
            break;
          case KuduColumnSchema::FLOAT:
            KUDU_CHECK_OK(row->SetFloat(prop, value.Get(prop).ToNumber()));
            break;
          case KuduColumnSchema::DOUBLE:
            KUDU_CHECK_OK(row->SetDouble(prop, value.Get(prop).ToNumber()));
            break;
          case KuduColumnSchema::BINARY:
            KUDU_CHECK_OK(row->SetBinary(prop, value.Get(prop).ToString().Utf8Value()));
            break;
          case KuduColumnSchema::UNIXTIME_MICROS:
            KUDU_CHECK_OK(row->SetUnixTimeMicros(prop, value.Get(prop).ToNumber()));
            break;
          
          default:
            break;
          }
        }
      }
    }
    KUDU_CHECK_OK(session->Apply(insert));
  }
  Status s = session->Flush();
  if (s.ok()) {
    return s;
  }

  // Test asynchronous flush.
  KuduStatusFunctionCallback<void*> status_cb(&StatusCB, NULL);
  session->FlushAsync(&status_cb);

  // Look at the session's errors.
  vector<KuduError*> errors;
  bool overflow;
  session->GetPendingErrors(&errors, &overflow);
  if (!errors.empty()) {
    s = overflow ? Status::IOError("Overflowed pending errors in session") :
        errors.front()->status();
    while (!errors.empty()) {
      delete errors.back();
      errors.pop_back();
    }
  }
  KUDU_RETURN_NOT_OK(s);

  // Close the session.
  return session->Close();
}

static Status InsertKuduRows(const shared_ptr<KuduTable>& table, int num_rows) {
  shared_ptr<KuduSession> session = table->client()->NewSession();
  KUDU_RETURN_NOT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  session->SetTimeoutMillis(5000);

  for (int i = 0; i < num_rows; i++) {
    KuduInsert* insert = table->NewInsert();
    KuduPartialRow* row = insert->mutable_row();
    KUDU_CHECK_OK(row->SetInt32("key", i));
    KUDU_CHECK_OK(row->SetInt32("integer_val", i * 2));
    KUDU_CHECK_OK(row->SetInt32("non_null_with_default", i * 5));
    KUDU_CHECK_OK(session->Apply(insert));
  }
  Status s = session->Flush();
  if (s.ok()) {
    return s;
  }

  // Test asynchronous flush.
  KuduStatusFunctionCallback<void*> status_cb(&StatusCB, NULL);
  session->FlushAsync(&status_cb);

  // Look at the session's errors.
  vector<KuduError*> errors;
  bool overflow;
  session->GetPendingErrors(&errors, &overflow);
  if (!errors.empty()) {
    s = overflow ? Status::IOError("Overflowed pending errors in session") :
        errors.front()->status();
    while (!errors.empty()) {
      delete errors.back();
      errors.pop_back();
    }
  }
  KUDU_RETURN_NOT_OK(s);

  // Close the session.
  return session->Close();
}

static Status ScanRows(const shared_ptr<KuduTable>& table) {
  const int kLowerBound = 5;
  const int kUpperBound = 600;

  KuduScanner scanner(table.get());

  // Add a predicate: WHERE key >= 5
  KuduPredicate* p = table->NewComparisonPredicate(
      "key", KuduPredicate::GREATER_EQUAL, KuduValue::FromInt(kLowerBound));
  KUDU_RETURN_NOT_OK(scanner.AddConjunctPredicate(p));

  // Add a predicate: WHERE key <= 600
  p = table->NewComparisonPredicate(
      "key", KuduPredicate::LESS_EQUAL, KuduValue::FromInt(kUpperBound));
  KUDU_RETURN_NOT_OK(scanner.AddConjunctPredicate(p));

  KUDU_RETURN_NOT_OK(scanner.Open());
  vector<KuduRowResult> results;

  int next_row = kLowerBound;
  while (scanner.HasMoreRows()) {
    KUDU_RETURN_NOT_OK(scanner.NextBatch(&results));
    for (vector<KuduRowResult>::iterator iter = results.begin();
        iter != results.end();
        iter++, next_row++) {
      const KuduRowResult& result = *iter;
      int32_t val;
      KUDU_RETURN_NOT_OK(result.GetInt32("key", &val));
      if (val != next_row) {
        ostringstream out;
        out << "Scan returned the wrong results. Expected key "
            << next_row << " but got " << val;
        return Status::IOError(out.str());
      }
    }
    results.clear();
  }

  // next_row is now one past the last row we read.
  int last_row_seen = next_row - 1;

  if (last_row_seen != kUpperBound) {
    ostringstream out;
    out << "Scan returned the wrong results. Expected last row to be "
        << kUpperBound << " rows but got " << last_row_seen;
    return Status::IOError(out.str());
  }
  return Status::OK();
}

Napi::Array KuduClass::ScanRow(const string tableName, const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  Napi::Array obj = Napi::Array::New(env);

  shared_ptr<KuduTable> table;
  KUDU_CHECK_OK(this->client_->OpenTable(tableName, &table));
  KuduScanner scanner(table.get());

  KUDU_LOG(INFO) << "Scanning rows out of table " + tableName;

  Napi::Array predicates = info[1].As<Napi::Array>();
  KuduPredicate* p;
  for (unsigned int i = 0; i < predicates.Length(); i++) {
    Napi::Object value = predicates.Get(i).ToObject();
    KuduPredicate::ComparisonOp c = static_cast<KuduPredicate::ComparisonOp>(value.Get("comparisonOp").ToNumber().Int32Value());
    p = table->NewComparisonPredicate(
      value.Get("colName").ToString().Utf8Value(), c, KuduValue::FromInt(value.Get("value").ToNumber()));
    scanner.AddConjunctPredicate(p);
  }

  KUDU_LOG(INFO) << "Added predicate " + tableName;

  scanner.Open();
  KuduScanBatch batch;

  int i = 0;
  KUDU_LOG(INFO) << "Iterating Scanner " + tableName;
  while (scanner.HasMoreRows()) {
    scanner.NextBatch(&batch);
    for (KuduScanBatch::RowPtr row : batch) {
      const KuduSchema *schema = batch.projection_schema();
      Napi::Object tmp = Napi::Object::New(env);
      for (int i = 0, l = schema->num_columns(); i < l; i++) {
        KuduColumnSchema col = schema->Column(i);
        switch (col.type())
        {
          case KuduColumnSchema::INT8:
          {
            int8_t val;
            row.GetInt8(col.name(), &val);
            tmp.Set(col.name(), val);
            std::cout << "Got: " << val << std::endl;
            break;
          }
          case KuduColumnSchema::INT16:
          {
            int16_t val;
            row.GetInt16(col.name(), &val);
            tmp.Set(col.name(), val);
            break;
          }
          case KuduColumnSchema::INT32:
          {
            int32_t val;
            row.GetInt32(col.name(), &val);
            tmp.Set(col.name(), val);
            break;
          }
          case KuduColumnSchema::INT64:
          {
            int64_t val;
            row.GetInt64(col.name(), &val);
            tmp.Set(col.name(), val);
            break;
          }
          case KuduColumnSchema::STRING:
          {
            kudu::Slice val;
            row.GetString(col.name(), &val);
            tmp.Set(col.name(), val.ToString());
            break;
          }
          case KuduColumnSchema::BOOL:
          {
            bool val;
            row.GetBool(col.name(), &val);
            tmp.Set(col.name(), val);
            break;
          }
          case KuduColumnSchema::FLOAT:
          {
            float val;
            row.GetFloat(col.name(), &val);
            tmp.Set(col.name(), val);
            break;
          }
          case KuduColumnSchema::DOUBLE:
          {
            double val;
            row.GetDouble(col.name(), &val);
            tmp.Set(col.name(), val);
            break;
          }
          case KuduColumnSchema::BINARY:
          {
            kudu::Slice val;
            row.GetBinary(col.name(), &val);
            tmp.Set(col.name(), val.ToString());
            break;
          }
          case KuduColumnSchema::UNIXTIME_MICROS:
          {
            int64_t val;
            row.GetUnixTimeMicros(col.name(), &val);
            tmp.Set(col.name(), val);
            break;
          }
          default:
            break;
          }
      }
      obj.Set(i++, tmp);
    }
  }
  // return Status::OK();
  return obj;
}

// A helper class providing custom logging callback. It also manages
// automatic callback installation and removal.
class LogCallbackHelper {
public:
  LogCallbackHelper() : log_cb_(&LogCallbackHelper::LogCb, NULL) {
    kudu::client::InstallLoggingCallback(&log_cb_);
  }

  ~LogCallbackHelper() {
    kudu::client::UninstallLoggingCallback();
  }

  static void LogCb(void* unused,
                    kudu::client::KuduLogSeverity severity,
                    const char* filename,
                    int line_number,
                    const struct ::tm* time,
                    const char* message,
                    size_t message_len) {
    KUDU_LOG(INFO) << "Received log message from Kudu client library";
    KUDU_LOG(INFO) << " Severity: " << severity;
    KUDU_LOG(INFO) << " Filename: " << filename;
    KUDU_LOG(INFO) << " Line number: " << line_number;
    char time_buf[32];
    // Example: Tue Mar 24 11:46:43 2015.
    KUDU_CHECK(strftime(time_buf, sizeof(time_buf), "%a %b %d %T %Y", time));
    KUDU_LOG(INFO) << " Time: " << time_buf;
    KUDU_LOG(INFO) << " Message: " << string(message, message_len);
  }

private:
  kudu::client::KuduLoggingFunctionCallback<void*> log_cb_;
};
