#include <sstream>
#include <kudu/client/client.h>
#include <napi.h>

using std::string;
using std::vector;
using kudu::client::KuduClient;
using kudu::client::sp::shared_ptr;
using kudu::Status;
using kudu::client::KuduSchema;

class KSchema {
  public:
    KSchema(string key, int type, bool primaryKey, bool notNull); // constructor
    string GetKey() const;
    int GetType() const;
    bool IsPrimaryKey() const;
    bool IsNotNull() const;
  private:
    string key_;
    int type_;
    bool primaryKey_;
    bool notNull_;
};

class KuduClass {
 public:
  KuduClass(vector<string> masters); //constructor
  string getValue(); //getter for the value
  string add(string toAdd); //adds the toAdd value to the value_
  void CreateTable(string tableName, vector<KSchema> schema, int numTablets, int partitioning, vector<string>& columns);
  void DeleteTable(string tableName);
  Status InsertRow(const string tableName, const Napi::Object value);
  Status UpdateRow(const string tableName, const Napi::Object value);
  Status UpsertRow(const string tableName, const Napi::Object value);
  Status InsertRows(const string tableName, const Napi::Array rows);
  Napi::Array ScanRow(const string tableName, const Napi::CallbackInfo& info);
 private:
  string value_;
  vector<string> masters_;
  shared_ptr<KuduClient> client_;
  Status CreateClient(const vector<string>& master_addrs, shared_ptr<KuduClient>* client);
  KuduSchema CreateSchema(const vector<KSchema> schema);
  Status DoesTableExist(const shared_ptr<KuduClient>& client, const string& table_name, bool *exists);
  Status CreateKuduTable(const shared_ptr<KuduClient>& client, const string& table_name, const KuduSchema& schema, int num_tablets, int partitioning, vector<string>& columns);
};

