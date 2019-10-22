const kudujsAddon = require('./build/Release/kudujs.node');

function multiRowGenerator() {
  const result = [];
  for (let i = 0; i < 5000; i += 1) {
    result.push({
      key: i, int_val: i * 100, string_val: `test_${i}`, non_null_with_default: i,
    });
  }
  return result;
}

console.log('addon', kudujsAddon);

const schema = [
  {
    key: 'key', type: kudujsAddon.DataType.INT32, primaryKey: true, notNull: true,
  },
  {
    key: 'int_val', type: kudujsAddon.DataType.INT32, primaryKey: false, notNull: false,
  },
  {
    key: 'string_val', type: kudujsAddon.DataType.STRING, primaryKey: false, notNull: false,
  },
  {
    key: 'non_null_with_default', type: kudujsAddon.DataType.INT32, primaryKey: false, notNull: false,
  },
];
const numTablets = 10;
const predicate = [
  { colName: 'key', comparisonOp: kudujsAddon.ComparisonOp.GREATER_EQUAL, value: 4999 },
  { colName: 'key', comparisonOp: kudujsAddon.ComparisonOp.LESS, value: 5004 },
];

const multiRow = multiRowGenerator();

const classInstance = new kudujsAddon.KuduJS(['prew1b2b.mipodo.com:7051', 'prew2b2b.mipodo.com:7051', 'prew3b2b.mipodo.com:7051']);
classInstance.createTable('test_table_2', schema, numTablets);
classInstance.insertRow('test_table_2', {
  key: 5001, int_val: 3, non_null_with_default: 5, string_val: 'hello',
});
classInstance.insertRow('test_table_2', {
  key: 5002, int_val: 5, non_null_with_default: 2, string_val: 'world',
});
classInstance.updateRow('test_table_2', {
  key: 5002, int_val: 10, non_null_with_default: 4, string_val: 'bar',
});
classInstance.upsertRow('test_table_2', {
  key: 5003, int_val: 7, non_null_with_default: 8, string_val: 'foo',
});
classInstance.insertRows('test_table_2', multiRow);
console.log(classInstance.scanRow('test_table_2', predicate));
classInstance.deleteTable('test_table_2');

module.exports = kudujsAddon;
