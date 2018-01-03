const uuid = require('uuid/v4')

module.exports = function addDoc (opts, columns, docDb, columnDb, row, cb) {
  let id = uuid()
  docDb.put(id, row, err => {
    if (err) return cb(err)
    let ops = columns.map((col, index) => {
      let colVal = row[index]
      let key = `${col}|${colVal}|${id}`
      let op = { type: 'put', key, value: id}
      return op
    })
    columnDb.batch(ops, cb)
  })
}
