module.exports = DecisionTable

const async = require('async')
const uuid = require('uuid/v4')
const levelup = require('levelup')
const memdown = require('memdown')
const through = require('through2')
const union = require('array-union')
const intersect = require('intersect')
const defaultWildcard = '*'

function DecisionTable (columns, opts) {
  if (!opts) opts = {}
  if (!opts.wildcard) opts.wildcard = defaultWildcard
  this.columns = columns
  this.opts = opts
  this.columnDb = levelup(new memdown())
  this.docDb = levelup(new memdown())
}

DecisionTable.prototype.load = function (ndjsonStream, done) {
  let docDb = this.docDb
  let columnDb = this.columnDb
  ndjsonStream.pipe(through.obj((row, enc, cb) => {
    let id = uuid()
    docDb.put(id, row, err => {
      if (err) return done(err)
      let ops = this.columns.map((col, index) => {
        let colVal = row[index]
        if (colVal === null) colVal = this.opts.wildcard
        let key = `${col}|${colVal}|${id}`
        let op = { type: 'put', key, value: id}
        return op
      })
      columnDb.batch(ops, cb)
    })
  })).on('finish', err => done(err))
}

DecisionTable.prototype.dump = function (end) {
  this.columnDb.createReadStream().on('data', function (data) {
    console.log(data.key.toString())
  }).on('end', end)
}

DecisionTable.prototype.decide = function (thing, done) {
  let db = this.columnDb
  let columnQueries = []
  if (Array.isArray(thing)) {
    for (var i = 0; i < thing.length; i++) {
      let col = this.columns[i]
      let colVal = thing[i]
      let valueMatchStartKey = `${col}|${colVal}|`
      let valueMatchEndKey = `${col}|${colVal}||`
      let wildcardMatchStartKey = `${col}|${this.opts.wildcard}|`
      let wildcardMatchEndKey = `${col}|${this.opts.wildcard}||`
      columnQueries.push({valueMatchStartKey, valueMatchEndKey, wildcardMatchStartKey, wildcardMatchEndKey})
    }
  }
  async.map(columnQueries, (oneColumn, cb) => query(db, oneColumn, cb), (err, ids) => {
    if (err) return done(err)
    let matches = intersect(ids)
    console.log('matches', matches)
    if (!matches.length) return done(new Error('no matches found'))
    if (matches.length > 1) return done(new Error('more than one row of matches'))
    let docId = matches[0]
    this.docDb.get(docId, (err, doc) => {
      if (err) return done(err)
      let value = doc.toString().split(',')[this.columns.length - 1]

      return done(null, value)
    })
  })
}


function query(db, oneColumn, done) {
  let valueMatch = cb => {
    let ids = []
    db.createKeyStream({gt: oneColumn.valueMatchStartKey, lt: oneColumn.valueMatchEndKey})
    .on('data', function (data) {
      ids.push(data.toString().split('|').pop())
    })
    .on('end', () => cb(null, ids))
  }
  let wildCardMatch = cb => {
    let ids = []
    db.createKeyStream({gt: oneColumn.wildcardMatchStartKey, lt: oneColumn.wildcardMatchEndKey})
    .on('data', function (data) {
      ids.push(data.toString().split('|').pop())
    })
    .on('end', () => cb(null, ids))
  }
  async.parallel([valueMatch, wildCardMatch], (err, results) => {
    if (err) return done(err)
    let combined = union.apply(null, results)
    console.log('da ids', combined)
    return done(null, combined)
  })
}
