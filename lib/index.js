module.exports = DecisionTable

const addDoc = require('./addDoc')

const async = require('async')
const levelup = require('levelup')
const Memdown = require('memdown')
const through = require('through2')
const union = require('array-union')
const intersect = require('intersect')
const defaultWildcard = '*'

function DecisionTable (columns, opts) {
  if (!opts) opts = {}
  if (!opts.wildcard) opts.wildcard = defaultWildcard
  this.columns = columns
  this.opts = opts
  this.columnDb = levelup(new Memdown())
  this.docDb = levelup(new Memdown())
}

DecisionTable.prototype.load = function (ndjsonStream, done) {
  ndjsonStream.pipe(through.obj((row, enc, cb) => {
    addDoc(this.opts, this.columns, this.docDb, this.columnDb, row, cb)
  })).on('finish', err => done(err))
}

DecisionTable.prototype.decide = function (thing, done) {
  let db = this.columnDb
  let columnQueries = []

  for (var i = 0; i < thing.length; i++) {
    let col = this.columns[i]
    let colVal = thing[i]
    let valueMatchStartKey = `${col}|${colVal}|`
    let valueMatchEndKey = `${col}|${colVal}||`
    let wildcardMatchStartKey = `${col}|${this.opts.wildcard}|`
    let wildcardMatchEndKey = `${col}|${this.opts.wildcard}||`
    columnQueries.push({valueMatchStartKey, valueMatchEndKey, wildcardMatchStartKey, wildcardMatchEndKey})
  }

  async.map(columnQueries, (oneColumn, cb) => query(db, oneColumn, cb), (err, ids) => {
    /* istanbul ignore if */
    if (err) return done(err)
    let matches = intersect(ids)
    if (!matches.length) return done(new Error('no matches found'))
    if (matches.length > 1) return done(new Error('more than one row of matches'))
    let docId = matches[0]
    this.docDb.get(docId, (err, doc) => {
      /* istanbul ignore if */
      if (err) return done(err)
      let value = doc.toString().split(',')[this.columns.length - 1]
      return done(null, value)
    })
  })
}

function query (db, oneColumn, done) {
  let valueMatch = cb => {
    let ids = []
    db.createKeyStream({gt: oneColumn.valueMatchStartKey, lt: oneColumn.valueMatchEndKey})
    .on('data', data => ids.push(data.toString().split('|').pop()))
    .on('end', () => cb(null, ids))
  }
  let wildCardMatch = cb => {
    let ids = []
    db.createKeyStream({gt: oneColumn.wildcardMatchStartKey, lt: oneColumn.wildcardMatchEndKey})
    .on('data', data => ids.push(data.toString().split('|').pop()))
    .on('end', () => cb(null, ids))
  }
  async.parallel([valueMatch, wildCardMatch], (err, results) => {
    /* istanbul ignore if */
    if (err) return done(err)
    let combined = union.apply(null, results)
    return done(null, combined)
  })
}
