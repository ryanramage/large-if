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

  async.map(columnQueries, (oneColumn, cb) => query(db, oneColumn, cb), (err, idsArrays) => {
    /* istanbul ignore if */
    if (err) return done(err)
    let valueMatchArrays = intersect(idsArrays.map(a => a[0]))
    let wildCardMatchArrays = intersect(idsArrays.map(a => a[1]))
    if (valueMatchArrays.length === 1) return fetchDoc(this.docDb, valueMatchArrays[0], done)
    if (valueMatchArrays.length === 0 && wildCardMatchArrays.length === 1) return fetchDoc(this.docDb, wildCardMatchArrays[0], done)

    let join = idsArrays.map(a => union(a[0], a[1]))
    let match = intersect(join)
    if (match.length === 1) return fetchDoc(this.docDb, match[0], done)
    if (match.length > 1) return done(new Error('more than one row of matches'))
    return done(new Error('no rows match'))
  })
}

function fetchDoc (docDb, docId, done) {
  docDb.get(docId, (err, doc) => {
    /* istanbul ignore if */
    if (err) return done(err)
    let value = doc.toString().split(',').pop()
    return done(null, value)
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
  async.parallel([valueMatch, wildCardMatch], done)
}
