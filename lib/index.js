module.exports = DecisionTable

const fs = require('fs')
const levelup = require('levelup')
const memdown = require('memdown')
const JSONStream = require('JSONStream')
const SearchIndex = require('search-index')

function DecisionTable (columns, decisionField, opts, ready) {
  if (typeof opts === 'function') {
    // initialized with only columns and a ready function
    ready = opts
    opts = {}
  }
  this.columns = columns
  this.decisionField = decisionField
  if (opts.memoryDb) opts.db = levelup(new memdown())
  SearchIndex(opts, (err, index) => {
    this.index = index
    ready(err)
  })
}

DecisionTable.prototype.load = function (ndjsonStream, done) {
  ndjsonStream.pipe(this.index.defaultPipeline()).pipe(this.index.add()).on('finish', done)
}

DecisionTable.prototype.decide = function (thing, cb) {
  let _and = {}
  let _or = {}
  if (Array.isArray(thing)) {
    for (var i = 0; i < thing.length; i++) {
      let propName = this.columns[i]
      let searchVal = thing[i]
      if (searchVal !== null && searchVal !== '*') {
        _and[propName] = [searchVal]
      }
    }
  }
  let query = { AND: _and }
  console.log(query)
  let results = []
  this.index.search({query: [query]})
    .on('data', d => results.push(d))
    .on('end', () => {
      if (!results.length) return cb(new Error('no matching rows'))
      else if (results.length === 1) {
        let decision = results[0].document[this.decisionField]
        return cb(null, decision)
      }
      else return cb(new Error('more than one matching row'))
    }).on('error', cb)
}
