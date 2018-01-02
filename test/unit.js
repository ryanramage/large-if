const DecisionTable = require('../lib/index')
const fs = require('fs')
const path = require('path')
const test = require('tape')
const csv = require('csv-parser')
const columns = ['a', 'b', 'c']
const decisionField = 'result'

const csvLocation = path.resolve(__dirname, 'table.csv')

test('basic test', function (t) {
  let opts = {memoryDb: '/a'}
  let table = new DecisionTable(columns, decisionField, opts, err => {
    t.error(err)
    let ndjsonStream = fs.createReadStream(csvLocation).pipe(csv())
    table.load(ndjsonStream, err => {
      t.error(err)
      table.decide(['happy', 'little', 'lots'], (err, category) => {
        t.error(err)
        t.equals(category, 'ok')
        t.ok(true)
        t.end()
      })
    })
  })
})
