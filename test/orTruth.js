const DecisionTable = require('../lib/index')
const fs = require('fs')
const path = require('path')
const test = require('tape')
const csv2 = require('csv2')
const columns = ['1', '2', '3', '4', 'outcome']

test('large OR truth table', t => {
  let run = table => {
    table.decide([0, 0, 0, 0], (err, category) => {
      t.error(err)
      t.equals(category, 'false')
      t.end()
    })
  }
  setUp(t, path.resolve(__dirname, 'assets/or-truth-table.csv'), null, run)
})

test('large OR truth table wildcard', t => {
  let run = table => {
    table.decide([1, 0, 0, 0], (err, category) => {
      t.error(err)
      t.equals(category, 'true')
      t.end()
    })
  }
  setUp(t, path.resolve(__dirname, 'assets/or-truth-table.csv'), null, run)
})

test('large OR truth table wildcard', t => {
  let run = table => {
    table.decide([1, 0, 1, 0], (err, category) => {
      t.error(err)
      t.equals(category, 'true')
      t.end()
    })
  }
  setUp(t, path.resolve(__dirname, 'assets/or-truth-table.csv'), null, run)
})

test('large OR truth table wildcard', t => {
  let run = table => {
    table.decide([1, 1, 1, 1], (err, category) => {
      t.error(err)
      t.equals(category, 'true')
      t.end()
    })
  }
  setUp(t, path.resolve(__dirname, 'assets/or-truth-table.csv'), null, run)
})

function setUp (t, csvLocation, opts, done) {
  let table = new DecisionTable(columns, opts)
  let ndjsonStream = fs.createReadStream(csvLocation).pipe(csv2())
  table.load(ndjsonStream, err => {
    t.error(err)
    done(table)
  })
}
