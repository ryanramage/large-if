const DecisionTable = require('../lib')
const async = require('async')
const path = require('path')
const test = require('tape')
const columns = ['1', '2', '3', '4', 'outcome']

test('outcomes', t => {
  let opts = { outcomeDir: path.resolve(__dirname, 'outcomes') }
  let table = new DecisionTable(columns, opts)
  setup(table, () => {
    let verify = colour => t.equals('black', colour)
    let context = { verify }
    table.if(['b', 'r', 'r', 'r'], context, err => {
      t.error(err)
      t.ok('done called')
      t.end()
    })
  })
})

test('missing outcome file', t => {
  let opts = {
    'allow-multi-match': true,
    outcomeDir: path.resolve(__dirname, 'outcomes')
  }
  let table = new DecisionTable(columns, opts)
  setup(table, () => {
    table.addRow(['*', 'b', '*', '*', 'yellow'], () => {
      let verify = colour => {
        t.notEqual('green', colour)
        t.notEqual('red', colour)
      }
      let context = { verify }
      table.if(['b', 'b', 'b', 'b'], context, err => {
        t.error(err)
        t.end()
      })
    })
  })
})

test('multi outcomes called', t => {
  let opts = { outcomeDir: path.resolve(__dirname, 'outcomes') }
  let table = new DecisionTable(columns, opts)
  setup(table, () => {
    table.if(['r', 'r', 'r', 'r'], err => {
      t.ok(err)
      t.end()
    })
  })
})

function setup (table, done) {
  async.parallel([
    cb => table.addRow(['b', '*', '*', '*', 'black'], cb),
    cb => table.addRow(['g', '*', '*', '*', 'green'], cb),
    cb => table.addRow(['y', '*', '*', '*', 'yellow'], cb),
    cb => table.addRow(['r', '*', '*', '*', 'red'], cb)
  ], done)
}
