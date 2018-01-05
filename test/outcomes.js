const DecisionTable = require('../lib')
const path = require('path')
const test = require('tape')
const columns = ['1', '2', '3', '4', 'outcome']

test('outcomes', t => {
  let opts = { outcomeDir: path.resolve(__dirname, 'outcomes') }
  let table = new DecisionTable(columns, opts)
  setup(table)
  process.nextTick(() => {
    let verify = colour => {
      console.log('got colour', colour)
      t.equals('black', c)
    }
    let context = { verify }
    table.if(['b', 'r', 'r', 'r'], context, () => {
      t.ok('done called')
      t.end()
    })
  })


})
function setup (table) {
  table.addRow(['b', '*', '*', '*', 'black'])
  table.addRow(['g', '*', '*', '*', 'green'])
  table.addRow(['y', '*', '*', '*', 'yellow'])
}
