const ram = require('random-access-memory')
const ddatabase = require('ddatabase')

const createStream = require('../..')

function createLocal (numRecords, recordSize, cb) {
  const base = ddatabase(ram)

  const records = []
  for (let i = 0; i < numRecords; i++) {
    const record = Buffer.allocUnsafe(recordSize).fill(Math.floor(Math.random() * 10))
    records.push(record)
  }

  base.append(records, err => {
    if (err) return cb(err)
    const stream = createStream()
    return cb(null, base, base, stream, records)
  })
}

function createRemote (numRecords, recordSize, cb) {
  const base1 = ddatabase(ram, { sparse: true })

  const records = []
  for (let i = 0; i < numRecords; i++) {
    const record = Buffer.allocUnsafe(recordSize).fill(Math.floor(Math.random() * 10))
    records.push(record)
  }

  base1.append(records, err => {
    if (err) return cb(err)

    const base2 = ddatabase(ram, base1.key, { sparse: true })

    const s1 = base1.replicate(true, { live: true })
    s1.pipe(base2.replicate(false, { live: true })).pipe(s1)

    const stream = createStream()
    return cb(null, base1, base2, stream, records)
  })
}

module.exports = {
  createLocal,
  createRemote
}
