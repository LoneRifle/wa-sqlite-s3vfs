import { S3 } from '@aws-sdk/client-s3'
import { createRequire } from 'module'
import path from 'path'
import * as SQLite from 'wa-sqlite'
import SQLiteESMFactory from 'wa-sqlite/dist/wa-sqlite-async.mjs'

import { S3VFS } from '../s3vfs'

// @ts-ignore
import * as localstackConfig from '../../jest.localstack.js'

const s3 = new S3({
  credentials: {
    accessKeyId: 'access-key',
    secretAccessKey: 'secret-key',
  },
  forcePathStyle: true,
  endpoint: 'http://localhost:4566',
})

const { Bucket: bucketName } = localstackConfig.S3Buckets[1]


describe('S3VFS with wa-sqlite', function() {
  let sqlite3: SQLiteAPI
  let db: number

  beforeAll(async () => {
    await s3.createBucket({ Bucket: bucketName })

    const s3vfs = new S3VFS(s3, bucketName)

    const currentDirName = globalThis.__dirname
    const originalRequire = globalThis.require
    const sqliteDistPath = path.resolve(__dirname, '../../node_modules/wa-sqlite/dist')

    if (typeof process === 'object') {
      globalThis.__dirname = sqliteDistPath + '/wa-sqlite-async.mjs'
      globalThis.require = createRequire(sqliteDistPath)
    }

    // Invoke the ES6 module factory to create the SQLite
    // Emscripten module. This will fetch and compile the
    // .wasm file.
    const module = await SQLiteESMFactory({ locateFile: (path: string) => sqliteDistPath + '/' + path })

    globalThis.require = originalRequire
    globalThis.__dirname = currentDirName

    // Use the module to build the API instance.
    sqlite3 = SQLite.Factory(module)
    sqlite3.vfs_register(s3vfs, true)
    db = await sqlite3.open_v2(bucketName)
  })
  it('should return a result for SELECT 1+1', async () => {
    const { rows, columns } = await new Promise((resolve) => {
      sqlite3.exec(db, 'SELECT 1+1', (rows, columns) => resolve({ rows, columns }))
    })
    expect(rows).toStrictEqual([ 2 ])
    expect(columns).toStrictEqual([ '1+1' ])
  })
  it('should CREATE TABLE, INSERT, UPDATE, SELECT', async () => {
    await sqlite3.exec(db, 'CREATE TABLE users (x INTEGER PRIMARY KEY ASC, y TEXT)')
    await sqlite3.exec(db, "INSERT INTO users (x,y) VALUES (1,'2');")
    await sqlite3.exec(db, "UPDATE users SET y='c' WHERE x=1;")

    const { rows, columns } = await new Promise((resolve) => {
      sqlite3.exec(db, 'SELECT * from users', (rows, columns) => resolve({ rows, columns }))
    })
    expect(rows).toStrictEqual([ 1, 'c' ])
    expect(columns).toStrictEqual([ 'x', 'y' ])
  })
})
