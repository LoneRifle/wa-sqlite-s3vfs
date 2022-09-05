module.exports = async function() {
  var SQLite = await import('wa-sqlite')
  var { default: SQLiteESMFactory } = await import('wa-sqlite/dist/wa-sqlite-async.mjs')

  const { S3 } = require('@aws-sdk/client-s3')
  const { S3VFS } = await import('../dist/index.js')

  const bucketName = 'mydb'

  const s3 = new S3({
    credentials: {
      accessKeyId: 'access-key',
      secretAccessKey: 'secret-key',
    },
    forcePathStyle: true,
    endpoint: 'http://localhost:4566',
  })

  await s3.createBucket({ Bucket: bucketName })

  const s3vfs = new S3VFS(s3, bucketName)

  const currentDirName = globalThis.__dirname
  const originalRequire = globalThis.require
  const sqliteDistPath = '../node_modules/wa-sqlite/dist'

  if (typeof process === 'object') {
    const { createRequire } = await import('module')
    globalThis.__dirname = sqliteDistPath
    globalThis.require = createRequire(__dirname + sqliteDistPath + '/wa-sqlite-async.mjs')
  }

  // Invoke the ES6 module factory to create the SQLite
  // Emscripten module. This will fetch and compile the
  // .wasm file.
  const module = await SQLiteESMFactory({ locateFile: function (path) { return sqliteDistPath + '/' + path; } })

  globalThis.require = originalRequire
  globalThis.__dirname = currentDirName

  // Use the module to build the API instance.
  const sqlite3 = SQLite.Factory(module)
  sqlite3.vfs_register(s3vfs, true)

  // Use the API to open and access a database.
  const db = await sqlite3.open_v2(bucketName)
  return { db, sqlite3 }
}
