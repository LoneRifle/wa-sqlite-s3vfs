import { S3VFS } from '../s3vfs'
// @ts-ignore
import { configureTests, TEST } from 'wa-sqlite/test/VFSTests'
import { S3 } from '@aws-sdk/client-s3'

// @ts-ignore
import * as localstackConfig from '../../jest.localstack.js'

const SKIP = [
  TEST.BATCH_ATOMIC,
  TEST.CONTENTION,
  TEST.REBLOCK,
]

const s3 = new S3({
  forcePathStyle: true,
  endpoint: 'http://localhost:4566',
})

const { Bucket: bucketName } = localstackConfig.S3Buckets[0]

async function clear() {
  const { Contents: objects } = await s3.listObjectsV2({ 
    Bucket: bucketName, 
  })
  for (const key of (objects || []).map(({ Key }) => Key)) {
    await s3.deleteObject({
      Bucket: bucketName,
      Key: key,
    })
  }
}

describe('S3VFS', function() {
  beforeAll(async () => {
    await s3.createBucket({ Bucket: bucketName })
  })
  configureTests(() => new S3VFS(s3, bucketName), clear, SKIP)
})
