import { NoSuchKey, S3 } from '@aws-sdk/client-s3'
import { v4 } from 'uuid'
import { Readable } from 'stream'

import { 
  SQLITE_OK, 
  SQLITE_ACCESS_EXISTS,
  SQLITE_ERROR,
  SQLITE_OPEN_DELETEONCLOSE,
} from 'wa-sqlite'
import { Base } from 'wa-sqlite/src/VFS.js'

/**
 * The default page size used by sqlite
 */
export const DEFAULT_PAGE_SIZE = 4096

/**
 * The fixed offset of the lock page used by sqlite
 */
export const LOCK_PAGE_OFFSET = 1073741824

export class S3VFS extends Base {
  private readonly mapIdToPrefix = new Map<number, string>()
  private readonly prefixToFlags = new Map<string, number>()

  constructor(
    private readonly s3: S3,
    private readonly bucketName: string,
    private readonly blockSize = DEFAULT_PAGE_SIZE,
    readonly name = `s3vfs-${bucketName}-${v4()}`,
  ){
    super()
  }

  xSectorSize(_fileId: number): number {
    return this.blockSize
  }

  xAccess(name: string, flags: number, pResOut: { set: (result: number) => void; }): Promise<number> {
    return this.handleAsync(async () => {
      if (flags === SQLITE_ACCESS_EXISTS) {
        const { Contents: objects } = await this.s3.listObjectsV2({ 
          Bucket: this.bucketName, 
          Prefix: `${name}/`,
        })
        const result = 
          Number(objects?.length) > 0 || [...this.prefixToFlags.keys()].includes(name)
            ? 1 
            : 0
        pResOut.set(result)
      } else {
        pResOut.set(0)
      }
      return SQLITE_OK
    })
  }

  xDelete(name: string, _syncDir: number): Promise<number> {
    return this.handleAsync(async () => {
      const { Contents: objects } = await this.s3.listObjectsV2({ 
        Bucket: this.bucketName, 
        Prefix: `${name}/`,
      })
      for (const key of (objects || []).map(({ Key }) => Key)) {
        await this.s3.deleteObject({
          Bucket: this.bucketName,
          Key: key,
        })
      }
      this.prefixToFlags.delete(name)
      return SQLITE_OK
    })
  }

  xOpen(name: string | null, fileId: number, flags: number, pOutFlags: { set: (result: number) => void; }): Promise<number> {
    return this.handleAsync(async () => {
      if (!this.mapIdToPrefix.has(fileId)) {
        const prefix = `${name}`.split('/').pop() || v4()
        this.mapIdToPrefix.set(fileId, prefix)
        this.prefixToFlags.set(prefix, flags)
      }
      pOutFlags.set(flags)
      return SQLITE_OK
    })
  }

  xClose(fileId: number): Promise<number> {
    return this.handleAsync(async () => {
      const name = this.mapIdToPrefix.get(fileId)
      this.mapIdToPrefix.delete(fileId)
      if (name) {
        const flags = this.prefixToFlags.get(name)
        if (flags && flags & SQLITE_OPEN_DELETEONCLOSE) {
          await this.xDelete(name, 0)
        }
      }
      return SQLITE_OK
    })
  }

  private async fetchObjects(fileId: number) {
    const prefix = this.mapIdToPrefix.get(fileId)
    if (!prefix) {
      throw new Error(`File handle id ${fileId} not found`)
    }
    const { Contents: objects } = await this.s3.listObjectsV2({ 
      Bucket: this.bucketName, 
      Prefix: `${prefix}/`,
    })
    return objects
  }

  private async readableToBuffer(body: Readable) {
    const buffer = await new Promise<Buffer>((resolve, reject) => {
      const chunks = [] as Uint8Array[]
      body.once('error', (err) => reject(err))
      body.on('data', (chunk) => chunks.push(chunk))
      body.on('end', () => resolve(Buffer.concat(chunks)))
    })
    return buffer
  }

  xFileSize(fileId: number, pSize64: { set: (size: number) => void; }): Promise<number> {
    return this.handleAsync(async () => {
      try {
        const objects = await this.fetchObjects(fileId)
        const size = (objects || []).reduce((size, { Size: blockSize }) => size + (blockSize || 0), 0)
        pSize64.set(size)
        return SQLITE_OK
      } catch (error) {
        pSize64.set(0)
        return SQLITE_ERROR
      }
    })
  }

  xTruncate(fileId: number, iSize: number): Promise<number> {
    return this.handleAsync(async () => {
      try {
        const objects = await this.fetchObjects(fileId)
        const truncatePromises = []
        let total = 0
        for (const object of (objects || [])) {
          const size = object.Size || 0
          const key = `${object.Key}`

          total += size
          const toKeep = Math.max(size - total + iSize, 0)

          if (toKeep === 0) {
            truncatePromises.push(this.s3.deleteObject({ Bucket: this.bucketName, Key: key }))
          } else if (toKeep < size) {
            const truncatePromise = this.s3.getObject({ Bucket: this.bucketName, Key: key })
              .then(async ({ Body: body }) => {
                const buffer = await this.readableToBuffer(body as Readable)
                return this.s3.putObject({
                  Bucket: this.bucketName,
                  Key: key,
                  Body: buffer.subarray(0, toKeep)
                })
              })
            truncatePromises.push(truncatePromise)
          }
        }
        await Promise.all(truncatePromises)
        return SQLITE_OK
      } catch (error) {
        return SQLITE_ERROR
      }
    })
  }

  private blockId(block: number) {
    return ('0000000000'+block).slice(-10)
  }

  private blockObject(prefix: string, block: number) {
    return this.s3.getObject({
      Bucket: this.bucketName,
      Key: `${prefix}/${this.blockId(block)}`,
    })
  }

  private async blockBytes(prefix: string, block: number) {
    try {
      const { Body: body } = await this.blockObject(prefix, block)
      return this.readableToBuffer(body as Readable)
    } catch (error) {
      if (error instanceof NoSuchKey) {
        return Buffer.from([])
      } else {
        throw error
      }
    }
  }

  private blocks(offset: number, amount: number) {
    const blockMetadata = []
    while (amount > 0) {
      const block = Math.floor(offset / this.blockSize)
      const start = offset % this.blockSize
      const consume = Math.min(this.blockSize - start, amount)
      blockMetadata.push({ block, start, consume })
      amount -= consume
      offset += consume
    }
    return blockMetadata
  }

  xRead(fileId: number, pData: { size: number; value: Int8Array; }, iOffset: number): Promise<number> {
    return this.handleAsync(async () => {
      try {
        const prefix = this.mapIdToPrefix.get(fileId)
        if (!prefix) {
          throw new Error(`File handle id ${fileId} not found`)
        }
        const buffers = await Promise.all(this.blocks(iOffset, pData.size)
          .map(async ({ block, start, consume }) => {
            const buffer = await this.blockBytes(prefix, block)
            return buffer.subarray(start, start + consume)
          })
        )
        pData.value.set(Buffer.concat(buffers))
        return SQLITE_OK
      } catch (error) {
        return SQLITE_ERROR
      }
    })
  }

  xWrite(fileId: number, pData: { size: number; value: Int8Array; }, iOffset: number): Promise<number> {
    return this.handleAsync(async () => {
      try {
        const prefix = this.mapIdToPrefix.get(fileId)
        if (!prefix) {
          throw new Error(`File handle id ${fileId} not found`)
        }
        if (iOffset === LOCK_PAGE_OFFSET + pData.size) {
          // Ensure the previous blocks have enough bytes for size calculations and serialization.
          // SQLite seems to always write pages sequentially, except that it skips the byte-lock
          // page, so we only check previous blocks if we know we're just after the byte-lock page.
          const dataFirstBlock = Math.floor(iOffset / this.blockSize)
          const lockPageBlock = Math.floor(LOCK_PAGE_OFFSET / this.blockSize)
          const writePromises = []
          for (let block = dataFirstBlock - 1; block > lockPageBlock - 1; --block) {
            const originalBlockBytes = await this.blockBytes(prefix, block)
            if (originalBlockBytes.length === this.blockSize) {
              break
            }
            writePromises.push(
              this.s3.putObject({
                Bucket: this.bucketName,
                Key: `${prefix}/${this.blockId(block)}`,
                Body: Buffer.concat([
                  originalBlockBytes, 
                  Buffer.alloc(this.blockSize - originalBlockBytes.length),
                ])
              })
            )
          }
          await Promise.all(writePromises)
        }
        let dataOffset = 0
        const writePromises = this.blocks(iOffset, pData.size).map(
          async ({ block, start, consume: write }) => {
            let dataToWrite = Buffer.from(pData.value.subarray(dataOffset, dataOffset + write))
            if (start !== 0 || dataToWrite.length !== this.blockSize) {
              const originalBlockBytes = await this.blockBytes(prefix, block)
              const originalBlockBytesPadded = Buffer.concat([
                originalBlockBytes, 
                Buffer.alloc(Math.max(start - originalBlockBytes.length, 0)),
              ])
              dataToWrite = Buffer.concat([
                originalBlockBytesPadded.subarray(0, start),
                dataToWrite,
                originalBlockBytesPadded.subarray(0 + write)
              ])
            }
            dataOffset += write
            return this.s3.putObject({
              Bucket: this.bucketName,
              Key: `${prefix}/${this.blockId(block)}`,
              Body: dataToWrite,
            })
          }
        )
        await Promise.all(writePromises)
        return SQLITE_OK
      } catch (error) {
        return SQLITE_ERROR
      }
    })
  }

  handleAsync(f: Function) {
    return f();
  }
}

export default S3VFS
