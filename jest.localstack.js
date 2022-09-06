module.exports = {
  services: ['s3'],
  // TODO: Logs refuse to show up, and we don't know why
  showLog: true,
  S3Buckets: [
    {
      Bucket: 'unit',
    },
    {
      Bucket: 'integration',
    },
  ],
}