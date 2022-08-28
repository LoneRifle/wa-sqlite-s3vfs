import type { Config } from '@jest/types'

const localstackPreset = require('@thadeu/jest-localstack-preset/jest-preset')

const config: Config.InitialOptions = {
  ...localstackPreset,
  preset: 'ts-jest',
  testEnvironment: 'node',
  testPathIgnorePatterns: ['<rootDir>/build/', '<rootDir>/node_modules/'],
  collectCoverageFrom: ['<rootDir>/src/**/*.{ts,js}'],
  coveragePathIgnorePatterns: ['<rootDir>/build', '<rootDir>/node_modules'],
  transformIgnorePatterns: [
    'jest.localstack.js',
    'node_modules/(?!(wa-sqlite))'
  ],
  transform: {
    '^.+\\.(ts|tsx)$': 'ts-jest',
    '^.+\\.(js|jsx)$': 'babel-jest',
  },
}

export default config
