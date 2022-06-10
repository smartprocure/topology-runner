import { describe, expect, test } from '@jest/globals'
import { missingKeys, findKeys } from './util'

describe('findKeys', () => {
  test('should find keys', () => {
    const data = {
      api: { status: 'completed' },
      details: { status: 'running' },
      attachments: { status: 'completed' },
    }
    const keys = findKeys({ status: 'completed' }, data)
    expect(keys).toEqual(['api', 'attachments'])
  })
})

describe('missingKeys', () => {
  test('should find keys', () => {
    const obj = {
      elastic: {},
      config: {},
    }
    const missing = missingKeys(['elastic', 'mongodb'], obj)
    expect(missing).toEqual(['mongodb'])
  })
})
