import { describe, expect, test } from '@jest/globals'
import { findKeys } from './util'

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
