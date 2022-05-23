import { describe, expect, test } from '@jest/globals'
import { setTimeout } from 'node:timers/promises'
import { missingKeys, findKeys, raceObject } from './util'
import { ObjectOfPromises } from './types'

describe('raceObject', () => {
  test('return key of first resolved', async () => {
    const promises: ObjectOfPromises = {
      tom: setTimeout(250),
      joe: setTimeout(350),
      frank: setTimeout(100),
    }
    expect(await raceObject(promises)).toBe('frank')
  })
  test('return key of first rejected', async () => {
    const promises: ObjectOfPromises = {
      tom: setTimeout(250),
      joe: Promise.reject('fail'),
      frank: setTimeout(100),
    }
    await expect(raceObject(promises)).rejects.toBe('fail')
  })
})

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
