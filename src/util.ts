import _ from 'lodash/fp'
import { ObjectOfPromises } from './types'

/**
 * Return the key of the first promise from the object that resolves.
 */
export const raceObject = async (obj: ObjectOfPromises): Promise<string> => {
  const objToArr = _.flow(
    _.toPairs,
    _.map(([key, prom]) => prom.then(() => key))
  )
  return Promise.race(objToArr(obj))
}

/**
 * Find keys where values match pred. pred can be a lodash iteratee.
 */
export const findKeys = (pred: string | object, obj: Record<string, any>) => {
  const iteratee = _.iteratee(pred)
  const keys = []
  for (const key in obj) {
    const val = obj[key]
    if (iteratee(val)) {
      keys.push(key)
    }
  }
  return keys
}

/**
 * Returns the keys from those passed that are missing in obj.
 */
export const missingKeys = (keys: string[], obj: object) =>
  _.flow(Object.keys, _.difference(keys))(obj)
