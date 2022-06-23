import _ from 'lodash/fp'

/**
 * Find keys where values match pred. pred can be a lodash iteratee.
 */
export function findKeys<A = any>(pred: string | object, obj: Record<string, A>) {
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
