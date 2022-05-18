import _ from 'lodash/fp'
import { ObjectOfPromises } from './types'

/**
 * Return the first promise from the object that resolves.
 * Rejections are not handled.
 */
export const raceObject = async (promises: ObjectOfPromises) => {
  const [key] = await Promise.race(_.toPairs(promises))
  return key
}
