import { openDB } from 'idb'
import { Base } from './base.js'

const defaultConfig = {
  headerKeyPrefix: 'fp.'
}

/* global localStorage */

export class Browser extends Base {
  constructor (name, config = {}) {
    super(name, Object.assign({}, defaultConfig, config))
    this.isBrowser = false
    try {
      this.isBrowser = window.localStorage && true
    } catch (e) {}
  }

  withDB = async dbWorkFun => {
    if (!this.idb) {
      this.idb = await openDB(`fp.${this.keyId}.${this.name}.valet`, 3, {
        upgrade (db, oldVersion, newVersion, transaction) {
          if (oldVersion < 1) {
            db.createObjectStore('cars')
          }
        }
      })
    }
    return await dbWorkFun(this.idb)
  }

  async writeCars (cars) {
    if (this.config.readonly) return
    return await this.withDB(async db => {
      const tx = db.transaction(['cars'], 'readwrite')
      for (const { cid, bytes, replaces } of cars) {
        await tx.objectStore('cars').put(bytes, cid.toString())
        // todo remove old maps
        if (replaces) {
          await tx.objectStore('cars').delete(replaces.toString())
        }
      }
      return await tx.done
    })
  }

  async readCar (carCid) {
    return await this.withDB(async db => {
      const tx = db.transaction(['cars'], 'readonly')
      // console.log('getCarReader', carCid)
      return await tx.objectStore('cars').get(carCid)
    })
  }

  loadHeader (branch = 'main') {
    return this.isBrowser && localStorage.getItem(this.headerKey(branch))
  }

  async writeHeader (branch, header) {
    if (this.config.readonly) return
    return this.isBrowser && localStorage.setItem(this.headerKey(branch), this.prepareHeader(header))
  }

  headerKey (branch = 'main') {
    return this.config.headerKeyPrefix + this.name + '.' + branch
  }
}