import {Readable} from 'stream';
import crypto from "crypto";

export class MemoryFileStore {
    #store;
    #baseUrl;

    constructor(baseUrl) {
        this.#store = new Map();
        this.#baseUrl = baseUrl;
    }

    clear() {
        this.#store.clear();
    }

    getObjectInfo(bucketname) {
        return {
            size: this.#store.get(bucketname)?.length,
            hash: crypto.createHash('md5').update(this.#store.get(bucketname) || '').digest('hex')
        };
    }

    getPostUrl(bucketname) {
        return `${this.#baseUrl}${bucketname}`;
    }

    async createObject(Key, Body) {
        this.#store.set(Key, Body);
    }

    async getObject(Key) {
        const file = this.#store.get(Key);
        if (!file) return null;

        const stream = new Readable();
        stream.push(file);
        stream.push(null);

        return {
            stream,
            ...this.getObjectInfo(Key),
            creation: new Date()
        }
    }

    async getObjectStream(Key) {
        return this.getObject(Key).then(obj => obj?.stream);
    }

    async deleteObjects(keys) {
        await Promise.all(keys.map(key => this.deleteObject(key)));
        return true;
    }

    async listObjects() {
        return Array.from(this.#store.keys());
    }

    async deleteObject(Key) {
        if (this.#store.has(Key)) {
            this.#store.delete(Key);
            return true;
        }
        return false;
    }

    async checkObjectExists(Key) {
        return this.#store.has(Key);
    }

    async close() {
    }
}