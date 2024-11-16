import {
    ERROR_FILE_ALREADY_PROCESSED,
    ERROR_FILE_INFECTED,
    ERROR_FILE_INVALID,
    ERROR_FILE_NOT_FOUND,
    ERROR_FILE_UPLOAD_FAILED,
    SUCCESS_FILE_PROCESSED,
    SUCCESS_FILE_VALIDATED
} from "./errors.mjs";

import {getBaseServicesProvider} from 'velor-services/injection/basePolicy.mjs';

export const FileManagerPolicy = policy => {

    const {
        getLogger
    } = getBaseServicesProvider(policy);


    return class FileManager {
        #fileStore;
        #allocTable;

        constructor(alloctable, filestore) {
            this.#fileStore = filestore;
            this.#allocTable = alloctable;
        }

        get fileStore() {
            return this.#fileStore;
        }

        async createEntry(...args) {
            return this.#allocTable.transact(async ({createEntry}) => {
                const entry = await createEntry(...args);
                const uploadURL = await this.#fileStore.getPostUrl(entry.bucketname);
                return {
                    entry,
                    uploadURL
                };
            });
        }

        async getFileSignedUrl(bucketname) {
            return this.#fileStore.getSignedUrl(bucketname);
        }

        async deleteFiles(...bucketnames) {
            return this.#allocTable.transact(async ({deleteEntries}) => {
                const entries = await deleteEntries(bucketnames);
                const ok = await this.#fileStore.deleteObjects(bucketnames);
                if (!ok) {
                    throw new Error("unable to delete file from store");
                }
                return entries;
            });
        }

        async removeFiles(...bucketnames) {
            const {removeEntries} = await this.#allocTable.open();
            return removeEntries(bucketnames);
        }

        async setFileAvailable(bucketname) {
            getLogger(this).debug(`Setting file available ${bucketname}`);
            const {setAvailable} = await this.#allocTable.open();
            return setAvailable(bucketname);
        }

        async setFileStatus(bucketname, status, size, hash) {
            const {setStatus} = await this.#allocTable.open();
            return setStatus(bucketname, status, size, hash);
        }

        async setFileRejected(bucketname, size, hash) {
            getLogger(this).debug(`Setting file rejected ${bucketname}`);
            const {setRejected} = await this.#allocTable.open();
            return setRejected(bucketname, size, hash);
        }

        async getEntries(bucketnames) {
            const {getEntries, getAllEntries} = await this.#allocTable.open();
            return bucketnames ? getEntries(bucketnames) : getAllEntries();
        }

        async getEntriesByHash(hash) {
            const {getEntriesByHash} = await this.#allocTable.open();
            return getEntriesByHash(hash);
        }

        async readFile(bucketname) {
            return this.#fileStore.getObject(bucketname);
        }

        async updateCreationTime(bucketname, datetime = new Date()) {
            const {setCreation} = await this.#allocTable.open();
            return setCreation(bucketname, datetime);
        }

        async cleanFileStore() {

            getLogger(this).info(`Removing files from file store not in database`);

            await this.#allocTable.transact(async ({getAllEntries}) => {

                getLogger(this).info('Getting all files from database');

                const entries = await getAllEntries();
                const bucketnames = entries.map(x => x.bucketname);

                if (entries.length > 0) {
                    getLogger(this).info(`${bucketnames.length} file(s) in database`);
                } else {
                    getLogger(this).info('No file in database, removing all files from file store');
                }

                let files = await this.#fileStore.listObjects();
                if (files === null) {
                    getLogger(this).error(`Unable to list files from file store`);
                    return;
                }


                if (files.length > 0) {
                    getLogger(this).info(`${files.length} file(s) in file store`);
                } else {
                    getLogger(this).info('No file in file store');
                }


                // find bucket files not in database
                const toRemove = [];
                for (let file of files) {
                    if (!bucketnames.includes(file)) {
                        toRemove.push(file);
                    }
                }

                // remove them from file store
                if (toRemove.length > 0) {
                    getLogger(this).info(`Removing ${toRemove.length} file(s) from file store`)

                    const ok = await this.#fileStore.deleteObjects(toRemove);
                    if (!ok) {
                        getLogger(this).error(`Unable to remove ${toRemove.length} file(s) from file store`);
                    }
                } else {
                    getLogger(this).info(`No file to remove, all clean`);
                }
            });
        }

        async cleanDatabase() {

            getLogger(this).info('Cleaning database for files not in file store');
            getLogger(this).info('Listing files from file store');

            const bucketnames = await this.#fileStore.listObjects();
            if (bucketnames === null) {
                getLogger(this).error('Unable to list files from file store');
            }

            await this.#allocTable.transact(async ({deleteAllEntries, keepEntries}) => {
                let result;

                // remove database files not in file store bucket
                if (bucketnames.length === 0) {
                    getLogger(this).info('No files in file store, purging all files from database');

                    result = await deleteAllEntries();
                } else {
                    result = await keepEntries(bucketnames);
                }

                if (result > 0) {
                    getLogger(this).info(`Removed ${result} files from database`);
                } else {
                    getLogger(this).info('No file to remove, all clean');
                }
            });
        }

        async cleanOldFiles({numDays = 3} = {}) {
            getLogger(this).info(`Cleaning database from old files not uploaded since ${numDays} day(s)`);

            await this.#allocTable.transact(async ({deleteOldEntries}) => {
                const result = await deleteOldEntries(numDays);
                if (result > 0) {
                    getLogger(this).info(`Deleted ${result} file(s) from database that where not uploaded more than ${numDays} days ago`);
                } else {
                    getLogger(this).info('No file to remove, all clean');
                }
            });
        }

        async processMissedNewFiles({numDays = 3} = {}) {
            // process files that were not processed for validation
            getLogger(this).info(`Processing files that were not processed for validation since ${numDays} day(s)`);

            // Do not run in a transaction as this takes a long time
            // and we want every file to be updated in the database
            // file by file and not in batch so if it fails somehow
            // we do not need to update from beginning.

            const {getUnprocessedEntries} = await this.#allocTable.open();
            const entries = await getUnprocessedEntries(numDays);

            if (entries.length > 0) {
                getLogger(this).info(`Starting process of ${entries.length} pending file(s)`);
            } else {
                getLogger(this).info(`Not file to process, all clean`);
            }

            let accepted = [], rejected = [], notFound = [];
            let i = 0;
            for (let {bucketname} of entries) {
                i++;

                const {status} = await this.processFile(bucketname);

                switch (status) {
                    case SUCCESS_FILE_PROCESSED:
                        accepted.push(bucketname);
                        break;
                    case ERROR_FILE_UPLOAD_FAILED:
                    case ERROR_FILE_NOT_FOUND:
                        notFound.push(bucketname);
                        break;
                    case ERROR_FILE_INFECTED:
                    case ERROR_FILE_INVALID:
                        rejected.push(bucketname);
                        break;
                }

                getLogger(this).info(`(${i}/${entries.length})\t\t${bucketname}\t${status}`);
            }

            if (entries.length > 0) {
                getLogger(this).info(`Processed ${entries.length} file(s) with ${accepted.length} accepted file(s) and ${rejected.length} rejected file(s)`);
            }
        }

        async _validateFile(entry, file) {
            return SUCCESS_FILE_VALIDATED;
        }

        async _processFile(entry, file) {
            return SUCCESS_FILE_PROCESSED;
        }

        async processFile(bucketname) {
            const {
                deleteEntry, setRejected,
                setReady, getEntry
            } = await this.#allocTable.open();

            getLogger(this).debug(`Processing file ${bucketname}`);
            let entry = await getEntry(bucketname);

            if (!entry) {
                getLogger(this).debug(`File ${bucketname} not found in entries`);
                return {
                    status: ERROR_FILE_NOT_FOUND,
                    bucketname
                };
            }

            if (entry.status === 'ready' || entry.status === 'rejected') {
                getLogger(this).debug(`File ${bucketname} already processed`);
                return {
                    status: ERROR_FILE_ALREADY_PROCESSED,
                    entry
                };
            }

            const file = await this.#fileStore.getObject(bucketname);

            if (file === null) {
                getLogger(this).debug(`File ${bucketname} not found in file store, deleting entry`);
                await deleteEntry(bucketname);
                return {
                    status: ERROR_FILE_NOT_FOUND,
                    entry
                };
            }

            let status = await this._validateFile(entry, file);

            switch (status) {
                case ERROR_FILE_INFECTED:
                case ERROR_FILE_INVALID:
                    getLogger(this).debug(`File ${bucketname} flagged as invalid, setting it as rejected`);
                    await setRejected(bucketname, file.size, file.hash);
                    return {
                        status,
                        entry
                    };
            }

            status = await this._processFile(entry, file);

            // the size and hash may have changed after processing.
            const info = await this.#fileStore.getObjectInfo(bucketname);

            if (status === SUCCESS_FILE_PROCESSED) {
                getLogger(this).debug(`File ${bucketname} processed successfully`);
                await setReady(bucketname, info.size, info.hash);
                entry = await getEntry(bucketname);
            } else {
                getLogger(this).debug(`File ${bucketname} processed with errors`);
            }

            return {
                status,
                entry
            };
        }

    }
}

export const FileManager = FileManagerPolicy();