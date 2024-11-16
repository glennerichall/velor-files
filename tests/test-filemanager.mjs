import {setupTestContext} from "velor-utils/test/setupTestContext.mjs";
import sinon from "sinon";
import {FileManager} from "../file/FileManager.mjs";
import {setLogger} from "velor-services/injection/services.mjs";

const {
    expect,
    describe,
    beforeEach,
    afterEach,
    it
} = setupTestContext();


describe('FileManager', () => {
    let fileManager, mockFileStore, mockAllocTable;

    beforeEach(() => {
        mockFileStore = {
            getPostUrl: sinon.stub(),
            getSignedUrl: sinon.stub(),
            deleteObjects: sinon.stub(),
            getObject: sinon.stub(),
            listObjects: sinon.stub(),
            getObjectInfo: sinon.stub()
        };

        mockAllocTable = {
            transact: sinon.stub(),
            open: sinon.stub()
        };

        fileManager = new FileManager(mockAllocTable, mockFileStore);
    });

    describe('createEntry', () => {
        it('should create an entry and return upload URL', async () => {
            const entry = {bucketname: 'test-bucket'};
            const createEntryStub = sinon.stub().resolves(entry);

            mockAllocTable.transact.callsFake(async (callback) => await callback({createEntry: createEntryStub}));
            mockFileStore.getPostUrl.resolves('upload-url');

            const result = await fileManager.createEntry('arg1', 'arg2');

            expect(result).to.have.property('entry', entry);
            expect(result).to.have.property('uploadURL', 'upload-url');

            expect(createEntryStub.calledWith('arg1', 'arg2')).to.be.true;
        });

        it('should handle error when transact fails', async () => {
            mockAllocTable.transact.rejects(new Error('Transact failed'));

            await expect(fileManager.createEntry('arg1')).to.be.rejectedWith('Transact failed');
        });
    });

    describe('getFileSignedUrl', () => {
        it('should return signed URL for a given bucket name', async () => {
            mockFileStore.getSignedUrl.resolves('signed-url');

            const result = await fileManager.getFileSignedUrl('bucketname');

            expect(result).to.equal('signed-url');
        });
    });

    describe('deleteFiles', () => {
        it('should delete files and return entries when successful', async () => {
            const deleteEntriesStub = sinon.stub().resolves(['entry1']);
            mockAllocTable.transact.callsFake(async (callback) => await callback({deleteEntries: deleteEntriesStub}));
            mockFileStore.deleteObjects.resolves(true);

            const result = await fileManager.deleteFiles('bucket1', 'bucket2');

            expect(result).to.deep.equal(['entry1']);
            expect(deleteEntriesStub.calledWith(['bucket1', 'bucket2'])).to.be.true;
            expect(mockFileStore.deleteObjects.calledWith(['bucket1', 'bucket2'])).to.be.true;
        });

        it('should throw an error if file deletion from store fails', async () => {
            const deleteEntriesStub = sinon.stub().resolves(['entry1']);
            mockAllocTable.transact.callsFake(async (callback) => await callback({deleteEntries: deleteEntriesStub}));
            mockFileStore.deleteObjects.resolves(false);

            await expect(fileManager.deleteFiles('bucket1', 'bucket2')).to.be.rejectedWith('unable to delete file from store');
            expect(deleteEntriesStub.calledWith(['bucket1', 'bucket2'])).to.be.true;
            expect(mockFileStore.deleteObjects.calledWith(['bucket1', 'bucket2'])).to.be.true;
        });
    });


    describe('cleanFileStore', () => {
        it('should delete files from file store not present in database', async () => {
            const mockEntries = [{bucketname: 'keep-file'}];
            const getAllEntriesStub = sinon.stub().resolves(mockEntries);
            mockAllocTable.transact.callsFake(async (callback) => await callback({getAllEntries: getAllEntriesStub}));
            mockFileStore.listObjects.resolves(['keep-file', 'remove-file']);
            mockFileStore.deleteObjects.resolves(true);

            await fileManager.cleanFileStore();

            expect(mockFileStore.deleteObjects.calledWith(['remove-file'])).to.be.true;
            expect(getAllEntriesStub.calledOnce).to.be.true;
            expect(mockFileStore.listObjects.calledOnce).to.be.true;
        });

        it('should handle error when unable to list files from file store', async () => {
            const getAllEntriesStub = sinon.stub().resolves([]);
            mockAllocTable.transact.callsFake(async (callback) => await callback({getAllEntries: getAllEntriesStub}));
            mockFileStore.listObjects.resolves(null);

            await fileManager.cleanFileStore();

            expect(mockFileStore.deleteObjects.called).to.be.false;
            expect(getAllEntriesStub.calledOnce).to.be.true;
            expect(mockFileStore.listObjects.calledOnce).to.be.true;
        });
    });


    describe('cleanDatabase', () => {
        it('should delete database entries not in file store', async () => {
            mockFileStore.listObjects.resolves(['keep-file']);
            mockAllocTable.transact.resolves({keepEntries: sinon.stub().resolves(1)});

            await fileManager.cleanDatabase();

            expect(mockAllocTable.transact.calledOnce).to.be.true;
        });

        it('should delete all entries if file store is empty', async () => {
            mockFileStore.listObjects.resolves([]);
            mockAllocTable.transact.resolves({deleteAllEntries: sinon.stub().resolves(5)});

            await fileManager.cleanDatabase();

            expect(mockAllocTable.transact.calledOnce).to.be.true;
        });
    });

    describe('processFile', () => {
        it('should return not found status if file is not in the file store', async () => {
            mockAllocTable.open.resolves({getEntry: sinon.stub().resolves(null)});

            const result = await fileManager.processFile('non-existent-bucket');

            expect(result).to.deep.equal({
                status: 'ERROR_FILE_NOT_FOUND',
                bucketname: 'non-existent-bucket'
            });
        });

        it('should process file if valid and set status to ready', async () => {
            const entry = {status: 'pending', bucketname: 'test-bucket'};
            const file = {size: 100, hash: 'hash123'};

            mockAllocTable.open.resolves({
                getEntry: sinon.stub().resolves(entry),
                setReady: sinon.stub()
            });
            mockFileStore.getObject.resolves(file);
            mockFileStore.getObjectInfo.resolves({size: 100, hash: 'hash123'});

            const result = await fileManager.processFile('test-bucket');

            expect(result.status).to.equal('SUCCESS_FILE_PROCESSED');
            expect(result.entry).to.deep.equal(entry);
        });

        it('should set file as rejected if it is invalid', async () => {
            const entry = {status: 'pending', bucketname: 'test-bucket'};
            const file = {size: 100, hash: 'hash123'};

            mockAllocTable.open.resolves({
                getEntry: sinon.stub().resolves(entry),
                setRejected: sinon.stub()
            });
            mockFileStore.getObject.resolves(file);

            sinon.stub(fileManager, '_validateFile').resolves('ERROR_FILE_INVALID');

            const result = await fileManager.processFile('test-bucket');

            expect(result.status).to.equal('ERROR_FILE_INVALID');
        });
    });

    describe('FileManager - Logging Tests', () => {
        let loggerSpy;

        beforeEach(() => {

            loggerSpy = {
                info: sinon.spy(),
                debug: sinon.spy(),
                error: sinon.spy()
            };

            setLogger(fileManager, loggerSpy);

        });

        afterEach(() => {
            sinon.restore();
        });

        describe('cleanFileStore - Logging', () => {
            it('should log info when cleaning file store', async () => {
                const getAllEntriesStub = sinon.stub().resolves([]);
                mockAllocTable.transact.callsFake(async (callback) => await callback({ getAllEntries: getAllEntriesStub }));
                mockFileStore.listObjects.resolves([]);

                await fileManager.cleanFileStore();

                expect(loggerSpy.info.calledWith('Removing files from file store not in database')).to.be.true;
                expect(loggerSpy.info.calledWith('No file in database, removing all files from file store')).to.be.true;
                expect(loggerSpy.info.calledWith('No file in file store')).to.be.true;
                expect(getAllEntriesStub.calledOnce).to.be.true;
                expect(mockFileStore.listObjects.calledOnce).to.be.true;
            });

            it('should log file removal information', async () => {
                const getAllEntriesStub = sinon.stub().resolves([{ bucketname: 'keep-file' }]);
                mockAllocTable.transact.callsFake(async (callback) => await callback({ getAllEntries: getAllEntriesStub }));
                mockFileStore.listObjects.resolves(['keep-file', 'remove-file']);
                mockFileStore.deleteObjects.resolves(true);

                await fileManager.cleanFileStore();

                expect(loggerSpy.info.calledWith('Removing files from file store not in database')).to.be.true;
                expect(loggerSpy.info.calledWith('1 file(s) in database')).to.be.true;
                expect(loggerSpy.info.calledWith('Removing 1 file(s) from file store')).to.be.true;
                expect(getAllEntriesStub.calledOnce).to.be.true;
                expect(mockFileStore.listObjects.calledOnce).to.be.true;
                expect(mockFileStore.deleteObjects.calledWith(['remove-file'])).to.be.true;
            });
        });


        describe('processFile - Logging', () => {
            it('should log when processing a file', async () => {
                const entry = {bucketname: 'test-bucket', status: 'pending'};
                mockAllocTable.open.resolves({
                    getEntry: sinon.stub().resolves(entry),
                    setReady: sinon.stub()
                });
                mockFileStore.getObject.resolves({size: 100, hash: 'hash123'});
                mockFileStore.getObjectInfo.resolves({size: 100, hash: 'hash123'});

                await fileManager.processFile('test-bucket');

                expect(loggerSpy.debug.calledWith('Processing file test-bucket')).to.be.true;
                expect(loggerSpy.debug.calledWith('File test-bucket processed successfully')).to.be.true;
            });

            it('should log if file is flagged as invalid', async () => {
                const entry = {bucketname: 'invalid-file', status: 'pending'};
                const file = {size: 100, hash: 'hash123'};
                mockAllocTable.open.resolves({
                    getEntry: sinon.stub().resolves(entry),
                    setRejected: sinon.stub()
                });
                mockFileStore.getObject.resolves(file);
                sinon.stub(fileManager, '_validateFile').resolves('ERROR_FILE_INVALID');

                await fileManager.processFile('invalid-file');

                expect(loggerSpy.debug.calledWith('File invalid-file flagged as invalid, setting it as rejected')).to.be.true;
            });
        });

        describe('cleanDatabase - Logging', () => {
            it('should log database cleaning info', async () => {
                const keepEntriesStub = sinon.stub().resolves(0);
                mockFileStore.listObjects.resolves(['keep-file']);
                mockAllocTable.transact.callsFake(async (callback) => await callback({ keepEntries: keepEntriesStub }));

                await fileManager.cleanDatabase();

                expect(loggerSpy.info.calledWith('Cleaning database for files not in file store')).to.be.true;
                expect(loggerSpy.info.calledWith('Listing files from file store')).to.be.true;
                expect(loggerSpy.info.calledWith('No file to remove, all clean')).to.be.true;
                expect(keepEntriesStub.calledOnce).to.be.true;
                expect(mockFileStore.listObjects.calledOnce).to.be.true;
            });
        });

    });

});
