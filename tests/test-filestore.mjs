import {AWSFileStore} from '../file/AWSFileStore.mjs';

import {setupTestContext} from "velor-utils/test/setupTestContext.mjs";
import {
    AWS_ACCESS_KEY_ID,
    AWS_BUCKET,
    AWS_REGION,
    AWS_SECRET_ACCESS_KEY
} from "../application/services/filesEnvKeys.mjs";
import dotenv from 'dotenv';
import {streamToString} from "velor-utils/utils/string.mjs";
import {MemoryFileStore} from "../file/MemoryFileStore.mjs";

const {
    expect,
    test
} = setupTestContext();


dotenv.config();

test.describe('FileStore', () => {


    test.describe('MemoryFileStore', () => {
        const fileStore = new MemoryFileStore();

        test("Checks if object exists.", async () => {
            const key = 'nonexistent_key';
            const exists = await fileStore.checkObjectExists(key);
            expect(exists).to.be.false;
        });

        test('Create and get an object.', async () => {
            const key = 'test_key';
            const body = 'test_body';
            await fileStore.createObject(key, body);

            const objectResult = await fileStore.getObject(key);

            expect(objectResult).to.have.property('size', 9);
            expect(objectResult).to.have.property('hash', '3a37d03e20b3b3245b460349de1e4057');
            expect(objectResult.creation).to.be.a('Date');

            let content = await streamToString(objectResult.stream);
            expect(content).to.equal(body);
        });

        test('List all objects in the bucket.', async () => {
            await fileStore.createObject('key1', 'test_body');
            await fileStore.createObject('key2', 'test_body');
            const allObjects = await fileStore.listObjects();
            expect(allObjects.length).to.be.above(0);
        });

        test('Delete a single object.', async () => {
            const key = 'test_key_to_delete';
            await fileStore.createObject(key, 'test_body');

            const deleteResult = await fileStore.deleteObject(key);
            expect(deleteResult).to.be.true;
        });
    })

    test.describe('AWSFileStore', () => {
        const fileStore = new AWSFileStore(process.env[AWS_BUCKET], {
            accessKeyId: process.env[AWS_ACCESS_KEY_ID],
            secretAccessKey: process.env[AWS_SECRET_ACCESS_KEY],
            region: process.env[AWS_REGION],
        });

        test("Checks if object exists.", async () => {
            const key = 'nonexistent_key';
            const exists = await fileStore.checkObjectExists(key);
            expect(exists).to.be.false;
        });

        test('Create and get an object.', async () => {
            const key = 'test_key';
            const body = 'test_body';
            await fileStore.createObject(key, body);

            const objectResult = await fileStore.getObject(key);

            expect(objectResult).to.have.property('size', 9);
            expect(objectResult).to.have.property('hash', '3a37d03e20b3b3245b460349de1e4057');
            expect(objectResult.creation).to.be.a('Date');

            let content = await streamToString(objectResult.stream);
            expect(content).to.equal(body);
        });

        test('List all objects in the bucket.', async () => {
            const allObjects = await fileStore.listObjects();
            expect(allObjects.length).to.be.above(0);
        });

        test('Delete a single object.', async () => {
            const key = 'test_key_to_delete';
            await fileStore.createObject(key, 'test_body');

            const deleteResult = await fileStore.deleteObject(key);
            expect(deleteResult).to.be.true;
        });
    })
});