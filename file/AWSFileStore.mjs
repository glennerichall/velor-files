import {
    CopyObjectCommand,
    DeleteObjectCommand,
    DeleteObjectsCommand,
    GetObjectCommand,
    HeadObjectCommand,
    ListObjectsV2Command,
    PutObjectCommand,
    S3Client
} from "@aws-sdk/client-s3";
import {getSignedUrl} from "@aws-sdk/s3-request-presigner";

// Change this value to adjust the signed URL's expiration
const URL_EXPIRATION_SECONDS = 60 * 1; // 1 minutes to upload or download

function handleError(e) {
    if (e.Code === "AccessDenied") {
        throw e;
    }
}

export class AWSFileStore {
    constructor(bucket, options) {
        this._s3client = null;
        this._bucket = bucket;

        const {
            accessKeyId,
            secretAccessKey,
            region
        } = options;

        this._accessKeyId = accessKeyId;
        this._secretAccessKey = secretAccessKey;
        this._region = region;

    }

    async getPostUrl(Key) {
        // Get signed URL from S3
        const s3Params = {
            Bucket: this._bucket,
            Key,
            ContentType: 'gcode',
            ACL: 'private'
        };

        // FIXME on a besoin d'un mecanisme de codification des exceptions
        const command = new PutObjectCommand(s3Params);
        const uploadURL = await getSignedUrl(this.getClient(), command, {
            expiresIn: URL_EXPIRATION_SECONDS
        });

        return uploadURL;
    }

    async getSignedUrl(Key) {
        const s3Params = {
            Bucket: this._bucket,
            Key,
            ContentType: 'gcode',
            ACL: 'private'
        };
        const command = new GetObjectCommand(s3Params);
        const downloadURL = await getSignedUrl(this.getClient(), command, {
            expiresIn: URL_EXPIRATION_SECONDS
        });

        return downloadURL;
    }

    getClient() {
        if (this._s3client === null) {
            // On heroku, setting these values through constructor options
            // does not work.
            // {
            //      accessKeyId: process.env.DEPLOYING_VALUE_AWS_ACCESS_KEY_ID,
            //      secretAccessKey: process.env.DEPLOYING_VALUE_AWS_SECRET_ACCESS_KEY
            // }
            // It is preferable to set it here because it creates a more uniform
            // procedure using ZUPFE_* to set env variables in heroku using a script.
            process.env.AWS_ACCESS_KEY_ID = this._accessKeyId;
            process.env.AWS_SECRET_ACCESS_KEY = this._secretAccessKey;

            this._s3client = new S3Client(
                {
                    region: this._region
                });
        }
        return this._s3client;
    }

    async createObject(Key, Body) {
        try {
            const command = new PutObjectCommand({
                Bucket: this._bucket,
                Key,
                Body
            });
            const response = await this.getClient().send(command)
            return response.Body;
        } catch (e) {
            handleError(e);
            return null;
        }
    }

    async getObject(Key) {
        try {
            const command = new GetObjectCommand({
                Bucket: this._bucket,
                Key
            });
            const response = await this.getClient().send(command);
            return {
                stream: response.Body,
                size: response.ContentLength,
                hash: JSON.parse(response.ETag),
                creation: response.LastModified
            };
        } catch (e) {
            handleError(e);
            return null;
        }
    }

    async getObjectInfo(Key) {
        try {
            const command = new HeadObjectCommand({
                Bucket: this._bucket,
                Key
            });
            const response = await this.getClient().send(command)
            return {
                size: response.ContentLength,
                hash: JSON.parse(response.ETag),
                creation: response.LastModified
            };
        } catch (e) {
            handleError(e);
            return null;
        }
    }

    async getObjectStream(Key) {
        return this.getObject(Key).then(obj => obj?.stream);
    }

    async putObject(Key, data, options = {}) {
        const command = new PutObjectCommand({
            Bucket: this._bucket,
            region: this._region,
            Key,
            ContentType: options.contentType,
            Body: data
        });
        await this.getClient().send(command);
    }

    async deleteObject(Key) {
        try {
            const command = new DeleteObjectCommand({
                Bucket: this._bucket,
                Key
            });
            const response = await this.getClient().send(command)
            return true;
        } catch (e) {
            handleError(e);
            return null;
        }
    }

    async deleteObjects(keys) {
        try {
            const command = new DeleteObjectsCommand(
                {
                    Bucket: this._bucket,
                    Delete: {
                        Objects: keys.map(Key => {
                            return {Key}
                        })
                    }
                }
            );
            await this.getClient().send(command);
            return true;
        } catch (e) {
            handleError(e);
            return false;
        }
    }

    async listObjects() {
        try {
            let continuationToken;
            let allObjects = [];
            let client = await this.getClient();

            do {
                const command = new ListObjectsV2Command({
                    Bucket: this._bucket,
                    ContinuationToken: continuationToken,
                });

                const response = await client.send(command);
                const objects = response.Contents.map(x => {
                    return {
                        ...x,
                        ETag: x.ETag.replaceAll('\"', '')
                    };
                });

                allObjects = allObjects.concat(objects);

                continuationToken = response.NextContinuationToken;
            } while (continuationToken);

            return allObjects;
        } catch (e) {
            handleError(e);
            return null;
        }
    }

    async readAll() {
        const list = await this.listObjects();
        return list.map(x => x.Key)
            .map(async Key => {
                const object = await this.getObject(Key);
                return {
                    name: Key,
                    data: object.stream
                };
            });
    }

    async checkObjectExists(Key) {
        try {
            const command = new HeadObjectCommand({
                Bucket: this._bucket,
                Key
            });
            await this.getClient().send(command)
            return true;
        } catch (e) {
            handleError(e);
            if (e.name === 'NotFound') {
                return false;
            } else {
                throw e;
            }
        }
    }

    close() {
        this._s3client?.destroy();
        this._s3client = null;
    }

    async copyObjects(objects, bucket) {
        const client = this.getClient();
        for (let key of objects) {
            const command = new CopyObjectCommand({
                CopySource: `/${bucket}/${key}`,
                Bucket: this._bucket,
                Key: key,
            });

            try {
                await client.send(command);
            } catch (error) {
                throw error;
            }
        }
    }
}