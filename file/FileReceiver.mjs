export const QUEUE_JOB_NAME = "PROCESS_FILE";

export class FileReceiver {
    constructor(fileManagers, queue) {
        this._fileManagers = fileManagers;
        this._queue = queue;
    }

    async receiveFile(info) {
        const {bucketname, bucket} = info;

        const file = await this._fileManagers[bucket]()
            .setFileAvailable(bucketname);

        if (file) {
            await this._queue.submit(QUEUE_JOB_NAME,
                {bucketname, bucket}, {jobId: bucketname});
            return true;
        }

        return false;
    }
}