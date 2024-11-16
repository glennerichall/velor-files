import {tryInsertUnique} from "velor-utils/database/database.mjs";

export async function queryFilesByHash(client, schema, hash) {
    const res = await client.query(`
        select * from ${schema}.files
        where files.hash = $1
    `, [hash]);

    return res.rows[0];
}

export async function createFile(client, schema, bucket, bucketname) {
    if (bucketname) {
        const res = await client
            .query(`insert into ${schema}.files
                    (bucket, bucketname)
                    values ($1, $2)
                    returning *`,
                [bucket, bucketname]);
        if (res.rowCount === 1) {
            return res.rows[0];
        }
        return null;
    }
    return tryInsertUnique(client, `insert into ${schema}.files
                    (bucket, bucketname)
                    values ($1, gen_random_uuid())
                    returning *`, [bucket]);
}

export async function updateSetUploaded(client, schema, bucketname) {
    const res = await client
        .query(`update ${schema}.files
                    set status = 'uploaded'::${schema}.filestatus
                    where bucketname = $1
                      and status = 'created'::${schema}.filestatus
                    returning *`,
            [bucketname]);
    if (res.rowCount === 1) {
        return res.rows[0];
    }
    return null;
}

// Worker
export async function updateSetDatetime(client, schema, bucketname, creation) {
    const res = await client
        .query(`update ${schema}.files
                    set creation = $2
                    where bucketname = $1`,
            [bucketname, creation]);
    return res.rowCount;
}


export async function updateSetStatus(client, schema, bucketname, size, hash, status) {
    const res = await client
        .query(`update ${schema}.files
                    set status = $3::${schema}.filestatus,
                        size   = COALESCE($2, size),
                        hash   = COALESCE($4, hash)
                    where bucketname = $1`,
            [bucketname, size, status, hash]);
    return res.rowCount;
}

export async function queryFilesForAll(client, schema, bucket) {
    const res = await client
        .query(`select * from ${schema}.files
                    where bucket = $1`,
            [bucket]);
    return res.rows;
}

export async function deleteByBucketname(client, schema, bucketname) {
    const res = await client
        .query(`delete from ${schema}.files
                    where bucketname = $1`,
            [bucketname]);
    return res.rowCount;
}

export async function deleteAllFiles(client, schema, bucket) {
    const res = await client
        .query(`delete
                   from ${schema}.files
                   where bucket = $1`,
            [bucket]);
    return res.rowCount;
}

export async function deleteOldFiles(client, schema, bucket, numDays) {
    const res = await client
        .query(`delete from ${schema}.files
                where bucket = $2 and
                    (status = 'uploading'::${schema}.filestatus
                    or status = 'created'::${schema}.filestatus)
                  and DATE_PART('day', current_timestamp - creation) >= $1
                returning *`,
            [numDays, bucket]);
    return res.rowCount;
}

export async function queryForUnprocessed(client, schema, numDays) {
    const res = await client.query(
        `select *
            from ${schema}.files
            where (status = 'uploading'::${schema}.filestatus
                or status = 'created'::${schema}.filestatus
                or status = 'uploaded'::${schema}.filestatus
                )
              and (
                    DATE_PART('day', current_timestamp - creation) >= $1
                    or creation is NULL
                )
            order by id`,
        [numDays]);
    return res.rows;
}


export async function deleteByBucketnames(client, schema, bucketnames) {
    const res = await client
        .query(`delete
                   from ${schema}.files
                   where bucketname = any ($1::text[])`,
            [bucketnames]);
    return res.rowCount;
}


export async function keepByBucketnames(client, schema, bucketnames) {
    const res = await client
        .query(`delete
                   from ${schema}.files
                   where bucketname <> all ($1::text[])`,
            [bucketnames]);
    return res.rowCount;
}


// Worker
export async function updateSetDone(client, schema, bucketname, size, hash) {
    return updateSetStatus(client, schema, bucketname, size, hash, 'ready');
}

// Worker
export async function updateSetRejected(client, schema, bucketname, size, hash) {
    return updateSetStatus(client, schema, bucketname, size, hash, 'rejected');
}

