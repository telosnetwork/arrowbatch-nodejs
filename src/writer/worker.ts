import fs from "node:fs";
import path from "node:path";
import {finished} from "node:stream/promises";
import { parentPort, workerData } from 'node:worker_threads';

import {RecordBatch, RecordBatchFileWriter, Table, tableFromArrays} from "apache-arrow";
import {format, LogEntry, Logger, loggers} from "winston";

import {
    ArrowBatchCompression,
    ArrowBatchProtocol,
    ArrowTableMapping,
    DEFAULT_STREAM_BUF_MEM,
    encodeRowValue,
    getArrayFor
} from "../protocol.js";

import {compressUint8Array, MemoryWriteStream, WorkerTransport} from "../utils.js";

export interface WriterControlRequest {
    tid: number
    method: 'addRow' | 'flush' | 'trim'
    params?: any
}

export interface WriterControlResponse {
    tid: number,
    method: string,
    status: 'ok' | 'error',
    extra?: any,
    error?: any
}

let {
    alias, tableMapping, compression, logLevel, streamBufMem
}: {

    alias: string,
    tableMapping: ArrowTableMapping[],
    compression:  ArrowBatchCompression,

    logLevel: string,
    streamBufMem: number

} = workerData;

const loggingOptions = {
    exitOnError: false,
    level: logLevel,
    format: format.json(),
    transports: [
        new WorkerTransport(
            (log: LogEntry) => {
                parentPort.postMessage({
                    method: 'workerLog',
                    log
                });
            },
            {})
    ]
}
const logger: Logger = loggers.add(`writer-internal-${alias}`, loggingOptions);

const streamBuffer = Buffer.alloc(streamBufMem);
logger.info(`write stream buffer of ${streamBufMem.toLocaleString()} bytes allocated!`);

const intermediateBuffers = {};
let intermediateSize = 0;
function _initBuffer() {
    intermediateSize = 0;
    for (const mapping of tableMapping)
        intermediateBuffers[mapping.name] = [];
    logger.debug(`initialized buffers for ${Object.keys(intermediateBuffers)}`);
}

function _generateArrowBatchTable(): Table {
    const arrays = {};
    for (const mapping of tableMapping)
        arrays[mapping.name] = getArrayFor(mapping).from(intermediateBuffers[mapping.name]);
    try {
        return tableFromArrays(arrays);
    } catch (e) {
        console.log('lol');
        throw e;
    }
}

function _initDiskBuffer(currentFile: string) {
    fs.writeFileSync(
        currentFile,
        ArrowBatchProtocol.newGlobalHeader()
    );
    logger.debug(`wrote global header on ${currentFile}`);
}

async function serializeTable(table: Table): Promise<Uint8Array> {
    const writeStream = new MemoryWriteStream(streamBuffer, DEFAULT_STREAM_BUF_MEM);
    // pipe record batch writer through it
    const blocksWriter = RecordBatchFileWriter.throughNode();
    blocksWriter.pipe(writeStream);
    // write batch, flush buffers
    const batch: RecordBatch = table.batches[0];
    blocksWriter.write(batch);
    blocksWriter.end();
    await finished(writeStream);
    return writeStream.getBufferData();
}

function flush(msg: WriterControlRequest) {
    /*
     * params:
     *     - unfinished: boolean -> is this a .wip or not?
     *     - writeDir: string -> bucket dir path,
     *     - startOrdinal: bigint -> root start ordinal of this batch
     *     - lastOrdinal: bigint -> root last ordinal of this batch
     */
    const fileName = `${alias}.ab${msg.params.unfinished ? '.wip' : ''}`;
    const currentFile = path.join(msg.params.writeDir, fileName);

    logger.debug(`generating arrow table from intermediate...`);
    const startTableGen = performance.now();
    const table = _generateArrowBatchTable();
    logger.debug(`table generated, took: ${performance.now() - startTableGen}`);

    _initBuffer();  // from here on, ready to let the parentPort listener run in bg

    logger.debug(`serializing table...`);
    const startSerialize = performance.now();
    serializeTable(table).then(serializedBatch => {
        logger.debug(`serialized, took: ${performance.now() - startSerialize}`);

        const write = (batchBytes: Uint8Array) => {
            if (!fs.existsSync(currentFile))
                _initDiskBuffer(currentFile);

            // header
            logger.debug(`writing batch to disk...`);
            const startWrite = performance.now();

            // Open the file descriptor
            const fd = fs.openSync(currentFile, 'a');

            const newSize = fs.fstatSync(fd).size + ArrowBatchProtocol.BATCH_HEADER_SIZE + batchBytes.length;
            try {
                // Write the batch header
                fs.appendFileSync(fd, ArrowBatchProtocol.newBatchHeader(
                    BigInt(batchBytes.length), compression,
                    msg.params.startOrdinal, msg.params.lastOrdinal
                ));

                // Write the batch content
                fs.appendFileSync(fd, batchBytes);

                // Flush the data to disk
                fs.fsyncSync(fd);

                logger.debug(`${batchBytes.length.toLocaleString()} bytes written to disk, took: ${performance.now() - startWrite}`);

            } finally {
                // Close the file descriptor
                fs.closeSync(fd);
            }

            parentPort.postMessage({
                tid: msg.tid,
                method: msg.method,
                status: 'ok',
                extra: {
                    newSize,
                    startOrdinal: msg.params.startOrdinal,
                    lastOrdinal: msg.params.lastOrdinal
                }
            });
        };
        switch (compression) {
            case ArrowBatchCompression.UNCOMPRESSED: {
                write(serializedBatch);
                break;
            }
            case ArrowBatchCompression.ZSTD: {
                const startCompress = performance.now();
                compressUint8Array(serializedBatch, 10).then(bytes => {
                    logger.debug(`${bytes.length.toLocaleString()} bytes after compression, took: ${performance.now() - startCompress}`);
                    write(bytes);
                });
                break;
            }
            default: {
                throw new Error(`Unknown compression format ${compression}`);
            }
        }
    });
}

function addRow(msg: WriterControlRequest) {
    /*
     * params: row to write
     */
    const typedRow = tableMapping.map(
        (fieldInfo, index) => {
            try {
                return encodeRowValue(fieldInfo, msg.params[index])
            } catch (e) {
                logger.error(`error encoding ${fieldInfo}, ${index}`);
                throw e;
            }
        });

    tableMapping.forEach(
        (fieldInfo, index) => {
            const fieldColumn = intermediateBuffers[fieldInfo.name];
            fieldColumn.push(typedRow[index])
        });

    intermediateSize++;

    parentPort.postMessage({
        tid: msg.tid,
        method: msg.method,
        status: 'ok'
    });
}

function trim(msg: WriterControlRequest) {
    /*
     * params:
     *     - trimIdx: number -> index to delete from <=
     */
    for (const mapping of tableMapping)
        intermediateBuffers[mapping.name].splice(msg.params.trimIdx);

    intermediateSize = intermediateBuffers[tableMapping[0].name].length;

    parentPort.postMessage({
        tid: msg.tid,
        method: msg.method,
        status: 'ok'
    });
}

const handlers = {
    addRow, flush, trim
};

_initBuffer();

parentPort.on('message', (msg: WriterControlRequest) => {
    const resp: WriterControlResponse = {
        tid: msg.tid,
        method: msg.method,
        status: 'ok'
    };
    const throwError = e => {
        resp.status = 'error';
        resp.error = e;
        parentPort.postMessage(resp);
    };
    try {
        if (!(msg.method in handlers))
            throwError(new Error(`Unknown method \'${msg.method}\'!`));

        handlers[msg.method](msg);
    } catch (e) {
        throwError(e);
    }
})
