import {Worker} from "node:worker_threads";
import path from "node:path";
import fs, {promises as pfs} from "node:fs";

import {format, Logger, loggers, transports} from "winston";

import {ArrowBatchReader} from "../reader/index.js";
import {ArrowBatchConfig} from "../types";
import {ArrowBatchContextDef} from "../context.js";
import {extendedStringify, isWorkerLogMessage, ROOT_DIR, waitEvent, WorkerLogMessage} from "../utils.js";
import {WriterControlRequest, WriterControlResponse} from "./worker.js";
import {ArrowTableMapping, DEFAULT_ALIAS, DEFAULT_STREAM_BUF_MEM, DUMP_CONDITION} from "../protocol.js";
import ArrowBatchBroadcaster from "./broadcast.js";
import bytes from "bytes";

export class ArrowBatchWriter extends ArrowBatchReader {

    private isFirstUpdate: boolean = true;
    private _currentWriteBucket: string;

    private writeWorker: {
        worker: Worker,
        status: 'running' | 'stopped',
        tid: number,
        ackTid: number,
        tasks: Map<number, {ref: any, ogMsg: Partial<WriterControlRequest>, stack: Error}>
    };
    private workerLogger: Logger;

    private readonly broadcaster: ArrowBatchBroadcaster;

    constructor(
        config: ArrowBatchConfig,
        definition: ArrowBatchContextDef,
        logger: Logger
    ) {
        super(config, definition, logger);

        const workerLogOptions = {
            exitOnError: false,
            level: this.config.writerLogLevel,
            format: format.combine(
                format.metadata(),
                format.colorize(),
                format.timestamp(),
                format.printf((info: any) => {
                    return `${info.timestamp} [WRITER] [${info.level}] : ${info.message} ${Object.keys(info.metadata).length > 0 ? JSON.stringify(info.metadata) : ''}`;
                })
            ),
            transports: [new transports.Console({level: this.config.writerLogLevel})]
        }
        this.workerLogger = loggers.add('writer', workerLogOptions);
        this.workerLogger.debug(`logger for writer initialized with level ${this.config.writerLogLevel}`);

        let alias = definition.alias ?? DEFAULT_ALIAS;

        let streamBufMem = definition.stream_size ?? DEFAULT_STREAM_BUF_MEM;
        if (streamBufMem && typeof streamBufMem === 'string')
            streamBufMem = bytes(streamBufMem);

        const worker = new Worker(
            path.join(ROOT_DIR, 'build/src/writer/worker.js'),
            {
                workerData: {
                    alias,
                    tableMapping: definition.map,
                    compression: this.config.compression,
                    logLevel: this.config.writerLogLevel,
                    streamBufMem
                }
            }
        );
        worker.on('message', (msg) => this.writerMessageHandler(msg));
        worker.on('error', (error) => {
            throw error;
        });
        worker.on('exit', (msg) => {
            this.writeWorker.status = 'stopped';
        });
        this.writeWorker = {
            worker, status: 'running',
            tid: 0,
            ackTid: -1,
            tasks: new Map<number, {ref: any, ogMsg: Partial<WriterControlRequest>, stack: Error}>()
        };

        this.broadcaster = new ArrowBatchBroadcaster(this);
    }

    private sendMessageToWriter(msg: Partial<WriterControlRequest>, ref?: any) {
        if (this.writeWorker.status !== 'running')
            throw new Error(
                `Tried to call method on writer worker but its ${this.writeWorker.status}`);

        const error = new Error();
        this.writeWorker.tasks.set(this.writeWorker.tid, {ref, ogMsg: msg, stack: error});
        msg.tid = this.writeWorker.tid;

        this.writeWorker.tid++;
        this.writeWorker.worker.postMessage(msg);

        if (msg.method === 'flush')
            this.logger.debug(`sent ${extendedStringify(msg)} to writer`);
    }

    private writerMessageHandler(msg: WriterControlResponse | WorkerLogMessage) {

        // catch log messages
        if (isWorkerLogMessage(msg)) {
            this.workerLogger.log(msg.log);
            return;
        }

        const workerInfo = this.writeWorker;

        if (msg.status !== 'ok') {
            this.logger.error(`error from writer!`);
            try {
                this.logger.error(`orginal ref:\n${JSON.stringify(workerInfo.tasks.get(msg.tid).ref, null, 4)}`)
            } catch (e) {}
            this.logger.error(`original stack trace:\n${workerInfo.tasks.get(msg.tid).stack}`);
            throw msg.error;
        }

        if (workerInfo.status !== 'running')
            throw new Error(
                `Received msg from writer worker but it has an unexpected status: ${workerInfo.status}`);

        workerInfo.ackTid = msg.tid;

        if (msg.method === 'flush') {
            const auxBuffs = this._auxiliaryBuffers;

            // clear all table column arrays (fields)
            [...auxBuffs.columns.keys()].forEach(
                column => auxBuffs.columns.set(column, [])
            );

            // flush is done when all table columns have been cleared on aux buffer
            let isDone = true;
            [...this._auxiliaryBuffers.columns.values()].forEach(
                column => isDone = isDone && column.length == 0
            );
            if (isDone) {
                global.gc && global.gc();

                // if we had wip files, delete them assuming we just wrote that data
                if (this.wipFile) {
                    fs.rmSync(this.wipFile);
                    this.wipFile = undefined;
                }

                this.reloadOnDiskBuckets().then(() => {
                    this.events.emit('flush');

                    if (this.broadcaster) {
                        const adjustedOrdinal = this.getOrdinal(msg.extra.startOrdinal);
                        this.cache.getMetadataFor(adjustedOrdinal).then(([metadata, _]) => {
                            const batchIndex = BigInt(metadata.batches.length - 1);
                            this.broadcaster.broadcastFlush(
                                adjustedOrdinal, batchIndex, msg.extra.lastOrdinal);
                        });
                    }
                });
            }
        }

        workerInfo.tasks.delete(msg.tid);

        if (msg.method === 'flush')
            this.logger.debug(`writer replied to ${msg.method} with id ${msg.tid}`);

        if (this.writeWorker.ackTid == this.writeWorker.tid - 1)
            this.events.emit('writer-ready');
    }

    async init(startOrdinal?: number | bigint) {
        await pfs.mkdir(this.config.dataDir, {recursive: true});

        // write context defintion
        await pfs.writeFile(
            path.join(this.config.dataDir, 'context.json'),
            JSON.stringify(this.definition, null, 4)
        );

        // context/reader init will:
        //   - context init will reload on disk buckets
        //   - reader init will load wip files into ram
        //   - reader init will set _lastOrdinal and _startOrdinal
        await super.init();

        this._currentWriteBucket = this.getOrdinalSuffix(this._lastOrdinal ?? startOrdinal);

        if (this.broadcaster)
            this.broadcaster.initUWS();
    }

    async deinit() {
        await this.writeWorker.worker.terminate();

        if (this.broadcaster)
            this.broadcaster.close();
    }

    get wipBucketPath(): string {
        return path.join(this.config.dataDir, this._currentWriteBucket + '.wip');
    }

    getWorkerFilePath(tableName: string, isUnfinished: boolean): string {
        return path.join(this.wipBucketPath, `${this.alias}.ab${isUnfinished ? '.wip' : ''}`);
    }

    protected _initIntermediate() {
        this._auxiliaryBuffers = this._intermediateBuffers;
        super._initIntermediate();
    }

    beginFlush() {
        // make sure auxiliary is empty (block concurrent flushes)
        if (this.auxiliarySize != 0)
            throw new Error(`beginFlush called but auxiliary buffers not empty, is system overloaded?`)

        const maybeOldBucket = this.wipBucketPath;
        this._currentWriteBucket = this.getOrdinalSuffix(this._lastOrdinal);
        if (maybeOldBucket !== this.wipBucketPath)
            fs.renameSync(maybeOldBucket, maybeOldBucket.replace('.wip', ''));

        // maybe create target dir
        if (!fs.existsSync(this.wipBucketPath))
            fs.mkdirSync(this.wipBucketPath, {recursive: true});

        const startOrdinal = this.intermediateFirstOrdinal;
        const lastOrdinal = this.intermediateLastOrdinal;
        const unfinished = !DUMP_CONDITION(lastOrdinal, this.config);

        // push intermediate to auxiliary and clear it
        this._initIntermediate();

        // send flush message to writer-workers
        this.sendMessageToWriter({
            method: 'flush',
            params: {
                writeDir: this.wipBucketPath,
                unfinished, startOrdinal, lastOrdinal
            }
        })
    }

    updateOrdinal(ordinal: number | bigint) {
        // first validate ordering
        ordinal = BigInt(ordinal);

        if (!this.isFirstUpdate) {
            const expected = this._lastOrdinal + 1n;
            if (ordinal != expected)
                throw new Error(`Expected argument ordinal to be ${expected.toLocaleString()} but was ${ordinal.toLocaleString()}`)
        } else
            this.isFirstUpdate = false;

        this._lastOrdinal = ordinal;

        if (!this._firstOrdinal)
            this._firstOrdinal = this._lastOrdinal;

        // maybe start flush
        if (DUMP_CONDITION(ordinal, this.config))
            this.beginFlush();
    }

    pushRow(row: any[]) {
        super.pushRow(row);

        this.sendMessageToWriter({
            method: 'addRow',
            params: row
        }, row);

        if (this.broadcaster)
            this.broadcaster.broadcastRow(row);

        this.updateOrdinal(row[this.ordinalIndex]);
    }

    private async trimOnBuffers(ref: any, refField: ArrowTableMapping) {
        // get referenced column
        const refFieldIndex = this.tableMapping.findIndex(
            map => map.name == refField.name);
        const refColumn = this._intermediateBuffers.columns[refFieldIndex];

        // linear search ref value on that column
        let trimIdx = 0;
        for (const val of refColumn) {
            if (val === ref)
                break;
            trimIdx++;
        }

        // send trim message to workers
        this.sendMessageToWriter({
            method: 'trim',
            params: { idx: trimIdx }
        });

        // if we need to trim whole table
        if (trimIdx == refColumn.length) {
            this._intermediateBuffers = this._createBuffer();
        } else {
            [...this._intermediateBuffers.columns.values()].forEach(column => column.splice(trimIdx));
        }

        await waitEvent(this.events, 'writer-ready');
    }
    private async trimOnDisk(ordinal: bigint) {
        const adjustedOrdinal = this.getOrdinal(ordinal);

        // delete every bucket bigger than adjustedOrdinal
        const bucketDeleteList = [...this.tableFileMap.keys()]
            .sort()
            .reverse()
            .filter(bucket => bucket > adjustedOrdinal);

        await Promise.all(bucketDeleteList.map(bucket => pfs.rm(
            path.dirname(this.tableFileMap.get(bucket)),
            {recursive: true}
        )));

        const [bucketMetadata, _] = await this.cache.getMetadataFor(adjustedOrdinal);

        // trim idx relative to bucket start
        const [batchIndex, __] = ArrowBatchReader.getRelativeTableIndex(ordinal, bucketMetadata);

        // table index might need to be loaded into buffers & be partially edited
        // everything after table index can be deleted
        if (batchIndex >= bucketMetadata.batches.length)
            return;

        // truncate files from next table onwards
        const tableIndexEnd = bucketMetadata.batches[batchIndex].end;
        const fileName = this.tableFileMap.get(adjustedOrdinal);
        await pfs.truncate(fileName, tableIndexEnd + 1);

        // unwrap adjustedOrdinal:tableIndex table into fresh intermediate
        this._intermediateBuffers = this._createBuffer();
        const [table, ___] = await this.cache.getTableFor(ordinal);

        for (let i = 0; i < table.numRows; i++)
            this.pushRow(table.get(i).toArray());

        // use trim buffers helper
        await this.trimOnBuffers(ordinal, this.tableMapping[this.ordinalIndex]);
    }

    // async trimFrom(ordinal: bigint) {
    //     // make sure all workers are idle
    //     await waitEvent(this.events, 'workers-ready');

    //     // if trimming for further back than known first ord, reset all state vars
    //     if (ordinal <= this.firstOrdinal) {
    //         this._firstOrdinal = null;
    //         this._lastOrdinal = null;
    //     } else
    //         this._lastOrdinal = ordinal - 1n;  // if trim is within written range, set state to ord - 1

    //     const rootInterBuffs = this._intermediateBuffers.get('root');
    //     const ordinalField = this.definition.root.ordinal;

    //     // if only have to trim buffers
    //     if (rootInterBuffs.columns.get(ordinalField).length > 0) {
    //         const oldestOnIntermediate = rootInterBuffs.columns.get(ordinalField)[0];
    //         const isOnIntermediate = ordinal >= oldestOnIntermediate
    //         if (isOnIntermediate) {
    //             await this.trimOnBuffers(ordinal);
    //             return;
    //         }
    //     }

    //     // if need to hit disk tables
    //     await this.trimOnDisk(ordinal);
    //     await this.reloadOnDiskBuckets();
    // }
}