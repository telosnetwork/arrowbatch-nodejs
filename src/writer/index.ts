import {Worker} from "node:worker_threads";
import path from "node:path";
import fs, {promises as pfs} from "node:fs";

import {format, Logger, loggers, transports} from "winston";

import {ArrowBatchReader} from "../reader/index.js";
import {ArrowBatchConfig} from "../types";
import {ArrowBatchContextDef, RowBuffers} from "../context.js";
import {isWorkerLogMessage, ROOT_DIR, waitEvent, WorkerLogMessage} from "../utils.js";
import {WriterControlRequest, WriterControlResponse} from "./worker.js";
import {ArrowTableMapping} from "../protocol";

export class ArrowBatchWriter extends ArrowBatchReader {

    private _currentWriteBucket: string;

    private writeWorkers = new Map<string, {
        alias: string,
        worker: Worker,
        status: 'running' | 'stopped',
        tid: number,
        ackTid: number,
        tasks: Map<number, {ref: any, ogMsg: Partial<WriterControlRequest>, stack: Error}>
    }>();
    private workerLoggers = new Map<string, Logger>();

    constructor(
        config: ArrowBatchConfig,
        definition: ArrowBatchContextDef,
        logger: Logger
    ) {
        super(config, definition, logger);

        [...this.tableMappings.entries()].forEach(
            ([name, mappings]) => {
                const workerLogOptions = {
                    exitOnError: false,
                    level: this.config.writerLogLevel,
                    format: format.combine(
                        format.metadata(),
                        format.colorize(),
                        format.timestamp(),
                        format.printf((info: any) => {
                            return `${info.timestamp} [WORKER-${name.toUpperCase()}] [${info.level}] : ${info.message} ${Object.keys(info.metadata).length > 0 ? JSON.stringify(info.metadata) : ''}`;
                        })
                    ),
                    transports: [new transports.Console({level: this.config.writerLogLevel})]
                }
                const workerLogger = loggers.add(`worker-${name}`, workerLogOptions);
                workerLogger.debug(`logger for worker ${name} initialized with level ${this.config.writerLogLevel}`);
                this.workerLoggers.set(name, workerLogger);

                let alias = name;
                if (name === 'root' && definition.root.name)
                    alias = definition.root.name;

                const worker = new Worker(
                    path.join(ROOT_DIR, 'build/writer/worker.js'),
                    {
                        workerData: {
                            tableName: name,
                            alias,
                            tableMappings: mappings,
                            compression: this.config.compression,
                            logLevel: this.config.writerLogLevel
                        }
                    }
                );
                worker.on('message', (msg) => this.writersMessageHandler(msg));
                worker.on('error', (error) => {
                    throw error;
                });
                worker.on('exit', (msg) => {
                    this.writeWorkers.get(name).status = 'stopped';
                });
                this.writeWorkers.set(name, {
                    alias,
                    worker, status: 'running',
                    tid: 0,
                    ackTid: -1,
                    tasks: new Map<number, {ref: any, ogMsg: Partial<WriterControlRequest>, stack: Error}>()
                });
            });
    }

    private sendMessageToWriter(name: string, msg: Partial<WriterControlRequest>, ref?: any) {
        if (name === this.definition.root.name)
            name = 'root';

        const workerInfo = this.writeWorkers.get(name);
        if (workerInfo.status !== 'running')
            throw new Error(
                `Tried to call method on writer worker but its ${workerInfo.status}`);

        const error = new Error();
        workerInfo.tasks.set(workerInfo.tid, {ref, ogMsg: msg, stack: error});
        msg.tid = workerInfo.tid;

        workerInfo.tid++;
        workerInfo.worker.postMessage(msg);
    }

    private writersMessageHandler(msg: WriterControlResponse | WorkerLogMessage) {

        // catch log messages
        if (isWorkerLogMessage(msg)) {
            this.workerLoggers.get(msg.name).log(msg.log);
            return;
        }

        const workerInfo = this.writeWorkers.get(msg.name);

        if (msg.status !== 'ok') {
            this.logger.error(`error from worker ${msg.name}!`);
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
            const auxBuffs = this._auxiliaryBuffers.get(msg.name);

            // clear all table column arrays (fields)
            [...auxBuffs.columns.keys()].forEach(
                column => auxBuffs.columns.set(column, [])
            );

            // flush is done when all table columns have been cleared on aux buffer
            let isDone = true;
            [...this._auxiliaryBuffers.values()].forEach(
                table => isDone = isDone && table.columns.get([...table.columns.keys()][0]).length == 0
            );
            if (isDone) {
                global.gc && global.gc();

                // if we had wip files, delete them assuming we just wrote that data
                if (this.wipFilesMap.size > 0) {
                    for (const wipTablePath of this.wipFilesMap.values())
                        fs.rmSync(wipTablePath);
                    this.wipFilesMap.clear();
                }

                this.reloadOnDiskBuckets().then(() => this.events.emit('flush'));
            }
        }

        workerInfo.tasks.delete(msg.tid);

        const allWorkersReady = [...this.writeWorkers.values()]
            .every(w => w.ackTid == w.tid - 1);

        if (allWorkersReady)
            this.events.emit('workers-ready')
    }

    async init(startOrdinal: number | bigint) {
        await super.init(startOrdinal);

        // push any rows loaded from wip into writers
        for (const [tableName, tableBuffer] of this._intermediateBuffers.entries()) {
            const tableSize = tableBuffer.columns.get([...tableBuffer.columns.keys()][0]).length;
            for (let i = 0; i < tableSize; i++) {
                this.sendMessageToWriter(tableName, {
                    method: 'addRow',
                    params: this.getBufferRow(tableName, i)
                });
            }
        }

        // write context defintion
        await pfs.writeFile(
            path.join(this.config.dataDir, 'context.json'),
            JSON.stringify(this.definition, null, 4)
        );

        this._currentWriteBucket = this.getOrdinalSuffix(this._lastOrdinal ?? startOrdinal);
    }

    async deinit() {
        await Promise.all(
            [...this.writeWorkers.values()]
                .map(workerInfo => workerInfo.worker.terminate())
        );
    }

    get wipBucketPath(): string {
        return path.join(this.config.dataDir, this._currentWriteBucket + '.wip');
    }

    getWorkerFilePath(tableName: string, isUnfinished: boolean): string {
        const worker = this.writeWorkers.get(tableName);
        return path.join(this.wipBucketPath, `${worker.alias}.ab${isUnfinished ? '.wip' : ''}`);
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

        const isUnfinished = this.intermediateSize < this.config.dumpSize;

        // push intermediate to auxiliary and clear it
        this._initIntermediate();

        // send flush message to writer-workers
        [...this.tableMappings.keys()].forEach(tableName => {
            this.sendMessageToWriter(tableName, {
                method: 'flush',
                params: {
                    writeDir: this.wipBucketPath,
                    unfinished: isUnfinished
                }
            })
        });
    }

    addRow(tableName: string, row: any[], ref?: any) {
        if (tableName === this.definition.root.name)
            tableName = 'root';

        const tableBuffers = this._intermediateBuffers.get(tableName);
        const mappings = this.tableMappings.get(tableName);
        for (const [i, mapping] of mappings.entries())
            tableBuffers.columns.get(mapping.name).push(row[i]);

        if (tableName === 'root' && typeof this._lastOrdinal === 'undefined')
            this._lastOrdinal = row[0];

        this.sendMessageToWriter(tableName, {
            method: 'addRow',
            params: row
        }, ref);
    }

    private async trimOnBuffers(ordinal: bigint) {
        const recursiveBufferTrim = (
            table: string, ref: any, refField: ArrowTableMapping
        ) => {
            const references = this.refMappings.get(table);

            const refColumn = this._intermediateBuffers
                .get(table).columns.get(refField.name);

            let trimIdx = 0;
            for (const val of refColumn) {
                if (val === ref)
                    break;
                trimIdx++;
            }

            if (trimIdx == refColumn.length) {
                this._intermediateBuffers.set(table, this._createBuffer(table));
                return;
            }

            const row = this.getBufferRow(table, trimIdx);

            for (const [childName, childRefInfo] of Object.entries(references))
                recursiveBufferTrim(childName, row[childRefInfo.parentIndex], childRefInfo.childMapping);

            this.sendMessageToWriter(table, {
                method: 'trim',
                params: { idx: trimIdx }
            });

            this.tableMappings.get(table).forEach(m =>
                this._intermediateBuffers.get(table).columns.get(m.name).splice(trimIdx)
            );
        };

        recursiveBufferTrim('root', ordinal, this.tableMappings.get('root')[0]);
        await waitEvent(this.events, 'workers-ready');
    }
    private async trimOnDisk(ordinal: bigint) {
        const adjustedOrdinal = this.getOrdinal(ordinal);

        // delete every bucket bigger than adjustedOrdinal
        const bucketDeleteList = [...this.tableFileMap.keys()]
            .sort()
            .reverse()
            .filter(bucket => bucket > adjustedOrdinal);

        await Promise.all(bucketDeleteList.map(bucket => pfs.rm(
            path.dirname(this.tableFileMap.get(bucket).get('root')),
            {recursive: true}
        )));

        const [bucketMetadata, _] = await this.cache.getMetadataFor(adjustedOrdinal, 'root');

        // trim idx relative to bucket start
        const relativeIndex = ordinal - bucketMetadata.startRow[0];

        // table index might need to be loaded into buffers & be partially edited
        // everything after table index can be deleted
        const tableIndex = Number(relativeIndex % BigInt(this.config.dumpSize));

        if (tableIndex >= bucketMetadata.meta.batches.length)
            return;

        // truncate files from next table onwards
        await Promise.all(
            [...this.tableMappings.keys()]
                .map(table => this.cache.getMetadataFor(adjustedOrdinal, table).then(
                    ([meta, _]) => {
                        const tableIndexEnd = meta.meta.batches[tableIndex].end;
                        const fileName = this.tableFileMap.get(adjustedOrdinal).get(table);
                        return pfs.truncate(fileName, tableIndexEnd + 1);
                    })));

        // unwrap adjustedOrdinal:tableIndex table into fresh intermediate
        this._intermediateBuffers = this._createBuffers();
        const tables = await this.cache.getTablesFor(ordinal);

        Object.entries(
            {root: tables.root, ...tables.others}
        ).forEach(([tableName, table]) => {
            for (let i = 0; i < table.numRows; i++)
                this._pushRawRow(tableName, table.get(i).toArray());
        });

        // use trim buffers helper
        await this.trimOnBuffers(ordinal);
    }

    async trimFrom(ordinal: bigint) {
        // make sure all workers are idle
        await waitEvent(this.events, 'workers-ready');

        // if trimming for further back than known first ord, reset all state vars
        if (ordinal <= this.firstOrdinal) {
            this._firstOrdinal = null;
            this._lastOrdinal = null;
        } else
            this._lastOrdinal = ordinal - 1n;  // if trim is within written range, set state to ord - 1

        const rootInterBuffs = this._intermediateBuffers.get('root');
        const ordinalField = this.definition.root.ordinal;

        // if only have to trim buffers
        if (rootInterBuffs.columns.get(ordinalField).length > 0) {
            const oldestOnIntermediate = rootInterBuffs.columns.get(ordinalField)[0];
            const isOnIntermediate = ordinal >= oldestOnIntermediate
            if (isOnIntermediate) {
                await this.trimOnBuffers(ordinal);
                return;
            }
        }

        // if need to hit disk tables
        await this.trimOnDisk(ordinal);
        await this.reloadOnDiskBuckets();
    }
}