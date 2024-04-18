import {Worker} from "node:worker_threads";
import path from "node:path";
import fs, {promises as pfs} from "node:fs";

import {ArrowBatchReader} from "../reader/index.js";
import {format, Logger, loggers, transports} from "winston";
import {ArrowBatchConfig} from "../types";
import {ArrowBatchContextDef, generateMappingsFromDefs, genereateReferenceMappings} from "../context.js";
import {isWorkerLogMessage, ROOT_DIR, WorkerLogMessage} from "../utils.js";
import {WriterControlRequest, WriterControlResponse} from "./worker.js";

export class ArrowBatchWriter extends ArrowBatchReader {

    private _currentWriteBucket: string;

    private writeWorkers = new Map<string, {
        worker: Worker,
        status: 'running' | 'stopped',
        tid: number,
        tasks: Map<number, {ref: any, stack: Error}>
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

                let alias = undefined;
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
                    worker, status: 'running',
                    tid: 0,
                    tasks: new Map<number, {ref: any, stack: Error}>()
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
        workerInfo.tasks.set(workerInfo.tid, {ref, stack: error});
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

        workerInfo.tasks.delete(msg.tid);

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
                this.events.emit('flush');
            }
        }
    }

    async init(startOrdinal: number | bigint) {
        await super.init(startOrdinal);

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
        [...this.tableMappings.keys()].forEach(tableName =>
            this.sendMessageToWriter(tableName, {
                method: 'flush',
                params: {
                    writeDir: this.wipBucketPath,
                    unfinished: isUnfinished
                }
            })
        );
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
}