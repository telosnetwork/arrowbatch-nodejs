import uWS, {TemplatedApp} from "uWebSockets.js";
import { v4 as uuidv4 } from 'uuid';

import {format, Logger, loggers, transports} from "winston";
import {RowWithRefs} from "../context.js";
import {ArrowBatchReader} from "../reader";
import {extendedStringify, sleep} from "../utils.js";

import {
    Request,
    RequestSchema,
    GetInfoRes,
    GetRowReq,
    GetRowRes,
    SyncAkReq,
    SyncAkRes,
    SyncReq,
    SyncRes,
    Response,
    SubReq,
    SubRes,
    FlushReq,
    FlushReqSchema, LiveRowReqSchema, LiveRowReq, SyncRowReqSchema

} from '../types.js';


export default class ArrowBatchBroadcaster {

    readonly reader: ArrowBatchReader;
    readonly logger: Logger;
    broadcastServer: TemplatedApp;

    private sockets: {[key: string]: uWS.WebSocket<Uint8Array>} = {};
    private listenSocket: uWS.us_listen_socket;

    private syncTasksInfo = new Map<string, {
        isCancelled: boolean,
        isSynced: boolean,
        isSyncUpdateRunning: boolean,
        initParams: {from: bigint, to?: bigint},
        cursor: bigint,
        akOrdinal: bigint
    }>();

    constructor(reader: ArrowBatchReader) {
        this.reader = reader;

        const logOptions = {
            exitOnError: false,
            level: reader.config.broadcastLogLevel,
            format: format.combine(
                format.metadata(),
                format.colorize(),
                format.timestamp(),
                format.printf((info: any) => {
                    return `${info.timestamp} [BROADCAST] [${info.level}] : ${info.message} ${Object.keys(info.metadata).length > 0 ? JSON.stringify(info.metadata) : ''}`;
                })
            ),
            transports: [new transports.Console({level: reader.config.broadcastLogLevel})]
        }
        this.logger = loggers.add(`broadcast`, logOptions);
    }

    private handleSub(uuid: string, params: SubReq['params']): SubRes['result'] {
        const ws = this.sockets[uuid];
        ws.subscribe(params.topic);
        return 'ok';
    }

    private handleGetInfo(): GetInfoRes['result'] {
        return {
            last_ordinal: this.reader.lastOrdinal.toString()
        }
    }

    private async handleGetRow(params: GetRowReq['params']): Promise<GetRowRes['result']> {
        return await this.reader.getRow(BigInt(params.ordinal));
    }

    private handleSync(uuid: string, params: SyncReq['params']): SyncRes['result'] {
        const from = BigInt(params.from);
        let to: bigint;
        if (params.to)
            to = BigInt(params.to);
        this.startOrUpdateSync(uuid, {from, to});
        return {
            distance: (this.reader.lastOrdinal - from).toString()
        };
    }

    private handleSyncAk(uuid: string, params: SyncAkReq['params']): SyncAkRes['result'] {
        const task = this.syncTasksInfo.get(uuid);
        if (!task) {
            throw new Error(`No sync task started...`);
        } else {
            task.akOrdinal += BigInt(params.amount);

            let taskStatus: 'running' | 'started' = 'running'
            if (!task.isSyncUpdateRunning) {
                setTimeout(async () => await this.updateSyncTask(uuid), 0);
                taskStatus = 'started'
            }
            return {
                task_status: taskStatus,
                last_ordinal: this.reader.lastOrdinal.toString()
            };
        }
    }
    
    private async wsMessageHandler(
        uuid: string,
        rawMessage: ArrayBuffer
    ) {
        let msgStr: string;
        let msgObj: Request;
        let id: string = '?';
        let result: Response['result'];
        let error: Response['error'];

        try {
            msgStr = Buffer.from(rawMessage).toString('utf-8')
            msgObj = RequestSchema.parse(JSON.parse(msgStr));
            id = msgObj.id;

            const method: string = msgObj.method;
            const params: any = msgObj.params;
            switch (method) {
                case 'sub':
                    result = this.handleSub(uuid, params);
                    break;

                case 'get_info':
                    result = this.handleGetInfo();
                    break;

                case 'get_row':
                    result = await this.handleGetRow(params);
                    break;

                case 'sync':
                    result = this.handleSync(uuid, params);
                    break;

                case 'sync_ak':
                    result = this.handleSyncAk(uuid, params);
                    break;

                default:
                    throw new Error(`Unknown method: ${method}`);
            }
        } catch (e) {
            this.logger.error('error handling ws client msg:\n');
            this.logger.error(msgStr);
            this.logger.error(e.message);
            this.logger.error(e.stack);
            error = {
                message: e.message,
                stack: e.stack
            };
        }

        return extendedStringify({result, error, id});
    }

    initUWS() {
        const host = this.reader.config.wsHost;
        const port = this.reader.config.wsPort;
        this.broadcastServer = uWS.App({}).ws(
            '/',
            {
                compression: 0,
                maxPayloadLength: 16 * 1024 * 1024,
                /* We need a slightly higher timeout for this crazy example */
                idleTimeout: 60,
                open: (ws: uWS.WebSocket<Uint8Array>) => {
                    const uuid = uuidv4();
                    // @ts-ignore
                    ws.uuid = uuid;
                    this.sockets[uuid] = ws;
                },
                message: async (ws, msg) => {
                    // @ts-ignore
                    const uuid = ws.uuid;
                    const resp: string = await this.wsMessageHandler(uuid, msg);
                    ws.send(resp);
                },
                drain: () => {
                },
                close: (ws) => {
                    // @ts-ignore
                    const uuid = ws.uuid;
                    if (uuid && uuid in this.sockets)
                        delete this.sockets[uuid];
                },
            }).listen(host, port, (listenSocket) => {
            if (listenSocket) {
                this.listenSocket = listenSocket;
                this.logger.info('Listening to port ' + port);
            } else {
                this.logger.error('Failed to listen to port ' + port);
            }
        });
    }

    broadcastFlush(adjustedOrdinal: number, batchIndex: bigint, lastOnDisk: bigint) {
        const flushReq: FlushReq = FlushReqSchema.parse({
            method: 'flush',
            params: {
                adjusted_ordinal: adjustedOrdinal.toString(),
                batch_index: batchIndex.toString(),
                last_on_disk: lastOnDisk.toString()
            },
            id: `flush-${adjustedOrdinal}-${batchIndex}`
        });
        this.broadcastServer.publish('flush', extendedStringify(flushReq));
    }

    broadcastRow(row: RowWithRefs) {
        const ordinal = row.row[0];

        const req: LiveRowReq = LiveRowReqSchema.parse({
            method: 'live_row',
            params: row,
            id: `live-row-${ordinal.toString()}`
        });

        const serializedReq: string = extendedStringify(req);

        for (const [uuid, task] of this.syncTasksInfo.entries()) {
            if (task.isSynced) {
                if (ordinal <= task.akOrdinal) {
                    const ws = this.sockets[uuid];
                    ws.send(serializedReq);
                } else {  // client has not awk'ed this ordinal yet, mark as unsync
                    task.isSynced = false;
                }
            } else {
                if (!task.isSyncUpdateRunning && task.cursor < task.akOrdinal)
                    setTimeout(async () => await this.updateSyncTask(uuid), 0);
            }
        }
    }

    // private broadcastData(type: string, data: any) {
    //     this.broadcastServer.publish(
    //         'broadcast',
    //         extendedStringify({type, data})
    //     );
    // }

    private startOrUpdateSync(uuid: string, params: {from: bigint, to?: bigint}) {
        const task = this.syncTasksInfo.get(uuid);
        if (task) {  // client has existing task, cancel and restart
            setTimeout(async () => {
                // if sync update is running, cancel then restart
                if (task.isSyncUpdateRunning) {
                    task.isCancelled = true;
                    while (task.isSyncUpdateRunning) {
                        await sleep(100);
                    }
                    task.isCancelled = false;
                }
                task.initParams = params;
                task.cursor = params.from;
                task.akOrdinal = params.from - 1n;
            }, 0);
        } else {  // client has no task, create
            this.syncTasksInfo.set(uuid, {
                isSynced: false,
                isSyncUpdateRunning: false,
                isCancelled: false,
                initParams: params,
                cursor: params.from,
                akOrdinal: params.from - 1n
            });
        }
    }

    private async updateSyncTask(uuid: string) {
        const task = this.syncTasksInfo.get(uuid);

        // if client reached head dont use this mechanism to send blocks
        if (task.isSynced)
            return;

        // ensure no two sync tasks can run at same time
        if (task.isSyncUpdateRunning)
            return;

        // adquire sync task lock
        task.isSyncUpdateRunning = true;

        const sock = this.sockets[uuid];

        // sent row from current cursor to client awk'ed ordinal
        let totalSent = 0;
        while(sock && task.cursor <= task.akOrdinal && task.cursor < this.reader.lastOrdinal) {
            const row = await this.reader.getRow(task.cursor);

            // check for cancelation right after await
            if (task.isCancelled || !sock) {
                task.isSyncUpdateRunning = false;
                return;
            }

            const syncRowReq = SyncRowReqSchema.parse({
                method: 'sync_row',
                params: row,
                id: `sync-row-${task.cursor}`
            });

            // queue up outbound message with row
            sock.send(extendedStringify(syncRowReq));
            task.cursor++;
            totalSent++;
        }

        this.logger.debug(`updateSyncTask sent ${totalSent} rows to ${uuid}`);

        // release task lock
        task.isSyncUpdateRunning = false;

        // if not cancelled evaluate task state
        if (!task.isCancelled) {
            // in case we reached end of range
            if (task.initParams.to && task.cursor == task.initParams.to) {
                this.syncTasksInfo.delete(uuid);
                this.logger.debug(`sync task for ${uuid} done.`);
            }

            // in case we reached head
            if (task.cursor >= this.reader.lastOrdinal - 1n) {
                task.isSynced = true;
            }
        }
    }

    close() {
        for (const ip in this.sockets)
            this.sockets[ip].close();

        uWS.us_listen_socket_close(this.listenSocket);
    }
}