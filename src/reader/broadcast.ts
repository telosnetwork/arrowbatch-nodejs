import WebSocket from 'ws';
import ReconnectingWebSocket from "reconnecting-websocket";
import {Logger} from "winston";
import {extendedStringify} from "../utils.js";
import {
    FlushReq, GetInfoRes, GetInfoResSchema, GetRowRes, GetRowResSchema,
    Request,
    RequestSchema,
    Response,
    StrictResponseSchema, SubResSchema, SyncAkRes, SyncAkResSchema,
    SyncReq,
    SyncResSchema,
    SyncRowReq
} from "../types.js";
import {v4 as uuidv4} from 'uuid';
import {DEFAULT_AWK_RANGE} from "../protocol.js";

export interface BroadcastClientParams {
    url: string,
    logger: Logger,
    ordinalIndex: number,
    handlers: {
        pushRow: (row: any[]) => void,
        flush: (info: FlushReq['params']) => void
    },
    syncAwkRange?: number
}

export class ArrowBatchBroadcastClient {
    private readonly url: string;
    private readonly logger: Logger;
    private readonly ordinalIndex: number;

    private ws: ReconnectingWebSocket;
    private pendingRequests: Map<string, (response: Response) => void>;
    private serverMethodHandlers: Map<string, (request: Request) => void>;

    private _isConnected: boolean = false;

    syncAwkRange: bigint;

    private syncTaskInfo: {
        cursor: bigint,
        akOrdinal: bigint
    }

    constructor(params: BroadcastClientParams) {
        this.url = params.url;
        this.logger = params.logger;
        this.ordinalIndex = params.ordinalIndex;

        this.syncAwkRange = BigInt(params.syncAwkRange ?? DEFAULT_AWK_RANGE);

        this.pendingRequests = new Map<string, (response: Response) => void>();

        this.serverMethodHandlers = new Map<string, (request: Request) => void>();

        const genericServerRowHandler = (request: SyncRowReq) => {
            const ordinal = BigInt(request.params[this.ordinalIndex]);
            const expected = this.syncTaskInfo.cursor + 1n;

            if (ordinal % 1000n == 0n)
                this.logger.debug(`server sent row ${ordinal}`);

            // simple ordering check
            if (ordinal != expected)
                throw new Error(`expected ${ordinal.toLocaleString()} to be ${expected.toLocaleString()}`);

            this.syncTaskInfo.cursor = ordinal;

            // awk next batch
            if (ordinal == this.syncTaskInfo.akOrdinal)
                setTimeout(async () => this.syncAwk(), 0);

            params.handlers.pushRow(request.params);
        };

        this.serverMethodHandlers.set('sync_row', genericServerRowHandler);
        this.serverMethodHandlers.set('flush', (request: Request) => {
            params.handlers.flush(request.params);
        });
    }

    getId(): string {
        return uuidv4();
    }

    connect() {
        this.ws = new ReconnectingWebSocket(this.url, [], {WebSocket});

        this.ws.addEventListener('message', (message) => {
            const msgObj = JSON.parse(message.data);

            let response: Response;
            let request: Request;

            let maybeResponse = StrictResponseSchema.safeParse(msgObj);
            if (maybeResponse.success)
                response = maybeResponse.data;

            else {
                let maybeRequest = RequestSchema.safeParse(msgObj);
                if (maybeRequest.success)
                    request = maybeRequest.data;
            }

            if (response) {
                const callback = this.pendingRequests.get(response.id);
                if (callback) {
                    callback(response);
                    this.pendingRequests.delete(response.id);
                }
            }

            if (request) {
                const handler = this.serverMethodHandlers.get(request.method);
                if (handler)
                    handler(request);
            }
        });

        this.ws.addEventListener('open', () => {
            this.logger.info('Connected to server');
            this._isConnected = true;
        });

        this.ws.addEventListener('close', () => {
            this.logger.info('Disconnected from server');
            this._isConnected = false;
        });

        this.ws.addEventListener('error', (error) => {
            this.logger.error('WebSocket error:', error);
        });
    }

    isConnected(): boolean {
        return this._isConnected;
    }

    private sendRequest(method: string, params: any): Promise<any> {
        return new Promise((resolve, reject) => {
            const id = this.getId()
            const message = extendedStringify({ id, method, params });

            this.pendingRequests.set(id, (response: Response) => {
                if (response.error) {
                    this.logger.error('received error from server:');
                    this.logger.error(response.error.message);
                    this.logger.error(response.error.stack);

                    const error = new Error(response.error.message);
                    error.stack = response.error.stack;
                    reject(error);
                } else {
                    resolve(response);
                }
            });

            this.ws.send(message);
        });
    }

    async getInfo(): Promise<GetInfoRes> {
        const getInfoRes = await this.sendRequest('get_info', {});
        // this.logger.info(JSON.stringify(getInfoRes, null, 4));
        return GetInfoResSchema.parse(getInfoRes);
    }

    async getRow(ordinal: bigint): Promise<GetRowRes> {
        return GetRowResSchema.parse(await this.sendRequest('get_row', { ordinal }));
    }

    private async syncAwk(): Promise<SyncAkRes> {
       const response = SyncAkResSchema.parse(
            await this.sendRequest('sync_ak', {
                amount: this.syncAwkRange.toString()
            })
       );
       this.syncTaskInfo.akOrdinal += this.syncAwkRange;
       return response;
    }

    async sync(
        params: SyncReq['params']
    ) {
        const from = BigInt(params.from);

        this.logger.debug(`requesting sync using params: ${JSON.stringify(params)}...`);
        const syncRes = SyncResSchema.parse(await this.sendRequest('sync', params));
        this.logger.debug(`server started sync session, distance: ${syncRes.result.distance}`);

        this.syncTaskInfo = {
            cursor: from - 1n,
            akOrdinal: from - 1n
        };

        this.logger.debug(`subscribing to flush topic...`);
        const subRes = SubResSchema.parse(
            await this.sendRequest('sub', {topic: 'flush'}));
        this.logger.debug(`subbed.`)

        await this.syncAwk();
    }
}