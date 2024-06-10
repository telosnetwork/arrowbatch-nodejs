import WebSocket from 'ws';
import ReconnectingWebSocket from "reconnecting-websocket";
import {Logger} from "winston";

interface Response {
    id: string;
    result?: any;
    error?: string;
}

export class ArrowBatchBroadcastClient {
    private readonly url: string;
    private readonly logger: Logger;

    private ws: ReconnectingWebSocket;
    private requestId: number;
    private pendingRequests: Map<string, (response: Response) => void>;

    private _isConnected: boolean = false;

    constructor(url: string, logger: Logger) {
        this.url = url;
        this.requestId = 0;
        this.pendingRequests = new Map<string, (response: Response) => void>();
        this.logger = logger;
    }

    connect() {
        this.ws = new ReconnectingWebSocket(this.url, [], {WebSocket});

        this.ws.addEventListener('message', (message) => {
            const response: Response = JSON.parse(message.data);
            const callback = this.pendingRequests.get(response.id);
            if (callback) {
                callback(response);
                this.pendingRequests.delete(response.id);
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

    private sendRequest(method: string, params?: any): Promise<any> {
        return new Promise((resolve, reject) => {
            const id = (this.requestId++).toString();
            const message = JSON.stringify({ id, method, params });

            this.pendingRequests.set(id, (response: Response) => {
                if (response.error) {
                    reject(new Error(response.error));
                } else {
                    resolve(response.result);
                }
            });

            this.ws.send(message);
        });
    }

    async getInfo(): Promise<any> {
        return this.sendRequest('get_info');
    }

    async getRow(ordinal: bigint): Promise<any> {
        return this.sendRequest('get_row', { ordinal });
    }
}