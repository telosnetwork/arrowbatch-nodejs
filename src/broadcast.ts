import uWS, {TemplatedApp} from "uWebSockets.js";
import { v4 as uuidv4 } from 'uuid';

import {Logger} from "winston";
import {RowWithRefs} from "./context.js";
import {ArrowBatchReader} from "./reader/index.js";
import {extendedStringify} from "./utils";


export default class ArrowBatchBroadcaster {

    readonly reader: ArrowBatchReader;
    readonly logger: Logger;
    broadcastServer: TemplatedApp;

    private sockets: {[key: string]: uWS.WebSocket<Uint8Array>} = {};
    private listenSocket: uWS.us_listen_socket;

    constructor(reader: ArrowBatchReader, logger: Logger) {
        this.reader = reader;
        this.logger = logger;
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
                    const msgObj = JSON.parse(Buffer.from(msg).toString('utf-8'));

                    try {
                        let result = undefined;
                        if (msgObj.method === 'get_info') {
                            result = {
                                lastOrdinal: this.reader.lastOrdinal
                            };
                        } else if (msgObj.method === 'get_row') {
                            const ordinal = BigInt(msgObj.params.ordinal);
                            result = await this.reader.getRow(ordinal);
                        } else if (msgObj.method === 'subscribe') {
                            ws.subscribe(msgObj.params.topic);
                            result = 'ok';
                        }

                        ws.send(extendedStringify({result, id: msgObj.id}));
                    } catch (e) {
                        this.logger.error(e.message);
                        this.logger.error(e.stack);
                        ws.send(extendedStringify({error: e.message, id: msgObj.id}));
                    }
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

    broadcastRow(row: RowWithRefs) {
        this.broadcastData('row', row);
    }

    private broadcastData(type: string, data: any) {
        this.broadcastServer.publish(
            'broadcast',
            extendedStringify({type, data})
        );
    }

    close() {
        for (const ip in this.sockets)
            this.sockets[ip].close();

        uWS.us_listen_socket_close(this.listenSocket);
    }
}