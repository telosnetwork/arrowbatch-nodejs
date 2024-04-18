import {Readable, Writable} from "node:stream";
import {finished} from "node:stream/promises";
import {fileURLToPath} from 'node:url';
import path from 'node:path';
import fs from 'node:fs';

import {format, LogEntry, loggers, transports} from "winston";
import Transport from "winston-transport";
import {ZSTDCompress} from 'simple-zstd';


// currentDir == build/ dir
const currentDir = path.dirname(fileURLToPath(import.meta.url));
export const ROOT_DIR = path.join(currentDir, '..')
export const SRC_DIR = path.join(ROOT_DIR, 'src');

const packageJsonFile = path.join(ROOT_DIR, 'package.json');
export const packageInfo = JSON.parse(fs.readFileSync(packageJsonFile, 'utf-8'));

export function bigintToUint8Array (big: bigint): Uint8Array {
    const byteArray = new Uint8Array(8);
    for (let i = 0; i < byteArray.length; i++) {
        byteArray[i] = Number(big >> BigInt(8 * i) & BigInt(0xff));
    }
    return byteArray;
}
export async function compressUint8Array(input: Uint8Array, compressionLevel = 3) {
    // Convert Uint8Array to a Buffer since Node.js streams work with Buffers
    const inputBuffer = Buffer.from(input);

    // Create a readable stream from the input buffer
    const readableStream = new Readable({
        read() {
            this.push(inputBuffer);
            this.push(null); // Signal end of stream
        }
    });

    // Create a writable stream to collect the output
    const chunks = [];
    const writableStream = new Writable({
        write(chunk, encoding, callback) {
            chunks.push(chunk);
            callback();
        }
    });

    // Pipe the readable stream through the compression stream and into the writable stream
    readableStream
        .pipe(ZSTDCompress(compressionLevel))
        .pipe(writableStream);

    // Wait for the stream to finish
    await finished(writableStream);

    // Combine the chunks into a single Buffer
    const outputBuffer = Buffer.concat(chunks);

    // Convert the output Buffer back to a Uint8Array and return it
    return new Uint8Array(outputBuffer);
}

export class MemoryWriteStream extends Writable {
    private buffer: Uint8Array;
    private maxSize: number;
    private currentSize: number;

    constructor(buffer: Buffer, maxSize: number) {
        super();
        this.maxSize = maxSize;
        this.buffer = buffer;
        this.currentSize = 0;
    }

    _write(chunk: Buffer, encoding: string, callback: (error?: Error | null) => void): void {
        if (chunk.length + this.currentSize > this.maxSize) {
            callback(new Error('Buffer overflow'));
            return;
        }

        this.buffer.set(chunk, this.currentSize);
        this.currentSize += chunk.length;
        callback();
    }

    getBufferData(): Buffer {
        return Buffer.from(this.buffer.buffer, this.buffer.byteOffset, this.currentSize);
    }

    clearBuffer(): void {
        this.currentSize = 0;
        // this.buffer.fill(0);
    }
}

export interface WorkerLogMessage {
    name: any;
    method: 'workerLog';
    log: LogEntry;
}

export function isWorkerLogMessage(msg: any): msg is WorkerLogMessage {
    return 'name' in msg &&
        'method' in msg && msg.method === 'workerLog' &&
        'log' in msg;
}

export class WorkerTransport extends Transport {

    private readonly postLog: (msg: LogEntry) => void;

    constructor(postLog, opts) {
        super(opts);
        this.postLog = postLog;
    }

    log(info: LogEntry, callback) {
        this.postLog(info);
        callback();
    }
}

export function createLogger(name: string, logLevel: string) {
    const loggingOptions = {
        exitOnError: false,
        level: logLevel,
        format: format.combine(
            format.metadata(),
            format.colorize(),
            format.timestamp(),
            format.printf((info: any) => {
                return `${info.timestamp} [PID:${process.pid}] [${info.level}] : ${info.message} ${Object.keys(info.metadata).length > 0 ? JSON.stringify(info.metadata) : ''}`;
            })
        ),
        transports: [
            new transports.Console({
                level: logLevel
            })
        ]
    }
    return loggers.add(name, loggingOptions);
}