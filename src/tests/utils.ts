import crypto from 'node:crypto';
import EventEmitter from 'node:events';

export function randomBytes(length: number): Buffer {
    return crypto.randomBytes(length);
}

export function randomHexString(length: number): string {
    return randomBytes(Math.ceil(length / 2)).toString('hex').slice(0, length);
}

export function randomInteger(min: number, max: number): number {
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

export async function waitEvent(emitter: EventEmitter, event: string): Promise<void> {
    new Promise(resolve => emitter.once(event, resolve));
}
