import z, {string} from 'zod';
import bytes from 'bytes';

import {
    ArrowBatchCompression, DEFAULT_AWK_RANGE,
    DEFAULT_BROADCAST_HOST, DEFAULT_BROADCAST_PORT,
    DEFAULT_BUCKET_SIZE,
    DEFAULT_DUMP_SIZE,
    DEFAULT_STREAM_BUF_MEM
} from "./protocol.js";

const BigIntSchema = z.union([
    z.string(),
    z.number(),
    z.bigint()
]).transform((val, ctx) => {
    try {
        return BigInt(val);
    } catch {
        ctx.addIssue({
            code: z.ZodIssueCode.custom,
            message: 'Invalid bigint',
        });
        return z.NEVER;
    }
});

const BufferSizeSchema = z.union([
    z.string().transform((val, ctx) => {
        const numericValue = bytes(val);
        if (typeof numericValue !== 'number' || isNaN(numericValue)) {
            ctx.addIssue({
                code: z.ZodIssueCode.custom,
                message: 'Invalid memory buffer size format',
            });
            return z.NEVER;
        }
        return numericValue;
    }),
    z.number()
]);

export const ArrowBatchConfigSchema = z.object({
    dataDir: z.string(),

    liveMode: z.boolean().default(false),
    wsHost: z.string().default(DEFAULT_BROADCAST_HOST),
    wsPort: z.number().default(DEFAULT_BROADCAST_PORT),
    wsAwkRange: z.number().default(DEFAULT_AWK_RANGE),
    writerLogLevel: z.string().default('warning'),
    broadcastLogLevel: z.string().default('warning'),
    bucketSize: BigIntSchema.default(DEFAULT_BUCKET_SIZE),
    dumpSize: BigIntSchema.default(DEFAULT_DUMP_SIZE),
    compression: z.nativeEnum(ArrowBatchCompression).default(ArrowBatchCompression.ZSTD),

    writeStreamSize: BufferSizeSchema.default(DEFAULT_STREAM_BUF_MEM)
});

export type ArrowBatchConfig = z.infer<typeof ArrowBatchConfigSchema>;

// broadcast message types

// base Request schema
export const RequestSchema = z.object({
    method: z.string(),
    params: z.any(),
    id: z.string()
});

// client side request schemas

// subscription request: notifty this client of messages on topic
export const SubReqSchema = RequestSchema.extend({
    method: z.literal('sub'),
    params: z.object({
        topic: z.string()
    })
});

// get info request: return information about current state
export const GetInfoReqSchema = RequestSchema.extend({
    method: z.literal('get_info'),
    params: z.object({})
});

// get row request: fetch a specific row from this server based on ordinal
export const GetRowReqSchema = RequestSchema.extend({
    method: z.literal('get_row'),
    params: z.object({
        ordinal: z.string()
    })
});

// synchronization request: stream rows from this server, if no `to` arg is provided rows are streamed forever
export const SyncReqSchema = RequestSchema.extend({
    method: z.literal('sync'),
    params: z.object({
        from: z.string(),
        to: z.string().optional()
    })
});

// synchronization acknowledgment: indicate the server client is ready to read `amount` rows
export const SyncAkReqSchema = RequestSchema.extend({
    method: z.literal('sync_ak'),
    params: z.object({
        amount: z.string()
    })
});

// server side request schemas

// synchronize row request: when a sync session is stablished and client is ready to receive more blocks
// the sync task will send rows using this message, once task reaches head, LiveRowReqs will be sent instead
export const SyncRowReqSchema = RequestSchema.extend({
    method: z.literal('sync_row'),
    params: z.union([
        z.object({
            row: z.array(z.any()),
            refs: z.record(z.array(z.any()))
        }),
        z.any() // assuming RowWithRefs is defined somewhere else
    ])
});

// apply live row request: sent when client has a sync task with task.cursor = lastOrdinal - 1 and task.akOrdinal >= lastOrdinal
export const LiveRowReqSchema = RequestSchema.extend({
    method: z.literal('live_row'),
    params: z.union([
        z.object({
            row: z.array(z.any()),
            refs: z.record(z.array(z.any()))
        }),
        z.any() // assuming RowWithRefs is defined somewhere else
    ])
});

// broadcasted on `flush` topic, indicates writer just did a disk flush
export const FlushReqSchema = RequestSchema.extend({
    method: z.literal('flush'),
    params: z.object({
        adjusted_ordinal: z.string(),
        batch_index: z.string(),
        last_on_disk: z.string()
    })
});

// base response schema
export const ResponseSchema = z.object({
    result: z.any().optional(),
    error: z.object({
        message: z.string(),
        stack: z.string()
    }).optional(),
    id: z.string()
});

export const StrictResponseSchema = ResponseSchema.refine(resp => (  // xor error & result fields
    (resp.result !== undefined || resp.error !== undefined)
    &&
    !(resp.result !== undefined && resp.error !== undefined)
), {
    message: 'Response must contain either result or error field.'
});

// generic success
export const OkResponseSchema = ResponseSchema.extend({
    result: z.literal('ok')
});

// client side response schemas

export const SubResSchema = OkResponseSchema;
export const GetInfoResSchema = ResponseSchema.extend({
    result: z.object({
        last_ordinal: z.string()
    })
});
export const GetRowResSchema = ResponseSchema.extend({
    result: z.union([
        z.object({
            row: z.array(z.any()),
            refs: z.record(z.array(z.any()))
        }),
        z.any() // assuming RowWithRefs is defined somewhere else
    ])
});
export const SyncResSchema = ResponseSchema.extend({
    result: z.object({
        distance: z.string()
    })
});
export const SyncAkResSchema = ResponseSchema.extend({
    result: z.object({
        task_status: z.enum(['started', 'running']),
        last_ordinal: z.string()
    })
});

export type Request = z.infer<typeof RequestSchema>;
export type SubReq = z.infer<typeof SubReqSchema>;
export type GetInfoReq = z.infer<typeof GetInfoReqSchema>;
export type GetRowReq = z.infer<typeof GetRowReqSchema>;
export type SyncReq = z.infer<typeof SyncReqSchema>;
export type SyncAkReq = z.infer<typeof SyncAkReqSchema>;
export type SyncRowReq = z.infer<typeof SyncRowReqSchema>;
export type LiveRowReq = z.infer<typeof LiveRowReqSchema>;
export type FlushReq = z.infer<typeof FlushReqSchema>;

export type Response = z.infer<typeof ResponseSchema>;
export type SubRes = z.infer<typeof SubResSchema>;
export type GetInfoRes = z.infer<typeof GetInfoResSchema>;
export type GetRowRes = z.infer<typeof GetRowResSchema>;
export type SyncRes = z.infer<typeof SyncResSchema>;
export type SyncAkRes = z.infer<typeof SyncAkResSchema>;
