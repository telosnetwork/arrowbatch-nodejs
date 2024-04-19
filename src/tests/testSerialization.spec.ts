import { encodeRowValue, decodeRowValue, ArrowTableMapping} from '../index.js';
import {expect} from "chai";


const tableName = 'test_table';

describe('u8 encode/decode', () => {
    const fieldInfo: ArrowTableMapping = {name: 'test_u8', type: 'u8'};

    it('should encode and decode u8', () => {
        const value = 255;
        const encoded = encodeRowValue(tableName, fieldInfo, value);
        const decoded = decodeRowValue(tableName, fieldInfo, encoded);
        expect(decoded).to.be.equal(value);
    });

    it('should encode and decode u8 from string', () => {
        const value = '42';
        const encoded = encodeRowValue(tableName, fieldInfo, value);
        const decoded = decodeRowValue(tableName, fieldInfo, encoded);
        expect(typeof decoded).to.be.equal('number');
        expect(decoded).to.be.equal(parseInt(value));
    });
});

describe('u16 encode/decode', () => {
    const fieldInfo: ArrowTableMapping = {name: 'test_u16', type: 'u16'};

    it('should encode and decode u16', () => {
        const value = 65535;
        const encoded = encodeRowValue(tableName, fieldInfo, value);
        const decoded = decodeRowValue(tableName, fieldInfo, encoded);
        expect(decoded).to.be.equal(value);
    });

    it('should encode and decode u16 from string', () => {
        const value = '1234';
        const encoded = encodeRowValue(tableName, fieldInfo, value);
        const decoded = decodeRowValue(tableName, fieldInfo, encoded);
        expect(typeof decoded).to.be.equal('number');
        expect(decoded).to.be.equal(parseInt(value));
    });
});

describe('u32 encode/decode', () => {
    const fieldInfo: ArrowTableMapping = {name: 'test_u32', type: 'u32'};

    it('should encode and decode u32', () => {
        const value = 4294967295;
        const encoded = encodeRowValue(tableName, fieldInfo, value);
        const decoded = decodeRowValue(tableName, fieldInfo, encoded);
        expect(decoded).to.be.equal(value);
    });

    it('should encode and decode u32 from string', () => {
        const value = '1234567';
        const encoded = encodeRowValue(tableName, fieldInfo, value);
        const decoded = decodeRowValue(tableName, fieldInfo, encoded);
        expect(typeof decoded).to.be.equal('number');
        expect(decoded).to.be.equal(parseInt(value));
    });
});

describe('u64 encode/decode', () => {
    const fieldInfo: ArrowTableMapping = {name: 'test_u64', type: 'u64'};
    it('should encode and decode u64', () => {
        const fieldInfo: ArrowTableMapping = {name: 'test_u64', type: 'u64'};
        const value = BigInt('18446744073709551615');
        const encoded = encodeRowValue(tableName, fieldInfo, value);
        const decoded = decodeRowValue(tableName, fieldInfo, encoded);
        expect(decoded).to.be.equal(value);
    });

    it('should encode and decode u64 from boolean', () => {
        const value = true;
        const encoded = encodeRowValue(tableName, fieldInfo, value);
        const decoded = decodeRowValue(tableName, fieldInfo, encoded);
        expect(decoded).to.be.equal(BigInt(1));
    });

    it('should encode and decode u64 from decimal string', () => {
        const value = '1234567890';
        const encoded = encodeRowValue(tableName, fieldInfo, value);
        const decoded = decodeRowValue(tableName, fieldInfo, encoded);
        expect(decoded).to.be.equal(BigInt(value));
    });

    it('should encode and decode u64 from hex string with 0x prefix', () => {
        const value = '0x1234567890abcdef';
        const encoded = encodeRowValue(tableName, fieldInfo, value);
        const decoded = decodeRowValue(tableName, fieldInfo, encoded);
        expect(decoded).to.be.equal(BigInt(value));
    });

    it('should throw an error when encoding u64 from hex string without 0x prefix', () => {
        const value = '1234567890abcdef';
        expect(() => {
            encodeRowValue(tableName, fieldInfo, value);
        }).to.throw();
    });

    it('should encode and decode u64 from BigInt', () => {
        const value = BigInt('18446744073709551615');
        const encoded = encodeRowValue(tableName, fieldInfo, value);
        const decoded = decodeRowValue(tableName, fieldInfo, encoded);
        expect(decoded).to.be.equal(value);
    });
});

describe('uintvar encode/decode', () => {
    const fieldInfo: ArrowTableMapping = {name: 'test_uintvar', type: 'uintvar'};

    it('should encode and decode uintvar from decimal string', () => {
        const value = '1234567890123456789012345678901234567890';
        const encoded = encodeRowValue(tableName, fieldInfo, value);
        const decoded = decodeRowValue(tableName, fieldInfo, encoded);
        expect(decoded).to.be.equal(BigInt(value));
    });

    it('should encode and decode uintvar from hex string with 0x prefix', () => {
        const value = '0x1234567890abcdef';
        const encoded = encodeRowValue(tableName, fieldInfo, value);
        const decoded = decodeRowValue(tableName, fieldInfo, encoded);
        expect(decoded).to.be.equal(BigInt(value));
    });

    it('should encode and decode uintvar from BigInt', () => {
        const value = BigInt('1234567890123456789012345678901234567890');
        const encoded = encodeRowValue(tableName, fieldInfo, value);
        const decoded = decodeRowValue(tableName, fieldInfo, encoded);
        expect(decoded).to.be.equal(value);
    });

    it('should encode and decode uintvar from number', () => {
        const value = 1234567890;
        const encoded = encodeRowValue(tableName, fieldInfo, value);
        const decoded = decodeRowValue(tableName, fieldInfo, encoded);
        expect(decoded).to.be.equal(BigInt(value));
    });

    it('should encode and decode uintvar from Uint8Array', () => {
        const value = new Uint8Array([0x12, 0x34, 0x56, 0x78]);
        const encoded = encodeRowValue(tableName, fieldInfo, value);
        const decoded = decodeRowValue(tableName, fieldInfo, encoded);
        expect(decoded).to.be.equal(BigInt(`0x${Buffer.from(value).toString('hex')}`));
    });

    it('should encode and decode uintvar from Buffer', () => {
        const value = Buffer.from([0x12, 0x34, 0x56, 0x78]);
        const encoded = encodeRowValue(tableName, fieldInfo, value);
        const decoded = decodeRowValue(tableName, fieldInfo, encoded);
        expect(decoded).to.be.equal(BigInt(`0x${value.toString('hex')}`));
    });
});

describe('i64 encode/decode', () => {
    const tableName = 'test_table';
    const fieldInfo: ArrowTableMapping = { name: 'test_i64', type: 'i64' };

    it('should encode and decode i64', () => {
        const value = BigInt('-9223372036854775808');

        const encoded = encodeRowValue(tableName, fieldInfo, value);
        const decoded = decodeRowValue(tableName, fieldInfo, encoded);

        expect(decoded).to.be.equal(value);
    });

    it('should encode and decode i64 from boolean', () => {
        const value = true;

        const encoded = encodeRowValue(tableName, fieldInfo, value);
        const decoded = decodeRowValue(tableName, fieldInfo, encoded);

        expect(decoded).to.be.equal(BigInt(1));
    });

    it('should encode and decode i64 from decimal string', () => {
        const value = '-1234567890';

        const encoded = encodeRowValue(tableName, fieldInfo, value);
        const decoded = decodeRowValue(tableName, fieldInfo, encoded);

        expect(decoded).to.be.equal(BigInt(value));
    });

    it('should throw an error when encoding negative i64 to small', () => {
        const value = '-9223372036854775809';

        expect(() => {
            encodeRowValue(tableName, fieldInfo, value);
        }).to.throw();
    });

    it('should throw an error when encoding positive i64 to large', () => {
        const value = '9223372036854775808';

        expect(() => {
            encodeRowValue(tableName, fieldInfo, value);
        }).to.throw();
    });

    it('should encode and decode i64 from BigInt', () => {
        const value = BigInt('-9223372036854775808');

        const encoded = encodeRowValue(tableName, fieldInfo, value);
        const decoded = decodeRowValue(tableName, fieldInfo, encoded);

        expect(decoded).to.be.equal(value);
    });
});

describe('bytefields encode/decode', () => {
    it('should encode and decode string', () => {
        const fieldInfo: ArrowTableMapping = {name: 'test_string', type: 'string'};
        const value = 'Hello, world!';
        const encoded = encodeRowValue(tableName, fieldInfo, value);
        const decoded = decodeRowValue(tableName, fieldInfo, encoded);
        expect(decoded).to.be.equal(value);
    });

    it('should encode and decode bytes', () => {
        const fieldInfo: ArrowTableMapping = {name: 'test_bytes', type: 'bytes', length: 4};
        const value = '1a2b3c4d';
        const encoded = encodeRowValue(tableName, fieldInfo, value);
        const decoded = decodeRowValue(tableName, fieldInfo, encoded);
        expect(decoded).to.be.deep.equal(Buffer.from(value, 'hex'));
    });

    it('should encode and decode checksum160', () => {
        const fieldInfo: ArrowTableMapping = {name: 'test_checksum160', type: 'checksum160'};
        const value = '0123456789abcdef0123456789abcdef01234567';
        const encoded = encodeRowValue(tableName, fieldInfo, value);
        const decoded = decodeRowValue(tableName, fieldInfo, encoded);
        expect(decoded).to.be.equal(value);
    });

    it('should encode and decode checksum256', () => {
        const fieldInfo: ArrowTableMapping = {name: 'test_checksum256', type: 'checksum256'};
        const value = '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef';
        const encoded = encodeRowValue(tableName, fieldInfo, value);
        const decoded = decodeRowValue(tableName, fieldInfo, encoded);
        expect(decoded).to.be.equal(value);
    });
});