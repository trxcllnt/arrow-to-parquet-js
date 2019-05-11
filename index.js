const parquet = require('parquetjs');
const { AsyncIterable } = require('ix');
const { Table, IntVector, Utf8Vector } = require('apache-arrow');

const { toParquetSchema } = require('./toparquettype');

const table = Table.new({
    int: IntVector.from(new Int32Array([0, 1, 2])),
    str: Utf8Vector.from(['foo', 'bar', 'baz']),
});

(async () => {
    const rows = AsyncIterable.from(table[Symbol.iterator]());
    const schema = new parquet.ParquetSchema(toParquetSchema(table.schema));
    const stream = rows.toNodeStream({ objectMode: true }).pipe(new parquet.ParquetTransformer(schema));
    const buffer = await AsyncIterable.fromNodeStream(stream).toArray().then((bufs) => Buffer.concat(bufs));
    const reader = await createParquetBufferReader(buffer);
    for await (const record of getCursor(reader)) {
        console.log(record);
    }
})().catch((e) => console.error(e) || process.exit(1));

async function* getCursor(reader) {
    const cursor = reader.getCursor();
    let record = null;
    while (record = await cursor.next()) {
        yield record;
    }
}

async function createParquetBufferReader(buffer) {

    const bufferReader = new BufferReader(buffer);
    const readFn = bufferReader.fread.bind(bufferReader);
    const closeFn = bufferReader.fclose.bind(bufferReader);
    const envelopeReader = new parquet.ParquetEnvelopeReader(readFn, closeFn, buffer.byteLength);

    try {
        await envelopeReader.readHeader();
        let metadata = await envelopeReader.readFooter();
        return new parquet.ParquetReader(metadata, envelopeReader);
    } catch (err) {
        await envelopeReader.close();
        throw err;
    }
}

class BufferReader {
    constructor(buffer) {
        this.buffer = buffer;
    }
    async fread(position, length) {
        return this.buffer.subarray(position, position + length);
    }
    async fclose() {
    }
}
