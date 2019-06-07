const parquet = require('parquetjs');
const { AsyncIterable } = require('ix');
const { RandomAccessFile } = require('apache-arrow/io/file');
const { Table, IntVector, Utf8Vector } = require('apache-arrow');

const { toParquetSchema } = require('./toparquettype');

const table = Table.new({
    int: IntVector.from(new Int32Array([0, 1, 2])),
    str: Utf8Vector.from(['foo', 'bar', 'baz']),
});

(async () => {
    const rows = AsyncIterable.from(table);
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

    const file = new RandomAccessFile(buffer);
    const reader = new parquet.ParquetEnvelopeReader(
        (x, y) => Buffer.from(file.readAt(x, y)),
        file.close.bind(file), buffer.byteLength
    );

    try {
        await reader.readHeader();
        let metadata = await reader.readFooter();
        return new parquet.ParquetReader(metadata, reader);
    } catch (err) {
        await reader.close();
        throw err;
    }
}
