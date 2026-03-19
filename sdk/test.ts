import { Lux } from './src/index';

const db = new Lux({ host: 'localhost', port: 6399 });

async function assert(name: string, fn: () => Promise<void>) {
    try {
        await fn();
        console.log(`  PASS: ${name}`);
    } catch (e: any) {
        console.log(`  FAIL: ${name} - ${e.message}`);
        process.exit(1);
    }
}

async function run() {
    await db.call('FLUSHALL');
    console.log('@luxdb/sdk tests\n');

    await assert('vset stores a vector', async () => {
        const result = await db.vset('test:1', [1.0, 0.0, 0.0]);
        if (result !== 'OK') throw new Error(`expected OK, got ${result}`);
    });

    await assert('vset with metadata', async () => {
        const result = await db.vset('test:2', [0.0, 1.0, 0.0], {
            metadata: { label: 'y-axis', score: 42 },
        });
        if (result !== 'OK') throw new Error(`expected OK, got ${result}`);
    });

    await assert('vset with TTL', async () => {
        const result = await db.vset('test:ttl', [0.5, 0.5, 0.0], { ex: 3600 });
        if (result !== 'OK') throw new Error(`expected OK, got ${result}`);
    });

    await assert('vget returns vector data', async () => {
        const result = await db.vget('test:1');
        if (!result) throw new Error('expected result');
        if (result.dims !== 3) throw new Error(`expected dims=3, got ${result.dims}`);
        if (result.vector.length !== 3) throw new Error(`expected 3 floats, got ${result.vector.length}`);
        if (Math.abs(result.vector[0] - 1.0) > 0.01) throw new Error(`expected ~1.0, got ${result.vector[0]}`);
    });

    await assert('vget returns metadata', async () => {
        const result = await db.vget('test:2');
        if (!result) throw new Error('expected result');
        if (!result.metadata) throw new Error('expected metadata');
        if (result.metadata.label !== 'y-axis') throw new Error(`expected y-axis, got ${result.metadata.label}`);
        if (result.metadata.score !== 42) throw new Error(`expected 42, got ${result.metadata.score}`);
    });

    await assert('vget returns null for missing key', async () => {
        const result = await db.vget('nonexistent');
        if (result !== null) throw new Error('expected null');
    });

    await assert('vcard counts vectors', async () => {
        const count = await db.vcard();
        if (count !== 3) throw new Error(`expected 3, got ${count}`);
    });

    await assert('vsearch finds nearest neighbors', async () => {
        const results = await db.vsearch([1.0, 0.0, 0.0], { k: 2 });
        if (results.length !== 2) throw new Error(`expected 2 results, got ${results.length}`);
        if (results[0].key !== 'test:1') throw new Error(`expected test:1 first, got ${results[0].key}`);
        if (results[0].similarity < 0.99) throw new Error(`expected ~1.0 similarity, got ${results[0].similarity}`);
    });

    await assert('vsearch with META returns metadata', async () => {
        const results = await db.vsearch([0.0, 1.0, 0.0], { k: 1, meta: true });
        if (results.length !== 1) throw new Error(`expected 1 result, got ${results.length}`);
        if (results[0].key !== 'test:2') throw new Error(`expected test:2, got ${results[0].key}`);
        if (!results[0].metadata) throw new Error('expected metadata');
        if (results[0].metadata.label !== 'y-axis') throw new Error(`expected y-axis`);
    });

    await assert('vsearch with FILTER', async () => {
        const results = await db.vsearch([1.0, 0.0, 0.0], {
            k: 10,
            filter: { key: 'label', value: 'y-axis' },
            meta: true,
        });
        if (results.length !== 1) throw new Error(`expected 1 filtered result, got ${results.length}`);
        if (results[0].key !== 'test:2') throw new Error(`expected test:2`);
    });

    await assert('vset overwrites existing key', async () => {
        await db.vset('test:1', [0.0, 0.0, 1.0]);
        const result = await db.vget('test:1');
        if (!result) throw new Error('expected result');
        if (Math.abs(result.vector[2] - 1.0) > 0.01) throw new Error(`expected z=1.0, got ${result.vector[2]}`);
        const count = await db.vcard();
        if (count !== 3) throw new Error(`expected 3 after overwrite, got ${count}`);
    });

    await assert('standard redis commands still work', async () => {
        await db.set('regular:key', 'hello');
        const val = await db.get('regular:key');
        if (val !== 'hello') throw new Error(`expected hello, got ${val}`);
    });

    await assert('delete removes vector', async () => {
        await db.del('test:ttl');
        const count = await db.vcard();
        if (count !== 2) throw new Error(`expected 2 after delete, got ${count}`);
    });

    console.log('\nAll tests passed.');
    db.disconnect();
}

run().catch((e) => {
    console.error(e);
    db.disconnect();
    process.exit(1);
});
