import { z } from "zod";
import path from "path";
import os from "os";
import { rm } from "node:fs/promises";
import { createDatabase, SatiDatabase, DatabaseRecord } from "../youtube-twitter-agent/databases/core"; // Adjust path
import { measure, MeasureFunction } from "./measure"; // Adjust path

// --- Configuration ---
const SIMULATION_ID = `SIM-${Date.now().toString().slice(-4)}`;
const SIMULATION_DURATION_MS = 35 * 1000;
const TRANSACTION_INTERVAL_MS = 500;
const CANDLE_INTERVAL_MS = 10 * 1000;
const TICKER_SYMBOL = "BTC-USD";
const DB_BASE_PATH = path.join(os.tmpdir(), `exchange_sim_measured_${Date.now()}`);

// --- Schemas ---
const CandleInputSchema = z.object({
    ticker: z.string(),
    timestamp: z.number().int(),
});
const CandleOutputSchema = z.object({
    open: z.number(),
    high: z.number(),
    low: z.number(),
    close: z.number(),
    volume: z.number(),
});

const TransactionInputSchema = z.object({
    ticker: z.string(),
    transactionId: z.string(),
});
const TransactionOutputSchema = z.object({
    price: z.number(),
    quantity: z.number(),
    timestamp: z.number(),
    side: z.enum(['buy', 'sell']),
});

const TickerInputSchema = z.object({
    ticker: z.string(),
});
const TickerOutputSchema = z.object({
    currentPrice: z.number().optional(),
    lastUpdatedAt: z.number().optional(),
});

// --- Type Aliases ---
type Transaction = DatabaseRecord<z.infer<typeof TransactionInputSchema>, z.infer<typeof TransactionOutputSchema>>;
type Candle = DatabaseRecord<z.infer<typeof CandleInputSchema>, z.infer<typeof CandleOutputSchema>>;
type Ticker = DatabaseRecord<z.infer<typeof TickerInputSchema>, z.infer<typeof TickerOutputSchema>>;

type ExchangeDatabases = {
    candlesDb: SatiDatabase<typeof CandleInputSchema, typeof CandleOutputSchema>;
    transactionsDb: SatiDatabase<typeof TransactionInputSchema, typeof TransactionOutputSchema>;
    tickersDb: SatiDatabase<typeof TickerInputSchema, typeof TickerOutputSchema>;
};

// --- Helper Functions ---
function generateRandomTransactionData(ticker: string): Omit<Transaction, 'id'> {
    const now = Date.now();
    const price = 30000 + (Math.random() - 0.5) * 1000;
    const quantity = Math.random() * 0.5 + 0.01;
    const side = Math.random() > 0.5 ? 'buy' : 'sell';
    return {
        input: {
            ticker: ticker,
            transactionId: `tx-${now}-${Math.random().toString(16).slice(2, 8)}`
        },
        output: {
            price: parseFloat(price.toFixed(2)),
            quantity: parseFloat(quantity.toFixed(6)),
            timestamp: now,
            side: side,
        }
    };
}

async function createExchangeDatabases(m: MeasureFunction): Promise<ExchangeDatabases> {
    return m(async (m2) => {
        const candlesDb = await m2(() => Promise.resolve(
            createDatabase("exchange_candles", CandleInputSchema, CandleOutputSchema, path.join(DB_BASE_PATH, "candles.sqlite"))
        ), "Create Candles DB Instance");

        const transactionsDb = await m2(() => Promise.resolve(
            createDatabase("exchange_transactions", TransactionInputSchema, TransactionOutputSchema, path.join(DB_BASE_PATH, "transactions.sqlite"))
        ), "Create Transactions DB Instance");

        const tickersDb = await m2(() => Promise.resolve(
            createDatabase("exchange_tickers", TickerInputSchema, TickerOutputSchema, path.join(DB_BASE_PATH, "tickers.sqlite"))
        ), "Create Tickers DB Instance");

        await m2(() => candlesDb.init(), "Initialize Candles DB");
        await m2(() => transactionsDb.init(), "Initialize Transactions DB");
        await m2(() => tickersDb.init(), "Initialize Tickers DB");

        await m2(() => tickersDb.insert({ ticker: TICKER_SYMBOL }, { currentPrice: undefined, lastUpdatedAt: undefined }),
            "Initialize Ticker Record");

        return { candlesDb, transactionsDb, tickersDb };
    }, "Setup Databases");
}

async function runTransactionSimulator(m: MeasureFunction, db: SatiDatabase<typeof TransactionInputSchema, typeof TransactionOutputSchema>): Promise<() => void> {
    return m(async () => {
        const intervalId = setInterval(async () => {
            await measure(async (m2) => {
                const newTxData = generateRandomTransactionData(TICKER_SYMBOL);
                await m2(() => db.insert(newTxData.input, newTxData.output),
                    `Insert Transaction ${newTxData.input.transactionId}`);
            }, "Simulate Single Transaction", { requestId: SIMULATION_ID, level: 1 }); // Use top-level measure for interval actions
        }, TRANSACTION_INTERVAL_MS);

        const stop = () => clearInterval(intervalId);
        return stop;
    }, "Start Transaction Simulator");
}


async function aggregateCandleAndBroadcast(m: MeasureFunction, dbs: ExchangeDatabases, lastProcessedTimestamp: number): Promise<number> {
    return m(async (m2) => {
        const newTransactions = await m2(() =>
            dbs.transactionsDb.find(
                { ticker: TICKER_SYMBOL }, // Input filter
                { timestamp: undefined } // Placeholder for output filter, needs adjustment if querying by output
            ).then(txs => txs.filter(tx => tx.output && tx.output.timestamp > lastProcessedTimestamp)
                             .sort((a, b) => a.output!.timestamp - b.output!.timestamp) // Ensure order
            ),
            `Find New Transactions since ${lastProcessedTimestamp}`
        );


        if (newTransactions.length === 0) {
            // No action needed, return the same timestamp
            return lastProcessedTimestamp;
        }

        const latestTimestamp = newTransactions[newTransactions.length - 1].output!.timestamp;

        await m2(async (m3) => {
            const transactionsToProcess = newTransactions;

            let open = transactionsToProcess[0].output!.price;
            let high = open;
            let low = open;
            let close = transactionsToProcess[transactionsToProcess.length - 1].output!.price;
            let volume = 0;
            let firstTimestamp = transactionsToProcess[0].output!.timestamp;
            const candleTimestampSec = Math.floor(firstTimestamp / 1000 / (CANDLE_INTERVAL_MS / 1000)) * (CANDLE_INTERVAL_MS / 1000);

            for (const tx of transactionsToProcess) {
                const price = tx.output!.price;
                high = Math.max(high, price);
                low = Math.min(low, price);
                volume += tx.output!.quantity;
            }

            const candleInput: z.infer<typeof CandleInputSchema> = {
                ticker: TICKER_SYMBOL,
                timestamp: candleTimestampSec,
            };
            const candleOutput: z.infer<typeof CandleOutputSchema> = {
                open: open,
                high: parseFloat(high.toFixed(2)),
                low: parseFloat(low.toFixed(2)),
                close: close,
                volume: parseFloat(volume.toFixed(6)),
            };

            const { id: candleId } = await m3(() => dbs.candlesDb.insert(candleInput, candleOutput),
                `Save Candle ${TICKER_SYMBOL} @ ${new Date(candleTimestampSec * 1000).toISOString()}`);

            const tickerInput: z.infer<typeof TickerInputSchema> = { ticker: TICKER_SYMBOL };
            const tickerOutput: z.infer<typeof TickerOutputSchema> = {
                currentPrice: close,
                lastUpdatedAt: Date.now(),
            };
            const { id: tickerId } = await m3(() => dbs.tickersDb.update(tickerInput, tickerOutput),
                `Update Ticker ${TICKER_SYMBOL} to ${close}`);

            await m3(async () => {
                 console.log(`--- BROADCAST START (Candle ${candleId}, Ticker ${tickerId}) ---`);
                 console.log(`[WebSocket Broadcast] New Candle:`, { input: candleInput, output: candleOutput });
                 console.log(`[WebSocket Broadcast] Ticker Update:`, { input: tickerInput, output: tickerOutput });
                 console.log(`[WebSocket Broadcast] New Transactions (${transactionsToProcess.length}):`, transactionsToProcess.map(tx => ({ id: tx.input.transactionId, ...tx.output })));
                 console.log(`--- BROADCAST END ---`);
            }, "Simulate WebSocket Broadcast");

        }, `Process ${newTransactions.length} Transactions into Candle`);

        return latestTimestamp; // Return the timestamp of the last processed transaction

    }, "Run Candle Aggregation Interval");
}


async function runCandleAggregator(m: MeasureFunction, dbs: ExchangeDatabases): Promise<() => void> {
    return m(async () => {
        let lastProcessedTimestamp = Date.now() - CANDLE_INTERVAL_MS; // Start by looking slightly back

        const intervalId = setInterval(async () => {
            // Use top-level measure for the interval action
             try {
                 const newLastTimestamp = await measure(
                     (m2) => aggregateCandleAndBroadcast(m2, dbs, lastProcessedTimestamp),
                    "Aggregate Candle and Broadcast",
                    { requestId: SIMULATION_ID, level: 1 }
                );
                lastProcessedTimestamp = newLastTimestamp;
            } catch (error) {
                // Error is already logged by measure, just prevent interval crash
                console.error("[Candle Aggregator Interval] Uncaught error during aggregation:", error);
            }
        }, CANDLE_INTERVAL_MS);

        const stop = () => clearInterval(intervalId);
        return stop;
    }, "Start Candle Aggregator");
}


async function cleanupDatabases(m: MeasureFunction, databases: ExchangeDatabases | null) {
    if (!databases) return;
    await m(async (m2) => {
        await m2(() => Promise.resolve(databases.candlesDb.close()), "Close Candles DB");
        await m2(() => Promise.resolve(databases.transactionsDb.close()), "Close Transactions DB");
        await m2(() => Promise.resolve(databases.tickersDb.close()), "Close Tickers DB");

        await m2(async () => {
             try {
                await rm(DB_BASE_PATH, { recursive: true, force: true });
            } catch (error: any) {
                 // Log but don't fail the cleanup process entirely if dir removal fails
                console.warn(`Warn: Failed to remove DB directory ${DB_BASE_PATH}: ${error.message}`)
            }
        }, `Remove DB Directory ${DB_BASE_PATH}`);
    }, "Cleanup Databases");
}


// --- Main Execution ---
async function main() {
    let databases: ExchangeDatabases | null = null;
    let stopTransactionSimulator: (() => void) | null = null;
    let stopCandleAggregator: (() => void) | null = null;

    await measure(async (m1) => {
        try {
            databases = await createExchangeDatabases(m1);

            // Start simulators concurrently
            [stopTransactionSimulator, stopCandleAggregator] = await Promise.all([
                 runTransactionSimulator(m1, databases.transactionsDb),
                 runCandleAggregator(m1, databases)
            ]);


            await m1(() => new Promise(resolve => setTimeout(resolve, SIMULATION_DURATION_MS)),
                `Run Simulation for ${SIMULATION_DURATION_MS / 1000}s`);

        } finally {
             await measure(async (m2) => { // Use a new top-level measure for stop/cleanup
                if (stopTransactionSimulator) {
                   await m2(() => Promise.resolve(stopTransactionSimulator!()), "Stop Transaction Simulator");
                }
                if (stopCandleAggregator) {
                    await m2(() => Promise.resolve(stopCandleAggregator!()), "Stop Candle Aggregator");
                }
                await cleanupDatabases(m2, databases);
            }, "Shutdown Simulation", { requestId: SIMULATION_ID });
        }
    }, "Exchange Simulation", { requestId: SIMULATION_ID }); // Assign simulation ID
}

// Execute main
main().catch(error => {
    console.error("Simulation exited with fatal error:", error);
    process.exit(1);
});

// Export elements potentially needed for testing (adjust as necessary)
export {
    CandleInputSchema, CandleOutputSchema,
    TransactionInputSchema, TransactionOutputSchema,
    TickerInputSchema, TickerOutputSchema,
    createExchangeDatabases, // Potentially useful for test setup
    cleanupDatabases,        // Potentially useful for test teardown
    TICKER_SYMBOL,
    CANDLE_INTERVAL_MS,
    TRANSACTION_INTERVAL_MS,
    DB_BASE_PATH as SIMULATION_DB_BASE_PATH
};