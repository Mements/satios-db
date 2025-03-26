// examples/exchange/simulation.ts
import { z } from "zod";
import path from "path";
import os from "os";
import { rm } from "node:fs/promises";
import { createDatabase, type SatiDatabase, type ChangeEvent, type DatabaseRecord } from "../src/database"

// --- Configuration ---
const SIMULATION_DURATION_MS = 35 * 1000; // Run for 35 seconds
const TRANSACTION_INTERVAL_MS = 500;    // ~2 transactions per second
const CANDLE_INTERVAL_MS = 10 * 1000;   // 1 candle every 10 seconds
const TICKER_SYMBOL = "BTC-USD";
const DB_BASE_PATH = path.join(os.tmpdir(), `exchange_sim_${Date.now()}`); // Use temp dir for isolation

// --- Schemas ---
const CandleInputSchema = z.object({
    ticker: z.string(),
    // Timestamp represents the *start* time of the candle interval
    timestamp: z.number().int(), // Unix timestamp (seconds)
});
const CandleOutputSchema = z.object({
    open: z.number(),
    high: z.number(),
    low: z.number(),
    close: z.number(),
    volume: z.number(),
});

// Need a unique identifier per transaction in the input
const TransactionInputSchema = z.object({
    ticker: z.string(),
    transactionId: z.string(), // Unique ID for each trade
});
const TransactionOutputSchema = z.object({
    price: z.number(),
    quantity: z.number(),
    timestamp: z.number(), // Unix timestamp (milliseconds)
    side: z.enum(['buy', 'sell']),
});

const TickerInputSchema = z.object({
    ticker: z.string(),
});
const TickerOutputSchema = z.object({
    currentPrice: z.number().optional(),
    lastUpdatedAt: z.number().optional(), // Unix timestamp (milliseconds)
});

// --- Type Aliases ---
type Transaction = DatabaseRecord<z.infer<typeof TransactionInputSchema>, z.infer<typeof TransactionOutputSchema>>;
type Candle = DatabaseRecord<z.infer<typeof CandleInputSchema>, z.infer<typeof CandleOutputSchema>>;
type Ticker = DatabaseRecord<z.infer<typeof TickerInputSchema>, z.infer<typeof TickerOutputSchema>>;

// --- Simulation State ---
let recentTransactions: Transaction[] = [];
let transactionListenerStop: (() => void) | null = null;
let transactionIntervalId: Timer | null = null;
let candleIntervalId: Timer | null = null;

// --- Helper Functions ---
function generateRandomTransaction(ticker: string): Omit<Transaction, 'id'> {
    const now = Date.now();
    const price = 30000 + (Math.random() - 0.5) * 1000; // Simulate price fluctuation
    const quantity = Math.random() * 0.5 + 0.01; // Simulate quantity
    const side = Math.random() > 0.5 ? 'buy' : 'sell';
    return {
        input: {
            ticker: ticker,
            // Simple unique ID using timestamp and random element
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

// --- Database Initialization ---
async function setupDatabases() {
    console.log(`Using database path: ${DB_BASE_PATH}`);

    const candlesDb = createDatabase("exchange_candles", CandleInputSchema, CandleOutputSchema, path.join(DB_BASE_PATH, "candles.sqlite"));
    const transactionsDb = createDatabase("exchange_transactions", TransactionInputSchema, TransactionOutputSchema, path.join(DB_BASE_PATH, "transactions.sqlite"));
    const tickersDb = createDatabase("exchange_tickers", TickerInputSchema, TickerOutputSchema, path.join(DB_BASE_PATH, "tickers.sqlite"));

    await candlesDb.init();
    await transactionsDb.init();
    await tickersDb.init();

    // Optional: Initialize ticker if it doesn't exist
    await tickersDb.insert({ ticker: TICKER_SYMBOL }, { currentPrice: undefined, lastUpdatedAt: undefined });

    return { candlesDb, transactionsDb, tickersDb };
}

// --- Simulation Logic ---
async function runSimulation(
    candlesDb: SatiDatabase<typeof CandleInputSchema, typeof CandleOutputSchema>,
    transactionsDb: SatiDatabase<typeof TransactionInputSchema, typeof TransactionOutputSchema>,
    tickersDb: SatiDatabase<typeof TickerInputSchema, typeof TickerOutputSchema>
) {
    console.log("Starting simulation...");

    // 1. Listen for new transactions and buffer them
    const listener = transactionsDb.listen((event: ChangeEvent<z.infer<typeof TransactionInputSchema>, z.infer<typeof TransactionOutputSchema>>) => {
        // We only care about newly added full records (input + output)
        if (event.type === 'output_added' && event.input && event.output) {
            // Ensure the transaction is for the ticker we care about (though in this sim, it always is)
            if (event.input.ticker === TICKER_SYMBOL) {
                console.log(`[Listener] Received new transaction: ${event.input.transactionId}, Price: ${event.output.price}`);
                recentTransactions.push({ id: event.id, input: event.input, output: event.output });
            }
        }
    });
    transactionListenerStop = listener.stop; // Store stop function for cleanup

    // 2. Simulate new transactions arriving
    transactionIntervalId = setInterval(async () => {
        const newTxData = generateRandomTransaction(TICKER_SYMBOL);
        try {
            const { id } = await transactionsDb.insert(newTxData.input, newTxData.output);
            // console.log(`[Generator] Inserted transaction ${newTxData.input.transactionId} (DB ID: ${id})`);
        } catch (error) {
            console.error(`[Generator] Error inserting transaction:`, error);
        }
    }, TRANSACTION_INTERVAL_MS);

    // 3. Periodically aggregate transactions into candles
    candleIntervalId = setInterval(async () => {
        const transactionsToProcess = [...recentTransactions]; // Copy buffer
        recentTransactions = []; // Clear buffer immediately

        if (transactionsToProcess.length === 0) {
            console.log(`[Candle Aggregator] No new transactions in this interval.`);
            return;
        }

        console.log(`[Candle Aggregator] Processing ${transactionsToProcess.length} transactions...`);

        // Calculate OHLCV
        let open = transactionsToProcess[0].output!.price;
        let high = open;
        let low = open;
        let close = transactionsToProcess[transactionsToProcess.length - 1].output!.price;
        let volume = 0;
        let firstTimestamp = transactionsToProcess[0].output!.timestamp;
        // Candle timestamp should align to the start of the interval
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

        try {
            // Insert/Update the candle
            const { id: candleId } = await candlesDb.insert(candleInput, candleOutput);
            console.log(`[Candle Aggregator] Saved Candle ${TICKER_SYMBOL} @ ${new Date(candleTimestampSec * 1000).toISOString()} (DB ID: ${candleId}) O:${open} H:${high} L:${low} C:${close} V:${volume}`);

            // Update the ticker's current price
            const tickerInput: z.infer<typeof TickerInputSchema> = { ticker: TICKER_SYMBOL };
            const tickerOutput: z.infer<typeof TickerOutputSchema> = {
                currentPrice: close,
                lastUpdatedAt: Date.now(),
            };
            const { id: tickerId } = await tickersDb.update(tickerInput, tickerOutput);
            console.log(`[Ticker Update] Updated ${TICKER_SYMBOL} price to ${close} (DB ID: ${tickerId})`);

            // --- Simulate Broadcasting via WebSockets (using console.log) ---
            console.log(`--- BROADCAST START ---`);
            // 1. Broadcast new candle
            console.log(`[WebSocket Broadcast] New Candle:`, { input: candleInput, output: candleOutput });
            // 2. Broadcast updated ticker
            console.log(`[WebSocket Broadcast] Ticker Update:`, { input: tickerInput, output: tickerOutput });
            // 3. Broadcast list of new transactions included in this candle interval
            console.log(`[WebSocket Broadcast] New Transactions (${transactionsToProcess.length}):`, transactionsToProcess.map(tx => ({ id: tx.input.transactionId, ...tx.output })));
            console.log(`--- BROADCAST END ---`);

        } catch (error) {
            console.error(`[Candle Aggregator] Error processing interval:`, error);
        }

    }, CANDLE_INTERVAL_MS);

    // --- Stop Simulation after duration ---
    await new Promise(resolve => setTimeout(resolve, SIMULATION_DURATION_MS));

    console.log("Stopping simulation...");
    if (transactionListenerStop) transactionListenerStop();
    if (transactionIntervalId) clearInterval(transactionIntervalId);
    if (candleIntervalId) clearInterval(candleIntervalId);

    console.log("Simulation finished.");
}

// --- Cleanup ---
async function cleanup(databases: { candlesDb: any; transactionsDb: any; tickersDb: any; }) {
    console.log("Closing database connections...");
    databases.candlesDb.close();
    databases.transactionsDb.close();
    databases.tickersDb.close();

    console.log(`Cleaning up database directory: ${DB_BASE_PATH}`);
    try {
        await rm(DB_BASE_PATH, { recursive: true, force: true });
        console.log("Cleanup successful.");
    } catch (error) {
        console.error(`Error during cleanup:`, error);
    }
}

// --- Main Execution ---
async function main() {
    let databases: {
        candlesDb: SatiDatabase<typeof CandleInputSchema, typeof CandleOutputSchema>;
        transactionsDb: SatiDatabase<typeof TransactionInputSchema, typeof TransactionOutputSchema>;
        tickersDb: SatiDatabase<typeof TickerInputSchema, typeof TickerOutputSchema>;
    } | null = null;

    try {
        databases = await setupDatabases();
        await runSimulation(databases.candlesDb, databases.transactionsDb, databases.tickersDb);
    } catch (error) {
        console.error("Simulation failed:", error);
    } finally {
        if (databases) {
            await cleanup(databases);
        }
        process.exit(0); // Exit cleanly
    }
}

// Run the main function if this script is executed directly
if (require.main === module) {
    main();
}

// Export elements needed for testing
export {
    CandleInputSchema, CandleOutputSchema,
    TransactionInputSchema, TransactionOutputSchema,
    TickerInputSchema, TickerOutputSchema,
    setupDatabases, // To reuse DB setup in tests
    cleanup,        // To reuse cleanup in tests
    runSimulation,  // Potentially test the whole flow
    TICKER_SYMBOL,
    CANDLE_INTERVAL_MS,
    TRANSACTION_INTERVAL_MS,
    DB_BASE_PATH as SIMULATION_DB_BASE_PATH // Export the path used by the sim
};
