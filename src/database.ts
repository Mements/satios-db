import { Database } from "bun:sqlite";
import { z } from "zod";
import path from "path";
import fs from "node:fs";
import { createHash } from "node:crypto";
import { EventEmitter } from "events";

// Re-using constants and types from previous version
// (Assuming RESERVED_SQLITE_WORDS is defined or imported)
const RESERVED_SQLITE_WORDS = new Set(["ABORT", "ACTION", "ADD", /* ... other reserved words ... */ "WITHOUT"]); // Ensure this is populated

// --- Base Database Interfaces (Mostly unchanged) ---
export interface DatabaseRecord<I, O> {
    id: string; // Hash of the input object
    input: I;
    output?: O; // Output is optional
}

export interface ChangeEvent<I, O> {
    type: 'input_added' | 'output_added' | 'record_updated' | 'record_deleted';
    id: string;
    input?: I;
    output?: O;
}

export type RangeFilter<T> = [T, T];
export type Filter<T extends z.ZodObject<any>> = Partial<z.infer<T> & {
    [K in keyof z.infer<T>]?: z.infer<T>[K] | RangeFilter<z.infer<T>[K]>
}>;

// Interface for the core database operations (remains internal or for advanced use)
interface SatiDatabase<I extends z.ZodObject<any>, O extends z.ZodObject<any>> {
    init: () => Promise<void>;
    insert: (input: z.infer<I>, output?: z.infer<O>) => Promise<{ id: string }>;
    find: (filter: { input?: Filter<I>, output?: Filter<O> }) => Promise<Array<DatabaseRecord<z.infer<I>, z.infer<O>>>>;
    findById: (id: string) => Promise<DatabaseRecord<z.infer<I>, z.infer<O>> | null>;
    update: (input: z.infer<I>, output: Partial<z.infer<O>>) => Promise<{ id: string }>; // Might be less used with new interface
    delete: (input: z.infer<I>) => Promise<{ id: string }>;
    query: <T = any>(sql: string, ...params: any[]) => Promise<T[]>;
    listen: (callback: (event: ChangeEvent<z.infer<I>, z.infer<O>>) => void) => { stop: () => void };
    getInputSchema: () => I;
    getOutputSchema: () => O;
    getName: () => string;
    close: () => void;
    _generateId: (input: z.infer<I>) => string; // Expose ID generation
    _getDbInstance: () => Database; // Expose Bun DB instance if needed
}

// --- Zod Type Guards (Helper Functions - unchanged) ---
function isZodString(value: z.ZodTypeAny): value is z.ZodString { return value._def.typeName === "ZodString"; }
function isZodNumber(value: z.ZodTypeAny): value is z.ZodNumber { return value._def.typeName === "ZodNumber"; }
function isZodBoolean(value: z.ZodTypeAny): value is z.ZodBoolean { return value._def.typeName === "ZodBoolean"; }
function isZodEnum(value: z.ZodTypeAny): value is z.ZodEnum<any> { return value._def.typeName === "ZodEnum"; }
function isZodDate(value: z.ZodTypeAny): value is z.ZodDate { return value._def.typeName === "ZodDate"; }
function isZodArray(value: z.ZodTypeAny): value is z.ZodArray<any> { return value._def.typeName === "ZodArray"; }
function isZodObject(value: z.ZodTypeAny): value is z.ZodObject<any> { return value._def.typeName === "ZodObject"; }
function isZodNullable(value: z.ZodTypeAny): value is z.ZodNullable<any> { return value._def.typeName === "ZodNullable"; }
function isZodOptional(value: z.ZodTypeAny): value is z.ZodOptional<any> { return value._def.typeName === "ZodOptional"; }
function isZodDefault(value: z.ZodTypeAny): value is z.ZodDefault<any> { return value._def.typeName === "ZodDefault"; }
// --- End Zod Type Guards ---

// --- Shared ID Generation Logic ---
// Ensures consistent hashing across different interface functions
function generateRecordId<I extends z.ZodObject<any>>(input: z.infer<I>, inputSchema: I): string {
     // Ensure consistent key order for hashing
     const orderedInput = Object.keys(input)
         .sort()
         .reduce((obj, key) => {
             let value = input[key];
             // Basic stable stringify for hashing consistency
             if (value !== null && typeof value === 'object') {
                  try {
                      value = JSON.stringify(value, (k, v) => {
                          if (v !== null && typeof v === 'object' && !Array.isArray(v)) {
                              return Object.keys(v).sort().reduce((sortedObj, innerKey) => {
                                  sortedObj[innerKey] = v[innerKey];
                                  return sortedObj;
                              }, {});
                          }
                          return v;
                      });
                  } catch { value = JSON.stringify(value); } // Fallback
             } else {
                 value = JSON.stringify(value); // Stringify primitives
             }
             obj[key] = value;
             return obj;
         }, {});
     return createHash('sha256')
         .update(JSON.stringify(orderedInput))
         .digest('hex')
         .substring(0, 16); // Use a shorter hash
}


// --- Core Database Implementation (Two Tables - largely unchanged, becomes more internal) ---
function useDatabaseInternal<I extends z.ZodObject<any>, O extends z.ZodObject<any>>(
    dbPath: string,
    inputSchema: I,
    outputSchema: O
): SatiDatabase<I, O> {
    // --- Start of code copied and adapted from previous `useDatabase` ---
    const baseName = path.basename(dbPath, path.extname(dbPath));
    const sanitizedName = sanitizeName(baseName);
    const inputTable = `input_${sanitizedName}`;
    const outputTable = `output_${sanitizedName}`;
    const changesTable = `_changes_${sanitizedName}`;

    if ('id' in inputSchema.shape || 'id' in outputSchema.shape) {
        throw new Error(`[Database Setup ${baseName}] Schemas cannot contain 'id'.`);
    }

    const dir = path.dirname(dbPath);
    try { fs.mkdirSync(dir, { recursive: true }); } catch (error: any) { if (error.code !== 'EEXIST') throw error; }

    const db = new Database(dbPath);
    let initialized = false;
    const eventEmitter = new EventEmitter();
    let listenerStopHandle: { stop: () => void } | null = null;
    let activeListenersCount = 0; // Track active listeners for polling control

    // --- Utility Functions (getSqliteType, getDefaultValueClause, etc. - Copied Here) ---
     function getSqliteType(zodType: z.ZodTypeAny): string | null { /* ... */ return "TEXT"; } // Simplified for brevity
     function getDefaultValueClause(zodType: z.ZodTypeAny): string { /* ... */ return ""; } // Simplified
     function createTableSql(schema: z.ZodObject<any>, tableName: string, isOutputTable: boolean = false): string { /* ... */ return `CREATE TABLE IF NOT EXISTS ${tableName} (id TEXT PRIMARY KEY)`; } // Simplified
     async function createAutomaticIndexes(dbInstance: Database, tblName: string, schema: z.ZodObject<any>): Promise<void> { /* ... */ } // Simplified
     function processValueForStorage(value: any, zodType: z.ZodTypeAny): any { /* ... */ return value; } // Simplified
     function processObjectForStorage(data: Record<string, any>, schema: z.ZodObject<any>): Record<string, any> { /* ... */ return data; } // Simplified
     function deserializeValue(value: any, zodType: z.ZodTypeAny): any { /* ... */ return value; } // Simplified
     function mapRowToRecord(row: any): DatabaseRecord<z.infer<I>, z.infer<O>> | null { /* ... */ return null; } // Simplified
     function buildWhereClause(/* ... */): string { /* ... */ return ""; } // Simplified
     function ensureTableColumns(/* ... */) { /* ... */ } // Simplified
     const setupTriggers = () => { /* ... */ }; // Simplified
     const pollChanges = async () => { /* ... */ }; // Simplified
     const getRecordByIdInternal = async (id: string): Promise<DatabaseRecord<z.infer<I>, z.infer<O>> | null> => { /* ... */ return null; }; // Simplified
    // --- End Copied Utility Functions ---


    // --- Public API (Now mostly internal or for advanced use) ---
    const internalApi: SatiDatabase<I, O> = {
        async init(): Promise<void> {
            if (initialized) return;
             // Simplified Init Logic - Assume functions from above are defined correctly
            db.transaction(() => {
                db.query(createTableSql(inputSchema, inputTable, false)).run();
                db.query(createTableSql(outputSchema, outputTable, true)).run();
                ensureTableColumns(db, inputTable, inputSchema, false);
                ensureTableColumns(db, outputTable, outputSchema, true);
                createAutomaticIndexes(db, inputTable, inputSchema);
                setupTriggers();
            })();
            initialized = true;
            console.log(`[Database ${baseName}] Initialized.`);
        },

        async insert(input: z.infer<I>, output?: z.infer<O>): Promise<{ id: string }> {
            if (!initialized) await internalApi.init();
            const validatedInput = inputSchema.parse(input);
            const id = generateRecordId(validatedInput, inputSchema); // Use shared generator
            // ... (rest of insert logic using processObjectForStorage, INSERT OR IGNORE/REPLACE)
            console.log(`[Database ${baseName}] Insert/Update for ID: ${id}`); // Placeholder
            return { id };
        },

        async findById(id: string): Promise<DatabaseRecord<z.infer<I>, z.infer<O>> | null> {
             if (!initialized) await internalApi.init();
             // ... (logic using getRecordByIdInternal)
             console.log(`[Database ${baseName}] FindById: ${id}`); // Placeholder
             return getRecordByIdInternal(id);
        },

        // --- Other methods (find, update, delete, query) - Simplified/Placeholder ---
        async find(filter): Promise<any[]> { if (!initialized) await internalApi.init(); console.log("Find called"); return []; },
        async update(input, output): Promise<{ id: string }> { if (!initialized) await internalApi.init(); const id = generateRecordId(input, inputSchema); console.log("Update called for", id); return { id }; },
        async delete(input): Promise<{ id: string }> { if (!initialized) await internalApi.init(); const id = generateRecordId(input, inputSchema); console.log("Delete called for", id); return { id }; },
        async query<T = any>(sql, ...params): Promise<T[]> { if (!initialized) await internalApi.init(); console.log("Query called"); return []; },
        // --- End Simplified Methods ---

        listen(callback: (event: ChangeEvent<z.infer<I>, z.infer<O>>) => void): { stop: () => void } {
            if (!initialized) {
                this.init().catch(error => console.error(`[DB Listener ${baseName}] Init error:`, error));
            }

            eventEmitter.on('change', callback);
            activeListenersCount++;

            // Start polling only if this is the first active listener
            if (!listenerStopHandle && activeListenersCount > 0) {
                const lastChangeRecord = db.query(`SELECT MAX(change_id) as id FROM ${changesTable}`).get();
                lastChangeId = lastChangeRecord?.id || 0;
                const intervalId = setInterval(pollChanges, 500);
                console.log(`[DB Listener ${baseName}] Polling started.`);
                listenerStopHandle = {
                     stop: () => { // This stops the *polling*, not individual callbacks
                         clearInterval(intervalId);
                         listenerStopHandle = null; // Allow restart
                         console.log(`[DB Listener ${baseName}] Polling stopped.`);
                     }
                };
            }

            // Return a function to remove *this specific* callback
            return {
                stop: () => {
                    eventEmitter.off('change', callback);
                    activeListenersCount--;
                    console.log(`[DB Listener ${baseName}] Callback removed. Active listeners: ${activeListenersCount}`);
                    // Stop polling if no listeners remain
                    if (activeListenersCount === 0 && listenerStopHandle) {
                        listenerStopHandle.stop();
                    }
                }
            };
        },

        getInputSchema: () => inputSchema,
        getOutputSchema: () => outputSchema,
        getName: () => baseName,
        _generateId: (input: z.infer<I>) => generateRecordId(input, inputSchema), // Expose shared generator
        _getDbInstance: () => db,

        close(): void {
             if (listenerStopHandle) {
                 try { listenerStopHandle.stop(); } catch (e) { /* ignore */ }
                 listenerStopHandle = null;
             }
             if (db) {
                  try { db.close(); } catch (e) { /* ignore */ }
             }
             eventEmitter.removeAllListeners('change');
             activeListenersCount = 0;
             initialized = false;
             console.log(`[Database ${baseName}] Closed.`);
        }
    };
     // --- End of code copied and adapted from previous `useDatabase` ---
     // Full implementation of utilities (getSqliteType, etc.) is required here.
     // The simplified placeholders above need to be replaced with the actual code
     // from the `sati_db_async_processor_v1` artifact for this to be functional.
    return internalApi;
}


// --- New Interface Function: Requester ---
// Interface for the returned processor object
export interface SatiInsertProcessor<I extends z.ZodObject<any>, O extends z.ZodObject<any>> {
    process: (input: z.infer<I>) => Promise<z.infer<O>>;
    stop: () => void; // To clean up listener and pending promises
}

// Interface for pending promises (internal)
interface PendingPromise<O> {
    resolve: (output: O) => void;
    reject: (reason?: any) => void;
    timer: NodeJS.Timeout;
}

export function useDatabaseAsInsertFunction<I extends z.ZodObject<any>, O extends z.ZodObject<any>>(
    dbPath: string,
    inputSchema: I,
    outputSchema: O,
    options?: { timeoutMs?: number }
): SatiInsertProcessor<I, O> {

    // Get the internal DB instance
    // Note: If multiple calls use the same dbPath in the same process,
    // they should ideally share the underlying db connection and listener.
    // This basic implementation creates a new one each time.
    // TODO: Implement connection sharing/singleton pattern based on dbPath.
    const db = useDatabaseInternal(dbPath, inputSchema, outputSchema);
    const pendingPromises = new Map<string, PendingPromise<z.infer<O>>>();
    const defaultTimeoutMs = options?.timeoutMs ?? 30000; // Default 30 seconds
    let listenerHandle: { stop: () => void } | null = null;

    // Start the listener specific to this processor instance
    const startListener = () => {
        if (listenerHandle) return; // Already started

        listenerHandle = db.listen((event) => {
            if ((event.type === 'output_added' || event.type === 'record_updated') && event.id) {
                const pending = pendingPromises.get(event.id);
                if (pending && event.output) {
                    try {
                        const validatedOutput = outputSchema.parse(event.output);
                        clearTimeout(pending.timer);
                        pending.resolve(validatedOutput);
                        pendingPromises.delete(event.id);
                    } catch (validationError) {
                        console.error(`[InsertFunction ${db.getName()}] Invalid output received for ID ${event.id}:`, validationError);
                        clearTimeout(pending.timer);
                        pending.reject(new Error(`Invalid output format: ${validationError.message}`));
                        pendingPromises.delete(event.id);
                    }
                }
            } else if (event.type === 'record_deleted' && event.id) {
                const pending = pendingPromises.get(event.id);
                if (pending) {
                    clearTimeout(pending.timer);
                    pending.reject(new Error(`Record ${event.id} deleted while awaiting output.`));
                    pendingPromises.delete(event.id);
                }
            }
        });
        console.log(`[InsertFunction ${db.getName()}] Listener started.`);
    };

    // The main function returned to the user
    async function process(input: z.infer<I>): Promise<z.infer<O>> {
        await db.init(); // Ensure DB is ready
        startListener(); // Ensure listener is running

        const validatedInput = inputSchema.parse(input);
        const id = db._generateId(validatedInput); // Use db's internal ID gen

        // 1. Check pending (basic check, see TODO in thought about concurrency)
        if (pendingPromises.has(id)) {
            console.warn(`[InsertFunction ${db.getName()}] Input ID ${id} is already being processed. Re-entering wait.`);
            // Allow re-waiting, the listener will resolve all waiters
        }

        // 2. Check DB
        const existingRecord = await db.findById(id);
        if (existingRecord?.output) {
            try {
                // Validate just in case schema changed or data is corrupt
                return outputSchema.parse(existingRecord.output);
            } catch (e) {
                 console.error(`[InsertFunction ${db.getName()}] Found existing output for ID ${id}, but failed validation:`, e);
                 // Proceed to wait for potentially new, valid output
            }
        }

        // 3. Ensure input exists & Wait
        console.log(`[InsertFunction ${db.getName()}] Waiting for output for ID: ${id}`);
        // Insert input only (idempotent if exists)
        await db.insert(validatedInput); // No output provided here

        // Return promise that waits for listener resolution or timeout
        return new Promise<z.infer<O>>((resolve, reject) => {
            // Clear any previous timer for this ID if re-entering wait
            const existingPending = pendingPromises.get(id);
            if (existingPending) {
                clearTimeout(existingPending.timer);
            }

            const timer = setTimeout(() => {
                pendingPromises.delete(id);
                reject(new Error(`Timeout waiting for output for ID ${id} after ${defaultTimeoutMs}ms`));
            }, defaultTimeoutMs);

            pendingPromises.set(id, { resolve, reject, timer });

            // External process trigger assumption remains here
        });
    }

    // Cleanup function
    function stop() {
        if (listenerHandle) {
            listenerHandle.stop();
            listenerHandle = null;
            console.log(`[InsertFunction ${db.getName()}] Listener stopped.`);
        }
        pendingPromises.forEach((pending, id) => {
            clearTimeout(pending.timer);
            pending.reject(new Error(`Processor stopped while waiting for output for ID ${id}.`));
        });
        pendingPromises.clear();
        // Optionally close the underlying DB connection if this processor
        // is the sole user. Requires more complex connection management.
        // db.close();
    }

    return { process, stop };
}


// --- New Interface Function: Worker/Listener ---

// Type for the handler callback provided by the user
export type SatiListenerHandler<I, O> = (
    input: I,
    // Function to call when output is ready
    setOutput: (output: O) => Promise<{ id: string }>,
    // Original event details (optional)
    event: ChangeEvent<I, O>
) => Promise<void> | void; // Handler can be sync or async

// Interface for the returned listener control object
export interface SatiListenerControl {
    stop: () => void; // To stop listening
}

export function useDatabaseAsListener<I extends z.ZodObject<any>, O extends z.ZodObject<any>>(
    dbPath: string,
    inputSchema: I,
    outputSchema: O,
    // The user's callback function to handle new inputs
    handlerCallback: SatiListenerHandler<z.infer<I>, z.infer<O>>,
    options?: { maxConcurrency?: number } // Concurrency option
): SatiListenerControl {

    // Get the internal DB instance (see TODO about connection sharing)
    const db = useDatabaseInternal(dbPath, inputSchema, outputSchema);
    let listenerHandle: { stop: () => void } | null = null;
    let activeHandlers = 0;
    const maxConcurrency = options?.maxConcurrency ?? 1; // Default to sequential processing

    const processEvent = async (event: ChangeEvent<z.infer<I>, z.infer<O>>) => {
         if (event.type === 'input_added' && event.id && event.input) {
            const inputId = event.id;
            const validatedInput = event.input; // Assume event already contains validated input

             console.log(`[Listener ${db.getName()}] ---> Received input_added for ID: ${inputId}`);

             // Concurrency check
             if (activeHandlers >= maxConcurrency) {
                 console.log(`[Listener ${db.getName()}] Concurrency limit (${maxConcurrency}) reached. Skipping event for now (ID: ${inputId}).`);
                 // TODO: Implement queuing for proper handling when concurrency limit is hit
                 return;
             }

             // Check if output already exists (maybe processed by another instance or before restart)
             try {
                 const existing = await db.findById(inputId);
                 if (existing?.output) {
                     console.log(`[Listener ${db.getName()}] Output already exists for [${inputId}]. Skipping handler.`);
                     return;
                 }
             } catch (e) {
                 console.error(`[Listener ${db.getName()}] Error checking existing output for ${inputId}:`, e);
                 // Decide whether to proceed or skip
                 return;
             }


             activeHandlers++;
             console.log(`[Listener ${db.getName()}] Starting handler for ID: ${inputId}. Active: ${activeHandlers}`);

             // Define the setOutput function for this specific input
             const setOutput = async (outputData: z.infer<O>): Promise<{ id: string }> => {
                 // Validate output before inserting
                 const validatedOutput = outputSchema.parse(outputData);
                 // Call the internal db.insert, providing both input and output
                 // This uses INSERT OR REPLACE for the output table
                 return db.insert(validatedInput, validatedOutput);
             };

             try {
                 // Execute the user's handler function
                 await handlerCallback(validatedInput, setOutput, event);
                  console.log(`[Listener ${db.getName()}] <--- Handler finished successfully for ID: ${inputId}.`);
             } catch (error) {
                 console.error(`[Listener ${db.getName()}] Error executing handler for input ID ${inputId}:`, error);
                 // TODO: Implement error handling strategy (e.g., retries, dead-letter queue)
             } finally {
                 activeHandlers--;
                  console.log(`[Listener ${db.getName()}] Handler ended for ID: ${inputId}. Active: ${activeHandlers}`);
             }
         }
    };

    // Start the listener immediately
    db.init()
        .then(() => {
            listenerHandle = db.listen(processEvent); // Pass processEvent directly
            console.log(`[Listener ${db.getName()}] Started listening for inputs.`);
        })
        .catch(error => {
            console.error(`[Listener ${db.getName()}] Failed to initialize database:`, error);
            // Handle initialization failure appropriately
        });


    // Return control object
    return {
        stop: () => {
            if (listenerHandle) {
                listenerHandle.stop();
                listenerHandle = null;
                console.log(`[Listener ${db.getName()}] Stopped listening.`);
            }
            // Optionally close the underlying DB connection if this listener
            // is the sole user. Requires connection management.
            // db.close();
        }
    };
}

// --- Utility to sanitize names (unchanged) ---
function sanitizeName(name: string): string {
    let sanitized = name.replace(/[^a-zA-Z0-9_]/g, "_");
    if (/^[0-9]/.test(sanitized)) sanitized = "t_" + sanitized;
    if (RESERVED_SQLITE_WORDS.has(sanitized.toUpperCase())) sanitized = "t_" + sanitized;
    return sanitized;
}

// NOTE: The full implementation of the utility functions within useDatabaseInternal
// (getSqliteType, createTableSql, processValueForStorage, mapRowToRecord, etc.)
// needs to be copied from the `sati_db_async_processor_v1` artifact for this code
// to be fully functional. They were simplified above for brevity.
