// examples/youtube-twitter-agent/databases/database_core_refactored.ts
import { Database } from "bun:sqlite";
import { z } from "zod";
import path from "path";
import fs from "node:fs"; // Use node:fs
import { createHash } from "crypto";
import { EventEmitter } from "events";
import { RESERVED_SQLITE_WORDS } from "./constants"; // Assuming this exists

export interface DatabaseRecord<I, O> {
    id: string;
    input: I;
    output?: O;
}

export interface ChangeEvent<I, O> {
    type: 'input_added' | 'output_added' | 'record_updated' | 'record_deleted';
    id: string;
    input?: I;
    output?: O;
}

// Range filter type for find
export type RangeFilter<T> = [T, T];

export interface ListenOptions {
    /** How often to poll the database for changes (milliseconds). Defaults to 1000ms. */
    pollIntervalMs?: number;
    /** The change ID to start listening from. Defaults to processing only new changes after listener starts. */
    startFromId?: number;
}

export interface SatiDatabase<I extends z.ZodObject<any>, O extends z.ZodObject<any>> {
    init: () => Promise<void>;
    insert: (input: z.infer<I>, output?: z.infer<O>) => Promise<{ id: string }>;
    find: (
        input?: Partial<z.infer<I> & { [K in keyof z.infer<I>]?: z.infer<I>[K] | RangeFilter<z.infer<I>[K]> }>,
        output?: Partial<z.infer<O> & { [K in keyof z.infer<O>]?: z.infer<O>[K] | RangeFilter<z.infer<O>[K]> }>
    ) => Promise<Array<DatabaseRecord<z.infer<I>, z.infer<O>>>>;
    update: (input: Partial<z.infer<I>>, output: Partial<z.infer<O>>) => Promise<{ id: string }>;
    delete: (input: Partial<z.infer<I>>) => Promise<{ id: string }>;
    query: <T = any>(sql: string, ...params: any[]) => Promise<T[]>;
    /**
     * Listen for changes to the database records. Each call creates an independent poller.
     * @param callback Function to call when a change event occurs.
     * @param options Configuration for polling interval and starting point.
     * @returns An object with a `stop` method to halt this specific listener.
     */
    listen: (
        callback: (event: ChangeEvent<z.infer<I>, z.infer<O>>) => Promise<void>,
        options?: ListenOptions
    ) => { stop: () => void };
    getInputSchema: () => I;
    getOutputSchema: () => O;
    getName: () => string;
    close: () => void;
}

export function createDatabase<I extends z.ZodObject<any>, O extends z.ZodObject<any>>(
    inputSchema: I,
    outputSchema: O,
    dbPath: string
): SatiDatabase<I, O> {
    const baseName = path.basename(dbPath, path.extname(dbPath));
    const sanitizedName = sanitizeName(baseName);
    const inputTable = `input_${sanitizedName}`;
    const outputTable = `output_${sanitizedName}`;
    const changesTable = `_${sanitizedName}_changes`;
    
    const dir = path.dirname(dbPath);
    try {
        fs.mkdirSync(dir, { recursive: true });
    } catch (error: any) {
        if (error.code !== 'EEXIST') {
            console.error(`[Database Setup] Failed to ensure directory ${dir} exists:`, error);
            throw error;
        }
    }

    const db = new Database(dbPath);
    let initialized = false;

    const setupTriggers = () => {
        // Note: These operations are idempotent due to "IF NOT EXISTS"
        db.query(`
          CREATE TABLE IF NOT EXISTS ${changesTable} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            record_id TEXT NOT NULL,
            change_type TEXT NOT NULL,
            table_name TEXT NOT NULL,
            changed_at INTEGER NOT NULL
          )
        `).run();
        db.query(`
          CREATE TRIGGER IF NOT EXISTS ${inputTable}_insert_trigger
          AFTER INSERT ON ${inputTable}
          BEGIN
            INSERT INTO ${changesTable} (record_id, change_type, table_name, changed_at)
            VALUES (NEW.id, 'input_added', '${inputTable}', unixepoch());
          END;
        `).run();
        db.query(`
          CREATE TRIGGER IF NOT EXISTS ${inputTable}_delete_trigger
          AFTER DELETE ON ${inputTable}
          BEGIN
            INSERT INTO ${changesTable} (record_id, change_type, table_name, changed_at)
            VALUES (OLD.id, 'record_deleted', '${inputTable}', unixepoch());
          END;
        `).run();
        db.query(`
          CREATE TRIGGER IF NOT EXISTS ${outputTable}_insert_trigger
          AFTER INSERT ON ${outputTable}
          BEGIN
            INSERT INTO ${changesTable} (record_id, change_type, table_name, changed_at)
            VALUES (NEW.id, 'output_added', '${outputTable}', unixepoch());
          END;
        `).run();
        db.query(`
          CREATE TRIGGER IF NOT EXISTS ${outputTable}_update_trigger
          AFTER UPDATE ON ${outputTable}
          BEGIN
            INSERT INTO ${changesTable} (record_id, change_type, table_name, changed_at)
            VALUES (NEW.id, 'record_updated', '${outputTable}', unixepoch());
          END;
        `).run();
    };


    let lastChangeId = 0;

    const pollChanges = async () => {
        try {
            const changes = db.query(`
                SELECT * FROM _${sanitizedName}_changes
                WHERE id > ?
                ORDER BY id ASC
            `).all(lastChangeId);

            if (changes.length === 0) return;
            lastChangeId = changes[changes.length - 1].id;

            for (const change of changes) {
                const record = await getRecordById(change.record_id);
                let eventType: ChangeEvent<any, any>['type'] | null = null;

                switch (change.change_type) {
                    case 'input_added': eventType = 'input_added'; break;
                    case 'output_added': eventType = 'output_added'; break;
                    case 'record_updated': eventType = 'record_updated'; break;
                    case 'record_deleted': eventType = 'record_deleted'; break;
                }

                if (eventType) {
                     eventEmitter.emit('change', {
                        type: eventType,
                        id: change.record_id,
                        input: record?.input,
                        output: record?.output,
                    });
                }
            }
        } catch (error) {
            console.error(`[Database Listener ${baseName}] Error polling changes:`, error);
        }
    };

    const isCompleteInput = (input: any): boolean => {
        const schemaKeys = Object.keys(inputSchema.shape);
        return schemaKeys.every(key => input.hasOwnProperty(key) && input[key] !== undefined);
    };

    const generateId = (input: any): string => {
        const orderedInput = Object.keys(input)
            .sort()
            .reduce((obj, key) => {
                obj[key] = input[key];
                return obj;
            }, {});
        return createHash('sha256')
            .update(JSON.stringify(orderedInput))
            .digest('hex')
            .substring(0, 16);
    };

    const getRecordById = async (id: string): Promise<DatabaseRecord<z.infer<I>, z.infer<O>> | null> => {
        const result = db.query(`
            SELECT i.*, o.*
            FROM ${inputTable} i
            LEFT JOIN ${outputTable} o ON i.id = o.id
            WHERE i.id = ?
        `).get(id);

        if (!result) return null;

        const inputData: Record<string, any> = {};
        const outputData: Record<string, any> = {};
        let recordId: string = "";

        for (const [key, value] of Object.entries(result)) {
            if (key === "id") {
                recordId = value as string;
                continue; // Don't put id field directly into input/output data objects
            }
            const inInput = key in inputSchema.shape;
            const inOutput = key in outputSchema.shape;

            if (inInput) {
                inputData[key] = deserializeValue(value, inputSchema.shape[key]);
            } else if (inOutput) {
                outputData[key] = deserializeValue(value, outputSchema.shape[key]);
            }
        }

        const hasOutput = Object.keys(outputData).length > 0 && Object.values(outputData).some(v => v !== null); // Check if output is not just full of nulls

        return {
            id: recordId,
            input: inputSchema.parse(inputData), // Validate on retrieval
             ...(hasOutput && { output: outputSchema.parse(outputData) }) // Validate on retrieval
        };
    };

    function ensureTableColumns(dbInstance: Database, tableName: string, schema: z.ZodObject<any>) {
        const tableInfo = dbInstance.query(`PRAGMA table_info(${tableName})`).all();
        const existingColumns = tableInfo.map(col => col.name);

        for (const [key, valueDef] of Object.entries(schema.shape)) {
            if (key !== "id" && !existingColumns.includes(key)) {
                let columnType = getSqliteType(valueDef);
                if (columnType) {
                    dbInstance.query(`ALTER TABLE ${tableName} ADD COLUMN ${key} ${columnType}`).run();
                    console.log(`[Database Schema ${baseName}] Added missing column ${key} (${columnType}) to table ${tableName}`);
                } else {
                     console.warn(`[Database Schema ${baseName}] Could not determine SQLite type for column ${key} in ${tableName}`);
                }
            }
        }
    }


    return {
        async init(): Promise<void> {
            if (initialized) return;
            const inputSql = createTableSql(inputSchema, inputTable);
            db.query(inputSql).run();
            const outputSql = createTableSql(outputSchema, outputTable);
            db.query(outputSql).run();

            ensureTableColumns(db, inputTable, inputSchema); // Ensure input table columns too
            ensureTableColumns(db, outputTable, outputSchema);

            await createAutomaticIndexes(db, inputTable, inputSchema);
            // No automatic indexes for output table

            setupTriggers();
            initialized = true;
             console.log(`[Database ${baseName}] Initialized.`);
        },

        async insert(input: z.infer<I>, output?: z.infer<O>): Promise<{ id: string }> {
            if (!initialized) await this.init();
            const validatedInput = inputSchema.parse(input);
            const id = generateId(validatedInput);

            const existing = db.query(`SELECT id FROM ${inputTable} WHERE id = ?`).get(id);
            if (existing) {
                if (output) {
                    await this.update(input, output); // Use validatedInput? No, update takes partial
                }
                return { id };
            }

            const processedInput = processObjectForStorage({ ...validatedInput, id });
            insertRecord(db, inputTable, processedInput);

            if (output) {
                const validatedOutput = outputSchema.parse(output);
                const processedOutput = processObjectForStorage({ ...validatedOutput, id });
                insertRecord(db, outputTable, processedOutput);
            }
            return { id };
        },

        async find(
            inputFilter?: Partial<z.infer<I> & { [K in keyof z.infer<I>]?: z.infer<I>[K] | RangeFilter<z.infer<I>[K]> }>,
            outputFilter?: Partial<z.infer<O> & { [K in keyof z.infer<O>]?: z.infer<O>[K] | RangeFilter<z.infer<O>[K]> }>
        ): Promise<Array<DatabaseRecord<z.infer<I>, z.infer<O>>>> {
            if (!initialized) await this.init();

             if (inputFilter && isCompleteInput(inputFilter) && !Object.values(inputFilter).some(v => Array.isArray(v))) {
                 const id = generateId(inputFilter);
                 const record = await getRecordById(id);
                 return record ? [record] : [];
             }

            let query = `SELECT i.*, o.* FROM ${inputTable} i LEFT JOIN ${outputTable} o ON i.id = o.id WHERE 1=1`;
            const params: any[] = [];

            const addFilters = (filter: Record<string, any> | undefined, schema: z.ZodObject<any>, alias: string) => {
                 if (!filter) return;
                 for (const [key, value] of Object.entries(filter)) {
                    if (value !== undefined && schema.shape[key]) { // Check if key is in schema
                         const zodType = schema.shape[key];
                        if (Array.isArray(value) && value.length === 2) {
                            const [min, max] = value;
                            const processedMin = processSingleValueForQuery(min, zodType);
                            const processedMax = processSingleValueForQuery(max, zodType);
                            query += ` AND ${alias}.${key} BETWEEN ? AND ?`;
                            params.push(processedMin, processedMax);
                        } else {
                            const processedValue = processSingleValueForQuery(value, zodType);
                            query += ` AND ${alias}.${key} = ?`;
                            params.push(processedValue);
                        }
                    }
                }
            }

            addFilters(inputFilter, inputSchema, 'i');
            addFilters(outputFilter, outputSchema, 'o');

            const results = db.query(query).all(...params);

            return results.map((row: any) => {
                const inputData: Record<string, any> = {};
                const outputData: Record<string, any> = {};
                let recordId: string = "";

                 for (const [key, value] of Object.entries(row)) {
                    if (key === "id") {
                         recordId = value as string;
                        continue;
                    }
                     const inInput = key in inputSchema.shape;
                     const inOutput = key in outputSchema.shape;

                     if (inInput) {
                        inputData[key] = deserializeValue(value, inputSchema.shape[key]);
                    } else if (inOutput) {
                         outputData[key] = deserializeValue(value, outputSchema.shape[key]);
                    }
                 }
                 const hasOutput = Object.keys(outputData).length > 0 && Object.values(outputData).some(v => v !== null);

                 // Use try-catch for parsing during map to isolate problematic rows
                 try {
                     return {
                        id: recordId,
                         input: inputSchema.parse(inputData),
                         ...(hasOutput && { output: outputSchema.parse(outputData) })
                    };
                 } catch (parseError) {
                     console.error(`[Database ${baseName}] Failed to parse row with id ${recordId}:`, parseError, { inputData, outputData });
                     return null; // Skip rows that fail validation
                 }
            }).filter(record => record !== null) as Array<DatabaseRecord<z.infer<I>, z.infer<O>>>; // Filter out nulls
        },


        async update(input: Partial<z.infer<I>>, output: Partial<z.infer<O>>): Promise<{ id: string }> {
            if (!initialized) await this.init();

            let id: string;
            let existing: DatabaseRecord<z.infer<I>, z.infer<O>> | null = null;

            if (isCompleteInput(input) && !Object.values(input).some(v => Array.isArray(v))) {
                id = generateId(input);
            } else {
                const results = await this.find(input); // Find uses partial input
                 if (results.length === 0) throw new Error(`Update failed: No record found matching input criteria in ${baseName}`);
                 if (results.length > 1) throw new Error(`Update failed: Multiple records found matching input criteria in ${baseName}. Provide complete input.`);
                id = results[0].id;
            }

            existing = await getRecordById(id); // Fetch full existing record

             if (!existing && isCompleteInput(input) && !Object.values(input).some(v => Array.isArray(v)) && output) {
                 // If we used a complete input to generate ID, but it wasn't found, and we have output, treat as insert.
                 // Need to cast 'input' because isCompleteInput doesn't narrow type sufficiently.
                 return this.insert(input as z.infer<I>, output);
             }

             if (!existing) {
                 throw new Error(`Update failed: Record with id ${id} (derived from input) not found in ${baseName}`);
             }

            const validatedOutputUpdate = outputSchema.partial().parse(output); // Validate partial output
            const outputExists = db.query(`SELECT 1 FROM ${outputTable} WHERE id = ?`).get(id);

            if (outputExists) {
                 const mergedOutput = { ...(existing.output || {}), ...validatedOutputUpdate };
                 const processedOutput = processObjectForStorage(mergedOutput);
                 // Ensure ID is not included in the SET clauses
                 const { id: _id, ...dataToUpdate } = processedOutput;
                 if (Object.keys(dataToUpdate).length > 0) {
                     updateRecord(db, outputTable, id, dataToUpdate);
                 } else {
                    // console.log(`[Database ${baseName}] Update called for id ${id}, but no output fields changed.`);
                 }
            } else if (Object.keys(validatedOutputUpdate).length > 0) {
                const processedOutput = processObjectForStorage({ ...validatedOutputUpdate, id });
                insertRecord(db, outputTable, processedOutput);
            }

            return { id };
        },

        async delete(input: Partial<z.infer<I>>): Promise<{ id: string }> {
            if (!initialized) await this.init();
            let id: string;

            if (isCompleteInput(input) && !Object.values(input).some(v => Array.isArray(v))) {
                id = generateId(input);
            } else {
                const results = await this.find(input);
                 if (results.length === 0) throw new Error(`Delete failed: No record found matching input criteria in ${baseName}`);
                 if (results.length > 1) throw new Error(`Delete failed: Multiple records found matching input criteria in ${baseName}. Provide complete input.`);
                id = results[0].id;
            }

            const deletedInputRows = db.query(`DELETE FROM ${inputTable} WHERE id = ? RETURNING id`).get(id);
            if (!deletedInputRows) {
                 throw new Error(`Delete failed: Record with id ${id} not found in input table ${inputTable}`);
            }
            db.query(`DELETE FROM ${outputTable} WHERE id = ?`).run(id);

            return { id };
        },

        async query<T = any>(sql: string, ...params: any[]): Promise<T[]> {
             // Consider adding init check? Or assume raw query users know what they're doing.
            return db.query<T>(sql).all(...params);
        },

        listen(
            callback: (event: ChangeEvent<z.infer<I>, z.infer<O>>) => void,
            options?: ListenOptions
        ): { stop: () => void } {
            const listenerId = Math.random().toString(36).substring(2, 8); // Simple ID for logging
            const pollIntervalMs = options?.pollIntervalMs ?? 1000; // Default to 1s
            let listenerLastChangeId: number = -1; // Initialize for this specific listener
            let intervalId: Timer | null = null;

            // Define the polling function specific to this listener's state
            const pollChangesForListener = async () => {
                try {
                    const changes = db.query(`
                        SELECT * FROM ${changesTable}
                        WHERE id > ?
                        ORDER BY id ASC
                        LIMIT 100 -- Add a limit to prevent huge batches
                    `).all(listenerLastChangeId);

                    if (changes.length === 0) return;

                    // Update the watermark *for this listener*
                    listenerLastChangeId = changes[changes.length - 1].id;

                    for (const change of changes) {
                        try {
                            // Fetch the full record (input/output) based on the change
                            // Avoid fetching record for 'deleted' if not needed by callback? For now, fetch always.
                            const record = change.change_type !== 'record_deleted'
                                ? await getRecordById(change.record_id)
                                : null; // Don't fetch if deleted

                            let eventType: ChangeEvent<any, any>['type'] | null = null;
                            switch (change.change_type) {
                                case 'input_added': eventType = 'input_added'; break;
                                case 'output_added': eventType = 'output_added'; break;
                                case 'record_updated': eventType = 'record_updated'; break;
                                case 'record_deleted': eventType = 'record_deleted'; break;
                            }

                            if (eventType) {
                                // Call the specific callback provided to this listen() call
                                callback({
                                    type: eventType,
                                    id: change.record_id,
                                    input: record?.input,
                                    output: record?.output,
                                    // previousInput/Output could be added by querying before update/delete triggers, more complex
                                });
                            }
                        } catch (processError) {
                             console.error(`[DB Listener ${baseName}-${listenerId}] Error processing change ID ${change.id}:`, processError);
                             // Continue processing next change
                        }
                    }
                } catch (pollError) {
                    console.error(`[DB Listener ${baseName}-${listenerId}] Error polling changes table:`, pollError);
                    // Optionally stop polling on certain errors?
                }
            };

            // Ensure DB is initialized (incl. triggers) before starting polling
            this.init().then(() => {
                 // Determine starting point after init ensures tables exist
                 if (options?.startFromId !== undefined) {
                     listenerLastChangeId = options.startFromId;
                 } else {
                     // Default: Start from the latest existing change ID, process only future changes
                     const lastChangeRecord = db.query(`SELECT MAX(id) as id FROM ${changesTable}`).get();
                     listenerLastChangeId = lastChangeRecord?.id || 0;
                 }
                 console.log(`[DB Listener ${baseName}-${listenerId}] Starting poll (every ${pollIntervalMs}ms) from change ID ${listenerLastChangeId}`);
                 intervalId = setInterval(pollChangesForListener, pollIntervalMs);
                 // Run once immediately? Optional, depends if you want instant check on listen start
                 // pollChangesForListener();
            }).catch(initError => {
                console.error(`[DB Listener ${baseName}-${listenerId}] Failed to initialize database for listener:`, initError);
                // Listener won't start
            });

            // Return the stop function for this specific listener
            return {
                stop: () => {
                    if (intervalId) {
                        clearInterval(intervalId);
                        intervalId = null; // Mark as stopped
                        console.log(`[DB Listener ${baseName}-${listenerId}] Polling stopped.`);
                    }
                }
            };
        },

        getInputSchema(): I { return inputSchema; },
        getOutputSchema(): O { return outputSchema; },
        getName(): string { return baseName; },
        close(): void { db.close(); console.log(`[Database ${baseName}] Connection closed.`); }
    };
}

// --- Utility Functions ---

function sanitizeName(name: string): string {
    let sanitized = name.replace(/[^a-zA-Z0-9_]/g, "_");
    if (/^[0-9]/.test(sanitized)) sanitized = "t_" + sanitized;
    if (RESERVED_SQLITE_WORDS.includes(sanitized.toUpperCase())) sanitized = "t_" + sanitized;
    return sanitized;
}

function getSqliteType(zodType: z.ZodTypeAny): string | null {
    const unwrapped = zodType.unwrap ? zodType.unwrap() : zodType; // Handle optional/nullable/default

     if (isZodString(unwrapped) || isZodEnum(unwrapped) || isZodArray(unwrapped) || isZodObject(unwrapped)) {
        return "TEXT";
    } else if (isZodNumber(unwrapped)) {
        return "NUMERIC";
    } else if (isZodBoolean(unwrapped) || isZodDate(unwrapped)) { // Store dates and booleans as integers
        return "INTEGER";
    }
    // If type is ZodDefault, get inner type
     if (isZodDefault(zodType)) {
         return getSqliteType(zodType._def.innerType);
     }

    return "TEXT"; // Default fallback
}

function getDefaultValueClause(zodType: z.ZodTypeAny): string {
     if (!isZodDefault(zodType)) return "";

     const defaultValue = typeof zodType._def.defaultValue === 'function'
            ? zodType._def.defaultValue()
            : zodType._def.defaultValue;

    if (defaultValue === undefined) return "";

    let formattedDefault: string | number | null;

     const innerType = zodType._def.innerType;

     if (typeof defaultValue === 'string') {
        formattedDefault = `'${defaultValue.replace(/'/g, "''")}'`; // Escape single quotes
    } else if (typeof defaultValue === 'number') {
        formattedDefault = defaultValue;
    } else if (typeof defaultValue === 'boolean') {
        formattedDefault = defaultValue ? 1 : 0;
    } else if (defaultValue instanceof Date) {
        formattedDefault = defaultValue.getTime();
    } else if (defaultValue !== null && typeof defaultValue === 'object') {
        formattedDefault = `'${JSON.stringify(defaultValue).replace(/'/g, "''")}'`;
    } else if (defaultValue === null) {
         // DEFAULT NULL is implicit if column allows NULL, explicit needed otherwise?
         // SQLite typically handles this, let's omit explicit DEFAULT NULL for now.
         return "";
     } else {
         return ""; // Unknown default type
     }

     return ` DEFAULT ${formattedDefault}`;
}

function createTableSql(schema: z.ZodObject<any>, tableName: string): string {
    const columns = ["id TEXT PRIMARY KEY"];
    for (const [key, valueDef] of Object.entries(schema.shape)) {
        if (key === "id") continue;

        let columnType = getSqliteType(valueDef);
        if (!columnType) continue; // Skip if type couldn't be determined

        const isOptional = valueDef.isOptional() || valueDef.isNullable();
        const defaultClause = getDefaultValueClause(valueDef);

         // If it's optional/nullable and NOT a ZodDefault, allow NULL explicitly.
         // ZodDefault implies nullability unless the default value itself is non-null.
         const nullClause = (isOptional && !isZodDefault(valueDef)) ? " NULL" : (defaultClause ? "" : " NOT NULL"); // Default to NOT NULL if no default and not optional/nullable

        columns.push(`${key} ${columnType}${nullClause}${defaultClause}`);
    }
    return `CREATE TABLE IF NOT EXISTS ${tableName} (${columns.join(", ")})`;
}

async function createAutomaticIndexes(db: Database, inputTable: string, inputSchema: z.ZodObject<any>): Promise<void> {
     const tableInfo = db.query(`PRAGMA table_info(${inputTable})`).all();
     for (const column of tableInfo) {
        if (column.name === "id") continue;
        const indexName = `idx_${inputTable}_${column.name}`;
         try {
             db.query(`CREATE INDEX IF NOT EXISTS ${indexName} ON ${inputTable}(${column.name})`).run();
         } catch (error) {
             console.warn(`[Database Indexing] Failed to create index ${indexName}:`, error);
         }
     }
}

function processObjectForStorage(data: any): Record<string, any> {
    const result: Record<string, any> = {};
    for (const [key, value] of Object.entries(data)) {
        if (value === undefined) continue;
        if (value instanceof Date) {
            result[key] = value.getTime();
        } else if (value !== null && typeof value === "object") {
            result[key] = JSON.stringify(value);
        } else if (typeof value === 'boolean') {
             result[key] = value ? 1 : 0;
        } else {
            result[key] = value;
        }
    }
    return result;
}

function processSingleValueForQuery(value: any, zodType: z.ZodTypeAny | undefined): any {
     // Undefined zodType might happen if filter key isn't in schema - treat as raw value
     if (value === null || value === undefined) return value;

     const unwrapped = zodType?.unwrap ? zodType.unwrap() : zodType;

     // Check based on SQLite storage type derived from schema, not just JS type
     if (unwrapped && (isZodArray(unwrapped) || isZodObject(unwrapped))) {
         return JSON.stringify(value);
     }
     if (value instanceof Date) {
         return value.getTime();
     }
     if (typeof value === 'boolean') {
         return value ? 1 : 0;
     }
     // Add specific handling for numbers if needed (e.g., ensure not NaN)
     if (typeof value === 'number' && isNaN(value)) {
         throw new Error("Cannot use NaN in database query.");
     }
     return value;
 }

function insertRecord(db: Database, table: string, data: Record<string, any>): void {
    const columns = Object.keys(data);
    if (columns.length === 0) return;
    const placeholders = columns.map(() => "?").join(", ");
    const values = Object.values(data);
    db.query(`INSERT INTO ${table} (${columns.join(", ")}) VALUES (${placeholders})`).run(...values);
}

function updateRecord(db: Database, table: string, id: string, data: Record<string, any>): void {
    const columns = Object.keys(data);
    if (columns.length === 0) return;
    const setClauses = columns.map(col => `${col} = ?`).join(", ");
    const values = [...Object.values(data), id];
    db.query(`UPDATE ${table} SET ${setClauses} WHERE id = ?`).run(...values);
}

function deserializeValue(value: any, zodType: z.ZodTypeAny): any {
    if (value === null) return null;
    const unwrapped = zodType.unwrap ? zodType.unwrap() : zodType; // Handle optional/nullable/default

     try {
         if (isZodArray(unwrapped) || isZodObject(unwrapped)) {
             return typeof value === 'string' ? JSON.parse(value) : value; // Avoid parsing if already object
         } else if (isZodDate(unwrapped)) {
             // Ensure value is numeric or string before creating Date
             const numValue = Number(value);
             return !isNaN(numValue) ? new Date(numValue) : new Date(value); // Attempt parsing if not number
         } else if (isZodNumber(unwrapped)) {
             const num = Number(value);
             return isNaN(num) ? undefined : num; // Or throw? Return undefined for now.
         } else if (isZodBoolean(unwrapped)) {
             return Boolean(Number(value)); // Convert 0/1 back to boolean
         } else if (isZodString(unwrapped) || isZodEnum(unwrapped)) {
             return String(value); // Ensure it's a string
         }
     } catch (e) {
         console.error(`[Database Deserialization] Error deserializing value "${value}" for type ${unwrapped?._def?.typeName}:`, e);
         return undefined; // Or throw? Return undefined if deserialization fails
     }

    return value; // Fallback for simple types not explicitly handled above
}

// --- Zod Type Guards ---
// (Keep these as they are helper functions, not comments)
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