// examples/youtube-twitter-agent/databases/core.ts
import { Database } from "bun:sqlite";
import { z } from "zod";
import { measure, verboseLog } from "./helpers";
import path from "path";
import os from "os";
import { mkdir } from "node:fs/promises";
import { RESERVED_SQLITE_WORDS } from "./constants";
import { createHash } from "crypto";
import { EventEmitter } from "events";

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
    previousInput?: I;
    previousOutput?: O;
}

export interface SatiDatabase<I extends z.ZodObject<any>, O extends z.ZodObject<any>> {
    init: () => Promise<void>;
    insert: (input: z.infer<I>, output?: z.infer<O>) => Promise<{ id: string }>;
    find: (input?: Partial<z.infer<I>>, output?: Partial<z.infer<O>>) => Promise<Array<DatabaseRecord<z.infer<I>, z.infer<O>>>>;
    update: (input: Partial<z.infer<I>>, output: Partial<z.infer<O>>) => Promise<{ id: string }>;
    delete: (input: Partial<z.infer<I>>) => Promise<{ id: string }>;
    query: <T = any>(sql: string, ...params: any[]) => Promise<T[]>;
    listen: (callback: (event: ChangeEvent<z.infer<I>, z.infer<O>>) => void) => { stop: () => void };
    similar: (input: Partial<z.infer<I>>, options?: any) => Promise<Array<DatabaseRecord<z.infer<I>, z.infer<O>>>>;
    getInputSchema: () => I;
    getOutputSchema: () => O;
    getName: () => string;
    close: () => void;
}

/**
 * Create a database with input/output tables
 * 
 * @param name Database name
 * @param inputSchema Input table schema (immutable natural keys)
 * @param outputSchema Output table schema (mutable data)
 * @param dbPath Optional custom database path
 * @returns Database interface
 */
export function createDatabase<I extends z.ZodObject<any>, O extends z.ZodObject<any>>(
    name: string,
    inputSchema: I,
    outputSchema: O,
    dbPath?: string
): SatiDatabase<I, O> {
    // Sanitize name for SQLite
    const sanitizedName = sanitizeName(name);
    const inputTable = `input_${sanitizedName}`;
    const outputTable = `output_${sanitizedName}`;

    // Set up database path
    const finalDbPath = dbPath || path.join(os.homedir(), ".mements", `${name}.sqlite`);
    console.log('===> database.ts:64 ~ finalDbPath', finalDbPath);
    ensureDirExists(path.dirname(finalDbPath));

    function ensureTableColumns(db, table, schema) {
        // Get existing columns
        const tableInfo = db.query(`PRAGMA table_info(${table})`).all();
        const existingColumns = tableInfo.map(col => col.name);

        // Check each field in the schema
        for (const [key, value] of Object.entries(schema.shape)) {
            if (key !== "id" && !existingColumns.includes(key)) {
                // Column doesn't exist, add it
                let columnType = "";

                // Determine column type (simplified version)
                if (value._def.typeName === "ZodString") {
                    columnType = "TEXT";
                } else if (value._def.typeName === "ZodNumber") {
                    columnType = "NUMERIC";
                } else if (value._def.typeName === "ZodBoolean") {
                    columnType = "INTEGER";
                } else if (value._def.typeName === "ZodEnum") {
                    columnType = "TEXT";
                } else {
                    columnType = "TEXT"; // Default for complex types
                }

                // Add the column
                db.query(`ALTER TABLE ${table} ADD COLUMN ${key} ${columnType}`).run();
                console.log(`Added missing column ${key} to table ${table}`);
            }
        }
    }

    // Create database connection
    const db = new Database(finalDbPath);
    console.log('===> database.ts:100 ~ db', db);
    let initialized = false;

    // Create event emitter for change notifications
    const eventEmitter = new EventEmitter();

    // Set up triggers for change notification
    const setupTriggers = () => {
        // Create trigger tables if they don't exist
        db.query(`
      CREATE TABLE IF NOT EXISTS _${sanitizedName}_changes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        record_id TEXT NOT NULL,
        change_type TEXT NOT NULL,
        table_name TEXT NOT NULL,
        changed_at INTEGER NOT NULL
      )
    `).run();

        // Create triggers for input table
        db.query(`
      CREATE TRIGGER IF NOT EXISTS ${inputTable}_insert_trigger
      AFTER INSERT ON ${inputTable}
      BEGIN
        INSERT INTO _${sanitizedName}_changes (record_id, change_type, table_name, changed_at)
        VALUES (NEW.id, 'input_added', '${inputTable}', unixepoch());
      END;
    `).run();

        db.query(`
      CREATE TRIGGER IF NOT EXISTS ${inputTable}_delete_trigger
      AFTER DELETE ON ${inputTable}
      BEGIN
        INSERT INTO _${sanitizedName}_changes (record_id, change_type, table_name, changed_at)
        VALUES (OLD.id, 'record_deleted', '${inputTable}', unixepoch());
      END;
    `).run();

        // Create triggers for output table
        db.query(`
      CREATE TRIGGER IF NOT EXISTS ${outputTable}_insert_trigger
      AFTER INSERT ON ${outputTable}
      BEGIN
        INSERT INTO _${sanitizedName}_changes (record_id, change_type, table_name, changed_at)
        VALUES (NEW.id, 'output_added', '${outputTable}', unixepoch());
      END;
    `).run();

        db.query(`
      CREATE TRIGGER IF NOT EXISTS ${outputTable}_update_trigger
      AFTER UPDATE ON ${outputTable}
      BEGIN
        INSERT INTO _${sanitizedName}_changes (record_id, change_type, table_name, changed_at)
        VALUES (NEW.id, 'record_updated', '${outputTable}', unixepoch());
      END;
    `).run();
    };

    // Track last processed change ID
    let lastChangeId = 0;

    // Poll for changes
    const pollChanges = async () => {
        try {
            // Get changes since last check
            const changes = db.query(`
        SELECT * FROM _${sanitizedName}_changes
        WHERE id > ?
        ORDER BY id ASC
      `).all(lastChangeId);

            if (changes.length === 0) return;

            // Update last change ID
            lastChangeId = changes[changes.length - 1].id;

            // Process each change
            for (const change of changes) {
                // Get the record
                const record = await getRecordById(change.record_id);

                // Determine change type and emit event
                switch (change.change_type) {
                    case 'input_added':
                        eventEmitter.emit('change', {
                            type: 'input_added',
                            id: change.record_id,
                            input: record?.input
                        });
                        break;

                    case 'output_added':
                        eventEmitter.emit('change', {
                            type: 'output_added',
                            id: change.record_id,
                            input: record?.input,
                            output: record?.output
                        });
                        break;

                    case 'record_updated':
                        eventEmitter.emit('change', {
                            type: 'record_updated',
                            id: change.record_id,
                            input: record?.input,
                            output: record?.output
                        });
                        break;

                    case 'record_deleted':
                        eventEmitter.emit('change', {
                            type: 'record_deleted',
                            id: change.record_id
                        });
                        break;
                }
            }
        } catch (error) {
            verboseLog("Database", `Error polling changes: ${error}`, null);
        }
    };

    // Check if an input object has all required fields from the schema
    const isCompleteInput = (input: any): boolean => {
        const schemaKeys = Object.keys(inputSchema.shape);
        const inputKeys = Object.keys(input);
        return schemaKeys.every(key => inputKeys.includes(key));
    };

    // Generate a deterministic ID from input object
    const generateId = (input: any): string => {
        // Sort keys to ensure consistent order
        const orderedInput = {};
        Object.keys(input)
            .sort()
            .forEach(key => {
                orderedInput[key] = input[key];
            });

        // Create a hash of the input
        const hash = createHash('sha256')
            .update(JSON.stringify(orderedInput))
            .digest('hex')
            .substring(0, 16); // Use first 16 chars of hash

        return hash;
    };

    // Get a full record by ID
    const getRecordById = async (id: string): Promise<DatabaseRecord<z.infer<I>, z.infer<O>> | null> => {
        const query = `
      SELECT i.*, o.* 
      FROM ${inputTable} i
      LEFT JOIN ${outputTable} o ON i.id = o.id
      WHERE i.id = ?
    `;

        const result = db.query(query).get(id);
        if (!result) return null;

        // Process the result
        const inputData: Record<string, any> = {};
        const outputData: Record<string, any> = {};

        for (const [key, value] of Object.entries(result)) {
            if (key === "id") {
                inputData.id = value;
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

        // Check if we have output data (non-empty)
        const hasOutput = Object.keys(outputData).length > 0;

        return {
            id: result.id,
            input: inputData as z.infer<I>,
            ...(hasOutput && { output: outputData as z.infer<O> })
        };
    };

    verboseLog("Database", `Created ${name} database`, { path: finalDbPath });

    // Database implementation
    return {
        /**
         * Initialize database tables and indexes
         */
        async init(): Promise<void> {
            if (initialized) return;

            // Create input table
            const inputSql = createTableSql(inputSchema, inputTable);
            db.query(inputSql).run();

            // Create output table
            const outputSql = createTableSql(outputSchema, outputTable);
            db.query(outputSql).run();

            ensureTableColumns(db, outputTable, outputSchema);

            // Create indexes for all fields in input table
            await createAutomaticIndexes(db, inputTable, inputSchema);

            // Add indexes for output table fields that are commonly queried
            await createCommonOutputIndexes(db, outputTable, outputSchema);

            // Set up change notification triggers
            setupTriggers();

            initialized = true;
            verboseLog("Database", `Initialized tables and indexes for ${name}`, null);
        },

        /**
         * Insert data into the database
         * If a record with the same input already exists, update its output
         */
        async insert(input: z.infer<I>, output?: z.infer<O>): Promise<{ id: string }> {
            return measure(
                async () => {
                    if (!initialized) await this.init();

                    // Generate deterministic ID from input
                    const id = generateId(input);

                    // Check if record with this ID already exists
                    const existing = await getRecordById(id);
                    if (existing) {
                        // If exists and output provided, update output
                        if (output) {
                            await this.update(input, output);
                        }
                        return { id };
                    }

                    // Prepare input data
                    const processedInput = processObjectForStorage({
                        ...input,
                        id
                    });

                    // Insert input
                    insertRecord(db, inputTable, processedInput);

                    // Insert output if provided
                    if (output) {
                        const processedOutput = processObjectForStorage({
                            ...output,
                            id
                        });
                        insertRecord(db, outputTable, processedOutput);
                    }

                    return { id };
                },
                "Database",
                `Insert data into ${name}`
            );
        },

        /**
         * Find records in the database
         * Optimized to use hash lookup for complete inputs
         */
        async find(
            input?: Partial<z.infer<I>>,
            output?: Partial<z.infer<O>>
        ): Promise<Array<DatabaseRecord<z.infer<I>, z.infer<O>>>> {
            return measure(
                async () => {
                    if (!initialized) await this.init();

                    // Fast path: if input is complete, use hash lookup
                    if (input && isCompleteInput(input)) {
                        const id = generateId(input);
                        const record = await getRecordById(id);
                        return record ? [record] : [];
                    }

                    // Regular path: construct a query
                    let query = `SELECT i.*, o.* FROM ${inputTable} i LEFT JOIN ${outputTable} o ON i.id = o.id WHERE 1=1`;
                    const params: any[] = [];

                    // Add input filters
                    if (input) {
                        for (const [key, value] of Object.entries(input)) {
                            if (value !== undefined) {
                                if (value !== null && typeof value === "object" && !(value instanceof Date)) {
                                    query += ` AND i.${key} = ?`;
                                    params.push(JSON.stringify(value));
                                } else if (value instanceof Date) {
                                    query += ` AND i.${key} = ?`;
                                    params.push(value.getTime());
                                } else {
                                    query += ` AND i.${key} = ?`;
                                    params.push(value);
                                }
                            }
                        }
                    }

                    // Add output filters
                    if (output) {
                        for (const [key, value] of Object.entries(output)) {
                            if (value !== undefined) {
                                if (value !== null && typeof value === "object" && !(value instanceof Date)) {
                                    query += ` AND o.${key} = ?`;
                                    params.push(JSON.stringify(value));
                                } else if (value instanceof Date) {
                                    query += ` AND o.${key} = ?`;
                                    params.push(value.getTime());
                                } else {
                                    query += ` AND o.${key} = ?`;
                                    params.push(value);
                                }
                            }
                        }
                    }

                    // Execute query
                    const results = db.query(query).all(...params);

                    // Process results
                    return results.map((row: any) => {
                        // Extract input and output data
                        const inputData: Record<string, any> = {};
                        const outputData: Record<string, any> = {};

                        // Process each column in the result
                        for (const [key, value] of Object.entries(row)) {
                            if (key === "id") {
                                inputData.id = value;
                                continue;
                            }

                            // Determine if the field belongs to input or output schema
                            const inInput = key in inputSchema.shape;
                            const inOutput = key in outputSchema.shape;

                            if (inInput) {
                                inputData[key] = deserializeValue(value, inputSchema.shape[key]);
                            } else if (inOutput) {
                                outputData[key] = deserializeValue(value, outputSchema.shape[key]);
                            }
                        }

                        // Check if we have output data (non-empty)
                        const hasOutput = Object.keys(outputData).length > 0;

                        return {
                            id: row.id,
                            input: inputData as z.infer<I>,
                            ...(hasOutput && { output: outputData as z.infer<O> })
                        };
                    });
                },
                "Database",
                `Find data in ${name}`
            );
        },

        /**
         * Update data in the database
         * Optimized to use hash lookup for complete inputs
         */
        async update(
            input: Partial<z.infer<I>>,
            output: Partial<z.infer<O>>
        ): Promise<{ id: string }> {
            return measure(
                async () => {
                    if (!initialized) await this.init();

                    let id: string;
                    let existing: DatabaseRecord<z.infer<I>, z.infer<O>> | null = null;

                    // Fast path: if input is complete, use hash lookup
                    if (isCompleteInput(input)) {
                        id = generateId(input);
                        existing = await getRecordById(id);
                    } else {
                        // Regular path: find by partial input
                        const results = await this.find(input);
                        if (results.length === 0) {
                            throw new Error(`No record found matching the input criteria`);
                        }
                        if (results.length > 1) {
                            throw new Error(`Multiple records found matching the input criteria. Please provide more specific input.`);
                        }

                        existing = results[0];
                        id = existing.id;
                    }

                    // If no record exists and we have complete input, insert a new one
                    if (!existing && isCompleteInput(input)) {
                        return this.insert(input as z.infer<I>, output);
                    }

                    // If no record exists and we have incomplete input, throw error
                    if (!existing) {
                        throw new Error(`No record found matching the input criteria`);
                    }

                    // Update output
                    // Check if output record exists
                    const outputExists = db.query(`SELECT 1 FROM ${outputTable} WHERE id = ?`).get(id);

                    if (outputExists) {
                        // Get the current output to merge with
                        const mergedOutput = {
                            ...(existing?.output || {}),
                            ...output,
                            id
                        };

                        // Process the merged output for storage
                        const processedOutput = processObjectForStorage(mergedOutput);

                        // Update the record
                        updateRecord(db, outputTable, id, processedOutput);
                    } else {
                        // Create new output record
                        const processedOutput = processObjectForStorage({
                            ...output,
                            id
                        });
                        insertRecord(db, outputTable, processedOutput);
                    }

                    return { id };
                },
                "Database",
                `Update data in ${name}`
            );
        },

        /**
         * Delete records from the database
         * Optimized to use hash lookup for complete inputs
         */
        async delete(input: Partial<z.infer<I>>): Promise<{ id: string }> {
            return measure(
                async () => {
                    if (!initialized) await this.init();

                    let id: string;

                    // Fast path: if input is complete, use hash lookup
                    if (isCompleteInput(input)) {
                        id = generateId(input);
                    } else {
                        // Regular path: find by partial input
                        const results = await this.find(input);
                        if (results.length === 0) {
                            throw new Error(`No record found matching the input criteria`);
                        }
                        if (results.length > 1) {
                            throw new Error(`Multiple records found matching the input criteria. Please provide more specific input.`);
                        }

                        id = results[0].id;
                    }

                    // Delete from input table
                    db.query(`DELETE FROM ${inputTable} WHERE id = ?`).run(id);

                    // Delete from output table
                    db.query(`DELETE FROM ${outputTable} WHERE id = ?`).run(id);

                    return { id };
                },
                "Database",
                `Delete data from ${name}`
            );
        },

        /**
         * Execute a raw SQL query
         */
        async query<T = any>(sql: string, ...params: any[]): Promise<T[]> {
            return measure(
                async () => {
                    return db.query<T>(sql).all(...params);
                },
                "Database",
                `Raw query on ${name}`
            );
        },

        /**
         * Listen for changes to the database
         */
        listen(callback: (event: ChangeEvent<z.infer<I>, z.infer<O>>) => void): { stop: () => void } {
            if (!initialized) {
                this.init().catch(error => {
                    verboseLog("Database", `Error initializing database for listener: ${error}`, null);
                });
            }

            // Add event listener
            eventEmitter.on('change', callback);

            // Get last change ID for starting point
            const lastChangeRecord = db.query(`
        SELECT MAX(id) as id FROM _${sanitizedName}_changes
      `).get();

            lastChangeId = lastChangeRecord?.id || 0;

            // Set up polling interval (every 500ms)
            const intervalId = setInterval(pollChanges, 500);

            // Return function to stop listening
            return {
                stop: () => {
                    clearInterval(intervalId);
                    eventEmitter.off('change', callback);
                }
            };
        },

        /**
         * Find similar records (placeholder for future implementation)
         */
        async similar(input: Partial<z.infer<I>>, options?: any): Promise<Array<DatabaseRecord<z.infer<I>, z.infer<O>>>> {
            // This is just a placeholder for future implementation
            throw new Error("Method not implemented yet");
        },

        /**
         * Get the input schema
         */
        getInputSchema(): I {
            return inputSchema;
        },

        /**
         * Get the output schema
         */
        getOutputSchema(): O {
            return outputSchema;
        },

        /**
         * Get the database name
         */
        getName(): string {
            return name;
        },

        /**
         * Close the database connection
         */
        close(): void {
            db.close();
        }
    };
}

// Utility functions remain the same...
function sanitizeName(name: string): string {
    // Replace invalid characters
    let sanitized = name.replace(/[^a-zA-Z0-9_]/g, "_");

    // Ensure it doesn't start with a number
    if (/^[0-9]/.test(sanitized)) {
        sanitized = "t_" + sanitized;
    }

    // Check if it's a reserved word
    if (RESERVED_SQLITE_WORDS.includes(sanitized.toUpperCase())) {
        sanitized = "t_" + sanitized;
    }

    return sanitized;
}

async function ensureDirExists(dir: string) {
    console.log('===> database.ts:688 ~ dir', dir);
    try {
        await mkdir(dir, { recursive: true });
    } catch (error) {
        // Directory likely already exists
    }
}

function createTableSql(schema: z.ZodObject<any>, tableName: string): string {
    const columns = [];

    // Add id column
    columns.push("id TEXT PRIMARY KEY");

    // Process schema
    for (const [key, value] of Object.entries(schema.shape)) {
        if (key === "id") continue; // Skip id if defined in schema

        let columnType = "";
        let defaultClause = ""; // For storing DEFAULT clause


        if (isZodString(value)) {
            columnType = "TEXT";
        } else if (isZodNumber(value)) {
            columnType = "NUMERIC";
        } else if (isZodBoolean(value)) {
            columnType = "INTEGER";
        } else if (isZodEnum(value)) {
            columnType = "TEXT";
        } else if (isZodDate(value)) {
            columnType = "INTEGER"; // Store as timestamp
        } else if (isZodArray(value) || isZodObject(value)) {
            columnType = "TEXT"; // Store as JSON
        } else if (isZodNullable(value)) {
            const innerType = value.unwrap();
            if (isZodString(innerType)) {
                columnType = "TEXT NULL";
            } else if (isZodNumber(innerType)) {
                columnType = "NUMERIC NULL";
            } else if (isZodBoolean(innerType)) {
                columnType = "INTEGER NULL";
            } else if (isZodEnum(innerType)) {
                columnType = "TEXT NULL";
            } else if (isZodDate(innerType)) {
                columnType = "INTEGER NULL";
            } else if (isZodArray(innerType) || isZodObject(innerType)) {
                columnType = "TEXT NULL";
            }
        } else if (isZodOptional(value)) {
            const innerType = value.unwrap();
            if (isZodString(innerType)) {
                columnType = "TEXT NULL";
            } else if (isZodNumber(innerType)) {
                columnType = "NUMERIC NULL";
            } else if (isZodBoolean(innerType)) {
                columnType = "INTEGER NULL";
            } else if (isZodEnum(innerType)) {
                columnType = "TEXT NULL";
            } else if (isZodDate(innerType)) {
                columnType = "INTEGER NULL";
            } else if (isZodArray(innerType) || isZodObject(innerType)) {
                columnType = "TEXT NULL";
            }
        } else if (isZodDefault(value)) {
            // Get the inner type from the default wrapper
            const innerType = value._def.innerType;

            if (isZodString(innerType)) {
                columnType = "TEXT NULL";
            } else if (isZodNumber(innerType)) {
                columnType = "NUMERIC NULL";
            } else if (isZodBoolean(innerType)) {
                columnType = "INTEGER NULL";
            } else if (isZodEnum(innerType)) {
                columnType = "TEXT NULL";
            } else if (isZodDate(innerType)) {
                columnType = "INTEGER NULL";
            } else if (isZodArray(innerType) || isZodObject(innerType)) {
                columnType = "TEXT NULL";
            }

            // Make sure to get the default value
            const defaultValue = getDefaultValue(value);
            if (defaultValue !== undefined) {
                // Format the default value based on its type
                if (typeof defaultValue === 'string') {
                    defaultClause = ` DEFAULT '${defaultValue}'`;
                } else if (typeof defaultValue === 'number' || typeof defaultValue === 'boolean') {
                    defaultClause = ` DEFAULT ${defaultValue}`;
                } else if (defaultValue instanceof Date) {
                    defaultClause = ` DEFAULT ${defaultValue.getTime()}`;
                } else if (defaultValue !== null && typeof defaultValue === 'object') {
                    defaultClause = ` DEFAULT '${JSON.stringify(defaultValue)}'`;
                }
            }
        }

        if (columnType) {
            columns.push(`${key} ${columnType}${defaultClause}`);
        }
    }
    console.log('===> database.ts:753 ~ columns', columns, tableName);

    return `CREATE TABLE IF NOT EXISTS ${tableName} (${columns.join(", ")})`;
}

async function createAutomaticIndexes(
    db: Database,
    inputTable: string,
    inputSchema: z.ZodObject<any>
): Promise<void> {
    // Get table info to check existing columns
    const tableInfo = db.query(`PRAGMA table_info(${inputTable})`).all();

    // Create index for each field except id (which is already indexed)
    for (const column of tableInfo) {
        if (column.name === "id") continue;

        const indexName = `idx_${inputTable}_${column.name}`;
        try {
            db.query(`CREATE INDEX IF NOT EXISTS ${indexName} ON ${inputTable}(${column.name})`).run();
        } catch (error) {
            verboseLog("Database", `Failed to create index for ${column.name}`, error);
        }
    }
}

async function createCommonOutputIndexes(
    db: Database,
    outputTable: string,
    outputSchema: z.ZodObject<any>
): Promise<void> {
    // Common fields to index in output tables
    const commonFields = [
        "status", "timestamp", "completedAt", "publishedAt", "processingStatus"
    ];

    // Create indexes for common fields if they exist
    for (const field of commonFields) {
        if (field in outputSchema.shape) {
            const indexName = `idx_${outputTable}_${field}`;
            try {
                db.query(`CREATE INDEX IF NOT EXISTS ${indexName} ON ${outputTable}(${field})`).run();
            } catch (error) {
                verboseLog("Database", `Failed to create index for ${field}`, error);
            }
        }
    }
}

function processObjectForStorage(data: any): Record<string, any> {
    const result: Record<string, any> = {};

    for (const [key, value] of Object.entries(data)) {
        if (value === undefined) continue;

        if (value !== null && typeof value === "object") {
            if (value instanceof Date) {
                result[key] = value.getTime();
            } else {
                result[key] = JSON.stringify(value);
            }
        } else {
            result[key] = value;
        }
    }

    return result;
}

function insertRecord(db: Database, table: string, data: Record<string, any>): void {
    const columns = Object.keys(data);
    const placeholders = columns.map(() => "?").join(", ");
    const values = Object.values(data);

    db.query(
        `INSERT INTO ${table} (${columns.join(", ")}) VALUES (${placeholders})`
    ).run(...values);
}

function updateRecord(db: Database, table: string, id: string, data: Record<string, any>): void {
    // Remove id from data since we don't want to update it
    const { id: _, ...dataWithoutId } = data;

    const columns = Object.keys(dataWithoutId);
    if (columns.length === 0) return;

    const setClauses = columns.map(col => `${col} = ?`).join(", ");
    const values = [...Object.values(dataWithoutId), id];

    db.query(
        `UPDATE ${table} SET ${setClauses} WHERE id = ?`
    ).run(...values);
}

function deserializeValue(value: any, zodType: z.ZodTypeAny): any {
    if (value === null) return null;

    if (isZodArray(zodType) || isZodObject(zodType)) {
        try {
            return JSON.parse(value);
        } catch {
            return value;
        }
    } else if (isZodDate(zodType)) {
        return new Date(value);
    } else if (isZodNumber(zodType)) {
        return Number(value);
    } else if (isZodBoolean(zodType)) {
        return Boolean(value);
    } else if (isZodNullable(zodType)) {
        return deserializeValue(value, zodType.unwrap());
    } else if (isZodOptional(zodType)) {
        return deserializeValue(value, zodType.unwrap());
    }

    return value;
}

// Type checking functions
function isZodString(value: z.ZodTypeAny): boolean {
    return value._def.typeName === "ZodString";
}

function isZodNumber(value: z.ZodTypeAny): boolean {
    return value._def.typeName === "ZodNumber";
}

function isZodBoolean(value: z.ZodTypeAny): boolean {
    return value._def.typeName === "ZodBoolean";
}

function isZodEnum(value: z.ZodTypeAny): boolean {
    return value._def.typeName === "ZodEnum";
}

function isZodDate(value: z.ZodTypeAny): boolean {
    return value._def.typeName === "ZodDate";
}

function isZodArray(value: z.ZodTypeAny): boolean {
    return value._def.typeName === "ZodArray";
}

function isZodObject(value: z.ZodTypeAny): boolean {
    return value._def.typeName === "ZodObject";
}

function isZodNullable(value: z.ZodTypeAny): boolean {
    return value._def.typeName === "ZodNullable";
}


function isZodOptional(value: z.ZodTypeAny): boolean {
    return value._def.typeName === "ZodOptional";
}

// Helper function to extract default value from a Zod schema
function getDefaultValue(zodSchema) {
    // Direct default case
    if (zodSchema._def.defaultValue !== undefined) {
        return typeof zodSchema._def.defaultValue === 'function'
            ? zodSchema._def.defaultValue()
            : zodSchema._def.defaultValue;
    }

    // Handle ZodDefault type - this is the case we were missing
    if (zodSchema._def.typeName === "ZodDefault") {
        return typeof zodSchema._def.defaultValue === 'function'
            ? zodSchema._def.defaultValue()
            : zodSchema._def.defaultValue;
    }

    // Handle wrapped types (optional, nullable)
    if (isZodOptional(zodSchema) || isZodNullable(zodSchema)) {
        const innerType = zodSchema.unwrap();
        return getDefaultValue(innerType); // Recursively check inner type
    }

    return undefined;
}

function isZodDefault(value: z.ZodTypeAny): boolean {
    return value._def.typeName === "ZodDefault";
}
