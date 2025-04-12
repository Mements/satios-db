# Sati-DB: SQLite Data Modeling & Inter-Process Communication

**Sati-DB** provides a structured approach to using SQLite databases for both data modeling and as a foundation for inter-process communication (IPC) or asynchronous workflows. It consists of two main parts:

1.  **`database.ts` (Core Layer):** A robust library for managing data using a specific, optimized two-table pattern (input/output).
2.  **`agent.ts` (Optional Workflow Layer):** Builds upon the core layer to enable asynchronous request/response patterns between processes using the database as a mediator.

This README focuses primarily on the design philosophy and advantages of the core `database.ts` layer's structure.

## Core Database Pattern (`database.ts`)

The foundation of Sati-DB is a deliberate two-table structure designed for clarity, efficient querying based on input criteria, and handling potentially asynchronous or optional associated data. For each logical entity (e.g., 'tweets', 'thoughts'), two tables are created:

* **`input_<table>` (e.g., `input_tweets`)**:
    * Stores the primary, defining data – the "input" or "key" information.
    * **Designed for Filtering:** The columns in this table directly correspond to your input schema fields and are intended to be used as filter criteria in queries (`findRaw`, `query`).
    * **Automatic Indexing:** The library automatically creates indexes on the columns of the `input_<table>` (specifically non-TEXT columns by default). This significantly optimizes lookup performance when you query records based on specific input field values.
    * Contains the primary key `id`, which is a deterministic hash generated from the *entire* input object content.

* **`output_<table>` (e.g., `output_tweets`)**:
    * Stores data that is *derived from* or *associated with* a specific input record (e.g., calculation results, generated text, status, metadata). Think of it as auxiliary data linked to the primary input.
    * Uses the *same* `id` as its primary key, establishing a strict **one-to-one relationship** with a corresponding record in the `input_<table>`.
    * Includes a foreign key constraint back to the `input_<table>` with `ON DELETE CASCADE`.
    * **Write-Once Enforcement:** The library ensures (via `INSERT OR IGNORE`) that once an output record is written for a given `id`, it cannot be accidentally overwritten by subsequent `insert` calls. Updates must use the specific `updateOutput` method, which itself fails if output already exists.

* **`_sati_metadata_<table>`**: Stores metadata about the database, including a representation of the input/output schemas used during initialization.
* **`_changes_<table>`**: Records all inserts/updates/deletes, enabling the optional `agent.ts` layer to poll for changes.

## Advantages of the Two-Table Structure

While storing everything in a single table is possible, the Sati-DB two-table approach offers distinct advantages:

1.  **Conceptual Clarity:** It enforces a clear separation between the primary data used for identification and filtering (input) and the associated results or metadata (output). This improves understanding of the data model.

2.  **Optimized Input-Based Queries:** By automatically indexing the `input_<table>` columns, the database is inherently optimized for efficiently finding records based on your input criteria using `findRaw` or custom `query` calls. In a single, wide table, you would need to manually ensure appropriate indexes are created on the input-related columns, and those indexes might be larger and potentially less performant due to the table's overall size.

3.  **Focused Indexing:** Indexes on the `input_<table>` are smaller and contain only the filterable input data, leading to potentially faster lookups compared to indexing the same fields within a larger, combined table containing both input and output data.

4.  **Efficient Handling of Optional/Asynchronous Output:** If the output data isn't always available immediately when the input is created (a common scenario in async workflows or caching), this structure avoids needing nullable columns for all output fields in the primary data table. The `output_<table>` only stores rows for inputs that *actually have* an output, keeping the `input_<table>` lean and preventing sparse rows. The `LEFT JOIN` used in `findRaw` naturally handles cases where input exists but output doesn't.

5.  **Clear Data Roles:** It reinforces the idea that the `id` (and therefore the record's identity) is solely determined by the `input` data. The `output` is auxiliary information linked *to* that specific input.

## Schema Versioning

To prevent runtime errors caused by application code using different schemas than what the database was created with, the `init()` function performs a schema check:
* On first initialization, it stores a representation of the input and output Zod schema shapes in the `_sati_metadata_<table>`.
* On subsequent initializations, it compares the provided schemas against the stored representation. If they differ, `init()` throws an error, ensuring schema consistency.

## Agent Layer (`agent.ts` - Optional)

Built upon this robust database structure, the optional `agent.ts` library provides `useAgent`, which implements an asynchronous request/response workflow. It uses the `_changes_<table>` and polling to allow one process to `await agent.request(input)` while another process uses `agent.handle(async (input, setOutput) => ...)` to listen for inputs and provide outputs via the `setOutput` callback. This layer directly benefits from the clear input/output separation and ID correlation provided by `database.ts`.

## Use Cases & Considerations

*(This section would remain largely the same as in the previous README, outlining suitable scenarios like local IPC, background jobs, simple AI pipelines, etc., and mentioning considerations like polling latency and SQLite write limitations.)*

## Setup & Usage

*(This section would also remain similar, outlining dependency installation and basic usage patterns for `useDatabase` and potentially `useAgent`.)*

---

This revised structure emphasizes the deliberate design choices behind the two-table pattern in `database.ts` and why it can be advantageous for specific use cases compared to a single-table model, particularly regarding querying, indexing, and managing associated data.
