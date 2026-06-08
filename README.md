# Supplier Invoice Ingestion Pipeline

An automated backend data pipeline built in n8n that monitors an inbox for supplier invoice batches via CSV, performs robust validation checks, prevents duplicate data insertion, logs records directly to a Microsoft SQL Server database, and dispatches a metrics digest email.

---

## 1. How the Workflow Runs (Triggers & Infrastructure)

This system operates completely unattended in **Production Mode**, utilizing an event-driven architecture to initiate execution context.

### The Trigger Mechanism:
* **Node Used:** `Gmail Trigger`
* **Operational Mode:** Continuous Background Polling / Webhook Push
* **Filters Applied:** The workflow isolates incoming traffic by checking for target inbox labels and strictly listening for messages containing file attachments with a `.csv` or `.txt` extension.
* **Execution Lifecycle:** Once the workflow toggle switch in n8n is set to **Publish**, the trigger engine runs natively in the background. Incoming provider files instantly fire the pipeline, passing the binary payload downstream without requiring manual operator interaction.

---

## 2. Input File Format & Key Mappings

The ingestion stream expects a standard, flat comma-separated tabular structure containing specific transactional properties.

### Expected CSV Layout:
Source data frames must declare headers exactly matching the minimum parameters below:
```csv
supplier_number,supplier_name,invoice_number,department,invoice_date,amount_excl,vat_rate

```

### Direct Field Mapping Matrix & Transformations:

Inside the workflow, raw text variables extracted from the file structure are passed through item-mapping nodes to translate properties cleanly into target database schema definitions:

| Source CSV Column | Data Type | Transformation / Mapping Logic | Target Database Column |
| --- | --- | --- | --- |
| `supplier_number` | String | Maps directly as alphanumeric tracking code | `supplier_number` |
| `supplier_name` | String | Normalizes business trading name | `supplier_name` |
| `invoice_number` | String | Maps directly as tracking reference string | `invoice_number` |
| `department` | String | Routes to internal corporate branch (e.g., `Ops`, `Sales`) | `department` |
| `invoice_date` | Date | Standardized to ISO string format (`YYYY-MM-DD`) | `invoice_date` |
| `amount_excl` | Decimal | Parsed as numeric base value via `parseFloat()` | `amount_excl_vat` |
| `vat_rate` | Integer | Read from file. If blank or omitted, defaults to statutory **`15`** | `vat_rate_applied` |
| *Calculated Value* | Decimal | Computed via: `Math.round((amount_excl * (vat_rate / 100)) * 100) / 100` | `vat` |
| *Calculated Value* | Decimal | Computed via: `amount_excl_vat + vat` ($\pm0.01$ variance tolerance) | `amount_incl_vat` |

---

## 3. Database Schema Creation (SQL Scripts)

Before initializing data transactions from n8n nodes, execution targets must be prepared within the SQL Server instance. Connect to your database engine via **SQL Server Management Studio (SSMS)** and run the following setup DDL script:

```sql
-- Target Schema Initialization
USE SupplierIngestDB;
GO

-- 1. IDEMPOTENCY TRACKER: FILE LOG INDEX
-- Records SHA-256 binary string fingerprints to catch duplicate file submissions.
CREATE TABLE dbo.processed_files_log (
    id INT IDENTITY(1,1) PRIMARY KEY,
    file_hash VARCHAR(64) NOT NULL UNIQUE,
    file_name VARCHAR(255) NULL,
    extracted_at DATETIME DEFAULT GETDATE() NOT NULL
);

-- 2. PRODUCTION COMPLIANT INVOICES TABLE
-- Core ledger housing successfully parsed, unique, and normalized entries.
CREATE TABLE dbo.supplier_invoices (
    id UNIQUEIDENTIFIER DEFAULT NEWID() PRIMARY KEY,
    invoice_number VARCHAR(100) NOT NULL,
    supplier_number VARCHAR(50) NOT NULL,
    supplier_name VARCHAR(100) NOT NULL,
    department VARCHAR(50) NOT NULL,
    amount_excl_vat DECIMAL(18,2) NOT NULL,
    vat DECIMAL(18,2) NOT NULL,
    amount_incl_vat DECIMAL(18,2) NOT NULL,
    invoice_date DATE NOT NULL,
    source_file_name VARCHAR(255) NULL,
    source_hash VARCHAR(64) NULL,
    ingest_timestamp DATETIMEOFFSET DEFAULT SYSDATETIMEOFFSET() NOT NULL,
    status VARCHAR(50) DEFAULT 'inserted' CHECK (status IN ('inserted', 'duplicate', 'failed')),
    validation_notes VARCHAR(MAX) NULL,

    -- Row-Level Uniqueness Constraint
    CONSTRAINT UQ_Supplier_Invoice UNIQUE (supplier_number, invoice_number)
);

-- 3. ISOLATION ZONE: DRY-RUN SIMULATION TABLE
-- Target landing table utilized when the pipeline is executed with dryRun == true.
CREATE TABLE dbo.supplier_invoices_staging (
    id INT IDENTITY(1,1) PRIMARY KEY,
    supplier_number VARCHAR(50),
    supplier_name VARCHAR(100),
    invoice_number VARCHAR(100),
    department VARCHAR(50),
    invoice_date DATE,
    amount_excl_vat DECIMAL(18,2),
    vat DECIMAL(18,2),
    amount_incl_vat DECIMAL(18,2),
    vat_rate_applied INT,
    status VARCHAR(50) DEFAULT 'staging',
    validation_notes VARCHAR(MAX)
);

```
---

## 4. n8n Database Connection Setup

To securely handle data updates without exposing production passwords on the workflow canvas, connection parameters are encapsulated within n8n’s encrypted **Credentials Manager**.

### Node Parameters Configuration:

Configure your Microsoft SQL Server transaction nodes using these parameters:

* **Credential Type:** `Microsoft SQL Server Credentials`
* **Host / Server IP:** `127.0.0.1` *(Localhost loopback location)*
* **Port:** `1433` *(Default SQL Server relational engine communication port)*
* **Database Name:** `SupplierIngestDB`
* **Authentication Method:** `SQL Server Authentication`
* **User (User ID):** `sa` *(System Administrator Account)*
* **Password:** `[Securely Hidden / Encrypted by n8n]`

*Developer Environment Note:* If testing inside a local environment against **SQL Server Express (`.\SQLEXPRESS`)**, ensure that the TCP/IP protocol is explicitly enabled within the *SQL Server Configuration Manager* utility and port `1433` is unblocked to receive inbound engine listener connections.

---
## 5. Row-Wise Validation Logic & Code Rules

Every single array element extracted from a batch spreadsheet drops into an n8n JavaScript code block to confirm structural alignment with regional corporate parameters.

### Validation Rules Matrix:

1. **Properties Verification:** Confirms that vital keys (`supplier_number`, `invoice_number`, `amount_excl`) possess non-null, populated items.
2. **South African Statutory VAT Fallback:** Checks for the presence of `vat_rate`. If the property is missing, null, or blank, the algorithm overrides the property and injects the South African statutory standard value of **`15%`**.
3. **Temporal Compliance (Future Date Check):** Isolates the runtime system execution clock to the local **`Africa/Johannesburg`** timezone. The transaction date is evaluated against this threshold; post-dated invoices are instantly rejected to prevent financial ledger irregularities.

### Dataset Testing Scenarios (Expected Code Payloads):

* **Scenario A: Clean Operational Data**
* *Input Row:* `S009,OfficeCo,OC-22119,Ops,2025-10-28,2175.00,15`
* *Code Output:* `{ "valid": true, "validation_notes": "Passed clean data validation parameters." }`


* **Scenario B: Missing Tracking Reference**
* *Input Row:* `S009,OfficeCo,,Sales,2025-10-29,450.00,15`
* *Code Output:* `{ "valid": false, "validation_notes": "Validation Failure: Missing one or more required fields." }`


* **Scenario C: Post-Dated Accounting Frame**
* *Input Row:* `S011,PaperMart,PM-77891,Ops,2027-11-01,1020.00,15`
* *Code Output:* `{ "valid": false, "validation_notes": "Validation Failure: Invoice date 2027-11-01 cannot be in the future." }`

---

## 6. Email Node Configuration

Following loop completion, execution trajectories hit the **Aggregate Metrics** engine node which reduces iteration arrays into unified runtime calculation numbers.

* **Node Class Type:** `Gmail Node / Send a message`
* **Workflow Connection:** Linked cleanly to the primary loop iteration node's **`done`** termination sequence path.
* **Notification Payload:** Generates a structured responsive HTML layout framing runtime metrics and error description grids.
* **Subject Header Expression Code:**
```text
Supplier Ingest: {{ $json.inserted }} ok, {{ $json.duplicates }} dup, {{ $json.failed }} failed

```
---

## 7. Execution Results (Row Status Audit Log)

The matrix below maps exactly how the pipeline evaluates, classifies, and commits your 4 target test lines to the persistence tier during a production deployment run:

| supplier_number | supplier_name | invoice_number | department | invoice_date | amount_excl | valid | validation_notes / Tracking Status | Target Table Destination |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `S009` | `OfficeCo` | `OC-22119` | `Ops` | `2025-10-28` | `2175.00` | `1` | Ingested successfully / `inserted` | `dbo.supplier_invoices` |
| `S009` | `OfficeCo` | `OC-22120` | `Sales` | `2025-10-29` | `450.00` | `1` | Ingested successfully / `inserted` | `dbo.supplier_invoices` |
| `S011` | `PaperMart` | `PM-77891` | `Ops` | `2025-11-01` | `1020.00` | `1` | Ingested successfully / `inserted` | `dbo.supplier_invoices` |
| `S011` | `PaperMart` | `PM-77891` | `Ops` | `2025-11-01` | `1020.00` | `1` | Row duplicate intercepted / `duplicate` | *Skipped (Logged to Metrics Only)* |

```
```
## 8. Pipeline Execution Verification & Screenshots

Below are the actual pipeline testing screenshots showing how the system reacts in real time to processing requests, duplicates, and communication handling.

### Screenshot 1: Successful Batch Insert (Canvas View)
![Successful Insert](Screenshot%202026-06-08%20172539.png)
* **Trigger Condition:** Fired when a brand new, completely unique CSV file payload is ingested.
* **Pipeline Behavior:** The workflow passes the file-level SHA-256 hash gate and maps the row matrices cleanly.
* **System Outcome:** The iteration loop processes all line items and writes them sequentially into the database.

***

### Screenshot 2: Duplicate File Skip (Early Exit Path)
![Duplicate Skip Workflow](Screenshot%202026-06-08%20172702.png)
* **Trigger Condition:** Fired when an identical file batch is re-sent or re-uploaded by an operator.
* **Pipeline Behavior:** The crypto engine calculates a matching hash, causing the gate logic to evaluate to `false`.
* **System Outcome:** Processing is instantly terminated at the entry point to preserve database integrity and compute resources.

***

### Screenshot 3: Idempotency Early-Exit Email Notification
![Duplicate File Email Alert](Screenshot%202026-06-08%20172820.png)
* **Incident Root Cause:** Interception of a pre-existing cryptographic file fingerprint at the early gate check.
* **Notification Channel:** Dispatched immediately to administration via the automated Gmail communication node.
* **Administrative Context:** Alerts stakeholders that the ingest run was skipped, avoiding duplicated record runs.

***

### Screenshot 4: Automated Execution Email Summary Report
![Email Summary Report](Screenshot%202026-06-08%20172744.png)
* **Delivery Condition:** Executed seamlessly along the `done` path after the invoice iteration finishes.
* **Aggregate Metrics Reported:** * **Total Processed:** 4 Rows
  * **Inserted Successfully:** 3 Rows
  * **Duplicates Bypassed:** 1 Row
  * **System Failures:** 0 Rows
* **Visual Structure:** Features a clean HTML table summarizing row landing states and diagnostic validation notes.

***

### Database Verification: Target SQL Server Results Grid
![DB Query Screenshot](Screenshot%202026-06-08%20172928.png)
* **Verification Interface:** Direct system query executed inside SQL Server Management Studio (SSMS).
* **Data Integrity Check:** Verifies rows landed cleanly with normalized field mappings and mathematically calculated VAT metrics.
* **Tracking Audit State:** Displays correct system assignments for tracking statuses (`inserted` vs `duplicate`) alongside corresponding validation notes.
```
```
## 9. Simulation Mode Configuration ("Dry-Run" Bonus Feature)

To allow safe testing of the pipeline without altering core operational datasets, the system includes a modular toggle framework that simulates ingestion runs.

### Operational Mechanics
* **Global Configuration:** An `Edit Fields` utility node is placed directly at the front of the workflow to inject a global boolean metadata flag named `dryRun`.
* **Conditional Branching:** Inside the core processing loop, an execution gate (`If1`) evaluates the status of this flag for every single row.
* **Staging Isolation:** If `dryRun` resolves as `true`, the standard deduplication checks and production table insertions are entirely bypassed. Instead, the items route down a simulation path that dumps the raw data into a dedicated staging table (`dbo.supplier_invoices_staging`) with a status tracking flag labeled as `staging`.

---

### Dry-Run Execution Evidence & Verification

Below is the visual verification tracing a dry-run test pass sequentially from parameter initialization to database isolation landing states:

#### Step 9.1: Setting the Simulation Parameter Flag
![Dry Run Flag Configuration](Screenshot%202026-06-08%20175019.png)
* **Configuration State:** Captured directly within the front-end input metadata configuration editor.
* **Variable Injection:** Injects a strict global user-defined variable stream, locking the `dryRun` boolean expression to `true`.
* **Testing Benefit:** Instructs downstream query processors to halt production writes, enabling risk-free sandbox executions.

***

#### Step 9.2: Simulation Conditional Loop Routing (Canvas View)
![Dry Run Canvas Path](Screenshot%202026-06-08%20175110.png)
* **Canvas Routing:** Captured visually during an active 4-item document batch iteration pass.
* **Gate Evaluation:** The conditional logical routing node checks the `dryRun` flag and evaluates to `true`.
* **Execution Divergence:** Reroutes execution trajectories away from live production scopes, cleanly firing the alternative "Insert Staging Rows" query engine node.

***

#### Step 9.3: Database Isolation Verification (Staging Results Grid)
![Staging Table Verification Grid](Screenshot%202026-06-08%20175200.png)
* **Verification Interface:** Direct data grid snapshot pulled from SQL Server Management Studio (SSMS).
* **Data Isolation Check:** Confirms that all 4 sample test records successfully bypassed the live ledger and nested safely inside `dbo.supplier_invoices_staging`.
* **Audit Compliance:** Verifies that all simulated line items were cleanly stamped with their dynamic tracking status value of `staging` for rapid auditing.
```
```
## 10. Global Error Handling & Resilience Framework

To ensure enterprise-grade stability and satisfy strict audit compliance, the ecosystem implements an isolated **Global Error Handler** workflow. This framework functions as an asynchronous catch-all monitoring system that decouples error handling from core business logic.

### Core Resilience Mechanisms
Upon activation, the Global Error Handler splits incoming data packets into two concurrent mitigation branches:

* **Administrative Email Dispatch:**
  * Parses out the crash payload via the n8n Gmail node engine.
  * Dynamically injects critical diagnostic traits into the alert context, targeting the explicit node name (`failed_node`) and system message (`error_message`).
* **Persistent Incident Logging:**
  * Executes an explicit, safe parameterized T-SQL statement against the logging framework.
  * Writes the runtime parameters (`workflow_id`, `execution_id`, `logged_at`) straight to the `dbo.supplier_invoices_failures` table.
  * Initializes the `retry_count` attribute to `0` to build a baseline for future automated reprocessing queues.

---

### Error Handler Execution Evidence & Verification

Below is the verification trace showing the global exception management engine monitoring, alerting, and persistently logging a simulated ingestion failure:

#### Step 10.1: Global Error Handler Workflow Structure
![Global Error Handler Canvas View](Screenshot%202026-06-08%20180132.png)
* **Canvas Layout:** Displays the isolated workflow triggered strictly by the native n8n Error Trigger node.
* **Action Routing:** The stream bifurcates into a recovery loop, triggering an immediate notification broadcast before invoking the persistence logger query.

***

#### Step 10.2: Relational Incident Logging State (SSMS Verification Grid)
![Database Failure Log Verification Grid](Screenshot%202026-06-08%20180224.png)
* **Verification Interface:** Query lookup targeting the log database via SQL Server Management Studio (SSMS).
* **Audit Trail Confirmation:** Validates that the crash handler caught the pipeline exception and successfully generated a failure record.
* **System Diagnostics Saved:** Confirms the exact missing column error message and node location were successfully locked into the table with `retry_count` initialized.

