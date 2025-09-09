-- Pipeline Metadata and Audit Tracking
CREATE TABLE IF NOT EXISTS pipeline_execution (
    execution_id SERIAL PRIMARY KEY,
    start_timestamp_utc TIMESTAMP WITH TIME ZONE NOT NULL,
    end_timestamp_utc TIMESTAMP WITH TIME ZONE,
    status VARCHAR(20) NOT NULL, -- RUNNING, SUCCESS, FAILED
    load_strategy VARCHAR(10) NOT NULL, -- FULL, DELTA
    source_file_version VARCHAR(100),
    records_processed INT,
    high_water_mark TIMESTAMP WITH TIME ZONE
);

-- Standardized Organization data (e.g., Marketing Authorization Holders) - Enriched via SPOR OMS
CREATE TABLE IF NOT EXISTS organizations (
    oms_id VARCHAR(50) PRIMARY KEY, -- SPOR OMS Identifier
    organization_name VARCHAR(500) NOT NULL,
    country_code CHAR(2),
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Standardized Substance data - Enriched via SPOR RMS/SMS
CREATE TABLE IF NOT EXISTS substances (
    spor_substance_id VARCHAR(50) PRIMARY KEY, -- SPOR Substance Identifier
    substance_name VARCHAR(500) NOT NULL,
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- The core EPAR table combining Metadata, Standard, and Full Representations
CREATE TABLE IF NOT EXISTS epar_index (
    epar_id VARCHAR(100) PRIMARY KEY, -- Unique ID from EMA source (e.g., Product Number)
    medicine_name VARCHAR(500) NOT NULL,
    authorization_status VARCHAR(50) NOT NULL,
    first_authorization_date DATE,
    withdrawal_date DATE,
    last_update_date_source DATE NOT NULL, -- CDC Marker from source file

    -- Standard Representation (Raw values from index file)
    active_substance_raw TEXT,
    marketing_authorization_holder_raw VARCHAR(500),
    therapeutic_area VARCHAR(500),

    -- Full Representation (Foreign Keys to standardized data)
    mah_oms_id VARCHAR(50) REFERENCES organizations(oms_id),

    -- Pipeline Metadata
    is_active BOOLEAN DEFAULT TRUE, -- For soft deletes/withdrawals
    source_url TEXT,
    etl_load_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    etl_execution_id INT REFERENCES pipeline_execution(execution_id)
);

-- Junction table for Many-to-Many relationship between EPARs and standardized Substances
CREATE TABLE IF NOT EXISTS epar_substance_link (
    epar_id VARCHAR(100) REFERENCES epar_index(epar_id),
    spor_substance_id VARCHAR(50) REFERENCES substances(spor_substance_id),
    PRIMARY KEY (epar_id, spor_substance_id)
);

-- Stores metadata and location for associated documents (SmPC, Package Leaflet, etc.)
CREATE TABLE IF NOT EXISTS epar_documents (
    document_id UUID PRIMARY KEY,
    epar_id VARCHAR(100) REFERENCES epar_index(epar_id),
    document_type VARCHAR(50), -- SmPC, PL, Assessment Report
    language_code CHAR(2),
    source_url TEXT NOT NULL,
    storage_location TEXT, -- Path in object storage (S3, Azure Blob)
    file_hash CHAR(64), -- SHA-256 for integrity
    download_timestamp TIMESTAMP WITH TIME ZONE
);
