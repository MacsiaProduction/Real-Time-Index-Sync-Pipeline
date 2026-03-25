-- Create the products table for CDC source
CREATE TABLE IF NOT EXISTS products (
    product_id   VARCHAR(36) PRIMARY KEY,
    name         VARCHAR(255) NOT NULL,
    category     VARCHAR(100),
    brand        VARCHAR(100),
    description  TEXT,
    updated_at   TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- REPLICA IDENTITY FULL ensures DELETE events contain the full row in "before"
ALTER TABLE products REPLICA IDENTITY FULL;

-- Index for faster scans during CDC snapshot
CREATE INDEX IF NOT EXISTS idx_products_updated_at ON products(updated_at);
