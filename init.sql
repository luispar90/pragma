-- Script de inicialización para la base de datos PRAGMA
-- Este script se ejecuta automáticamente cuando se crea el contenedor

-- Crear la tabla de transacciones
CREATE TABLE IF NOT EXISTS transactions (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    user_id VARCHAR(50) NOT NULL,
    file_source VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Crear índices para optimizar consultas
CREATE INDEX IF NOT EXISTS idx_timestamp ON transactions(timestamp);
CREATE INDEX IF NOT EXISTS idx_user_id ON transactions(user_id);
CREATE INDEX IF NOT EXISTS idx_file_source ON transactions(file_source);
CREATE INDEX IF NOT EXISTS idx_price ON transactions(price);

-- Crear tabla para estadísticas del pipeline (opcional)
CREATE TABLE IF NOT EXISTS pipeline_stats (
    id SERIAL PRIMARY KEY,
    stat_name VARCHAR(50) UNIQUE,
    stat_value DECIMAL(15,2),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Crear tabla de logs de procesamiento (opcional)
CREATE TABLE IF NOT EXISTS processing_logs (
    id SERIAL PRIMARY KEY,
    file_name VARCHAR(100),
    rows_processed INTEGER,
    processing_time_seconds DECIMAL(10,2),
    started_at TIMESTAMP,
    completed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) DEFAULT 'completed'
);

-- Crear vista para análisis rápido
CREATE OR REPLACE VIEW transaction_summary AS
SELECT
    file_source,
    COUNT(*) as total_transactions,
    COUNT(DISTINCT user_id) as unique_users,
    MIN(price) as min_price,
    MAX(price) as max_price,
    AVG(price)::DECIMAL(10,2) as avg_price,
    SUM(price)::DECIMAL(15,2) as total_volume,
    MIN(timestamp) as first_transaction,
    MAX(timestamp) as last_transaction
FROM transactions
GROUP BY file_source
ORDER BY file_source;

-- Función para obtener estadísticas globales
CREATE OR REPLACE FUNCTION get_global_stats()
RETURNS TABLE (
    total_count BIGINT,
    avg_price DECIMAL(10,2),
    min_price DECIMAL(10,2),
    max_price DECIMAL(10,2),
    total_volume DECIMAL(15,2),
    unique_users BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        COUNT(*),
        AVG(price)::DECIMAL(10,2),
        MIN(price),
        MAX(price),
        SUM(price)::DECIMAL(15,2),
        COUNT(DISTINCT user_id)::BIGINT
    FROM transactions;
END;
$$ LANGUAGE plpgsql;

-- Mensaje de confirmación
DO $$
BEGIN
    RAISE NOTICE 'Base de datos PRAGMA inicializada correctamente';
    RAISE NOTICE 'Tablas creadas: transactions, pipeline_stats, processing_logs';
    RAISE NOTICE 'Vista creada: transaction_summary';
    RAISE NOTICE 'Función creada: get_global_stats()';
END $$;