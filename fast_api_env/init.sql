-- Crear tabla para los pagos
CREATE TABLE IF NOT EXISTS pays (
    pay_date DATE,
    total INTEGER,
    user_id INTEGER,
    value_prop VARCHAR(255)
);

-- Crear tabla para los taps
CREATE TABLE IF NOT EXISTS taps (
    user_id INTEGER,
    day_taps DATE,
    position_taps INTEGER,
    value_prop_taps VARCHAR(255)
);

-- Crear tabla para los prints
CREATE TABLE IF NOT EXISTS prints (
    user_id INTEGER,
    day_prints DATE,
    position_prints INTEGER,
    value_prop_prints VARCHAR(255)
);
