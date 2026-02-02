-- Update table for stock market cap history with new fields
DROP TABLE IF EXISTS public.stock_valuation_history;

CREATE TABLE public.stock_valuation_history (
    id TEXT PRIMARY KEY, -- 8-digit sequential index (e.g., 00000001)
    symbol TEXT NOT NULL,
    date DATE NOT NULL,
    "Market_cap" NUMERIC, -- Using double quotes for case sensitivity if requested
    unit TEXT, -- For "bn/mn" field
    currency TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Add comments for clarity
COMMENT ON TABLE public.stock_valuation_history IS 'Historical market capitalization data for stocks with unit and currency';
COMMENT ON COLUMN public.stock_valuation_history.id IS '8-digit padded sequential ID';
COMMENT ON COLUMN public.stock_valuation_history."Market_cap" IS 'Market capitalization';
COMMENT ON COLUMN public.stock_valuation_history.unit IS 'Unit (bn/mn)';
COMMENT ON COLUMN public.stock_valuation_history.currency IS 'Currency (cny)';
