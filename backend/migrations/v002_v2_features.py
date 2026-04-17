"""V2 - Manual transactions, custom categories, custom orders."""
VERSION = "002"
DESCRIPTION = "V2 features - manual transactions, transaction categories, custom orders"


def up(cursor):
    cursor.execute("""
    -- Manual Transactions
    CREATE TABLE IF NOT EXISTS manual_transactions (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        type VARCHAR(20) NOT NULL CHECK (type IN ('income', 'expense')),
        category VARCHAR(100) NOT NULL,
        description TEXT,
        amount DECIMAL(12,2) NOT NULL,
        transaction_date DATE NOT NULL DEFAULT CURRENT_DATE,
        reference VARCHAR(255),
        created_by UUID REFERENCES users(id),
        created_at TIMESTAMPTZ DEFAULT NOW()
    );

    -- Transaction Categories
    CREATE TABLE IF NOT EXISTS transaction_categories (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        name VARCHAR(100) UNIQUE NOT NULL,
        type VARCHAR(20) NOT NULL CHECK (type IN ('income', 'expense')),
        is_default BOOLEAN DEFAULT FALSE,
        created_at TIMESTAMPTZ DEFAULT NOW()
    );

    -- Custom Orders
    CREATE TABLE IF NOT EXISTS custom_orders (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        order_number VARCHAR(50) UNIQUE NOT NULL,
        customer_id UUID REFERENCES customers(id),
        customer_name VARCHAR(255),
        customer_mobile VARCHAR(50),
        status VARCHAR(50) DEFAULT 'order_taken' CHECK (status IN ('order_taken','in_progress','ready_for_pickup','delivered','cancelled')),
        description TEXT,
        total_amount DECIMAL(12,2) NOT NULL,
        amount_paid DECIMAL(12,2) DEFAULT 0,
        balance_due DECIMAL(12,2) DEFAULT 0,
        estimated_date TIMESTAMPTZ,
        delivery_date TIMESTAMPTZ,
        notes TEXT,
        created_by UUID REFERENCES users(id),
        created_at TIMESTAMPTZ DEFAULT NOW()
    );

    -- Custom Order Items
    CREATE TABLE IF NOT EXISTS custom_order_items (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        custom_order_id UUID REFERENCES custom_orders(id) ON DELETE CASCADE,
        item_type VARCHAR(50) DEFAULT 'service',
        product_id UUID REFERENCES products(id),
        product_name VARCHAR(255),
        description TEXT,
        quantity DECIMAL(12,2) DEFAULT 1,
        unit_price DECIMAL(12,2) NOT NULL,
        total DECIMAL(12,2) NOT NULL
    );

    -- Custom Order Payments
    CREATE TABLE IF NOT EXISTS custom_order_payments (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        custom_order_id UUID REFERENCES custom_orders(id) ON DELETE CASCADE,
        amount DECIMAL(12,2) NOT NULL,
        payment_method VARCHAR(50) DEFAULT 'cash',
        payment_type VARCHAR(50) DEFAULT 'advance' CHECK (payment_type IN ('advance','balance','full')),
        reference VARCHAR(255),
        created_at TIMESTAMPTZ DEFAULT NOW()
    );

    -- Indexes
    CREATE INDEX IF NOT EXISTS idx_manual_transactions_date ON manual_transactions(transaction_date);
    CREATE INDEX IF NOT EXISTS idx_manual_transactions_type ON manual_transactions(type);
    CREATE INDEX IF NOT EXISTS idx_custom_orders_status ON custom_orders(status);
    CREATE INDEX IF NOT EXISTS idx_custom_orders_customer ON custom_orders(customer_id);
    CREATE INDEX IF NOT EXISTS idx_custom_order_payments_date ON custom_order_payments(created_at);
    """)

    # RLS policies
    new_tables = [
        "manual_transactions", "transaction_categories",
        "custom_orders", "custom_order_items", "custom_order_payments",
    ]
    for table in new_tables:
        cursor.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY;")
        cursor.execute(f"""
            DO $$ BEGIN
                CREATE POLICY "Allow all for {table}" ON {table}
                    FOR ALL USING (true) WITH CHECK (true);
            EXCEPTION WHEN duplicate_object THEN NULL;
            END $$;
        """)

    # Seed default transaction categories
    cursor.execute("""
        INSERT INTO transaction_categories (name, type, is_default) VALUES
        ('Scrap Fabric Sales', 'income', true),
        ('Thread Waste Sales', 'income', true),
        ('Equipment Rental', 'income', true),
        ('Consultation Fee', 'income', true),
        ('Rent', 'expense', true),
        ('Utilities', 'expense', true),
        ('Salaries', 'expense', true),
        ('Transport', 'expense', true),
        ('Maintenance', 'expense', true),
        ('Marketing', 'expense', true),
        ('Insurance', 'expense', true),
        ('Office Supplies', 'expense', true)
        ON CONFLICT (name) DO NOTHING;
    """)
