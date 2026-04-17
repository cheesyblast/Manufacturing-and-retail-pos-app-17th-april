"""V1 - Initial schema: core ERP tables."""
VERSION = "001"
DESCRIPTION = "Initial schema - users, suppliers, raw materials, purchase orders, locations, products, inventory, BOM, production, customers, sales, expenses, settings"


def up(cursor):
    cursor.execute("""
    -- Users
    CREATE TABLE IF NOT EXISTS users (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        email VARCHAR(255) UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        name VARCHAR(255) NOT NULL,
        role VARCHAR(50) NOT NULL DEFAULT 'cashier',
        is_active BOOLEAN DEFAULT TRUE,
        created_at TIMESTAMPTZ DEFAULT NOW()
    );

    -- Suppliers
    CREATE TABLE IF NOT EXISTS suppliers (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        name VARCHAR(255) NOT NULL,
        contact_person VARCHAR(255),
        phone VARCHAR(50),
        email VARCHAR(255),
        address TEXT,
        is_active BOOLEAN DEFAULT TRUE,
        created_at TIMESTAMPTZ DEFAULT NOW()
    );

    -- Raw Materials
    CREATE TABLE IF NOT EXISTS raw_materials (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        name VARCHAR(255) NOT NULL,
        sku VARCHAR(100) UNIQUE,
        unit VARCHAR(50) DEFAULT 'kg',
        quantity DECIMAL(12,2) DEFAULT 0,
        unit_cost DECIMAL(12,2) DEFAULT 0,
        reorder_level DECIMAL(12,2) DEFAULT 0,
        supplier_id UUID REFERENCES suppliers(id),
        created_at TIMESTAMPTZ DEFAULT NOW()
    );

    -- Purchase Orders
    CREATE TABLE IF NOT EXISTS purchase_orders (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        po_number VARCHAR(50) UNIQUE NOT NULL,
        supplier_id UUID REFERENCES suppliers(id),
        status VARCHAR(50) DEFAULT 'draft',
        total_amount DECIMAL(12,2) DEFAULT 0,
        notes TEXT,
        order_date TIMESTAMPTZ DEFAULT NOW(),
        received_date TIMESTAMPTZ,
        created_by UUID REFERENCES users(id),
        created_at TIMESTAMPTZ DEFAULT NOW()
    );

    -- Purchase Order Items
    CREATE TABLE IF NOT EXISTS purchase_order_items (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        purchase_order_id UUID REFERENCES purchase_orders(id) ON DELETE CASCADE,
        raw_material_id UUID REFERENCES raw_materials(id),
        raw_material_name VARCHAR(255),
        quantity DECIMAL(12,2) NOT NULL,
        unit_cost DECIMAL(12,2) NOT NULL,
        total_cost DECIMAL(12,2) NOT NULL,
        received_quantity DECIMAL(12,2) DEFAULT 0
    );

    -- Locations
    CREATE TABLE IF NOT EXISTS locations (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        name VARCHAR(255) NOT NULL,
        type VARCHAR(50) NOT NULL,
        address TEXT,
        is_active BOOLEAN DEFAULT TRUE,
        created_at TIMESTAMPTZ DEFAULT NOW()
    );

    -- Products
    CREATE TABLE IF NOT EXISTS products (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        name VARCHAR(255) NOT NULL,
        sku VARCHAR(100) UNIQUE NOT NULL,
        barcode VARCHAR(100) UNIQUE,
        category VARCHAR(100),
        description TEXT,
        unit_price DECIMAL(12,2) NOT NULL,
        cost_price DECIMAL(12,2) DEFAULT 0,
        image_url TEXT,
        is_active BOOLEAN DEFAULT TRUE,
        created_at TIMESTAMPTZ DEFAULT NOW()
    );

    -- Inventory
    CREATE TABLE IF NOT EXISTS inventory (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        product_id UUID REFERENCES products(id),
        location_id UUID REFERENCES locations(id),
        quantity DECIMAL(12,2) DEFAULT 0,
        min_stock_level DECIMAL(12,2) DEFAULT 0,
        updated_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(product_id, location_id)
    );

    -- Stock Transfers
    CREATE TABLE IF NOT EXISTS stock_transfers (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        transfer_number VARCHAR(50) UNIQUE NOT NULL,
        from_location_id UUID REFERENCES locations(id),
        to_location_id UUID REFERENCES locations(id),
        status VARCHAR(50) DEFAULT 'pending',
        notes TEXT,
        created_by UUID REFERENCES users(id),
        created_at TIMESTAMPTZ DEFAULT NOW()
    );

    -- Stock Transfer Items
    CREATE TABLE IF NOT EXISTS stock_transfer_items (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        transfer_id UUID REFERENCES stock_transfers(id) ON DELETE CASCADE,
        product_id UUID REFERENCES products(id),
        product_name VARCHAR(255),
        quantity DECIMAL(12,2) NOT NULL
    );

    -- Bill of Materials
    CREATE TABLE IF NOT EXISTS bill_of_materials (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        product_id UUID REFERENCES products(id),
        name VARCHAR(255) NOT NULL,
        description TEXT,
        output_quantity DECIMAL(12,2) DEFAULT 1,
        created_at TIMESTAMPTZ DEFAULT NOW()
    );

    -- BOM Items
    CREATE TABLE IF NOT EXISTS bom_items (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        bom_id UUID REFERENCES bill_of_materials(id) ON DELETE CASCADE,
        raw_material_id UUID REFERENCES raw_materials(id),
        raw_material_name VARCHAR(255),
        quantity DECIMAL(12,2) NOT NULL,
        unit VARCHAR(50) DEFAULT 'kg'
    );

    -- Production Orders
    CREATE TABLE IF NOT EXISTS production_orders (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        order_number VARCHAR(50) UNIQUE NOT NULL,
        bom_id UUID REFERENCES bill_of_materials(id),
        product_id UUID REFERENCES products(id),
        product_name VARCHAR(255),
        quantity_planned DECIMAL(12,2) NOT NULL,
        quantity_produced DECIMAL(12,2) DEFAULT 0,
        status VARCHAR(50) DEFAULT 'planned',
        start_date TIMESTAMPTZ,
        end_date TIMESTAMPTZ,
        location_id UUID REFERENCES locations(id),
        notes TEXT,
        created_by UUID REFERENCES users(id),
        created_at TIMESTAMPTZ DEFAULT NOW()
    );

    -- Production Logs
    CREATE TABLE IF NOT EXISTS production_logs (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        production_order_id UUID REFERENCES production_orders(id) ON DELETE CASCADE,
        logged_by UUID REFERENCES users(id),
        logged_by_name VARCHAR(255),
        quantity_produced DECIMAL(12,2) NOT NULL,
        notes TEXT,
        logged_at TIMESTAMPTZ DEFAULT NOW()
    );

    -- Customers
    CREATE TABLE IF NOT EXISTS customers (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        name VARCHAR(255) NOT NULL,
        mobile VARCHAR(50) UNIQUE NOT NULL,
        email VARCHAR(255),
        loyalty_points INT DEFAULT 0,
        total_purchases DECIMAL(12,2) DEFAULT 0,
        created_at TIMESTAMPTZ DEFAULT NOW()
    );

    -- Sales
    CREATE TABLE IF NOT EXISTS sales (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        invoice_number VARCHAR(50) UNIQUE NOT NULL,
        customer_id UUID,
        customer_name VARCHAR(255),
        customer_mobile VARCHAR(50),
        location_id UUID REFERENCES locations(id),
        cashier_id UUID REFERENCES users(id),
        cashier_name VARCHAR(255),
        subtotal DECIMAL(12,2) NOT NULL,
        discount_amount DECIMAL(12,2) DEFAULT 0,
        tax_amount DECIMAL(12,2) DEFAULT 0,
        total DECIMAL(12,2) NOT NULL,
        payment_method VARCHAR(50) DEFAULT 'cash',
        payment_status VARCHAR(50) DEFAULT 'paid',
        status VARCHAR(50) DEFAULT 'completed',
        notes TEXT,
        created_at TIMESTAMPTZ DEFAULT NOW()
    );

    -- Sale Items
    CREATE TABLE IF NOT EXISTS sale_items (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        sale_id UUID REFERENCES sales(id) ON DELETE CASCADE,
        product_id UUID REFERENCES products(id),
        product_name VARCHAR(255),
        product_sku VARCHAR(100),
        quantity DECIMAL(12,2) NOT NULL,
        unit_price DECIMAL(12,2) NOT NULL,
        discount DECIMAL(12,2) DEFAULT 0,
        total DECIMAL(12,2) NOT NULL
    );

    -- Payments
    CREATE TABLE IF NOT EXISTS payments (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        sale_id UUID REFERENCES sales(id) ON DELETE CASCADE,
        method VARCHAR(50) NOT NULL,
        amount DECIMAL(12,2) NOT NULL,
        reference VARCHAR(255),
        created_at TIMESTAMPTZ DEFAULT NOW()
    );

    -- Expenses
    CREATE TABLE IF NOT EXISTS expenses (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        category VARCHAR(100) NOT NULL,
        description TEXT,
        amount DECIMAL(12,2) NOT NULL,
        expense_date DATE NOT NULL DEFAULT CURRENT_DATE,
        created_by UUID REFERENCES users(id),
        created_at TIMESTAMPTZ DEFAULT NOW()
    );

    -- App Settings
    CREATE TABLE IF NOT EXISTS app_settings (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        key VARCHAR(255) UNIQUE NOT NULL,
        value TEXT,
        updated_at TIMESTAMPTZ DEFAULT NOW()
    );

    -- Indexes
    CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
    CREATE INDEX IF NOT EXISTS idx_products_sku ON products(sku);
    CREATE INDEX IF NOT EXISTS idx_products_barcode ON products(barcode);
    CREATE INDEX IF NOT EXISTS idx_customers_mobile ON customers(mobile);
    CREATE INDEX IF NOT EXISTS idx_sales_created_at ON sales(created_at);
    CREATE INDEX IF NOT EXISTS idx_sales_location ON sales(location_id);
    CREATE INDEX IF NOT EXISTS idx_inventory_product_location ON inventory(product_id, location_id);
    CREATE INDEX IF NOT EXISTS idx_purchase_orders_status ON purchase_orders(status);
    CREATE INDEX IF NOT EXISTS idx_production_orders_status ON production_orders(status);
    CREATE INDEX IF NOT EXISTS idx_expenses_date ON expenses(expense_date);
    """)

    # RLS policies - use DO block to handle IF NOT EXISTS for policies
    tables = [
        "users", "suppliers", "raw_materials", "purchase_orders",
        "purchase_order_items", "locations", "products", "inventory",
        "stock_transfers", "stock_transfer_items", "bill_of_materials",
        "bom_items", "production_orders", "production_logs", "customers",
        "sales", "sale_items", "payments", "expenses", "app_settings",
    ]
    for table in tables:
        cursor.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY;")
        cursor.execute(f"""
            DO $$ BEGIN
                CREATE POLICY "Allow all for {table}" ON {table}
                    FOR ALL USING (true) WITH CHECK (true);
            EXCEPTION WHEN duplicate_object THEN NULL;
            END $$;
        """)
