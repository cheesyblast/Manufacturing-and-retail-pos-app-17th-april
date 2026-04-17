"""V3 - Phase 1: Location linkage on all entities + dynamic product attributes."""
VERSION = "003"
DESCRIPTION = "Phase 1 - location_id on users/expenses/manual_transactions, product attributes & variants"


def up(cursor):
    # ===== Add location_id to existing tables =====
    cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS location_id UUID REFERENCES locations(id);")
    cursor.execute("ALTER TABLE expenses ADD COLUMN IF NOT EXISTS location_id UUID REFERENCES locations(id);")
    cursor.execute("ALTER TABLE manual_transactions ADD COLUMN IF NOT EXISTS location_id UUID REFERENCES locations(id);")

    # ===== Product Attributes (dynamic: Color, Batch, Size, etc.) =====
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS product_attributes (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            name VARCHAR(100) UNIQUE NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
    """)

    # ===== Product Variants (specific combinations per product) =====
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS product_variants (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            product_id UUID NOT NULL REFERENCES products(id) ON DELETE CASCADE,
            variant_sku VARCHAR(100) UNIQUE,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
    """)

    # ===== Variant attribute values =====
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS product_variant_attributes (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            variant_id UUID NOT NULL REFERENCES product_variants(id) ON DELETE CASCADE,
            attribute_id UUID NOT NULL REFERENCES product_attributes(id),
            value VARCHAR(255) NOT NULL,
            UNIQUE(variant_id, attribute_id)
        );
    """)

    # ===== Add variant_id to inventory, sale_items, purchase_order_items =====
    cursor.execute("ALTER TABLE inventory ADD COLUMN IF NOT EXISTS variant_id UUID REFERENCES product_variants(id);")
    cursor.execute("ALTER TABLE sale_items ADD COLUMN IF NOT EXISTS variant_id UUID REFERENCES product_variants(id);")
    cursor.execute("ALTER TABLE purchase_order_items ADD COLUMN IF NOT EXISTS variant_id UUID REFERENCES product_variants(id);")

    # Drop and recreate unique constraint on inventory to include variant_id
    cursor.execute("""
        DO $$ BEGIN
            ALTER TABLE inventory DROP CONSTRAINT IF EXISTS inventory_product_id_location_id_key;
        EXCEPTION WHEN undefined_object THEN NULL;
        END $$;
    """)
    cursor.execute("""
        DO $$ BEGIN
            CREATE UNIQUE INDEX IF NOT EXISTS idx_inventory_product_location_variant
                ON inventory(product_id, location_id, COALESCE(variant_id, '00000000-0000-0000-0000-000000000000'));
        EXCEPTION WHEN duplicate_table THEN NULL;
        END $$;
    """)

    # Indexes
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_users_location ON users(location_id);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_expenses_location ON expenses(location_id);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_manual_transactions_location ON manual_transactions(location_id);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_product_variants_product ON product_variants(product_id);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_variant_attrs_variant ON product_variant_attributes(variant_id);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_inventory_variant ON inventory(variant_id);")

    # RLS for new tables
    new_tables = ["product_attributes", "product_variants", "product_variant_attributes"]
    for table in new_tables:
        cursor.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY;")
        cursor.execute(f"""
            DO $$ BEGIN
                CREATE POLICY "Allow all for {table}" ON {table}
                    FOR ALL USING (true) WITH CHECK (true);
            EXCEPTION WHEN duplicate_object THEN NULL;
            END $$;
        """)

    # ===== Seed default locations (idempotent) =====
    cursor.execute("""
        INSERT INTO locations (name, type, address, is_active) VALUES
        ('Main Factory', 'factory', 'Production Facility', true),
        ('Central Warehouse', 'warehouse', 'Storage Facility', true),
        ('Flagship Outlet', 'outlet', 'Retail Location', true)
        ON CONFLICT DO NOTHING;
    """)

    # Seed default product attributes
    cursor.execute("""
        INSERT INTO product_attributes (name) VALUES
        ('Color'),
        ('Batch')
        ON CONFLICT (name) DO NOTHING;
    """)
