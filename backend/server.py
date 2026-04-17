from dotenv import load_dotenv
from pathlib import Path
load_dotenv(Path(__file__).parent / '.env')

from fastapi import FastAPI, APIRouter, Request, HTTPException, Depends, Response, File, UploadFile
from starlette.middleware.cors import CORSMiddleware
import os
import logging
import uuid
import base64
import csv
import io
from datetime import datetime, timezone, date, timedelta
from pydantic import BaseModel, Field
from typing import List, Optional
from database import supabase
from auth import (
    hash_password, verify_password, create_access_token,
    get_current_user, require_role, decode_token
)

# Simple in-memory cache
_cache = {}
_cache_ttl = {}
CACHE_DURATION = 30  # seconds

def get_cached(key):
    if key in _cache and _cache_ttl.get(key, 0) > datetime.now(timezone.utc).timestamp():
        return _cache[key]
    return None

def set_cached(key, value):
    _cache[key] = value
    _cache_ttl[key] = datetime.now(timezone.utc).timestamp() + CACHE_DURATION

def invalidate_cache(prefix=""):
    keys_to_del = [k for k in _cache if k.startswith(prefix)] if prefix else list(_cache.keys())
    for k in keys_to_del:
        _cache.pop(k, None)
        _cache_ttl.pop(k, None)

app = FastAPI(title="ERP Manufacturing & Retail")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

api = APIRouter(prefix="/api")

# ===================== PYDANTIC MODELS =====================

class LoginRequest(BaseModel):
    email: str
    password: str

class RegisterRequest(BaseModel):
    email: str
    password: str
    name: str
    role: str = "cashier"

class SupplierCreate(BaseModel):
    name: str
    contact_person: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    address: Optional[str] = None

class RawMaterialCreate(BaseModel):
    name: str
    sku: Optional[str] = None
    unit: str = "kg"
    quantity: float = 0
    unit_cost: float = 0
    reorder_level: float = 0
    supplier_id: Optional[str] = None

class PurchaseOrderCreate(BaseModel):
    supplier_id: str
    items: List[dict]
    notes: Optional[str] = None

class LocationCreate(BaseModel):
    name: str
    type: str
    address: Optional[str] = None

class ProductCreate(BaseModel):
    name: str
    sku: str
    barcode: Optional[str] = None
    category: Optional[str] = None
    description: Optional[str] = None
    unit_price: float
    cost_price: float = 0
    image_url: Optional[str] = None

class InventoryUpdate(BaseModel):
    product_id: str
    location_id: str
    quantity: float
    min_stock_level: float = 0

class StockTransferCreate(BaseModel):
    from_location_id: str
    to_location_id: str
    items: List[dict]
    notes: Optional[str] = None

class BOMCreate(BaseModel):
    product_id: str
    name: str
    description: Optional[str] = None
    output_quantity: float = 1
    items: List[dict]

class ProductionOrderCreate(BaseModel):
    bom_id: str
    product_id: str
    quantity_planned: float
    location_id: Optional[str] = None
    notes: Optional[str] = None

class ProductionLogCreate(BaseModel):
    quantity_produced: float
    notes: Optional[str] = None

class CustomerCreate(BaseModel):
    name: str
    mobile: str
    email: Optional[str] = None

class SaleCreate(BaseModel):
    customer_id: Optional[str] = None
    customer_name: Optional[str] = None
    customer_mobile: Optional[str] = None
    location_id: Optional[str] = None
    items: List[dict]
    discount_amount: float = 0
    tax_amount: float = 0
    payment_method: str = "cash"
    notes: Optional[str] = None

class ExpenseCreate(BaseModel):
    category: str
    description: Optional[str] = None
    amount: float
    expense_date: Optional[str] = None

class SettingUpdate(BaseModel):
    key: str
    value: str

class ManualTransactionCreate(BaseModel):
    type: str  # income or expense
    category: str
    description: Optional[str] = None
    amount: float
    transaction_date: Optional[str] = None
    reference: Optional[str] = None

class TransactionCategoryCreate(BaseModel):
    name: str
    type: str  # income or expense

class CustomOrderCreate(BaseModel):
    customer_id: Optional[str] = None
    customer_name: Optional[str] = None
    customer_mobile: Optional[str] = None
    description: Optional[str] = None
    total_amount: float
    items: List[dict] = []
    advance_payment: float = 0
    payment_method: str = "cash"
    estimated_date: Optional[str] = None
    notes: Optional[str] = None

class CustomOrderPaymentCreate(BaseModel):
    amount: float
    payment_method: str = "cash"
    payment_type: str = "balance"
    reference: Optional[str] = None

# ===================== AUTH ROUTES =====================

@api.post("/auth/login")
async def login(req: LoginRequest, response: Response):
    try:
        result = supabase.table("users").select("*").eq("email", req.email.lower()).execute()
        if not result.data:
            raise HTTPException(status_code=401, detail="Invalid credentials")
        user = result.data[0]
        if not user.get("is_active", True):
            raise HTTPException(status_code=401, detail="Account disabled")
        if not verify_password(req.password, user["password_hash"]):
            raise HTTPException(status_code=401, detail="Invalid credentials")
        token = create_access_token(user["id"], user["email"], user["role"])
        response.set_cookie(key="access_token", value=token, httponly=True, secure=False, samesite="lax", max_age=86400, path="/")
        return {"token": token, "user": {"id": user["id"], "email": user["email"], "name": user["name"], "role": user["role"]}}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Login error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@api.post("/auth/register")
async def register(req: RegisterRequest, response: Response):
    try:
        existing = supabase.table("users").select("id").eq("email", req.email.lower()).execute()
        if existing.data:
            raise HTTPException(status_code=400, detail="Email already registered")
        user_data = {
            "email": req.email.lower(),
            "password_hash": hash_password(req.password),
            "name": req.name,
            "role": req.role,
            "is_active": True
        }
        result = supabase.table("users").insert(user_data).execute()
        user = result.data[0]
        token = create_access_token(user["id"], user["email"], user["role"])
        response.set_cookie(key="access_token", value=token, httponly=True, secure=False, samesite="lax", max_age=86400, path="/")
        return {"token": token, "user": {"id": user["id"], "email": user["email"], "name": user["name"], "role": user["role"]}}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Register error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@api.get("/auth/me")
async def get_me(request: Request):
    user_payload = await get_current_user(request)
    result = supabase.table("users").select("id, email, name, role, is_active").eq("id", user_payload["sub"]).execute()
    if not result.data:
        raise HTTPException(status_code=404, detail="User not found")
    return result.data[0]

@api.post("/auth/logout")
async def logout(response: Response):
    response.delete_cookie("access_token", path="/")
    return {"message": "Logged out"}

# ===================== USERS ROUTES =====================

@api.get("/users")
async def list_users(request: Request):
    await require_role("admin")(request)
    result = supabase.table("users").select("id, email, name, role, is_active, created_at").order("created_at", desc=True).execute()
    return result.data

@api.post("/users")
async def create_user(req: RegisterRequest, request: Request):
    await require_role("admin")(request)
    existing = supabase.table("users").select("id").eq("email", req.email.lower()).execute()
    if existing.data:
        raise HTTPException(status_code=400, detail="Email already exists")
    user_data = {"email": req.email.lower(), "password_hash": hash_password(req.password), "name": req.name, "role": req.role, "is_active": True}
    result = supabase.table("users").insert(user_data).execute()
    u = result.data[0]
    return {"id": u["id"], "email": u["email"], "name": u["name"], "role": u["role"]}

@api.put("/users/{user_id}")
async def update_user(user_id: str, request: Request):
    await require_role("admin")(request)
    body = await request.json()
    update_data = {}
    for k in ["name", "role", "is_active", "email"]:
        if k in body:
            update_data[k] = body[k]
    if "password" in body and body["password"]:
        update_data["password_hash"] = hash_password(body["password"])
    result = supabase.table("users").update(update_data).eq("id", user_id).execute()
    if not result.data:
        raise HTTPException(status_code=404, detail="User not found")
    u = result.data[0]
    return {"id": u["id"], "email": u["email"], "name": u["name"], "role": u["role"]}

@api.delete("/users/{user_id}")
async def delete_user(user_id: str, request: Request):
    await require_role("admin")(request)
    supabase.table("users").delete().eq("id", user_id).execute()
    return {"message": "User deleted"}

# ===================== SUPPLIERS ROUTES =====================

@api.get("/suppliers")
async def list_suppliers(request: Request):
    await get_current_user(request)
    result = supabase.table("suppliers").select("*").eq("is_active", True).order("name").execute()
    return result.data

@api.post("/suppliers")
async def create_supplier(req: SupplierCreate, request: Request):
    await get_current_user(request)
    data = req.model_dump(exclude_none=True)
    data["is_active"] = True
    result = supabase.table("suppliers").insert(data).execute()
    return result.data[0]

@api.put("/suppliers/{sid}")
async def update_supplier(sid: str, request: Request):
    await get_current_user(request)
    body = await request.json()
    result = supabase.table("suppliers").update(body).eq("id", sid).execute()
    if not result.data:
        raise HTTPException(status_code=404, detail="Supplier not found")
    return result.data[0]

@api.delete("/suppliers/{sid}")
async def delete_supplier(sid: str, request: Request):
    await get_current_user(request)
    supabase.table("suppliers").update({"is_active": False}).eq("id", sid).execute()
    return {"message": "Supplier deactivated"}

# ===================== RAW MATERIALS ROUTES =====================

@api.get("/raw-materials")
async def list_raw_materials(request: Request):
    await get_current_user(request)
    result = supabase.table("raw_materials").select("*, suppliers(name)").order("name").execute()
    return result.data

@api.post("/raw-materials")
async def create_raw_material(req: RawMaterialCreate, request: Request):
    await get_current_user(request)
    data = req.model_dump(exclude_none=True)
    if not data.get("sku"):
        data["sku"] = f"RM-{str(uuid.uuid4())[:8].upper()}"
    result = supabase.table("raw_materials").insert(data).execute()
    return result.data[0]

@api.put("/raw-materials/{rmid}")
async def update_raw_material(rmid: str, request: Request):
    await get_current_user(request)
    body = await request.json()
    result = supabase.table("raw_materials").update(body).eq("id", rmid).execute()
    return result.data[0] if result.data else {}

# ===================== PURCHASE ORDERS ROUTES =====================

@api.get("/purchase-orders")
async def list_purchase_orders(request: Request):
    await get_current_user(request)
    result = supabase.table("purchase_orders").select("*, suppliers(name)").order("created_at", desc=True).execute()
    return result.data

@api.get("/purchase-orders/{po_id}")
async def get_purchase_order(po_id: str, request: Request):
    await get_current_user(request)
    po = supabase.table("purchase_orders").select("*, suppliers(name)").eq("id", po_id).execute()
    if not po.data:
        raise HTTPException(status_code=404, detail="PO not found")
    items = supabase.table("purchase_order_items").select("*").eq("purchase_order_id", po_id).execute()
    return {**po.data[0], "items": items.data}

@api.post("/purchase-orders")
async def create_purchase_order(req: PurchaseOrderCreate, request: Request):
    user = await get_current_user(request)
    po_number = f"PO-{datetime.now(timezone.utc).strftime('%Y%m%d')}-{str(uuid.uuid4())[:4].upper()}"
    total = sum(item.get("quantity", 0) * item.get("unit_cost", 0) for item in req.items)
    po_data = {"po_number": po_number, "supplier_id": req.supplier_id, "status": "draft", "total_amount": total, "notes": req.notes, "created_by": user["sub"]}
    po_result = supabase.table("purchase_orders").insert(po_data).execute()
    po = po_result.data[0]
    for item in req.items:
        item_data = {
            "purchase_order_id": po["id"],
            "raw_material_id": item.get("raw_material_id"),
            "raw_material_name": item.get("raw_material_name", ""),
            "quantity": item["quantity"],
            "unit_cost": item["unit_cost"],
            "total_cost": item["quantity"] * item["unit_cost"]
        }
        supabase.table("purchase_order_items").insert(item_data).execute()
    return po

@api.put("/purchase-orders/{po_id}")
async def update_purchase_order(po_id: str, request: Request):
    await get_current_user(request)
    body = await request.json()
    update_fields = {k: v for k, v in body.items() if k in ["status", "notes"]}
    result = supabase.table("purchase_orders").update(update_fields).eq("id", po_id).execute()
    return result.data[0] if result.data else {}

@api.post("/purchase-orders/{po_id}/receive")
async def receive_purchase_order(po_id: str, request: Request):
    await get_current_user(request)
    items = supabase.table("purchase_order_items").select("*").eq("purchase_order_id", po_id).execute()
    for item in items.data:
        rm_id = item.get("raw_material_id")
        if rm_id:
            rm = supabase.table("raw_materials").select("quantity").eq("id", rm_id).execute()
            if rm.data:
                new_qty = float(rm.data[0]["quantity"]) + float(item["quantity"])
                supabase.table("raw_materials").update({"quantity": new_qty}).eq("id", rm_id).execute()
            supabase.table("purchase_order_items").update({"received_quantity": item["quantity"]}).eq("id", item["id"]).execute()
    supabase.table("purchase_orders").update({"status": "received", "received_date": datetime.now(timezone.utc).isoformat()}).eq("id", po_id).execute()
    return {"message": "Purchase order received and materials updated"}

# ===================== LOCATIONS ROUTES =====================

@api.get("/locations")
async def list_locations(request: Request):
    await get_current_user(request)
    result = supabase.table("locations").select("*").eq("is_active", True).order("name").execute()
    return result.data

@api.post("/locations")
async def create_location(req: LocationCreate, request: Request):
    await get_current_user(request)
    data = req.model_dump()
    data["is_active"] = True
    result = supabase.table("locations").insert(data).execute()
    return result.data[0]

@api.put("/locations/{lid}")
async def update_location(lid: str, request: Request):
    await get_current_user(request)
    body = await request.json()
    result = supabase.table("locations").update(body).eq("id", lid).execute()
    return result.data[0] if result.data else {}

# ===================== PRODUCTS ROUTES =====================

@api.get("/products")
async def list_products(request: Request, search: Optional[str] = None, category: Optional[str] = None, limit: int = 100, offset: int = 0):
    await get_current_user(request)
    cache_key = f"products:{search}:{category}:{limit}:{offset}"
    cached = get_cached(cache_key)
    if cached:
        return cached
    query = supabase.table("products").select("*", count="exact").eq("is_active", True)
    if category:
        query = query.eq("category", category)
    if search:
        query = query.or_(f"name.ilike.%{search}%,sku.ilike.%{search}%,barcode.ilike.%{search}%")
    result = query.order("name").range(offset, offset + limit - 1).execute()
    response = {"data": result.data, "total": result.count or len(result.data)}
    set_cached(cache_key, response)
    return response

@api.get("/products/template-csv")
async def product_csv_template():
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["name", "sku", "barcode", "category", "unit_price", "cost_price", "description"])
    writer.writerow(["Cotton Shirt", "CS-001", "BC100001", "Shirts", "2500.00", "1200.00", "Premium cotton shirt"])
    return Response(content=output.getvalue(), media_type="text/csv", headers={"Content-Disposition": "attachment; filename=products_template.csv"})

@api.post("/products/bulk-import")
async def bulk_import_products(request: Request, file: UploadFile = File(...)):
    await get_current_user(request)
    contents = await file.read()
    reader = csv.DictReader(io.StringIO(contents.decode("utf-8")))
    results = {"created": 0, "skipped": 0, "errors": []}
    for i, row in enumerate(reader):
        try:
            if not row.get("name") or not row.get("sku"):
                results["errors"].append(f"Row {i+2}: name and sku required")
                continue
            existing = supabase.table("products").select("id").eq("sku", row["sku"]).execute()
            if existing.data:
                results["skipped"] += 1
                results["errors"].append(f"Row {i+2}: SKU '{row['sku']}' already exists")
                continue
            data = {
                "name": row["name"], "sku": row["sku"],
                "barcode": row.get("barcode") or f"BC{str(uuid.uuid4().int)[:12]}",
                "category": row.get("category", ""),
                "unit_price": float(row.get("unit_price", 0)),
                "cost_price": float(row.get("cost_price", 0)),
                "description": row.get("description", ""),
                "is_active": True
            }
            supabase.table("products").insert(data).execute()
            results["created"] += 1
        except Exception as e:
            results["errors"].append(f"Row {i+2}: {str(e)[:80]}")
    invalidate_cache("products")
    return results

@api.get("/products/barcode/{barcode}")
async def get_product_by_barcode(barcode: str, request: Request):
    await get_current_user(request)
    result = supabase.table("products").select("*").eq("barcode", barcode).eq("is_active", True).execute()
    if not result.data:
        raise HTTPException(status_code=404, detail="Product not found")
    return result.data[0]

@api.get("/products/{pid}")
async def get_product(pid: str, request: Request):
    await get_current_user(request)
    result = supabase.table("products").select("*").eq("id", pid).execute()
    if not result.data:
        raise HTTPException(status_code=404, detail="Product not found")
    return result.data[0]

@api.post("/products")
async def create_product(req: ProductCreate, request: Request):
    await get_current_user(request)
    data = req.model_dump(exclude_none=True)
    data["is_active"] = True
    if not data.get("barcode"):
        data["barcode"] = f"BC{str(uuid.uuid4().int)[:12]}"
    result = supabase.table("products").insert(data).execute()
    invalidate_cache("products")
    return result.data[0]

@api.put("/products/{pid}")
async def update_product(pid: str, request: Request):
    await get_current_user(request)
    body = await request.json()
    result = supabase.table("products").update(body).eq("id", pid).execute()
    return result.data[0] if result.data else {}

@api.delete("/products/{pid}")
async def delete_product(pid: str, request: Request):
    await get_current_user(request)
    supabase.table("products").update({"is_active": False}).eq("id", pid).execute()
    return {"message": "Product deactivated"}

# ===================== INVENTORY ROUTES =====================

@api.get("/inventory")
async def list_inventory(request: Request, location_id: Optional[str] = None, limit: int = 200, offset: int = 0):
    await get_current_user(request)
    query = supabase.table("inventory").select("*, products(name, sku, barcode, unit_price, cost_price, category), locations(name, type)", count="exact")
    if location_id:
        query = query.eq("location_id", location_id)
    result = query.range(offset, offset + limit - 1).execute()
    return {"data": result.data, "total": result.count or len(result.data)}

@api.post("/inventory")
async def upsert_inventory(req: InventoryUpdate, request: Request):
    await get_current_user(request)
    existing = supabase.table("inventory").select("id, quantity").eq("product_id", req.product_id).eq("location_id", req.location_id).execute()
    if existing.data:
        result = supabase.table("inventory").update({"quantity": req.quantity, "min_stock_level": req.min_stock_level, "updated_at": datetime.now(timezone.utc).isoformat()}).eq("id", existing.data[0]["id"]).execute()
    else:
        data = {"product_id": req.product_id, "location_id": req.location_id, "quantity": req.quantity, "min_stock_level": req.min_stock_level}
        result = supabase.table("inventory").insert(data).execute()
    return result.data[0] if result.data else {}

@api.post("/inventory/transfer")
async def create_stock_transfer(req: StockTransferCreate, request: Request):
    user = await get_current_user(request)
    transfer_number = f"ST-{datetime.now(timezone.utc).strftime('%Y%m%d')}-{str(uuid.uuid4())[:4].upper()}"
    transfer_data = {"transfer_number": transfer_number, "from_location_id": req.from_location_id, "to_location_id": req.to_location_id, "status": "completed", "notes": req.notes, "created_by": user["sub"]}
    transfer_result = supabase.table("stock_transfers").insert(transfer_data).execute()
    transfer = transfer_result.data[0]
    for item in req.items:
        supabase.table("stock_transfer_items").insert({"transfer_id": transfer["id"], "product_id": item["product_id"], "product_name": item.get("product_name", ""), "quantity": item["quantity"]}).execute()
        from_inv = supabase.table("inventory").select("id, quantity").eq("product_id", item["product_id"]).eq("location_id", req.from_location_id).execute()
        if from_inv.data:
            new_from_qty = max(0, float(from_inv.data[0]["quantity"]) - float(item["quantity"]))
            supabase.table("inventory").update({"quantity": new_from_qty, "updated_at": datetime.now(timezone.utc).isoformat()}).eq("id", from_inv.data[0]["id"]).execute()
        to_inv = supabase.table("inventory").select("id, quantity").eq("product_id", item["product_id"]).eq("location_id", req.to_location_id).execute()
        if to_inv.data:
            new_to_qty = float(to_inv.data[0]["quantity"]) + float(item["quantity"])
            supabase.table("inventory").update({"quantity": new_to_qty, "updated_at": datetime.now(timezone.utc).isoformat()}).eq("id", to_inv.data[0]["id"]).execute()
        else:
            supabase.table("inventory").insert({"product_id": item["product_id"], "location_id": req.to_location_id, "quantity": item["quantity"]}).execute()
    return transfer

@api.get("/stock-transfers")
async def list_stock_transfers(request: Request):
    await get_current_user(request)
    result = supabase.table("stock_transfers").select("*, from_location:locations!stock_transfers_from_location_id_fkey(name), to_location:locations!stock_transfers_to_location_id_fkey(name)").order("created_at", desc=True).execute()
    return result.data

@api.get("/inventory/template-csv")
async def inventory_csv_template(request: Request):
    await get_current_user(request)
    products = supabase.table("products").select("sku, name").eq("is_active", True).limit(5).execute()
    locations = supabase.table("locations").select("name").eq("is_active", True).limit(5).execute()
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["product_sku", "location_name", "quantity", "min_stock_level"])
    if products.data and locations.data:
        writer.writerow([products.data[0]["sku"], locations.data[0]["name"], "100", "10"])
    else:
        writer.writerow(["CS-001", "Main Outlet", "100", "10"])
    return Response(content=output.getvalue(), media_type="text/csv", headers={"Content-Disposition": "attachment; filename=inventory_template.csv"})

@api.post("/inventory/bulk-import")
async def bulk_import_inventory(request: Request, file: UploadFile = File(...)):
    await get_current_user(request)
    contents = await file.read()
    reader = csv.DictReader(io.StringIO(contents.decode("utf-8")))
    results = {"updated": 0, "created": 0, "errors": []}
    for i, row in enumerate(reader):
        try:
            product = supabase.table("products").select("id").eq("sku", row.get("product_sku", "")).execute()
            if not product.data:
                results["errors"].append(f"Row {i+2}: Product SKU '{row.get('product_sku')}' not found")
                continue
            location = supabase.table("locations").select("id").eq("name", row.get("location_name", "")).execute()
            if not location.data:
                results["errors"].append(f"Row {i+2}: Location '{row.get('location_name')}' not found")
                continue
            pid, lid = product.data[0]["id"], location.data[0]["id"]
            qty = float(row.get("quantity", 0))
            min_stock = float(row.get("min_stock_level", 0))
            existing = supabase.table("inventory").select("id").eq("product_id", pid).eq("location_id", lid).execute()
            if existing.data:
                supabase.table("inventory").update({"quantity": qty, "min_stock_level": min_stock, "updated_at": datetime.now(timezone.utc).isoformat()}).eq("id", existing.data[0]["id"]).execute()
                results["updated"] += 1
            else:
                supabase.table("inventory").insert({"product_id": pid, "location_id": lid, "quantity": qty, "min_stock_level": min_stock}).execute()
                results["created"] += 1
        except Exception as e:
            results["errors"].append(f"Row {i+2}: {str(e)[:80]}")
    invalidate_cache("inventory")
    return results

# ===================== BOM ROUTES =====================

@api.get("/bom")
async def list_bom(request: Request):
    await get_current_user(request)
    result = supabase.table("bill_of_materials").select("*, products(name, sku)").order("name").execute()
    return result.data

@api.get("/bom/{bom_id}")
async def get_bom(bom_id: str, request: Request):
    await get_current_user(request)
    bom = supabase.table("bill_of_materials").select("*, products(name, sku)").eq("id", bom_id).execute()
    if not bom.data:
        raise HTTPException(status_code=404, detail="BOM not found")
    items = supabase.table("bom_items").select("*").eq("bom_id", bom_id).execute()
    return {**bom.data[0], "items": items.data}

@api.post("/bom")
async def create_bom(req: BOMCreate, request: Request):
    await get_current_user(request)
    bom_data = {"product_id": req.product_id, "name": req.name, "description": req.description, "output_quantity": req.output_quantity}
    bom_result = supabase.table("bill_of_materials").insert(bom_data).execute()
    bom = bom_result.data[0]
    for item in req.items:
        supabase.table("bom_items").insert({"bom_id": bom["id"], "raw_material_id": item["raw_material_id"], "raw_material_name": item.get("raw_material_name", ""), "quantity": item["quantity"], "unit": item.get("unit", "kg")}).execute()
    return bom

@api.put("/bom/{bom_id}")
async def update_bom(bom_id: str, request: Request):
    await get_current_user(request)
    body = await request.json()
    update_fields = {k: v for k, v in body.items() if k in ["name", "description", "output_quantity", "product_id"]}
    if update_fields:
        supabase.table("bill_of_materials").update(update_fields).eq("id", bom_id).execute()
    if "items" in body:
        supabase.table("bom_items").delete().eq("bom_id", bom_id).execute()
        for item in body["items"]:
            supabase.table("bom_items").insert({"bom_id": bom_id, "raw_material_id": item["raw_material_id"], "raw_material_name": item.get("raw_material_name", ""), "quantity": item["quantity"], "unit": item.get("unit", "kg")}).execute()
    return {"message": "BOM updated"}

# ===================== PRODUCTION ORDERS ROUTES =====================

@api.get("/production-orders")
async def list_production_orders(request: Request):
    await get_current_user(request)
    result = supabase.table("production_orders").select("*").order("created_at", desc=True).execute()
    return result.data

@api.get("/production-orders/{po_id}")
async def get_production_order(po_id: str, request: Request):
    await get_current_user(request)
    po = supabase.table("production_orders").select("*").eq("id", po_id).execute()
    if not po.data:
        raise HTTPException(status_code=404, detail="Production order not found")
    logs = supabase.table("production_logs").select("*").eq("production_order_id", po_id).order("logged_at", desc=True).execute()
    return {**po.data[0], "logs": logs.data}

@api.post("/production-orders")
async def create_production_order(req: ProductionOrderCreate, request: Request):
    user = await get_current_user(request)
    order_number = f"PRD-{datetime.now(timezone.utc).strftime('%Y%m%d')}-{str(uuid.uuid4())[:4].upper()}"
    product = supabase.table("products").select("name").eq("id", req.product_id).execute()
    product_name = product.data[0]["name"] if product.data else ""
    data = {
        "order_number": order_number, "bom_id": req.bom_id, "product_id": req.product_id,
        "product_name": product_name, "quantity_planned": req.quantity_planned,
        "status": "planned", "location_id": req.location_id, "notes": req.notes, "created_by": user["sub"]
    }
    result = supabase.table("production_orders").insert(data).execute()
    return result.data[0]

@api.put("/production-orders/{po_id}")
async def update_production_order(po_id: str, request: Request):
    await get_current_user(request)
    body = await request.json()
    update_fields = {k: v for k, v in body.items() if k in ["status", "notes", "start_date", "end_date"]}
    result = supabase.table("production_orders").update(update_fields).eq("id", po_id).execute()
    return result.data[0] if result.data else {}

@api.post("/production-orders/{po_id}/log")
async def log_production(po_id: str, req: ProductionLogCreate, request: Request):
    user = await get_current_user(request)
    log_data = {"production_order_id": po_id, "logged_by": user["sub"], "logged_by_name": user.get("email", ""), "quantity_produced": req.quantity_produced, "notes": req.notes}
    supabase.table("production_logs").insert(log_data).execute()
    po = supabase.table("production_orders").select("quantity_produced, quantity_planned, product_id, location_id").eq("id", po_id).execute()
    if po.data:
        new_total = float(po.data[0]["quantity_produced"] or 0) + req.quantity_produced
        update_data = {"quantity_produced": new_total}
        if new_total >= float(po.data[0]["quantity_planned"]):
            update_data["status"] = "completed"
            update_data["end_date"] = datetime.now(timezone.utc).isoformat()
        elif float(po.data[0].get("quantity_produced", 0)) == 0:
            update_data["status"] = "in_progress"
            update_data["start_date"] = datetime.now(timezone.utc).isoformat()
        supabase.table("production_orders").update(update_data).eq("id", po_id).execute()
        product_id = po.data[0]["product_id"]
        location_id = po.data[0].get("location_id")
        if product_id and location_id:
            inv = supabase.table("inventory").select("id, quantity").eq("product_id", product_id).eq("location_id", location_id).execute()
            if inv.data:
                supabase.table("inventory").update({"quantity": float(inv.data[0]["quantity"]) + req.quantity_produced, "updated_at": datetime.now(timezone.utc).isoformat()}).eq("id", inv.data[0]["id"]).execute()
            else:
                supabase.table("inventory").insert({"product_id": product_id, "location_id": location_id, "quantity": req.quantity_produced}).execute()
    return {"message": "Production logged", "quantity_produced": req.quantity_produced}

# ===================== CUSTOMERS ROUTES =====================

@api.get("/customers")
async def list_customers(request: Request, search: Optional[str] = None):
    await get_current_user(request)
    result = supabase.table("customers").select("*").order("name").execute()
    data = result.data
    if search:
        s = search.lower()
        data = [c for c in data if s in c["name"].lower() or s in (c.get("mobile") or "")]
    return data

@api.get("/customers/mobile/{mobile}")
async def get_customer_by_mobile(mobile: str, request: Request):
    await get_current_user(request)
    result = supabase.table("customers").select("*").eq("mobile", mobile).execute()
    if not result.data:
        raise HTTPException(status_code=404, detail="Customer not found")
    return result.data[0]

@api.post("/customers")
async def create_customer(req: CustomerCreate, request: Request):
    await get_current_user(request)
    data = req.model_dump(exclude_none=True)
    result = supabase.table("customers").insert(data).execute()
    return result.data[0]

@api.put("/customers/{cid}")
async def update_customer(cid: str, request: Request):
    await get_current_user(request)
    body = await request.json()
    result = supabase.table("customers").update(body).eq("id", cid).execute()
    return result.data[0] if result.data else {}

# ===================== SALES / POS ROUTES =====================

@api.post("/sales")
async def create_sale(req: SaleCreate, request: Request):
    user = await get_current_user(request)
    invoice_number = f"INV-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{str(uuid.uuid4())[:4].upper()}"
    subtotal = sum(float(item.get("quantity", 0)) * float(item.get("unit_price", 0)) for item in req.items)
    total = subtotal - req.discount_amount + req.tax_amount
    sale_data = {
        "invoice_number": invoice_number,
        "customer_id": req.customer_id,
        "customer_name": req.customer_name,
        "customer_mobile": req.customer_mobile,
        "location_id": req.location_id,
        "cashier_id": user["sub"],
        "cashier_name": user.get("email", ""),
        "subtotal": subtotal,
        "discount_amount": req.discount_amount,
        "tax_amount": req.tax_amount,
        "total": total,
        "payment_method": req.payment_method,
        "payment_status": "paid",
        "status": "completed",
        "notes": req.notes
    }
    sale_result = supabase.table("sales").insert(sale_data).execute()
    sale = sale_result.data[0]
    for item in req.items:
        item_total = float(item["quantity"]) * float(item["unit_price"]) - float(item.get("discount", 0))
        sale_item = {
            "sale_id": sale["id"],
            "product_id": item["product_id"],
            "product_name": item.get("product_name", ""),
            "product_sku": item.get("product_sku", ""),
            "quantity": item["quantity"],
            "unit_price": item["unit_price"],
            "discount": item.get("discount", 0),
            "total": item_total
        }
        supabase.table("sale_items").insert(sale_item).execute()
        if req.location_id:
            inv = supabase.table("inventory").select("id, quantity").eq("product_id", item["product_id"]).eq("location_id", req.location_id).execute()
            if inv.data:
                new_qty = max(0, float(inv.data[0]["quantity"]) - float(item["quantity"]))
                supabase.table("inventory").update({"quantity": new_qty, "updated_at": datetime.now(timezone.utc).isoformat()}).eq("id", inv.data[0]["id"]).execute()
    supabase.table("payments").insert({"sale_id": sale["id"], "method": req.payment_method, "amount": total}).execute()
    if req.customer_id:
        cust = supabase.table("customers").select("total_purchases, loyalty_points").eq("id", req.customer_id).execute()
        if cust.data:
            supabase.table("customers").update({
                "total_purchases": float(cust.data[0].get("total_purchases", 0)) + total,
                "loyalty_points": int(cust.data[0].get("loyalty_points", 0)) + int(total // 100)
            }).eq("id", req.customer_id).execute()
    return sale

@api.get("/sales")
async def list_sales(request: Request, start_date: Optional[str] = None, end_date: Optional[str] = None, location_id: Optional[str] = None, limit: int = 100, offset: int = 0):
    await get_current_user(request)
    query = supabase.table("sales").select("*", count="exact")
    if start_date:
        query = query.gte("created_at", start_date)
    if end_date:
        query = query.lte("created_at", end_date + "T23:59:59")
    if location_id:
        query = query.eq("location_id", location_id)
    result = query.order("created_at", desc=True).range(offset, offset + limit - 1).execute()
    return {"data": result.data, "total": result.count or len(result.data)}

@api.get("/sales/{sale_id}")
async def get_sale(sale_id: str, request: Request):
    await get_current_user(request)
    sale = supabase.table("sales").select("*").eq("id", sale_id).execute()
    if not sale.data:
        raise HTTPException(status_code=404, detail="Sale not found")
    items = supabase.table("sale_items").select("*").eq("sale_id", sale_id).execute()
    payments = supabase.table("payments").select("*").eq("sale_id", sale_id).execute()
    return {**sale.data[0], "items": items.data, "payments": payments.data}

@api.get("/sales/{sale_id}/receipt")
async def get_receipt(sale_id: str, request: Request):
    await get_current_user(request)
    sale = supabase.table("sales").select("*").eq("id", sale_id).execute()
    if not sale.data:
        raise HTTPException(status_code=404, detail="Sale not found")
    items = supabase.table("sale_items").select("*").eq("sale_id", sale_id).execute()
    settings = supabase.table("app_settings").select("key, value").execute()
    settings_dict = {s["key"]: s["value"] for s in settings.data} if settings.data else {}
    return {
        "sale": sale.data[0],
        "items": items.data,
        "business_name": settings_dict.get("business_name", "TextileERP"),
        "business_address": settings_dict.get("business_address", ""),
        "business_phone": settings_dict.get("business_phone", ""),
        "business_logo": settings_dict.get("logo_url", "")
    }

# ===================== EXPENSES ROUTES =====================

@api.get("/expenses")
async def list_expenses(request: Request, start_date: Optional[str] = None, end_date: Optional[str] = None):
    await get_current_user(request)
    query = supabase.table("expenses").select("*")
    if start_date:
        query = query.gte("expense_date", start_date)
    if end_date:
        query = query.lte("expense_date", end_date)
    result = query.order("expense_date", desc=True).execute()
    return result.data

@api.post("/expenses")
async def create_expense(req: ExpenseCreate, request: Request):
    user = await get_current_user(request)
    data = {"category": req.category, "description": req.description, "amount": req.amount, "expense_date": req.expense_date or date.today().isoformat(), "created_by": user["sub"]}
    result = supabase.table("expenses").insert(data).execute()
    return result.data[0]

# ===================== ACCOUNTING ROUTES =====================

@api.get("/accounting/daily-sales")
async def daily_sales_report(request: Request, report_date: Optional[str] = None):
    await get_current_user(request)
    target_date = report_date or date.today().isoformat()
    start = f"{target_date}T00:00:00"
    end = f"{target_date}T23:59:59"
    # POS Sales
    sales = supabase.table("sales").select("*").gte("created_at", start).lte("created_at", end).eq("status", "completed").execute()
    total_revenue = sum(float(s["total"]) for s in sales.data)
    total_discount = sum(float(s.get("discount_amount", 0)) for s in sales.data)
    total_tax = sum(float(s.get("tax_amount", 0)) for s in sales.data)
    transaction_count = len(sales.data)
    payment_breakdown = {}
    for s in sales.data:
        method = s.get("payment_method", "cash")
        payment_breakdown[method] = payment_breakdown.get(method, 0) + float(s["total"])
    # Custom order payments received today
    co_payments = supabase.table("custom_order_payments").select("amount, payment_method").gte("created_at", start).lte("created_at", end).execute()
    co_total = sum(float(p["amount"]) for p in co_payments.data)
    for p in co_payments.data:
        method = p.get("payment_method", "cash")
        payment_breakdown[method] = payment_breakdown.get(method, 0) + float(p["amount"])
    # Manual income
    manual_income = supabase.table("manual_transactions").select("amount").eq("type", "income").eq("transaction_date", target_date).execute()
    manual_income_total = sum(float(m["amount"]) for m in manual_income.data)
    # COGS
    cogs = 0
    for s in sales.data:
        items = supabase.table("sale_items").select("product_id, quantity").eq("sale_id", s["id"]).execute()
        for item in items.data:
            product = supabase.table("products").select("cost_price").eq("id", item["product_id"]).execute()
            if product.data:
                cogs += float(product.data[0].get("cost_price", 0)) * float(item["quantity"])
    combined_revenue = total_revenue + co_total + manual_income_total
    return {
        "date": target_date,
        "pos_revenue": round(total_revenue, 2),
        "custom_order_payments": round(co_total, 2),
        "manual_income": round(manual_income_total, 2),
        "total_revenue": round(combined_revenue, 2),
        "total_discount": round(total_discount, 2),
        "total_tax": round(total_tax, 2),
        "cogs": round(cogs, 2),
        "gross_profit": round(combined_revenue - cogs, 2),
        "transaction_count": transaction_count,
        "payment_breakdown": payment_breakdown,
        "sales": sales.data
    }

@api.get("/accounting/income-statement")
async def income_statement(request: Request, start_date: Optional[str] = None, end_date: Optional[str] = None):
    await get_current_user(request)
    if not start_date:
        start_date = date.today().replace(day=1).isoformat()
    if not end_date:
        end_date = date.today().isoformat()
    # POS Sales
    sales = supabase.table("sales").select("id, total, discount_amount, tax_amount").gte("created_at", start_date + "T00:00:00").lte("created_at", end_date + "T23:59:59").eq("status", "completed").execute()
    pos_revenue = sum(float(s["total"]) for s in sales.data)
    # Manual income
    manual_inc = supabase.table("manual_transactions").select("category, amount").eq("type", "income").gte("transaction_date", start_date).lte("transaction_date", end_date).execute()
    manual_income_total = sum(float(m["amount"]) for m in manual_inc.data)
    manual_income_by_cat = {}
    for m in manual_inc.data:
        manual_income_by_cat[m["category"]] = manual_income_by_cat.get(m["category"], 0) + float(m["amount"])
    # Custom order payments (only for delivered orders = earned revenue, plus advance = unearned)
    co_payments = supabase.table("custom_order_payments").select("amount").gte("created_at", start_date + "T00:00:00").lte("created_at", end_date + "T23:59:59").execute()
    co_revenue = sum(float(p["amount"]) for p in co_payments.data)
    revenue = pos_revenue + manual_income_total + co_revenue
    # COGS
    cogs = 0
    for s in sales.data:
        items = supabase.table("sale_items").select("product_id, quantity").eq("sale_id", s["id"]).execute()
        for item in items.data:
            product = supabase.table("products").select("cost_price").eq("id", item["product_id"]).execute()
            if product.data:
                cogs += float(product.data[0].get("cost_price", 0)) * float(item["quantity"])
    # Manual expenses
    manual_exp = supabase.table("manual_transactions").select("category, amount").eq("type", "expense").gte("transaction_date", start_date).lte("transaction_date", end_date).execute()
    manual_expense_total = sum(float(e["amount"]) for e in manual_exp.data)
    # Legacy expenses table
    expenses = supabase.table("expenses").select("category, amount").gte("expense_date", start_date).lte("expense_date", end_date).execute()
    legacy_expense_total = sum(float(e["amount"]) for e in expenses.data)
    total_expenses = manual_expense_total + legacy_expense_total
    expense_by_category = {}
    for e in manual_exp.data:
        expense_by_category[e["category"]] = expense_by_category.get(e["category"], 0) + float(e["amount"])
    for e in expenses.data:
        expense_by_category[e["category"]] = expense_by_category.get(e["category"], 0) + float(e["amount"])
    gross_profit = revenue - cogs
    net_income = gross_profit - total_expenses
    return {
        "period": {"start": start_date, "end": end_date},
        "revenue_breakdown": {
            "pos_sales": round(pos_revenue, 2),
            "custom_orders": round(co_revenue, 2),
            "manual_income": round(manual_income_total, 2),
            "manual_income_by_category": {k: round(v, 2) for k, v in manual_income_by_cat.items()}
        },
        "revenue": round(revenue, 2),
        "cogs": round(cogs, 2),
        "gross_profit": round(gross_profit, 2),
        "operating_expenses": round(total_expenses, 2),
        "expense_breakdown": {k: round(v, 2) for k, v in expense_by_category.items()},
        "net_income": round(net_income, 2),
        "gross_margin": round((gross_profit / revenue * 100) if revenue > 0 else 0, 2),
        "net_margin": round((net_income / revenue * 100) if revenue > 0 else 0, 2)
    }

@api.get("/accounting/balance-sheet")
async def balance_sheet(request: Request):
    await get_current_user(request)
    inventory_result = supabase.table("inventory").select("quantity, products(cost_price)").execute()
    inventory_value = sum(float(i["quantity"]) * float(i["products"]["cost_price"]) for i in inventory_result.data if i.get("products"))
    rm_result = supabase.table("raw_materials").select("quantity, unit_cost").execute()
    rm_value = sum(float(r["quantity"]) * float(r["unit_cost"]) for r in rm_result.data)
    sales_total = supabase.table("sales").select("total").eq("status", "completed").execute()
    total_revenue = sum(float(s["total"]) for s in sales_total.data)
    # Manual income
    manual_inc = supabase.table("manual_transactions").select("amount").eq("type", "income").execute()
    total_manual_income = sum(float(m["amount"]) for m in manual_inc.data)
    # Custom order payments received
    co_payments = supabase.table("custom_order_payments").select("amount").execute()
    total_co_payments = sum(float(p["amount"]) for p in co_payments.data)
    # Unearned revenue (advance payments for non-delivered orders)
    undelivered = supabase.table("custom_orders").select("amount_paid").in_("status", ["order_taken", "in_progress", "ready_for_pickup"]).execute()
    unearned_revenue = sum(float(o["amount_paid"]) for o in undelivered.data)
    po_total = supabase.table("purchase_orders").select("total_amount").eq("status", "received").execute()
    total_purchases = sum(float(p["total_amount"]) for p in po_total.data)
    expenses_total = supabase.table("expenses").select("amount").execute()
    total_expenses = sum(float(e["amount"]) for e in expenses_total.data)
    manual_exp = supabase.table("manual_transactions").select("amount").eq("type", "expense").execute()
    total_manual_expenses = sum(float(e["amount"]) for e in manual_exp.data)
    all_expenses = total_expenses + total_manual_expenses
    all_income = total_revenue + total_manual_income + total_co_payments
    cash_balance = all_income - total_purchases - all_expenses
    total_assets = cash_balance + inventory_value + rm_value
    return {
        "assets": {
            "cash": round(cash_balance, 2),
            "inventory": round(inventory_value, 2),
            "raw_materials": round(rm_value, 2),
            "total_assets": round(total_assets, 2)
        },
        "liabilities": {
            "unearned_revenue": round(unearned_revenue, 2),
            "total_liabilities": round(unearned_revenue, 2)
        },
        "equity": {
            "retained_earnings": round(total_assets - unearned_revenue, 2),
            "total_equity": round(total_assets - unearned_revenue, 2)
        },
        "summary": {
            "total_revenue": round(all_income, 2),
            "total_purchases": round(total_purchases, 2),
            "total_expenses": round(all_expenses, 2)
        }
    }

# ===================== DASHBOARD ROUTES =====================

@api.get("/dashboard/stats")
async def dashboard_stats(request: Request):
    await get_current_user(request)
    today = date.today().isoformat()
    today_start = f"{today}T00:00:00"
    today_end = f"{today}T23:59:59"
    today_sales = supabase.table("sales").select("total").gte("created_at", today_start).lte("created_at", today_end).eq("status", "completed").execute()
    today_revenue = sum(float(s["total"]) for s in today_sales.data)
    products_count = supabase.table("products").select("id", count="exact").eq("is_active", True).execute()
    low_stock = supabase.table("inventory").select("id", count="exact").lt("quantity", 10).execute()
    pending_orders = supabase.table("production_orders").select("id", count="exact").in_("status", ["planned", "in_progress"]).execute()
    pending_po = supabase.table("purchase_orders").select("id", count="exact").in_("status", ["draft", "ordered"]).execute()
    active_custom = supabase.table("custom_orders").select("id", count="exact").in_("status", ["order_taken", "in_progress", "ready_for_pickup"]).execute()
    return {
        "today_revenue": round(today_revenue, 2),
        "today_transactions": len(today_sales.data),
        "total_products": products_count.count or 0,
        "low_stock_items": low_stock.count or 0,
        "pending_production": pending_orders.count or 0,
        "pending_purchases": pending_po.count or 0,
        "active_custom_orders": active_custom.count or 0
    }

# ===================== SETTINGS ROUTES =====================

@api.get("/settings")
async def get_settings(request: Request):
    await get_current_user(request)
    result = supabase.table("app_settings").select("key, value").execute()
    return {s["key"]: s["value"] for s in result.data} if result.data else {}

@api.put("/settings")
async def update_setting(req: SettingUpdate, request: Request):
    await require_role("admin")(request)
    existing = supabase.table("app_settings").select("id").eq("key", req.key).execute()
    if existing.data:
        supabase.table("app_settings").update({"value": req.value, "updated_at": datetime.now(timezone.utc).isoformat()}).eq("key", req.key).execute()
    else:
        supabase.table("app_settings").insert({"key": req.key, "value": req.value}).execute()
    return {"message": "Setting updated"}

# ===================== MANUAL TRANSACTIONS =====================

@api.get("/manual-transactions")
async def list_manual_transactions(request: Request, type: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None, limit: int = 100, offset: int = 0):
    await get_current_user(request)
    query = supabase.table("manual_transactions").select("*", count="exact")
    if type:
        query = query.eq("type", type)
    if start_date:
        query = query.gte("transaction_date", start_date)
    if end_date:
        query = query.lte("transaction_date", end_date)
    result = query.order("transaction_date", desc=True).range(offset, offset + limit - 1).execute()
    return {"data": result.data, "total": result.count or len(result.data)}

@api.post("/manual-transactions")
async def create_manual_transaction(req: ManualTransactionCreate, request: Request):
    user = await get_current_user(request)
    data = {
        "type": req.type, "category": req.category, "description": req.description,
        "amount": req.amount, "transaction_date": req.transaction_date or date.today().isoformat(),
        "reference": req.reference, "created_by": user["sub"]
    }
    result = supabase.table("manual_transactions").insert(data).execute()
    return result.data[0]

@api.delete("/manual-transactions/{tid}")
async def delete_manual_transaction(tid: str, request: Request):
    await get_current_user(request)
    supabase.table("manual_transactions").delete().eq("id", tid).execute()
    return {"message": "Deleted"}

# ===================== TRANSACTION CATEGORIES =====================

@api.get("/transaction-categories")
async def list_transaction_categories(request: Request, type: Optional[str] = None):
    await get_current_user(request)
    query = supabase.table("transaction_categories").select("*")
    if type:
        query = query.eq("type", type)
    result = query.order("name").execute()
    return result.data

@api.post("/transaction-categories")
async def create_transaction_category(req: TransactionCategoryCreate, request: Request):
    await get_current_user(request)
    data = {"name": req.name, "type": req.type, "is_default": False}
    result = supabase.table("transaction_categories").insert(data).execute()
    return result.data[0]

@api.delete("/transaction-categories/{cid}")
async def delete_transaction_category(cid: str, request: Request):
    await get_current_user(request)
    supabase.table("transaction_categories").delete().eq("id", cid).execute()
    return {"message": "Deleted"}

# ===================== LOGO UPLOAD =====================

@api.post("/upload/logo")
async def upload_logo(request: Request, file: UploadFile = File(...)):
    await require_role("admin")(request)
    contents = await file.read()
    if len(contents) > 500000:
        raise HTTPException(status_code=400, detail="Logo must be under 500KB")
    b64 = base64.b64encode(contents).decode("utf-8")
    content_type = file.content_type or "image/png"
    data_url = f"data:{content_type};base64,{b64}"
    existing = supabase.table("app_settings").select("id").eq("key", "logo_url").execute()
    if existing.data:
        supabase.table("app_settings").update({"value": data_url, "updated_at": datetime.now(timezone.utc).isoformat()}).eq("key", "logo_url").execute()
    else:
        supabase.table("app_settings").insert({"key": "logo_url", "value": data_url}).execute()
    invalidate_cache("settings")
    return {"logo_url": data_url}

# ===================== CUSTOM ORDERS =====================

@api.get("/custom-orders")
async def list_custom_orders(request: Request, status: Optional[str] = None, limit: int = 100, offset: int = 0):
    await get_current_user(request)
    query = supabase.table("custom_orders").select("*", count="exact")
    if status:
        query = query.eq("status", status)
    result = query.order("created_at", desc=True).range(offset, offset + limit - 1).execute()
    return {"data": result.data, "total": result.count or len(result.data)}

@api.get("/custom-orders/{order_id}")
async def get_custom_order(order_id: str, request: Request):
    await get_current_user(request)
    order = supabase.table("custom_orders").select("*").eq("id", order_id).execute()
    if not order.data:
        raise HTTPException(status_code=404, detail="Order not found")
    items = supabase.table("custom_order_items").select("*").eq("custom_order_id", order_id).execute()
    payments = supabase.table("custom_order_payments").select("*").eq("custom_order_id", order_id).order("created_at", desc=True).execute()
    return {**order.data[0], "items": items.data, "payments": payments.data}

@api.post("/custom-orders")
async def create_custom_order(req: CustomOrderCreate, request: Request):
    user = await get_current_user(request)
    order_number = f"CO-{datetime.now(timezone.utc).strftime('%Y%m%d')}-{str(uuid.uuid4())[:4].upper()}"
    balance = req.total_amount - req.advance_payment
    order_data = {
        "order_number": order_number,
        "customer_id": req.customer_id, "customer_name": req.customer_name,
        "customer_mobile": req.customer_mobile, "description": req.description,
        "total_amount": req.total_amount, "amount_paid": req.advance_payment,
        "balance_due": balance, "status": "order_taken",
        "estimated_date": req.estimated_date, "notes": req.notes,
        "created_by": user["sub"]
    }
    result = supabase.table("custom_orders").insert(order_data).execute()
    order = result.data[0]
    for item in req.items:
        supabase.table("custom_order_items").insert({
            "custom_order_id": order["id"], "item_type": item.get("item_type", "service"),
            "product_id": item.get("product_id"), "product_name": item.get("product_name", ""),
            "description": item.get("description", ""), "quantity": float(item.get("quantity", 1)),
            "unit_price": float(item.get("unit_price", 0)),
            "total": float(item.get("quantity", 1)) * float(item.get("unit_price", 0))
        }).execute()
    if req.advance_payment > 0:
        supabase.table("custom_order_payments").insert({
            "custom_order_id": order["id"], "amount": req.advance_payment,
            "payment_method": req.payment_method, "payment_type": "advance"
        }).execute()
    return order

@api.put("/custom-orders/{order_id}/status")
async def update_custom_order_status(order_id: str, request: Request):
    user = await get_current_user(request)
    body = await request.json()
    new_status = body.get("status")
    if new_status not in ["order_taken", "in_progress", "ready_for_pickup", "delivered", "cancelled"]:
        raise HTTPException(status_code=400, detail="Invalid status")
    update_data = {"status": new_status}
    if new_status == "delivered":
        update_data["delivery_date"] = datetime.now(timezone.utc).isoformat()
    supabase.table("custom_orders").update(update_data).eq("id", order_id).execute()
    # Send SMS notification if ready_for_pickup
    if new_status == "ready_for_pickup":
        order = supabase.table("custom_orders").select("customer_mobile, customer_name, order_number").eq("id", order_id).execute()
        if order.data and order.data[0].get("customer_mobile"):
            settings = supabase.table("app_settings").select("key, value").in_("key", ["sms_api_key", "sms_sender_id", "business_name"]).execute()
            settings_dict = {s["key"]: s["value"] for s in settings.data} if settings.data else {}
            if settings_dict.get("sms_api_key"):
                logger.info(f"SMS notification: Order {order.data[0]['order_number']} ready for pickup - {order.data[0]['customer_mobile']}")
                # TODO: Integrate notify.lk/WhatsApp API when keys are configured
    return {"message": f"Status updated to {new_status}"}

@api.post("/custom-orders/{order_id}/payment")
async def add_custom_order_payment(order_id: str, req: CustomOrderPaymentCreate, request: Request):
    await get_current_user(request)
    order = supabase.table("custom_orders").select("amount_paid, balance_due, total_amount, status").eq("id", order_id).execute()
    if not order.data:
        raise HTTPException(status_code=404, detail="Order not found")
    o = order.data[0]
    new_paid = float(o["amount_paid"]) + req.amount
    new_balance = float(o["total_amount"]) - new_paid
    supabase.table("custom_order_payments").insert({
        "custom_order_id": order_id, "amount": req.amount,
        "payment_method": req.payment_method, "payment_type": req.payment_type,
        "reference": req.reference
    }).execute()
    supabase.table("custom_orders").update({"amount_paid": new_paid, "balance_due": max(0, new_balance)}).eq("id", order_id).execute()
    return {"message": "Payment recorded", "amount_paid": new_paid, "balance_due": max(0, new_balance)}

# ===================== SETUP / HEALTH =====================

@api.get("/health")
async def health_check():
    try:
        supabase.table("users").select("id", count="exact").limit(1).execute()
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "database": "disconnected", "error": str(e)}

@api.get("/setup/check")
async def check_setup():
    tables = ["users", "suppliers", "raw_materials", "purchase_orders", "locations", "products", "inventory", "bill_of_materials", "production_orders", "customers", "sales", "expenses", "app_settings", "manual_transactions", "transaction_categories", "custom_orders", "custom_order_items", "custom_order_payments"]
    status = {}
    for table in tables:
        try:
            result = supabase.table(table).select("id", count="exact").limit(1).execute()
            status[table] = "ok"
        except Exception as e:
            error_str = str(e)
            if "PGRST205" in error_str or "schema cache" in error_str or "404" in error_str:
                status[table] = "missing"
            else:
                status[table] = "error"
            logger.info(f"Table check {table}: {error_str[:100]}")
    all_ok = all(v == "ok" for v in status.values())
    return {"all_tables_ready": all_ok, "tables": status}

@api.get("/setup/migration-sql")
async def get_migration_sql():
    sql_path = Path(__file__).parent / "migration.sql"
    if sql_path.exists():
        return {"sql": sql_path.read_text()}
    return {"sql": "Migration file not found"}

# ===================== SEED ADMIN =====================

@app.on_event("startup")
async def startup():
    logger.info("Starting ERP application...")
    # Run database migrations
    try:
        from migrations import run_migrations
        success = run_migrations()
        if success:
            logger.info("Database migrations completed successfully.")
        else:
            logger.warning("Some migrations failed. Check logs for details.")
    except Exception as e:
        logger.warning(f"Migration runner error (non-fatal): {e}")
    # Seed admin user
    try:
        admin_email = os.environ.get("ADMIN_EMAIL", "admin@erp.com")
        admin_password = os.environ.get("ADMIN_PASSWORD", "admin123")
        existing = supabase.table("users").select("id, password_hash").eq("email", admin_email).execute()
        if not existing.data:
            supabase.table("users").insert({
                "email": admin_email,
                "password_hash": hash_password(admin_password),
                "name": "Administrator",
                "role": "admin",
                "is_active": True
            }).execute()
            logger.info(f"Admin user created: {admin_email}")
        else:
            if not verify_password(admin_password, existing.data[0]["password_hash"]):
                supabase.table("users").update({"password_hash": hash_password(admin_password)}).eq("id", existing.data[0]["id"]).execute()
                logger.info("Admin password updated")
        # Write test credentials
        cred_dir = Path("/app/memory")
        cred_dir.mkdir(exist_ok=True)
        (cred_dir / "test_credentials.md").write_text(
            f"# Test Credentials\n\n"
            f"## Admin\n- Email: {admin_email}\n- Password: {admin_password}\n- Role: admin\n\n"
            f"## Auth Endpoints\n- POST /api/auth/login\n- POST /api/auth/register\n- GET /api/auth/me\n- POST /api/auth/logout\n"
        )
    except Exception as e:
        logger.warning(f"Startup seed failed (tables may not exist yet): {e}")

app.include_router(api)
