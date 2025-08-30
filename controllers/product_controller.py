from flask import Blueprint, request, jsonify # type: ignore
from dbf_database import DBFDatabase, DBFTable
import json

with open("config.json") as f:
    config = json.load(f)

db_path = config["DB_PATH"]

# Inisialisasi database
db = DBFDatabase(db_path)
db.register_table("tbl_vendor", "S_VENDOR.G8A")
db.register_table("tbl_product", "S_STOCK.G8A")

# Model Product untuk mapping
class Product:
    def __init__(self, id, sku, nama, harga, vendor, stok):
        self.id = id
        self.sku = sku
        self.nama = nama
        self.harga = harga
        self.vendor = vendor
        self.stok = stok

    def to_dict(self):
        return {
            "id": self.id,
            "sku": self.sku,
            "nama": self.nama,
            "harga": self.harga,
            "vendor_id": self.vendor,
            "stok": self.stok
        }


# Blueprint Flask
product_bp = Blueprint("products", __name__)


@product_bp.route("/products", methods=["GET"])
def get_products():
    """
    GET /products?page=1&limit=10&sort_by=NAMA&sort_order=desc&filter_by[NAMA]=ABC
    """
    page = int(request.args.get("page", 1))
    limit = int(request.args.get("limit", 10))
    sort_by = request.args.get("sort_by")
    sort_order = request.args.get("sort_order", "asc")

    # Filter pakai query string: ?filter_by[FIELD]=VALUE
    filter_by = {}
    for key, value in request.args.items():
        if key.startswith("filter_by[") and key.endswith("]"):
            field = key[len("filter_by["):-1]
            filter_by[field] = value

    # Ambil data produk
    products = db.table("tbl_product").select(
        columns=["ID", "KODE_C", "NAMA", "HARGA_J_01", "VENDOR", "SISA_STK01"],
        limit=limit,
        offset=page
    )

    product_count = db.table("tbl_product").count()

    # Filter manual
    if filter_by:
        for key, value in filter_by.items():
            products = [
                product for product in products
                if str(product.get(key, "")).upper() == str(value).upper()
            ]

    # Sort manual
    if sort_by:
        reverse = (sort_order.lower() == "desc")
        products.sort(key=lambda x: x.get(sort_by), reverse=reverse)

    # Mapping ke model product
    products = [
        Product(
            id=product["ID"],
            sku=product["KODE_C"],
            nama=product["NAMA"],
            harga=product["HARGA_J_01"],
            vendor=product["VENDOR"],
            stok=product["SISA_STK01"]
        ).to_dict()
        for product in products
    ]

    return jsonify({
        "data": products,
        "pagination": {
            "page": page,
            "limit": limit,
            "total_records": product_count,
            "total_pages": (product_count + limit - 1) // limit  # ceiling division
        }
    })
