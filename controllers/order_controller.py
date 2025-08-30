from flask import Blueprint, request, jsonify # type: ignore
from dbf_database import DBFDatabase, DBFTable
import json

with open("config.json") as f:
    config = json.load(f)

db_path = config["DB_PATH"]

# Inisialisasi database
db = DBFDatabase(db_path)
db.register_table("tbl_vendor", "S_VENDOR.G8A")
db.register_table("tbl_order", "S_PURCOR.G8A")
db.register_table("tbl_order_detail", "S_PURCOD.G8A")


# Model Order untuk mapping
class Order:
    def __init__(self, id, tanggal, tot_harga, input_oleh, vendor, nama=None):
        self.id = id
        self.tanggal = tanggal
        self.tot_harga = tot_harga
        self.input_oleh = input_oleh
        self.vendor = vendor
        self.nama = nama

    def to_dict(self):
        return {
            "id": self.id,
            "tanggal": self.tanggal,
            "total_harga": self.tot_harga,
            "input_oleh": self.input_oleh,
            "vendor_id": self.vendor,
            "vendor_nama": self.nama
        }

class OrderDetail:
    def __init__(self, id, purcor_id, stock_id, jumlah, harga):
        self.id = id
        self.purcor_id = purcor_id
        self.stock_id = stock_id
        self.jumlah = jumlah
        self.harga = harga

    def to_dict(self):
        return {
            "id": self.id,
            "purcor_id": self.purcor_id,
            "stock_id": self.stock_id,
            "jumlah": self.jumlah,
            "harga": self.harga
        }


# Blueprint Flask
order_bp = Blueprint("orders", __name__)


@order_bp.route("/orders", methods=["GET"])
def get_orders():
    """
    GET /orders?page=1&limit=10&sort_by=TANGGAL&sort_order=desc&filter_by[NAMA]=ABC
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

    # Ambil data order
    all_orders = db.table("tbl_order").select(
        columns=["ID", "TANGGAL", "TOT_HARGA", "INPUT_OLEH", "VENDOR"],
        limit=limit,
        offset=(page - 1) * limit
    )

    order_count = db.table("tbl_order").count()

    # Filter manual
    if filter_by:
        for key, value in filter_by.items():
            all_orders = [
                order for order in all_orders
                if str(order.get(key, "")).upper() == str(value).upper()
            ]

    # Sort manual
    if sort_by:
        reverse = (sort_order.lower() == "desc")
        all_orders.sort(key=lambda x: x.get(sort_by), reverse=reverse)

    # Mapping ke model Order
    orders = [
        Order(
            id=o["ID"],
            tanggal=o["TANGGAL"],
            tot_harga=o["TOT_HARGA"],
            input_oleh=o["INPUT_OLEH"],
            vendor=o["VENDOR"]
        ).to_dict()
        for o in all_orders
    ]

    return jsonify({
        "data": orders,
        "pagination": {
            "page": page,
            "limit": limit,
            "total_records": order_count,
            "total_pages": (order_count + limit - 1) // limit  # ceiling division
        }
    })


@order_bp.route("/orders/<id>", methods=["GET"])
def get_order_by_id(id):
    orders = db.table("tbl_order").join(
        columns=["ID", "TANGGAL", "TOT_HARGA", "INPUT_OLEH", "VENDOR", "NAMA"],
        other=db.table("tbl_vendor"),
        left_key="VENDOR",
        right_key="ID",
        where={"ID": id},
        limit=1
    )

    if not orders:
        return jsonify({"error": "Order not found"}), 404

    order_row = orders[0]
    order = Order(
        id=order_row["ID"],
        tanggal=order_row["TANGGAL"],
        tot_harga=order_row["TOT_HARGA"],
        input_oleh=order_row["INPUT_OLEH"],
        vendor=order_row["VENDOR"],
        nama=order_row["NAMA"]
    ).to_dict()

    details_raw = db.table("tbl_order_detail").select(
        where={"PURCOR_ID": id},
        columns=["ID", "PURCOR_ID", "STOCK_ID", "JUMLAH", "HARGA"]
    )

    details = [
        OrderDetail(
            id=d["ID"],
            purcor_id=d["PURCOR_ID"],
            stock_id=d["STOCK_ID"],
            jumlah=d["JUMLAH"],
            harga=d["HARGA"]
        ).to_dict()
        for d in details_raw
    ]

    return jsonify({
        "data": order,
        "details": details
    })
