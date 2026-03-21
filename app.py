import os, json, logging, requests, mysql.connector
from datetime import datetime
from flask import Flask, request, jsonify
from dotenv import load_dotenv

load_dotenv()
app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "caboose.proxy.rlwy.net"),
    "port":     int(os.getenv("DB_PORT", 40540)),
    "database": os.getenv("DB_NAME", "railway"),
    "user":     os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", "FhvVYldnOznZDRshCVtRDNelhEMmUMIQ"),
}

BSALE_TOKEN = "4128ca5461bb908cc3fc93a75dd8addcc843dce0"
API = "https://api.bsale.io/v1"
H = {"access_token": BSALE_TOKEN, "Content-Type": "application/json"}

def safe(v): return str(v).strip() if v else ""
def ts(v): return datetime.fromtimestamp(v) if v else None

def get_db(): return mysql.connector.connect(**DB_CONFIG)

def run(cursor, sql, args=()):
    try:
        cursor.execute(sql, args)
    except Exception as e:
        logger.error(f"SQL Error: {e} -> {sql}")

# ─────────────────────────────────────────
# PROCESADORES (WEBHOOKS)
# ─────────────────────────────────────────

def process_product(pid: int):
    """Procesa un producto o variante"""
    r = requests.get(f"{API}/products/{pid}.json", headers=H)
    if r.status_code != 200: return
    p = r.json()

    clasif_map = {0: "Producto", 1: "Servicio", 2: "Pack", 3: "Pack"}
    clasificacion = clasif_map.get(p.get("classification", 0), "Producto")

    tipo = ""
    if p.get("productTypeId"):
        rt = requests.get(f"{API}/product_types/{p['productTypeId']}.json", headers=H)
        if rt.status_code == 200: tipo = safe(rt.json().get("name", ""))

    marca = ""
    if p.get("brandId"):
        rb = requests.get(f"{API}/brands/{p['brandId']}.json", headers=H)
        if rb.status_code == 200: marca = safe(rb.json().get("name", ""))

    estado_prod = "Activo" if p.get("state") == 0 else "Inactivo"

    rv = requests.get(f"{API}/products/{pid}/variants.json", headers=H)
    variantes = rv.json().get("items", []) if rv.status_code == 200 else []

    conn = get_db()
    try:
        cursor = conn.cursor()
        for v in variantes:
            vid = v.get("id")
            estado_var = "Activo" if v.get("state") == 0 else "Inactivo"
            sku = safe(v.get("code",""))
            if not sku: continue
            
            # Since Pandas to_sql has no PK, we DELETE before INSERT to act like REPLACE
            run(cursor, "DELETE FROM productos WHERE sku=%s", (sku,))
            
            run(cursor, """
                INSERT INTO productos
                  (sku, nombre_del_producto, clasificacion, tipo_de_producto, estado, variante,
                   codigo_de_barras, marca, estado_variante, fecha_de_creacion)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                sku, safe(p.get("name","")), clasificacion, tipo,
                estado_prod, safe(v.get("description","")), safe(v.get("barcode","")),
                marca, estado_var, safe(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            ))

            # Costos
            run(cursor, "DELETE FROM costos WHERE sku=%s", (sku,))
            avg_cost, last_cost = 0.0, 0.0
            r_cost = requests.get(f"{API}/variants/{vid}/costs.json", headers=H)
            if r_cost.status_code == 200:
                cost_data = r_cost.json()
                avg_cost = float(cost_data.get("averageCost") or 0)
                history = cost_data.get("history", [])
                if history:
                    lc = sorted(history, key=lambda x: x.get("admissionDate",0), reverse=True)[0].get("cost")
                    if lc is not None: last_cost = float(lc)

            run(cursor, "INSERT INTO costos (sku, costo_neto_unitario, ultimo_costo) VALUES (%s,%s,%s)", (sku, avg_cost, last_cost))

            # Precios
            run(cursor, "DELETE FROM lista_de_precio WHERE sku=%s", (sku,))
            rp = requests.get(f"{API}/variants/{vid}/prices.json", headers=H)
            if rp.status_code == 200:
                for pr in rp.json().get("items", []):
                    pl = pr.get("priceList", {})
                    pl_name = pl.get("name","") if pl else ""
                    val = float(pr.get("variantValuePrice", 0) or pr.get("price", 0))
                    run(cursor, "INSERT INTO lista_de_precio (lista_de_precio, sku, precio_unitario) VALUES (%s,%s,%s)", (safe(pl_name), sku, val))
        conn.commit()
    finally:
        cursor.close()
        conn.close()

def process_client(cid: int):
    r = requests.get(f"{API}/clients/{cid}.json", headers=H)
    if r.status_code != 200: return
    c = r.json()

    estado = "Activo" if c.get("state") == 0 else "Inactivo"
    ruc = safe(c.get("code",""))
    if not ruc: return
    
    conn = get_db()
    try:
        cursor = conn.cursor()
        run(cursor, "DELETE FROM cliente WHERE ruc=%s", (ruc,))
        run(cursor, """
            INSERT INTO cliente
              (ruc, apellido, correo, direccion_del_cliente, distrito_del_cliente,
               estado, fecha_de_actualizacion, fecha_de_creacion)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            ruc, safe(c.get("lastName","") + " " + c.get("firstName","")), safe(c.get("email","")),
            safe(c.get("address","")), safe(c.get("district","")), estado,
            safe(datetime.now().strftime("%Y-%m-%d %H:%M:%S")), safe(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        ))
        conn.commit()
    finally:
        cursor.close()
        conn.close()

def process_document(did: int):
    r = requests.get(f"{API}/documents/{did}.json", headers=H)
    if r.status_code != 200: return
    doc = r.json()

    estado_map = {0: "VIGENTE", 1: "ANULADO", 2: "NULO"}
    
    tipo_doc = ""
    if doc.get("documentTypeId"):
        rdt = requests.get(f"{API}/document_types/{doc['documentTypeId']}.json", headers=H)
        if rdt.status_code == 200: tipo_doc = safe(rdt.json().get("name",""))

    nro = str(doc.get("number",""))
    partes = nro.split("-") if "-" in nro else ["", nro]
    prefijo = partes[0] if len(partes) > 1 else ""
    num_serie = partes[1] if len(partes) > 1 else nro

    em_ts = doc.get("emissionDate") or doc.get("createdAt")
    fecha_emision = ts(em_ts).date() if ts(em_ts) else None
    fecha_venc = ts(doc.get("expirationDate")).date() if doc.get("expirationDate") else None
    fecha_gen = ts(doc.get("createdAt"))

    sucursal, oid = "", doc.get("officeId")
    if oid:
        ro = requests.get(f"{API}/offices/{oid}.json", headers=H)
        if ro.status_code == 200: sucursal = safe(ro.json().get("name",""))

    vendedor = ""
    if doc.get("userId"):
        ru = requests.get(f"{API}/users/{doc['userId']}.json", headers=H)
        if ru.status_code == 200:
            ud = ru.json()
            vendedor = safe(f"{ud.get('firstName','')} {ud.get('lastName','')}")

    metodo_pago = ""
    rpa = requests.get(f"{API}/documents/{did}/payments.json", headers=H)
    if rpa.status_code == 200:
        pagos = rpa.json().get("items", [])
        if pagos and pagos[0].get("paymentType"):
            metodo_pago = safe(pagos[0]["paymentType"].get("name",""))

    ruc_cli, cli_id = "", None
    if doc.get("client"):
        ruc_cli = safe(doc["client"].get("code",""))
        cli_id = doc["client"].get("id")

    # --- ORQUESTADOR: Sincronizar cliente inmediatamente ---
    if cli_id:
        process_client(cli_id)

    conn = get_db()
    try:
        cursor = conn.cursor()
        run(cursor, "DELETE FROM documentos WHERE numero_de_serie=%s", (num_serie,))
        run(cursor, """
            INSERT INTO documentos
              (tipo_documento, n_documento, prefijo_del_numero_de_serie, numero_de_serie,
               ruc_cliente, fecha_emision, fecha_vencimiento, fecha_de_generacion, sucursal, 
               emisor, vendedor, estado, metodo_de_pago, monto_de_documento)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            tipo_doc, nro, prefijo, num_serie, ruc_cli,
            fecha_emision, fecha_venc, fecha_gen, sucursal, "",
            vendedor, estado_map.get(doc.get("state",0), "VIGENTE"),
            metodo_pago, float(doc.get("totalAmount") or 0)
        ))

        # Detalles (Ventas)
        cursor.execute("DELETE FROM venta WHERE numero_de_serie=%s", (num_serie,))
        rdet = requests.get(f"{API}/documents/{did}/details.json", headers=H)
        variantes_vendidas = set()
        if rdet.status_code == 200:
            for det in rdet.json().get("items", []):
                vid = det.get("variantId")
                sku_det = ""
                if vid:
                    variantes_vendidas.add(vid)
                    rv_det = requests.get(f"{API}/variants/{vid}.json", headers=H)
                    if rv_det.status_code == 200:
                        sku_det = rv_det.json().get("code","")

                fecha_dt = ts(em_ts)
                run(cursor, """
                    INSERT INTO venta
                      (tipo_movimiento, numero_de_serie, fecha_venta, hora_venta, sku, cantidad, venta_total_neta)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                """, (
                    tipo_doc, num_serie, fecha_emision,
                    fecha_dt.strftime("%H:%M:%S") if fecha_dt else None,
                    safe(sku_det), float(det.get("quantity") or 0),
                    float(det.get("totalUnitValue") or 0) * float(det.get("quantity") or 0)
                ))
        conn.commit()
    finally:
        cursor.close()
        conn.close()

    # --- ORQUESTADOR: Descontar stock inmediatamente tras procesar el documento ---
    for vid in variantes_vendidas:
        process_stock_for_variant(vid)

def process_stock_for_variant(vid: int):
    r = requests.get(f"{API}/stocks.json?variantid={vid}", headers=H)
    if r.status_code != 200: return
    stock_items = r.json().get("items", [])
    if not stock_items: return

    conn = get_db()
    try:
        cur = conn.cursor()
        r_vid = requests.get(f"{API}/variants/{vid}.json", headers=H)
        sku = r_vid.json().get("code","") if r_vid.status_code == 200 else ""
        if not sku: return
        
        # Eliminar stock actual de esta variante para reinsertarlo (todas las sucursales)
        run(cur, "DELETE FROM stock WHERE sku=%s", (sku,))
        
        for s_data in stock_items:
            oid = s_data.get("officeId")
            if not oid: continue
            
            oname = ""
            ro = requests.get(f"{API}/offices/{oid}.json", headers=H)
            if ro.status_code == 200: oname = safe(ro.json().get("name",""))
            
            run(cur, """
                INSERT INTO stock
                  (sku, stock, cantidad_por_despachar, cantidad_disponible, por_recibir, sucursal)
                VALUES (%s,%s,%s,%s,%s,%s)
            """, (
                sku, float(s_data.get("quantity") or 0), float(s_data.get("quantityReserved") or 0),
                float(s_data.get("quantityAvailable") or s_data.get("quantity") or 0),
                float(s_data.get("quantityOnOrder") or 0), oname
            ))
        conn.commit()
    finally:
        cur.close()
        conn.close()

# ─────────────────────────────────────────
# ENDPOINTS
# ─────────────────────────────────────────
@app.route("/", methods=["GET"])
def health():
    return jsonify({"status": "ok", "mensaje": "Servidor Bsale→MySQL Excel Schema activo"}), 200

@app.route("/webhook/bsale", methods=["POST"])
def webhook_bsale():
    try:
        payload = request.get_json(force=True)
        topic = payload.get("topic", "")
        res_id = payload.get("resourceId") or payload.get("id")
        if not res_id: return jsonify({"error": "Sin resourceId"}), 400
        res_id = int(res_id)

        if topic in ("document/create", "document.create", "document/update", "document.update", "document/annul", "document.annul", "document"):
            process_document(res_id)
        elif topic in ("product/create", "product.create", "product/update", "product.update", "product"):
            process_product(res_id)
        elif topic in ("client/create", "client.create", "client/update", "client.update", "client"):
            process_client(res_id)
        elif topic in ("stock/update", "stock.update", "stock"):
            r_stock = requests.get(f"{API}/stocks/{res_id}.json", headers=H)
            if r_stock.status_code == 200 and r_stock.json().get("variantId"):
                vid = r_stock.json().get("variantId")
                process_stock_for_variant(vid)
            else:
                process_stock_for_variant(res_id)

        logger.info(f"✅ Webhook procesado: {topic} {res_id}")
        return jsonify({"status": "ok"}), 200
    except Exception as e:
        logger.error(f"❌ Error en webhook: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)), debug=False)
