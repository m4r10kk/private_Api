"""
=============================================================
  BSALE → MYSQL WEBHOOK SERVER (NUEVO SCHEMA 7 TABLAS)
  Servidor Flask que recibe eventos de Bsale en tiempo real
  y los persiste en MySQL (Railway)
=============================================================
"""
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
    "password": os.getenv("DB_PASSWORD", ""),
}

BSALE_TOKEN = os.getenv("BSALE_TOKEN", "")
API = "https://api.bsale.io/v1"
H = {"access_token": BSALE_TOKEN, "Content-Type": "application/json"}

def safe(v, length=255): return str(v or "")[:length]
def ts(v): return datetime.fromtimestamp(v) if v else None

def get_db(): return mysql.connector.connect(**DB_CONFIG)

def run(cursor, sql, args=()):
    cursor.execute(sql, args)

# ─────────────────────────────────────────
# PROCESADORES (WEBHOOKS)
# ─────────────────────────────────────────

def process_product(pid: int):
    """Procesa un producto que ha sido creado o actualizado en Bsale"""
    r = requests.get(f"{API}/products/{pid}.json", headers=H)
    if r.status_code != 200: return
    p = r.json()

    # Clasificacion
    clasif_map = {0: "Producto", 1: "Servicio", 2: "Pack", 3: "Pack"}
    clasificacion = clasif_map.get(p.get("classification", 0), "Producto")

    # Tipo de producto
    tipo = ""
    if p.get("productTypeId"):
        rt = requests.get(f"{API}/product_types/{p['productTypeId']}.json", headers=H)
        if rt.status_code == 200: tipo = safe(rt.json().get("name", ""))

    # Marca (brand)
    marca = ""
    if p.get("brandId"):
        rb = requests.get(f"{API}/brands/{p['brandId']}.json", headers=H)
        if rb.status_code == 200: marca = safe(rb.json().get("name", ""))

    estado_prod = "Activo" if p.get("state") == 0 else "Inactivo"

    # Variantes
    rv = requests.get(f"{API}/products/{pid}/variants.json", headers=H)
    variantes = rv.json().get("items", []) if rv.status_code == 200 else []

    conn = get_db()
    try:
        cursor = conn.cursor()
        for v in variantes:
            vid = v.get("id")
            estado_var = "Activo" if v.get("state") == 0 else "Inactivo"
            sku = safe(v.get("code",""), 100)
            
            run(cursor, """
                INSERT INTO productos
                  (id, sku, nombre, clasificacion, tipo_producto, estado, variante,
                   codigo_barra, marca, estado_variante, fecha_creacion,
                   variante_id, producto_id_bsale)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                  sku=VALUES(sku), nombre=VALUES(nombre),
                  clasificacion=VALUES(clasificacion), tipo_producto=VALUES(tipo_producto),
                  estado=VALUES(estado), variante=VALUES(variante),
                  codigo_barra=VALUES(codigo_barra), marca=VALUES(marca),
                  estado_variante=VALUES(estado_variante),
                  fecha_creacion=VALUES(fecha_creacion)
            """, (
                vid, sku, safe(p.get("name",""), 255), clasificacion[:150], tipo[:150],
                estado_prod, safe(v.get("description",""), 255), safe(v.get("barCode",""), 100),
                marca[:150], estado_var, ts(p.get("createdAt")), vid, pid
            ))

            # Costos (endpoint correcto)
            avg_cost, last_cost = 0.0, 0.0
            r_cost = requests.get(f"{API}/variants/{vid}/costs.json", headers=H)
            if r_cost.status_code == 200:
                cost_data = r_cost.json()
                avg_cost = float(cost_data.get("averageCost") or 0)
                history = cost_data.get("history", [])
                if history:
                    last_cost = float(sorted(history, key=lambda x: x.get("admissionDate",0), reverse=True)[0].get("cost") or 0)

            run(cursor, """
                INSERT INTO costos (sku, variante_id, costo_neto_unitario, ultimo_costo, fecha_actualizacion)
                VALUES (%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                  costo_neto_unitario=VALUES(costo_neto_unitario),
                  ultimo_costo=VALUES(ultimo_costo),
                  fecha_actualizacion=VALUES(fecha_actualizacion)
            """, (sku, vid, avg_cost, last_cost, datetime.now()))

            # Precios
            rp = requests.get(f"{API}/variants/{vid}/prices.json", headers=H)
            if rp.status_code == 200:
                for pr in rp.json().get("items", []):
                    run(cursor, """
                        INSERT INTO listas_precios
                          (lista_precio, lista_precio_id, sku, variante_id, precio_unitario, moneda, fecha_actualizacion)
                        VALUES (%s,%s,%s,%s,%s,'PEN',%s)
                        ON DUPLICATE KEY UPDATE
                          precio_unitario=VALUES(precio_unitario),
                          fecha_actualizacion=VALUES(fecha_actualizacion)
                    """, (
                        safe(pr.get("priceList", {}).get("name","") if pr.get("priceList") else "", 150),
                        pr.get("priceListId"), sku, vid, float(pr.get("variantValuePrice") or 0), datetime.now()
                    ))
        conn.commit()
    finally:
        cursor.close()
        conn.close()

def process_client(cid: int):
    r = requests.get(f"{API}/clients/{cid}.json", headers=H)
    if r.status_code != 200: return
    c = r.json()

    estado = "Activo" if c.get("state") == 0 else "Inactivo"
    tipo = "Empresa" if c.get("company") else "Persona Natural"

    conn = get_db()
    try:
        cursor = conn.cursor()
        run(cursor, """
            INSERT INTO clientes
              (id, ruc, nombre, apellido, distrito, ciudad, direccion,
               correo, razon_social, estado, tipo_cliente,
               fecha_creacion, fecha_actualizacion)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
              ruc=VALUES(ruc), nombre=VALUES(nombre), apellido=VALUES(apellido),
              distrito=VALUES(distrito), ciudad=VALUES(ciudad),
              direccion=VALUES(direccion), correo=VALUES(correo),
              razon_social=VALUES(razon_social), estado=VALUES(estado),
              tipo_cliente=VALUES(tipo_cliente), fecha_actualizacion=VALUES(fecha_actualizacion)
        """, (
            c.get("id"), safe(c.get("code",""), 20), safe(c.get("firstName",""), 255),
            safe(c.get("lastName",""), 255), safe(c.get("district",""), 100),
            safe(c.get("city",""), 100), safe(c.get("address",""), 500),
            safe(c.get("email",""), 255), safe(c.get("company",""), 255),
            estado, tipo, ts(c.get("createdAt")), ts(c.get("updatedAt"))
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
        if rdt.status_code == 200: tipo_doc = safe(rdt.json().get("name",""), 100)

    nro = str(doc.get("number",""))
    partes = nro.split("-") if "-" in nro else ["", nro]
    prefijo = partes[0][:20] if len(partes) > 1 else ""
    num_serie = partes[1][:20] if len(partes) > 1 else nro[:20]

    em_ts = doc.get("emissionDate") or doc.get("createdAt")
    fecha_emision = ts(em_ts).date() if ts(em_ts) else None
    fecha_venc = ts(doc.get("expirationDate")).date() if doc.get("expirationDate") else None
    fecha_gen = ts(doc.get("createdAt"))

    sucursal, oid = "", doc.get("officeId")
    if oid:
        ro = requests.get(f"{API}/offices/{oid}.json", headers=H)
        if ro.status_code == 200: sucursal = safe(ro.json().get("name",""), 150)

    vendedor = ""
    if doc.get("userId"):
        ru = requests.get(f"{API}/users/{doc['userId']}.json", headers=H)
        if ru.status_code == 200:
            ud = ru.json()
            vendedor = safe(f"{ud.get('firstName','')} {ud.get('lastName','')}".strip(), 150)

    metodo_pago = ""
    rpa = requests.get(f"{API}/documents/{did}/payments.json", headers=H)
    if rpa.status_code == 200:
        pagos = rpa.json().get("items", [])
        if pagos and pagos[0].get("paymentType"):
            metodo_pago = safe(pagos[0]["paymentType"].get("name",""), 100)

    ruc_cli, cli_id = "", None
    if doc.get("client"):
        ruc_cli = safe(doc["client"].get("code",""), 20)
        cli_id = doc["client"].get("id")

    conn = get_db()
    try:
        cursor = conn.cursor()
        run(cursor, """
            INSERT INTO documentos
              (id, tipo_documento, nro_documento, prefijo_serie, numero_serie,
               ruc_cliente, cliente_id, fecha_emision, fecha_vencimiento,
               fecha_generacion, sucursal, oficina_id, emisor, vendedor,
               estado, metodo_pago, monto_documento)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
              tipo_documento=VALUES(tipo_documento),
              estado=VALUES(estado), metodo_pago=VALUES(metodo_pago),
              monto_documento=VALUES(monto_documento),
              vendedor=VALUES(vendedor)
        """, (
            did, tipo_doc, nro[:50], prefijo, num_serie, ruc_cli, cli_id,
            fecha_emision, fecha_venc, fecha_gen, sucursal, oid, "",
            vendedor, estado_map.get(doc.get("state",0), "VIGENTE"),
            metodo_pago, float(doc.get("totalAmount") or 0)
        ))

        # Detalles
        cursor.execute("DELETE FROM ventas WHERE documento_id=%s", (did,))
        rdet = requests.get(f"{API}/documents/{did}/details.json", headers=H)
        if rdet.status_code == 200:
            for det in rdet.json().get("items", []):
                vid = det.get("variantId")
                sku_det = ""
                if vid:
                    cursor.execute("SELECT sku FROM productos WHERE variante_id=%s LIMIT 1", (vid,))
                    row = cursor.fetchone()
                    sku_det = row[0] if row else ""

                fecha_dt = ts(em_ts)
                run(cursor, """
                    INSERT INTO ventas
                      (documento_id, tipo_movimiento, numero_serie,
                       fecha_venta, hora_venta, sku, variante_id,
                       cantidad, subtotal_neto)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    did, tipo_doc, nro[:50], fecha_emision,
                    fecha_dt.strftime("%H:%M:%S") if fecha_dt else None,
                    sku_det[:100], vid, float(det.get("quantity") or 0),
                    float(det.get("totalUnitValue") or 0) * float(det.get("quantity") or 0)
                ))
        conn.commit()
    finally:
        cursor.close()
        conn.close()

# ─────────────────────────────────────────
# ENDPOINTS
# ─────────────────────────────────────────
@app.route("/", methods=["GET"])
def health():
    return jsonify({"status": "ok", "mensaje": "Servidor Bsale→MySQL activo (Nuevo Schema)"}), 200

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
            # En webhooks v2 de stock, el resourceId suele ser el variantId. Consultamos el endpoint de stocks filtrando por variante.
            vid = res_id
            r = requests.get(f"{API}/stocks.json?variantid={vid}", headers=H)
            if r.status_code == 200:
                stock_items = r.json().get("items", [])
                for s_data in stock_items:
                    oid = s_data.get("officeId")
                    
                    # Fetch office name
                    oname = ""
                    if oid:
                        ro = requests.get(f"{API}/offices/{oid}.json", headers=H)
                        if ro.status_code == 200: oname = safe(ro.json().get("name",""), 150)
                    
                    # Insert/Update stock
                    if oid:
                        conn = get_db()
                        cur = conn.cursor()
                        cur.execute("SELECT sku FROM productos WHERE variante_id=%s LIMIT 1", (vid,))
                        row = cur.fetchone()
                        sku = row[0] if row else ""
                        
                        run(cur, """
                            INSERT INTO stock
                              (sku, variante_id, sucursal, oficina_id, stock,
                               cantidad_por_despachar, cantidad_disponible, por_recibir, fecha_actualizacion)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            ON DUPLICATE KEY UPDATE
                              stock=VALUES(stock), cantidad_por_despachar=VALUES(cantidad_por_despachar),
                              cantidad_disponible=VALUES(cantidad_disponible), por_recibir=VALUES(por_recibir),
                              fecha_actualizacion=VALUES(fecha_actualizacion)
                        """, (
                            sku[:100], vid, oname, oid,
                            float(s_data.get("quantity") or 0), float(s_data.get("quantityReserved") or 0),
                            float(s_data.get("quantityAvailable") or s_data.get("quantity") or 0),
                            float(s_data.get("quantityOnOrder") or 0), datetime.now()
                        ))
                        conn.commit()
                        cur.close()
                        conn.close()

        logger.info(f"✅ Webhook procesado: {topic} {res_id}")
        return jsonify({"status": "ok"}), 200
    except Exception as e:
        logger.error(f"❌ Error en webhook: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)), debug=False)
