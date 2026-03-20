"""
=============================================================
  BSALE → MYSQL WEBHOOK SERVER
  Servidor Flask que recibe eventos de Bsale en tiempo real
  y los persiste en MySQL (Railway)
=============================================================
"""

import os
import json
import logging
import requests
import mysql.connector
from datetime import datetime
from flask import Flask, request, jsonify
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────
# CONFIGURACIÓN DE BASE DE DATOS
# ──────────────────────────────────────────

DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "caboose.proxy.rlwy.net"),
    "port":     int(os.getenv("DB_PORT", 40540)),
    "database": os.getenv("DB_NAME", "railway"),
    "user":     os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", ""),
}

BSALE_TOKEN = os.getenv("BSALE_TOKEN", "")
BSALE_API   = "https://api.bsale.io/v1"
HEADERS_BSALE = {"access_token": BSALE_TOKEN, "Content-Type": "application/json"}


def get_db():
    """Obtiene una conexión fresca a MySQL."""
    return mysql.connector.connect(**DB_CONFIG)


# ──────────────────────────────────────────
# HELPERS: UPSERT GENÉRICO
# ──────────────────────────────────────────

def upsert(cursor, table: str, data: dict, pk: str = "id"):
    """
    Inserta o actualiza un registro.
    Si la PK ya existe, actualiza todos los campos.
    """
    cols   = ", ".join(f"`{k}`" for k in data)
    vals   = ", ".join(["%s"] * len(data))
    update = ", ".join(f"`{k}`=VALUES(`{k}`)" for k in data if k != pk)
    sql    = f"INSERT INTO `{table}` ({cols}) VALUES ({vals}) ON DUPLICATE KEY UPDATE {update}"
    cursor.execute(sql, list(data.values()))


# ──────────────────────────────────────────
# PROCESADORES POR ENTIDAD
# ──────────────────────────────────────────

def process_product(product_id: int):
    """Descarga producto completo de Bsale y lo guarda en MySQL."""
    r = requests.get(f"{BSALE_API}/products/{product_id}.json", headers=HEADERS_BSALE)
    if r.status_code != 200:
        logger.error(f"Error al obtener producto {product_id}: {r.text}")
        return

    p = r.json()
    conn = get_db()
    try:
        cursor = conn.cursor()

        # Guardar producto principal
        producto_data = {
            "id":                  p.get("id"),
            "nombre":              p.get("name", ""),
            "codigo":              p.get("code", ""),
            "descripcion":         p.get("description", ""),
            "tipo":                p.get("productType", {}).get("name", "") if p.get("productType") else "",
            "estado":              p.get("state", 1),
            "tiene_costo":         p.get("hasCost", 0),
            "tiene_impuesto":      p.get("hasIva", 0),
            "clasificacion_id":    p.get("classificationId"),
            "fecha_creacion":      datetime.fromtimestamp(p["createdAt"]) if p.get("createdAt") else None,
            "fecha_actualizacion": datetime.fromtimestamp(p["updatedAt"]) if p.get("updatedAt") else None,
            "raw_json":            json.dumps(p),
        }
        upsert(cursor, "productos", producto_data)

        # Obtener y guardar variantes
        rv = requests.get(f"{BSALE_API}/products/{product_id}/variants.json", headers=HEADERS_BSALE)
        if rv.status_code == 200:
            variantes = rv.json().get("items", [])
            for v in variantes:
                variante_data = {
                    "id":                  v.get("id"),
                    "producto_id":         product_id,
                    "sku":                 v.get("code", ""),
                    "codigo_barra":        v.get("barCode", ""),
                    "descripcion":         v.get("description", ""),
                    "stock_actual":        v.get("stockQuantity", 0),
                    "costo":               v.get("cost", 0),
                    "estado":              v.get("state", 1),
                    "fecha_actualizacion": datetime.fromtimestamp(v["updatedAt"]) if v.get("updatedAt") else None,
                }
                upsert(cursor, "variantes", variante_data)

                # Precios de la variante
                rp = requests.get(f"{BSALE_API}/variants/{v['id']}/prices.json", headers=HEADERS_BSALE)
                if rp.status_code == 200:
                    precios = rp.json().get("items", [])
                    for precio in precios:
                        precio_sin_iva = float(precio.get("variantValuePrice", 0))
                        precio_con_iva = round(precio_sin_iva * 1.18, 2)  # IGV Perú 18%
                        precio_data = {
                            "id":                  precio.get("id"),
                            "variante_id":         v.get("id"),
                            "lista_precio_id":     precio.get("priceListId"),
                            "nombre_lista":        precio.get("priceList", {}).get("name", "") if precio.get("priceList") else "",
                            "precio":              precio_sin_iva,
                            "precio_con_iva":      precio_con_iva,
                            "moneda":              "PEN",
                            "fecha_actualizacion": datetime.now(),
                        }
                        upsert(cursor, "precios", precio_data)

        conn.commit()
        logger.info(f"✅ Producto {product_id} guardado correctamente.")
    finally:
        cursor.close()
        conn.close()


def process_client(client_id: int):
    """Descarga cliente de Bsale y lo guarda en MySQL."""
    r = requests.get(f"{BSALE_API}/clients/{client_id}.json", headers=HEADERS_BSALE)
    if r.status_code != 200:
        logger.error(f"Error al obtener cliente {client_id}: {r.text}")
        return

    c = r.json()
    conn = get_db()
    try:
        cursor = conn.cursor()
        cliente_data = {
            "id":                  c.get("id"),
            "ruc":                 c.get("code", ""),          # RUC/DNI en Bsale
            "razon_social":        c.get("company", ""),
            "nombre":              c.get("firstName", ""),
            "apellido":            c.get("lastName", ""),
            "email":               c.get("email", ""),
            "telefono":            c.get("phone", ""),
            "direccion":           c.get("address", ""),
            "ciudad":              c.get("city", ""),
            "departamento":        c.get("district", ""),
            "tipo_cliente":        "Empresa" if c.get("company") else "Persona Natural",
            "estado":              c.get("state", 1),
            "fecha_creacion":      datetime.fromtimestamp(c["createdAt"]) if c.get("createdAt") else None,
            "fecha_actualizacion": datetime.fromtimestamp(c["updatedAt"]) if c.get("updatedAt") else None,
        }
        upsert(cursor, "clientes", cliente_data)
        conn.commit()
        logger.info(f"✅ Cliente {client_id} guardado correctamente.")
    finally:
        cursor.close()
        conn.close()


def process_document(document_id: int):
    """Descarga documento (venta) de Bsale y lo guarda en MySQL con detalle y márgenes."""
    r = requests.get(f"{BSALE_API}/documents/{document_id}.json", headers=HEADERS_BSALE)
    if r.status_code != 200:
        logger.error(f"Error al obtener documento {document_id}: {r.text}")
        return

    doc = r.json()
    conn = get_db()
    try:
        cursor = conn.cursor()

        # Asegurar que el cliente exista
        client_id = None
        if doc.get("client") and doc["client"].get("id"):
            client_id = doc["client"]["id"]
            process_client(client_id)

        # Estado del documento
        estado_map = {0: "VIGENTE", 1: "ANULADO", 2: "NULO"}
        estado = estado_map.get(doc.get("state", 0), "VIGENTE")

        # Fecha de emisión
        emision_ts = doc.get("emissionDate") or doc.get("createdAt")
        fecha_emision = datetime.fromtimestamp(emision_ts) if emision_ts else None

        # Tipo de documento
        tipo_doc = ""
        tipo_doc_id = None
        if doc.get("documentType"):
            tipo_doc    = doc["documentType"].get("name", "")
            tipo_doc_id = doc["documentType"].get("id")

        # Número de documento (serie + correlativo)
        numero_doc = ""
        if doc.get("number") and doc.get("officeId"):
            numero_doc = str(doc.get("number", ""))

        # Oficina y vendedor
        oficina_nombre = ""
        oficina_id = doc.get("officeId")
        if doc.get("office"):
            oficina_nombre = doc["office"].get("name", "")

        vendedor_nombre = ""
        vendedor_id = doc.get("userId")
        if doc.get("user"):
            vendedor_nombre = doc["user"].get("firstName", "") + " " + doc["user"].get("lastName", "")

        venta_data = {
            "id":                 doc.get("id"),
            "numero_documento":   numero_doc,
            "tipo_documento":     tipo_doc,
            "tipo_documento_id":  tipo_doc_id,
            "cliente_id":         client_id,
            "ruc_cliente":        doc.get("client", {}).get("code", "") if doc.get("client") else "",
            "razon_social":       doc.get("client", {}).get("company", "") or
                                  (doc.get("client", {}).get("firstName", "") + " " + doc.get("client", {}).get("lastName", "")) if doc.get("client") else "",
            "fecha_emision":      fecha_emision.date() if fecha_emision else None,
            "fecha_hora_emision": fecha_emision,
            "estado":             estado,
            "total_neto":         float(doc.get("netAmount", 0)),
            "total_iva":          float(doc.get("taxAmount", 0)),
            "total_con_iva":      float(doc.get("totalAmount", 0)),
            "moneda":             "PEN",
            "vendedor_id":        vendedor_id,
            "vendedor_nombre":    vendedor_nombre.strip(),
            "oficina_id":         oficina_id,
            "oficina_nombre":     oficina_nombre,
            "almacen_id":         doc.get("warehouseId"),
            "enlace_pdf":         doc.get("urlPublicView", ""),
            "raw_json":           json.dumps(doc),
        }
        upsert(cursor, "ventas", venta_data)

        # Detalle del documento
        rd = requests.get(f"{BSALE_API}/documents/{document_id}/details.json", headers=HEADERS_BSALE)
        if rd.status_code == 200:
            detalles = rd.json().get("items", [])
            # Primero eliminamos los detalles viejos para re-insertar
            cursor.execute("DELETE FROM detalle_ventas WHERE venta_id = %s", (document_id,))

            for det in detalles:
                qty          = float(det.get("quantity", 0))
                precio_unit  = float(det.get("netUnitValue", 0))
                descuento    = float(det.get("discount", 0))
                total_neto   = float(det.get("totalUnitValue", 0)) * qty
                total_iva    = float(det.get("taxAmount", 0))
                total        = total_neto + total_iva

                # Obtener costo de la variante desde MySQL
                variante_id = det.get("variantId") or (det.get("variant", {}).get("id") if det.get("variant") else None)
                costo_unit   = 0.0
                if variante_id:
                    cursor.execute("SELECT costo FROM variantes WHERE id = %s", (variante_id,))
                    row = cursor.fetchone()
                    if row:
                        costo_unit = float(row[0])

                costo_total  = round(costo_unit * qty, 2)
                margen_bruto = round(total_neto - costo_total, 2)
                margen_pct   = round((margen_bruto / total_neto * 100) if total_neto > 0 else 0, 2)

                detalle_data = {
                    "venta_id":      document_id,
                    "variante_id":   variante_id,
                    "producto_id":   det.get("productId") or (det.get("variant", {}).get("productId") if det.get("variant") else None),
                    "sku":           det.get("code", ""),
                    "descripcion":   det.get("netUnitValue", det.get("comment", "")),
                    "cantidad":      qty,
                    "precio_unitario": precio_unit,
                    "descuento_pct": descuento,
                    "total_neto":    total_neto,
                    "total_iva":     total_iva,
                    "total":         total,
                    "costo_unitario": costo_unit,
                    "costo_total":   costo_total,
                    "margen_bruto":  margen_bruto,
                    "margen_pct":    margen_pct,
                }
                upsert(cursor, "detalle_ventas", detalle_data)

        conn.commit()
        logger.info(f"✅ Documento {document_id} guardado correctamente (estado: {estado}).")
    finally:
        cursor.close()
        conn.close()


# ──────────────────────────────────────────
# ENDPOINTS FLASK
# ──────────────────────────────────────────

@app.route("/", methods=["GET"])
def health():
    return jsonify({"status": "ok", "mensaje": "Servidor Bsale→MySQL activo ✅"}), 200


@app.route("/webhook/bsale", methods=["POST"])
def webhook_bsale():
    """
    Endpoint principal que recibe TODOS los webhooks de Bsale.
    Bsale envía un JSON con:
      - topic: nombre del evento (ej: "document/create")
      - resourceId: ID del recurso afectado
    """
    try:
        payload = request.get_json(force=True)
        logger.info(f"📩 Webhook recibido: {json.dumps(payload)}")

        topic       = payload.get("topic", "")
        resource_id = payload.get("resourceId") or payload.get("id")

        if not resource_id:
            return jsonify({"error": "Sin resourceId"}), 400

        resource_id = int(resource_id)

        # ── Documentos (Ventas) ──
        if topic in ("document/create", "document.create",
                     "document/update", "document.update",
                     "document/annul", "document.annul"):
            process_document(resource_id)

        # ── Productos ──
        elif topic in ("product/create", "product.create",
                       "product/update", "product.update"):
            process_product(resource_id)

        # ── Clientes ──
        elif topic in ("client/create", "client.create",
                       "client/update", "client.update"):
            process_client(resource_id)

        # ── Stock ──
        elif topic in ("stock/update", "stock.update"):
            # Actualizamos el stock de la variante directamente
            r = requests.get(f"{BSALE_API}/stocks/{resource_id}.json", headers=HEADERS_BSALE)
            if r.status_code == 200:
                stock_data = r.json()
                variante_id = stock_data.get("variantId")
                cantidad    = stock_data.get("quantity", 0)
                if variante_id:
                    conn = get_db()
                    cursor = conn.cursor()
                    cursor.execute(
                        "UPDATE variantes SET stock_actual=%s, fecha_actualizacion=%s WHERE id=%s",
                        (cantidad, datetime.now(), variante_id)
                    )
                    conn.commit()
                    cursor.close()
                    conn.close()

        else:
            logger.warning(f"⚠️ Evento no manejado: {topic}")

        return jsonify({"status": "procesado", "topic": topic, "id": resource_id}), 200

    except Exception as e:
        logger.error(f"❌ Error en webhook: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


# ──────────────────────────────────────────
# CARGA HISTÓRICA INICIAL (opcional)
# ──────────────────────────────────────────

@app.route("/sync/initial", methods=["POST"])
def sync_initial():
    """
    Endpoint para disparar la carga histórica completa desde Bsale.
    Llámalo UNA SOLA VEZ después de desplegar.
    POST a /sync/initial
    """
    try:
        results = {"productos": 0, "clientes": 0, "documentos": 0}

        # Sincronizar Productos
        page = 0
        while True:
            r = requests.get(f"{BSALE_API}/products.json?state=1&limit=50&offset={page*50}", headers=HEADERS_BSALE)
            if r.status_code != 200:
                break
            items = r.json().get("items", [])
            if not items:
                break
            for item in items:
                process_product(item["id"])
                results["productos"] += 1
            page += 1

        # Sincronizar Clientes
        page = 0
        while True:
            r = requests.get(f"{BSALE_API}/clients.json?limit=50&offset={page*50}", headers=HEADERS_BSALE)
            if r.status_code != 200:
                break
            items = r.json().get("items", [])
            if not items:
                break
            for item in items:
                process_client(item["id"])
                results["clientes"] += 1
            page += 1

        # Sincronizar documentos del último año
        import time
        fecha_inicio = int(datetime(2024, 1, 1).timestamp())
        page = 0
        while True:
            r = requests.get(
                f"{BSALE_API}/documents.json?limit=50&offset={page*50}&emissiondaterange=[{fecha_inicio},{int(time.time())}]",
                headers=HEADERS_BSALE
            )
            if r.status_code != 200:
                break
            items = r.json().get("items", [])
            if not items:
                break
            for item in items:
                process_document(item["id"])
                results["documentos"] += 1
            page += 1

        return jsonify({"status": "ok", "sincronizados": results}), 200

    except Exception as e:
        logger.error(f"❌ Error en sync inicial: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)), debug=False)
