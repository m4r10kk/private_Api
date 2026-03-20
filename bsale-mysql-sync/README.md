# Bsale MySQL Sync — Servidor Webhook

Servidor Flask que recibe eventos de Bsale en tiempo real y los persiste en MySQL (Railway).

## Estructura del proyecto

```
bsale-mysql-sync/
├── app.py              # Servidor Flask principal
├── requirements.txt    # Dependencias Python
├── Procfile            # Instrucción de arranque para Railway
└── .env.example        # Plantilla de variables de entorno
```

## Variables de entorno necesarias

| Variable | Valor |
|---|---|
| `DB_HOST` | `caboose.proxy.rlwy.net` |
| `DB_PORT` | `40540` |
| `DB_NAME` | `railway` |
| `DB_USER` | `root` |
| `DB_PASSWORD` | (tu password de Railway) |
| `BSALE_TOKEN` | (tu token de la API de Bsale) |

## Endpoints disponibles

| Método | Ruta | Descripción |
|---|---|---|
| `GET` | `/` | Health check del servidor |
| `POST` | `/webhook/bsale` | Recibe todos los webhooks de Bsale |
| `POST` | `/sync/initial` | Carga histórica total (usar 1 sola vez) |

## Eventos de Bsale manejados

- `document/create`, `document/update`, `document/annul` → tabla `ventas` + `detalle_ventas`
- `product/create`, `product/update` → tabla `productos` + `variantes` + `precios`
- `client/create`, `client/update` → tabla `clientes`
- `stock/update` → campo `stock_actual` en `variantes`
