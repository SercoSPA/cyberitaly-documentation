"""
mongodb_smoke_test.py
=====================
Smoke test standalone per validare l'accesso al MongoDB IRIDE CyberItaly
(gestito da MEEO).

Credenziali fornite da MEEO su CIMS-45 (16/06/2026):
    username: monitoring  (read-only)
    password: Cyber1taly!
    db accessibili: admin, catalog, dapapi

NOTA: MEEO non ha specificato hostname/porta di connessione dall'esterno
del cluster K8s. Lo smoke test prova connessioni a host plausibili e
mostra l'errore esatto se nessuno funziona, cosi' sai cosa chiedere.

Prerequisiti:
    pip install pymongo

Uso:
    python mongodb_smoke_test.py
"""

import sys
import time

try:
    from pymongo import MongoClient
    from pymongo.errors import (
        ServerSelectionTimeoutError,
        ConnectionFailure,
        OperationFailure,
        ConfigurationError,
    )
except ImportError:
    print("ERRORE: serve installare pymongo")
    print("    pip install pymongo")
    sys.exit(1)


# ============================================================
# CONFIG
# ============================================================
USERNAME = "monitoring"
PASSWORD = "Cyber1taly!"

# Lista di host candidati da provare in ordine.
# MEEO non ha fornito l'hostname esatto, proviamo i pattern probabili.
CANDIDATE_HOSTS = [
    # FQDN possibili interni IRIDE
    ("mongodb.iride-cyberitaly.space", 27017),
    ("mongo.iride-cyberitaly.space", 27017),
    ("mongo-0.adam-mongo.iride-cyberitaly.space", 27017),
    ("dapapi-mongo.iride-cyberitaly.space", 27017),
    # Nome service K8s tipico (interno cluster, probabilmente non risolto)
    ("mongo-0.mongo.adam-dapapi", 27017),
    ("mongodb.adam-dapapi", 27017),
    # Eventuali IP della rete privata (vRack)
    # Aggiungi qui IP specifici se ne hai
]

AUTH_DB = "admin"
TARGET_DBS = ["admin", "catalog", "dapapi"]


# ============================================================
# Helpers
# ============================================================
def header(title):
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)


def ok(msg):
    print(f"  ✅  {msg}")


def warn(msg):
    print(f"  ⚠️   {msg}")


def fail(msg):
    print(f"  ❌  {msg}")


# ============================================================
# Main
# ============================================================
def try_connect(host, port, timeout_ms=3000):
    """Tenta connessione a un singolo host:port. Restituisce client o None."""
    uri = (f"mongodb://{USERNAME}:{PASSWORD}@{host}:{port}/"
           f"?authSource={AUTH_DB}&serverSelectionTimeoutMS={timeout_ms}")
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=timeout_ms)
        # Forza un round-trip per validare connessione + auth
        client.admin.command("ping")
        return client, None
    except ServerSelectionTimeoutError as e:
        return None, f"ServerSelectionTimeout: {str(e)[:150]}"
    except OperationFailure as e:
        return None, f"OperationFailure (auth?): {e}"
    except ConfigurationError as e:
        return None, f"ConfigError: {e}"
    except Exception as e:
        return None, f"{type(e).__name__}: {str(e)[:150]}"


def main():
    print("\n🔍 MongoDB IRIDE Smoke Test")
    print(f"   Username: {USERNAME}")
    print(f"   Target DBs: {TARGET_DBS}")
    print(f"   Hosts da provare: {len(CANDIDATE_HOSTS)}")

    # --- Step 1: trova un host raggiungibile ---
    header("1. Connessione (probe sui candidate host)")
    working_client = None
    working_host = None
    for host, port in CANDIDATE_HOSTS:
        print(f"\n  Provo {host}:{port}...")
        t0 = time.perf_counter()
        client, err = try_connect(host, port)
        elapsed_ms = (time.perf_counter() - t0) * 1000
        if client:
            ok(f"CONNESSO in {elapsed_ms:.0f}ms!")
            working_client = client
            working_host = f"{host}:{port}"
            break
        else:
            fail(f"({elapsed_ms:.0f}ms) {err}")

    if not working_client:
        print()
        fail("Nessun host raggiungibile.")
        print()
        print("  Cosa significa:")
        print("  - Il MongoDB e' interno al cluster K8s e non esposto esternamente")
        print("  - Oppure usa un hostname/IP diverso da quelli provati")
        print("  - Oppure la VM non ha route verso la rete del MongoDB")
        print()
        print("  Cosa fare:")
        print("  - Chiedere a MEEO su CIMS-45: 'da che hostname/IP la VM")
        print("    ci-mon-dash-01 puo' connettersi al MongoDB?'")
        return

    # --- Step 2: lista database ---
    header(f"2. Database disponibili (host: {working_host})")
    try:
        dbs = working_client.list_database_names()
        ok(f"{len(dbs)} database visibili: {dbs}")
    except Exception as e:
        fail(f"Impossibile listare DB: {e}")
        return

    # --- Step 3: per ogni DB target, lista collection ---
    for db_name in TARGET_DBS:
        header(f"3. Database '{db_name}' — collection")
        try:
            db = working_client[db_name]
            collections = db.list_collection_names()
            ok(f"{len(collections)} collection trovate")
            for c in collections[:20]:
                # Conta documenti (stima veloce)
                try:
                    count = db[c].estimated_document_count()
                    print(f"        - {c:40s}  ~{count} docs")
                except Exception as e:
                    print(f"        - {c:40s}  (count failed: {e})")
            if len(collections) > 20:
                print(f"        ... e altre {len(collections) - 20} collection")
        except OperationFailure as e:
            warn(f"Access denied su {db_name}: {e}")
        except Exception as e:
            warn(f"Errore: {type(e).__name__}: {e}")

    # --- Step 4: campione documenti da una collection 'catalog' ---
    header("4. Sample documenti da 'catalog' (1 documento per collection)")
    try:
        catalog_db = working_client["catalog"]
        for c in catalog_db.list_collection_names()[:5]:
            doc = catalog_db[c].find_one()
            if doc:
                # Mostra solo le chiavi top-level per non sporcare
                print(f"\n  Collection '{c}':")
                print(f"    Keys: {list(doc.keys())[:15]}")
                if "_id" in doc:
                    print(f"    _id sample: {doc['_id']}")
            else:
                print(f"\n  Collection '{c}': (vuota)")
    except Exception as e:
        warn(f"Errore campionamento: {e}")

    # --- Report finale ---
    header("📊  Report finale")
    print(f"  MongoDB raggiungibile su: {working_host}")
    print(f"  Pronto a sviluppare i collector di monitoring 🎉\n")

    working_client.close()


if __name__ == "__main__":
    main()
