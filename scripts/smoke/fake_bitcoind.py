#!/usr/bin/env python3
"""Fake bitcoind for the GoStratumPool integration smoke test.

Serves the JSON-RPC subset the pool, rewardd, and payoutd need over POST, and
exposes recorded state over GET for the miner/harness to assert against:

  GET /            -> list of raw submitted block hex strings
  GET /sendmany    -> list of {address: "amount"} maps captured from sendmany

RPC implemented:
  getblocktemplate  easy regtest-like target (bits 207fffff), coinbasevalue
  submitblock       records the raw block; tip jumps so the block matures
  getblockcount     current tip
  getblockhash      our submitted hash at its height; -8 past the tip
  validateaddress / getaddressinfo   accept any address
  sendmany          records amounts; returns a deterministic fake txid
"""
import json, threading, hashlib
from http.server import HTTPServer, BaseHTTPRequestHandler

state = {"submitted": [], "submitted_hash": None, "tip": 100, "sendmany": []}
lock = threading.Lock()


def dsha(b):
    return hashlib.sha256(hashlib.sha256(b).digest()).digest()


def handle_rpc(method, params):
    result, error = None, None
    if method == "getblocktemplate":
        result = {
            "version": 536870912, "rules": ["segwit"],
            "previousblockhash": "ab" * 32, "transactions": [],
            "coinbasevalue": 312500000, "mintime": 1700000000,
            "curtime": 1700000600, "bits": "207fffff", "height": 101,
        }
    elif method == "submitblock":
        blk = bytes.fromhex(params[0])
        state["submitted"].append(params[0])
        state["submitted_hash"] = dsha(blk[:80])[::-1].hex()
        state["tip"] = 250  # chain roars ahead -> instant maturity
        result = None
    elif method == "getblockcount":
        result = state["tip"]
    elif method == "getblockhash":
        h = params[0]
        if h > state["tip"]:
            error = {"code": -8, "message": "Block height out of range"}
        elif h == 101 and state["submitted_hash"]:
            result = state["submitted_hash"]
        else:
            result = "ee" * 32
    elif method in ("validateaddress", "getaddressinfo"):
        addr = params[0]
        result = {"isvalid": True, "address": addr, "ismine": False}
    elif method == "sendmany":
        amounts = params[1]
        state["sendmany"].append(amounts)
        digest = hashlib.sha256(json.dumps(amounts, sort_keys=True).encode()).hexdigest()
        result = "txid_" + digest[:16]
    return result, error


class Handler(BaseHTTPRequestHandler):
    def _send(self, code, body):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        req = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        with lock:
            result, error = handle_rpc(req["method"], req.get("params", []))
        body = json.dumps({"id": req["id"], "result": result, "error": error}).encode()
        self._send(500 if error else 200, body)

    def do_GET(self):
        with lock:
            if self.path.startswith("/sendmany"):
                body = json.dumps(state["sendmany"]).encode()
            else:
                body = json.dumps(state["submitted"]).encode()
        self._send(200, body)

    def log_message(self, *a):
        pass


if __name__ == "__main__":
    HTTPServer(("127.0.0.1", 18333), Handler).serve_forever()
