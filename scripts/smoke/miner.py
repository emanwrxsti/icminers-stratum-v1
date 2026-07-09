import socket, json, time, hashlib, struct, urllib.request

def dsha(b): return hashlib.sha256(hashlib.sha256(b).digest()).digest()

s = socket.create_connection(("127.0.0.1", 13033), timeout=10)
f = s.makefile("rw")
def send(m): f.write(json.dumps(m)+"\n"); f.flush()
def recv(): return json.loads(f.readline())

send({"id":1,"method":"mining.subscribe","params":["smokeminer/3.0"]})
sub = recv(); en1 = sub["result"][1]; en2size = sub["result"][2]
send({"id":2,"method":"mining.authorize","params":["bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4.rig1","x"]})

diff = None; notify = None
while notify is None or diff is None:
    m = recv()
    if m.get("method") == "mining.set_difficulty": diff = m["params"][0]
    elif m.get("method") == "mining.notify": notify = m["params"]
    elif m.get("id") == 2: assert m["result"] is True

jobid, prevhash, coinb1, coinb2, branch, version, nbits, ntime, clean = notify
print(f"JOB {jobid} diff={diff} branch={len(branch)} clean={clean}")

# share target = diff1 / diff
diff1 = 0x00000000FFFF0000000000000000000000000000000000000000000000000000
target = int(diff1 / diff)

en2 = "00000042"
cb = bytes.fromhex(coinb1) + bytes.fromhex(en1) + bytes.fromhex(en2) + bytes.fromhex(coinb2)
root = dsha(cb)
for step in branch:
    root = dsha(root + bytes.fromhex(step))

header_pre = (struct.pack("<I", int(version,16))
    + b"".join(bytes.fromhex(prevhash)[i:i+4][::-1] for i in range(0,32,4))
    + root
    + struct.pack("<I", int(ntime,16))
    + struct.pack("<I", int(nbits,16)))

nonce = None
for n in range(50_000_000):
    h = dsha(header_pre + struct.pack("<I", n))
    if int.from_bytes(h, "little") <= target:
        nonce = n; break
assert nonce is not None, "failed to mine"
print(f"MINED nonce={nonce} after {n+1} tries")

send({"id":3,"method":"mining.submit","params":[
    "bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4.rig1", jobid, en2, ntime, "%08x"%nonce]})
resp = recv()
print("SUBMIT RESP:", json.dumps(resp))
assert resp["result"] is True and resp["error"] is None, "share rejected"

# duplicate must be rejected with code 22
send({"id":4,"method":"mining.submit","params":[
    "bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4.rig1", jobid, en2, ntime, "%08x"%nonce]})
resp = recv()
print("DUP RESP:", json.dumps(resp))
assert resp["result"] is None and resp["error"][0] == 22, "dup not code 22"

# stale job id -> code 21
send({"id":5,"method":"mining.submit","params":[
    "bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4.rig1", "ffff", en2, ntime, "%08x"%nonce]})
resp = recv()
print("STALE RESP:", json.dumps(resp))
assert resp["error"][0] == 21, "stale not code 21"

# with regtest bits our share is also a block: daemon must have received submitblock
time.sleep(0.8)
submitted = json.loads(urllib.request.urlopen("http://127.0.0.1:18333").read())
assert len(submitted) == 1, f"submitblock count = {len(submitted)}"
blk = bytes.fromhex(submitted[0])
assert dsha(blk[:80]) == dsha(header_pre + struct.pack("<I", nonce)), "daemon got a different header"
print("SMOKE TEST PASSED: share accepted, dup=22, stale=21, block submitted to daemon")
