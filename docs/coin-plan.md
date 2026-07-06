# Initial coin plan

GoStratumPool is intentionally scoped to four initial coins. The core remains
adapter-based so more coins can be added later, but the first production path is
not "support every coin". It is:

1. BTC
2. Radiant / RXD
3. SCASH
4. Alephium / ALPH

## Required pool IDs

- `btc-shared`
- `btc-solo`
- `rxd-shared`
- `rxd-solo`
- `scash-shared`
- `scash-solo`
- `alph-shared`
- `alph-solo`

Each pool ID has its own lifecycle. Putting `alph-shared` into maintenance must
not stop `alph-solo`, BTC, RXD, or SCASH.

## BTC

Family: Bitcoin-like  
PoW: SHA256d  
RPC style: `getblocktemplate` / `submitblock`  
Implementation order: first

BTC proves the generic Stratum V1 server, subscribe/authorize/notify/submit
flow, coinbase building, merkle handling, target math, and block submission.

## Radiant / RXD

Family: Bitcoin-like derived  
PoW: SHA512/256d  
RPC style: Bitcoin-like RPC behavior  
Implementation order: second

RXD should reuse shared Bitcoin-like job/template/coinbase code where possible,
but hashing must live behind a separate hash engine. Do not hardcode Radiant
inside the BTC adapter.

## SCASH

Family: Bitcoin-like protocol with RandomX PoW  
PoW: RandomX  
RPC style: `getblocktemplate` / `submitblock`  
Implementation order: third

Do not write RandomX from scratch in Go. Keep it behind a `HashEngine` and use a
native RandomX library through cgo or a clean external validation process.

## Alephium / ALPH

Family: Alephium-specific  
PoW/protocol: Alephium mining model  
Implementation order: fourth

ALPH must be its own adapter. Do not force it through the Bitcoin-like
`getblocktemplate` model. Its multi-chain/address-group mining behavior belongs
inside `internal/coins/alephium` and shared ALPH-specific types, not the stratum
core.
