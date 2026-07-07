package bitcoinbase

import (
	"encoding/hex"
	"fmt"
)

// Job is a fully-derived mining job for a bitcoinlike coin: the coinbase
// split, merkle branch, and every header field a miner (and later, the
// submit-side validator) needs.
type Job struct {
	JobID    string
	Height   int64
	CleanJob bool

	PrevHashStratum string // notify encoding (word-swapped)
	PrevHashDisplay string // as delivered by the daemon
	Coinb1Hex       string
	Coinb2Hex       string
	MerkleBranchHex []string // little-endian step hex, notify order
	VersionHex      string   // big-endian hex, 8 chars
	BitsHex         string   // as delivered (big-endian hex)
	NTimeHex        string   // big-endian hex, 8 chars
	CurTime         int64
	MinTime         int64

	CoinbaseParts *CoinbaseParts
	// HasWitnessCommitment records whether the coinbase carries a witness
	// commitment output (drives witness serialization on block assembly).
	HasWitnessCommitment bool
	// TxDataHex holds the raw transactions (template order) for block assembly
	// on submit.
	TxDataHex []string
	// MerkleBranchLE keeps the branch steps as bytes for fold computation.
	MerkleBranchLE [][]byte
}

// NewJob derives a Job from a parsed template.
func NewJob(jobID string, tpl *Template, spec CoinbaseSpec, cleanJob bool) (*Job, error) {
	parts, err := BuildCoinbase(spec)
	if err != nil {
		return nil, err
	}

	txids := tpl.TxIDs()
	branchLE := make([][]byte, 0)
	txHashesLE := make([][]byte, 0, len(txids))
	for _, id := range txids {
		le, err := HashLEFromDisplayHex(id)
		if err != nil {
			return nil, fmt.Errorf("template txid: %w", err)
		}
		txHashesLE = append(txHashesLE, le)
	}
	branchLE = MerkleBranch(txHashesLE)
	branchHex := make([]string, len(branchLE))
	for i, s := range branchLE {
		branchHex[i] = hex.EncodeToString(s)
	}

	prevStratum, err := PrevHashToStratum(tpl.PreviousBlockHash)
	if err != nil {
		return nil, err
	}

	txData := make([]string, 0, len(tpl.Transactions))
	for _, tx := range tpl.Transactions {
		txData = append(txData, tx.Data)
	}

	return &Job{
		HasWitnessCommitment: len(spec.WitnessCommitmentScript) > 0,
		JobID:                jobID,
		Height:               tpl.Height,
		CleanJob:             cleanJob,
		PrevHashStratum:      prevStratum,
		PrevHashDisplay:      tpl.PreviousBlockHash,
		Coinb1Hex:            hex.EncodeToString(parts.Coinb1),
		Coinb2Hex:            hex.EncodeToString(parts.Coinb2),
		MerkleBranchHex:      branchHex,
		VersionHex:           fmt.Sprintf("%08x", uint32(tpl.Version)),
		BitsHex:              tpl.Bits,
		NTimeHex:             fmt.Sprintf("%08x", uint32(tpl.CurTime)),
		CurTime:              tpl.CurTime,
		MinTime:              tpl.MinTime,
		CoinbaseParts:        parts,
		TxDataHex:            txData,
		MerkleBranchLE:       branchLE,
	}, nil
}

// NotifyParams builds the positional mining.notify parameter array:
//
//	[jobId, prevhash, coinb1, coinb2, merkleBranch, version, nbits, ntime, cleanJobs]
func (j *Job) NotifyParams() []any {
	branch := make([]any, len(j.MerkleBranchHex))
	for i, s := range j.MerkleBranchHex {
		branch[i] = s
	}
	return []any{
		j.JobID,
		j.PrevHashStratum,
		j.Coinb1Hex,
		j.Coinb2Hex,
		branch,
		j.VersionHex,
		j.BitsHex,
		j.NTimeHex,
		j.CleanJob,
	}
}
