package xbootstrap

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/compose-network/specs/compose"
	"github.com/compose-network/specs/compose/sbcp"
	"github.com/rs/zerolog"
)

// SequencerProver implementation that calls an HTTP prover service and returns
// the proof bytes synchronously to the SBCP sequencer.
//
// It POSTs a small JSON payload to `${baseURL}/prove`:
//
//	{
//	  "block_number":        <uint64>,
//	  "block_hash":          "0x…",
//	  "state_root":          "0x…",
//	  "superblock_number":   <uint64>
//	}
//
// and expects a response body:
//
//	{ "proof": "0x…" }
//
// If the call fails or the payload is missing, it returns nil so the caller can
// decide how to handle the missing proof.
type HTTPProver struct {
	client  *http.Client
	baseURL string
	log     zerolog.Logger
}

// NewHTTPProver creates a prover pointing at the given baseURL.
func NewHTTPProver(baseURL string, log zerolog.Logger) *HTTPProver {
	if baseURL == "" {
		baseURL = "http://localhost:38080"
	}
	return &HTTPProver{
		client:  &http.Client{Timeout: 30 * time.Second},
		baseURL: strings.TrimRight(baseURL, "/"),
		log:     log.With().Str("component", "http-prover").Str("base_url", baseURL).Logger(),
	}
}

// NewSimpleProver keeps the original bootstrap signature but now returns a real
// HTTP-backed prover. Configure PROVER_URL to override the default.
func NewSimpleProver() sbcp.SequencerProver {
	base := strings.TrimSpace(os.Getenv("PROVER_URL"))
	return NewHTTPProver(base, zerolog.Nop())
}

type proveRequest struct {
	BlockNumber      uint64 `json:"block_number"`
	BlockHash        string `json:"block_hash"`
	StateRoot        string `json:"state_root"`
	SuperblockNumber uint64 `json:"superblock_number"`
}

type proveResponse struct {
	Proof string `json:"proof"`
}

// RequestProofs contacts the prover. On any error it logs and returns nil.
func (p *HTTPProver) RequestProofs(blockHeader *sbcp.BlockHeader, superblockNumber compose.SuperblockNumber) []byte {
	if blockHeader == nil {
		p.log.Warn().Msg("no block header available; cannot request proof")
		return nil
	}

	payload := proveRequest{
		BlockNumber:      uint64(blockHeader.Number),
		BlockHash:        fmt.Sprintf("0x%x", blockHeader.BlockHash),
		StateRoot:        fmt.Sprintf("0x%x", blockHeader.StateRoot),
		SuperblockNumber: uint64(superblockNumber),
	}

	body, err := json.Marshal(payload)
	if err != nil {
		p.log.Error().Err(err).Msg("failed to marshal prover request")
		return nil
	}

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, p.baseURL+"/prove", bytes.NewReader(body))
	if err != nil {
		p.log.Error().Err(err).Msg("failed to build prover request")
		return nil
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.client.Do(req)
	if err != nil {
		p.log.Error().Err(err).Msg("prover request failed")
		return nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		p.log.Error().Int("status", resp.StatusCode).Msg("prover returned non-200 status")
		return nil
	}

	var pr proveResponse
	if err := json.NewDecoder(resp.Body).Decode(&pr); err != nil {
		p.log.Error().Err(err).Msg("failed to decode prover response")
		return nil
	}
	if pr.Proof == "" {
		p.log.Warn().Msg("prover response contained empty proof")
		return nil
	}

	// Accept 0x-prefixed hex; fall back to raw bytes if decode fails.
	proofStr := strings.TrimPrefix(pr.Proof, "0x")
	proofBytes, err := hex.DecodeString(proofStr)
	if err != nil {
		p.log.Error().Err(err).Msg("failed to hex-decode proof")
		return nil
	}

	return proofBytes
}
