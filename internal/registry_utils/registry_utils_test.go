package registry_utils

import (
	"testing"
)

func TestSequencerAddrs_Hoodi(t *testing.T) {
	ru, err := New("", "hoodi")
	if err != nil {
		t.Fatalf("new resolver: %v", err)
	}
	got, err := ru.SequencerAddrs()
	if err != nil {
		t.Fatalf("SequencerAddrs: %v", err)
	}
	// Expect at least the two known chains
	wantA := "optimism-stack-geth:9898"
	wantB := "optimism-stack-2-geth:9898"
	if got[77777] != wantA {
		t.Fatalf("chain 77777 sequencer = %q, want %q", got[77777], wantA)
	}
	if got[88888] != wantB {
		t.Fatalf("chain 88888 sequencer = %q, want %q", got[88888], wantB)
	}
	// Verify join is deterministic and sorted for a subset
	sub := map[uint64]string{77777: got[77777], 88888: got[88888]}
	joined := JoinSequencerAddrs(sub)
	wantJoined := "77777:optimism-stack-geth:9898,88888:optimism-stack-2-geth:9898"
	if joined != wantJoined {
		t.Fatalf("JoinSequencerAddrs = %q, want %q", joined, wantJoined)
	}
}

func TestMailboxes_Hoodi(t *testing.T) {
	ru, err := New("", "hoodi")
	if err != nil {
		t.Fatalf("new resolver: %v", err)
	}
	got, err := ru.Mailboxes()
	if err != nil {
		t.Fatalf("Mailboxes: %v", err)
	}
	want := "0x248721a59a2756E579026aDA017bd9B6adFe3e57"
	if got[77777] != want {
		t.Fatalf("chain 77777 mailbox = %q, want %q", got[77777], want)
	}
	if got[88888] != want {
		t.Fatalf("chain 88888 mailbox = %q, want %q", got[88888], want)
	}
}
