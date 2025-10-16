package registry_utils

import (
	"testing"
)

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
