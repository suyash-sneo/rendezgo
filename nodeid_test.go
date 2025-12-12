package workdist

import "testing"

func TestDefaultNodeIDProviderStable(t *testing.T) {
	t.Setenv("HOSTNAME", "MyHost")
	p := NewDefaultNodeIDProvider(WithNodePrefix("cluster"), WithoutRandomSuffix())

	first, err := p.NodeID()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	second, err := p.NodeID()
	if err != nil {
		t.Fatalf("unexpected error on second call: %v", err)
	}
	if first != second {
		t.Fatalf("node id changed: %q vs %q", first, second)
	}
	if first != "cluster-myhost" {
		t.Fatalf("unexpected node id %q", first)
	}
}
