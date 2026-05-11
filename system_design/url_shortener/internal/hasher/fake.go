package hasher

type FakeHasher struct {
}

func NewFakeHasher() *FakeHasher {
	return &FakeHasher{}
}

func (f *FakeHasher) Encode(input string) string { return input + "+hello" }
func (f *FakeHasher) Decode(input string) string { return "" }
