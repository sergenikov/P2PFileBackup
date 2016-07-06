package cnc

import "testing"

/*
	Test function needs to have word Test prepended to the
	actual function name. In this case there is Reverse() function
	in cnc.go and hence the test. Test filename needs to end
	with "_test".
*/
func TestReverse(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"Hello, world", "dlrow ,olleH"},
		{"Hello, 世界", "界世 ,olleH"},
		{"", ""},
	}
	for _, c := range cases {
		got := Reverse(c.in)
		if got != c.want {
			t.Errorf("Reverse(%q) == %q, want %q", c.in, got, c.want)
		}
	}
}
