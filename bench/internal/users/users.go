// Package users derives the synthetic benchmark users shared by mockpanel
// and loadgen, so the load generator can authenticate as any user the panel
// hands to the node without the two exchanging a list.
package users

import "fmt"

// UUID returns the UUID of the i-th (0-based) benchmark user.
func UUID(i int) string {
	return fmt.Sprintf("%08x-0000-4000-8000-%012x", i, i)
}
