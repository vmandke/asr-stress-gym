// Package backend is the Client interface the coordinator calls across the
// network: Open/Push/Flush/Restore/Close/Health. It is the only place HTTP
// to a worker happens. Filled in at M1.
package backend
