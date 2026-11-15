// The management page's script. Ticket #32 fills this in; this
// placeholder keeps the embedded-asset route live for ticket #31's seam
// tests (issue #31). The real page exchanges the token at /v1/session
// and rides the HttpOnly session cookie — the JS never holds the token
// again (issue #24 point 3).
