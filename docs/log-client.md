# Verified log client (D16c)

The Go client reads one attempt/stream cursor page at a time. It checks the
response identity, ordered ranges, gap bounds, completion gaps, and each
short-lived download grant before making a storage request.

Storage requests carry no project token, cookie, proxy settings, or redirect
permission. The client requires the signed exact object version, expected
size, and SHA-256. It then decodes the bounded binary segment and compares
attempt ID, stream, first/last sequence, and every missing sequence against
the catalog. A mismatch yields no records to the caller.

Tests cover raw binary payloads, absent project credentials at storage,
tampered bytes, changed object versions, unsafe headers, and catalog gaps that
do not match the object. The CLI still needs to render these verified records
without terminal control sequences and poll cursors for follow mode.
