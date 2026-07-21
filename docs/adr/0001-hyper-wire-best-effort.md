# Hyper owns HTTP wire representation

The HTTP response-policy Module uses Hyper's status and header model as the
best-effort wire representation; it does not reproduce Java reason phrases,
error text, header ordering, or serialized bytes exactly. It must nevertheless
apply flow control and statistics consistently to the final response model and
to every body frame produced, including fallback and incomplete responses.
