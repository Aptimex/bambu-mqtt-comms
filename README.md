# bambu-mqtt-comms

Minimal Python library for MQTT communication with Bambu Lab 3D printers over
the local network.

It handles the connection, request/response correlation, working out whether a
given printer requires signed commands, and the certificate bootstrap that
those commands require. It deliberately knows nothing about
command formats: building payloads, signing them, and parsing replies are the
job of the companion library **bambu-mqtt-generator**, which also documents a
complete read-modify-verify workflow using both libraries.

While intended to be a generic library, this was built and tested for the express purposes of building a program that monitors and controls AMS filament settings. 
Some of the documentation and functionality may skew towards that specific use case.

This library's code is primarily AI generated and maintained, but is validated against real printers to ensure correctness. Nevertheless, you use it at your own risk.

## Installation

From a source checkout:

```bash
pip install .          # or: pip install -e .  for development
```

Requires Python 3.8+ and `paho-mqtt>=2.0.0`.

## Quick start

```python
from bambu_mqtt_comms import BambuMQTTClient, PrinterConfig

config = PrinterConfig(
    ip="192.168.1.100",
    serial="01S00A123456789",
    access_code="abcdef12",       # from the printer's display
)

with BambuMQTTClient(config) as client:
    status = client.request_status()          # full push_status
    print(status["ams"]["ams"][0]["tray"])

    reply = client.send_and_wait({
        "info": {"sequence_id": "20001", "command": "get_version"}
    })
```

## PrinterConfig

| Field | Default | Notes |
|---|---|---|
| `ip` | — | Printer IP or hostname |
| `serial` | — | Printer serial number |
| `access_code` | — | LAN access code from the printer display |
| `port` | `8883` | MQTT TLS port |
| `username` | `"bblp"` | MQTT username |
| `keepalive` | `60` | Seconds |
| `response_timeout` | `10.0` | Default `send_and_wait` timeout |

Also exposes `request_topic` (`device/<serial>/request`) and `report_topic`
(`device/<serial>/report`).

TLS is used, but the printer's certificate is not verified.

## BambuMQTTClient

### Connection

`connect(timeout=15.0, prime_status=True, detect_signing=True)` / `disconnect()`
— or use the client as a context manager, which does both. `is_connected`
reports the current state.

`detect_signing=True` runs the ladder in [Signing policy](#signing-policy) as
part of connecting. Pass `False` to skip it entirely and do your own signing.

A signer may be supplied at construction:

```python
client = BambuMQTTClient(config, signer=signer)   # signer is optional
```

Any object with `sign(payload) -> str`, `get_cert_id() -> str` and
`build_app_cert_install(sequence_id=None) -> str` will do; **bambu-mqtt-generator**'s
`MQTTSigner` satisfies it as-is. The type is declared as `SignerProtocol`
structurally, so this library takes no dependency on that one.

### Signing policy

Printers on firmware newer than (approximately) January 2025 reject unsigned
commands **unless** the printer is in LAN-Only + Developer Mode. Which case a
given printer is in is detectable, and this library detects it rather than
asking you to configure it.

**`probe_signing_required(timeout=None) -> bool | None`**

Publishes an unsigned command whose name is randomly generated and therefore
matches nothing in the firmware. The printer verifies the signature *before* it
looks up the command name, so a printer that requires signing answers
`err_code` 84033543 while one that does not answers success, having found
nothing to do. Because no such command exists there is no handler to reach,
which makes this safe to send in any printer state, including mid-print.

It needs no certificate and no bootstrap — that is the point. Registration is
the one slow step in the whole process, and this establishes whether paying for
it is necessary before paying for it.

Returns `True`, `False`, or `None` if the printer did not answer. Treat `None`
as "not required"; `send_command` catches the other case on the first real
command.

**`connect(detect_signing=True)`** runs, in order:

1. the probe above — one round trip. Most printers stop here.
2. `get_trusted_certs()`, only if signing is required: the certificate may
   already be registered from earlier this power cycle, in which case there is
   nothing to do.
3. `install_app_cert()` on a background thread, only if it is not.

**`send_command(payload, timeout=None, retry_signed=True) -> dict`**

The method to use for anything a printer might refuse over signing. It signs
when signing is both required and possible, waits for an in-flight
registration first, and if a command still comes back with 84033543 it records
that, registers the certificate, and retries once. That last part is the
backstop for a printer whose policy changed since connecting, or one the probe
could not reach. A printer that does not require signing is sent the payload
untouched, so a caller with no signer is fully supported and pays nothing.

**`signing` → `SigningState`** — what has been worked out so far: `required`,
`probe_inconclusive`, `detected_by`, `credentials_available`, `supported`,
`cert_trusted`, `bootstrap_pending`, `bootstrap_error`. It lives and dies with
one connection, since printers forget certificates on power cycle and
Developer mode can be toggled between sessions.

**`can_sign`** — whether a signed command would verify right now.

**`await_signing_ready(timeout=45.0) -> bool`** — block until an in-flight
registration settles.

**`signing_status_message() -> str | None`** — a one-line, user-facing
explanation of anything the user needs to act on, or `None` when there is
nothing to say. `None` is the normal outcome for a printer in LAN+DEV mode.

```python
with BambuMQTTClient(config, signer=signer) as client:
    reply = client.send_command(payload)
    if msg := client.signing_status_message():
        print(msg)
```

### Sending

**`send_and_wait(payload, timeout=None, qos=1) -> dict`**

Publishes a payload and blocks until a reply with a matching `sequence_id`
arrives. Accepts a dict or a JSON string. Raises `TimeoutError` if no reply
arrives, `ValueError` if the payload has no `sequence_id`.

For most uses, the payload should be passed as a dict. This function will normalize the dict data and serialize it into the same JSON format that Bambu Studio uses (key ordering, whitespace removal, etc) to ensure the printer is able to parse it correctly. 

A payload passed as a **string is published byte-for-byte as given** with minimal validation. This
matters for pre-signed payloads: the signature covers the exact bytes of the
command object, so parsing and re-serializing it could break the signature.

```python
reply = client.send_and_wait(payload)
```

**`publish(payload, qos=1)`** — fire-and-forget version of send_and_wait(), no reply awaited.

**`random_sequence_id() -> str`** — a random 5-digit id starting with `2`,
matching the range Bambu Studio uses.

### Status

**`request_status(sequence_id=None, timeout=15.0) -> dict`**

Sends `pushall` and returns the resulting `push_status`. The reply is
asynchronous and carries its own `sequence_id`, so it is matched by type.

Printers also emit incremental `push_status` diffs containing only the fields
that changed. Those are skipped, because a diff arriving first would look like
a printer with no AMS. Raises `TimeoutError` if no full report arrives.

**`wait_for_status(timeout=10.0) -> dict | None`** — wait for the next
`push_status` of any kind, full or incremental, without sending anything.

### Certificate bootstrap

`connect()` and `send_command()` handle this for you. The primitives below are
public for callers doing their own signing, or skipping detection.

Trusted certificates for signing have to be "bootstrapped" into the printer's memory and are lost at every power cycle. 
Some printers can hold multiple trusted certs simultaneously, while others can only hold one that is replaced by a new bootstrapping command.
Each Bambu application has application-specific certificates and signing key, and always perform the bootstrapping step during the MQTT connection process.

This library does not include or provide ready-to-use certificates or keys for bootstrapping or singing. 
However, if you [obtain a key](https://github.com/danielwoz/BambuSlicerKeySaver) and its associated [certificate chain and CRL](https://bambuzled.github.io/posts/bambu-auth-control/#the-current-cert-api), this library can bootstrap the certificate to help restore interoperability. Use the companion **bambu-mqtt-generator** library to generate the command payloads to do so.

**`install_app_cert(bootstrap_message, cert_id=None, attempts=6, interval=5.0, query_timeout=5.0) -> dict`**

Publishes an `app_cert_install` message and confirms the printer accepted it.
The printer never acknowledges that message directly, so registration is
verified by polling `app_cert_list` until `cert_id` appears among the trusted
certificates; a fixed sleep is unreliable. The first poll happens immediately,
since the certificate may already be trusted from an earlier bootstrap.

Returns `{"trusted": bool, "cert_ids": [...], "attempts_used": int}`. Treat
`trusted=False` (bootstrap failure) as "do not send signed commands" — they will come back with
`err_code` 84033545.

```python
result = client.install_app_cert(
    signer.build_app_cert_install(),
    cert_id=signer.get_cert_id(),
)
if not result["trusted"]:
    raise RuntimeError("printer never trusted our certificate")
```

Build `bootstrap_message` using the companion **bambu-mqtt-generator** library.

**`get_trusted_certs(timeout=10.0) -> dict`** — query `app_cert_list` directly.
Returns `{"result": str | None, "cert_ids": [...], "cert_count": int}`.

## Exceptions

All inherit from `BambuMQTTError`:

```python
from bambu_mqtt_comms import BambuMQTTError, ConnectionError, TimeoutError

try:
    reply = client.send_and_wait(payload)
except ConnectionError:      # not connected, or connection failed
    ...
except TimeoutError:         # no reply within the timeout
    ...
```

Note that these shadow the built-in `ConnectionError` and `TimeoutError` when
imported by name.

## License and scope

MIT licensed. This is an independent project, not affiliated with or endorsed
by Bambu Lab. It speaks an undocumented protocol that vendor firmware updates
can change without notice.
