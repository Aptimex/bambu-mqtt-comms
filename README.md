# bambu-mqtt-comms

Minimal Python library for MQTT communication with Bambu Lab 3D printers.

Focuses purely on connection management and request/response handling.  
Command formatting and response parsing are handled by the caller (e.g., `bambu-mqtt-generator`).

## Features

- **TLS MQTT connection** (server cert verification disabled)
- **Request/response correlation** via `sequence_id` with configurable timeout
- **Fire-and-forget publish** for async commands
- **Context manager** support for automatic connection lifecycle
- **No command-specific logic** - send any valid JSON payload

## Installation

```bash
pip install -e /path/to/bambu-mqtt-comms
```

Requires Python 3.8+ and:
- `paho-mqtt>=2.0.0`

## Quick Start

```python
from bambu_mqtt_comms import BambuMQTTClient, PrinterConfig

# Printer configuration
config = PrinterConfig(
    ip="192.168.1.100",
    serial="01S00A123456789",
    access_code="abcdef12",
)

with BambuMQTTClient(config) as client:
    # Send command, wait for response (matched by sequence_id)
    response = client.send_and_wait({
        "print": {"sequence_id": "123", "command": "get_version"}
    })
    print(response)
    # {"print": {"sequence_id": "123", "command": "get_version", "version": "01.05.06.06", "err_code": 0}}
```

## API

### PrinterConfig

```python
config = PrinterConfig(
    ip="192.168.1.100",              # Required: printer IP/hostname
    serial="01S00A123456789",        # Required: printer serial
    access_code="abcdef12",          # Required: LAN access code
    
    # Optional MQTT settings
    port=8883,                       # Default: 8883 (TLS)
    username="bblp",                 # Default: "bblp"
    keepalive=60,                    # MQTT keepalive (seconds)
    response_timeout=10.0,           # Default command timeout
)

# Topic helpers
config.request_topic  # "device/<serial>/request"
config.report_topic   # "device/<serial>/report"
```

### BambuMQTTClient

```python
client = BambuMQTTClient(config)
```

#### `connect(timeout=15.0)`
Establish MQTT connection. Called automatically by context manager.

#### `disconnect()`
Close connection. Called automatically by context manager.

#### `send_and_wait(payload, timeout=None, qos=1) -> dict`
Send a JSON payload and wait for matching response.

- `payload`: dict or JSON string containing `sequence_id`
- Returns: response dict from printer
- Raises: `TimeoutError` if no response, `ConnectionError` if not connected

```python
response = client.send_and_wait({
    "print": {"sequence_id": "123", "command": "get_version"}
})
```

#### `publish(payload, qos=1)`
Fire-and-forget publish (no response waiting).

```python
client.publish({"print": {"sequence_id": "456", "command": "pause"}})
```

#### `is_connected` (property)
Check connection status.

#### Context Manager
```python
with BambuMQTTClient(config) as client:
    response = client.send_and_wait(...)
# Automatic disconnect
```

### Exceptions

```python
from bambu_mqtt_comms import BambuMQTTError, ConnectionError, TimeoutError

try:
    response = client.send_and_wait(...)
except ConnectionError:
    print("Not connected")
except TimeoutError:
    print("Printer didn't respond")
except BambuMQTTError:
    print("Other MQTT error")
```

## Example: Full Filament Change Workflow

```python
from bambu_mqtt_comms import BambuMQTTClient, PrinterConfig
from bambu_mqtt_generator import PayloadBuilder, get_payload_builder, ExternalSpool

# 1. Build payload using bambu-mqtt-generator
builder = get_payload_builder("X1 Carbon", "01.05.06.06")
payload = builder.build_filament_setting(
    tray_info_idx="GFA00",
    tray_color="00FF00",
    ams_id=ExternalSpool.MAIN,
    tray_id=0,
)

# 2. Send via bambu-mqtt-comms
config = PrinterConfig(
    ip="192.168.1.100",
    serial="01S00A123456789",
    access_code="abcdef12",
)

with BambuMQTTClient(config) as client:
    response = client.send_and_wait(payload)
    print("Response:", response)
```
