"""
bambu-mqtt-comms - Python library for raw MQTT communication with Bambu Lab printers.

Provides minimal connection management and request/response handling.
Command formatting and response parsing are handled by the caller (e.g., bambu-mqtt-generator).

Example:
    from bambu_mqtt_comms import BambuMQTTClient, PrinterConfig
    
    config = PrinterConfig(
        ip="192.168.1.100",
        serial="01S00A123456789",
        access_code="abcdef12",
    )

    with BambuMQTTClient(config) as client:
        # Send arbitrary JSON, wait for response (matched by sequence_id)
        response = client.send_and_wait({
            "info": {"sequence_id": "20001", "command": "get_version"}
        })

        # Fire-and-forget
        client.publish({"print": {"sequence_id": "20002", "command": "pause"}})
"""

from .client import BambuMQTTClient, BambuMQTTError, ConnectionError, TimeoutError
from .config import PrinterConfig

__version__ = "0.1.0"

__all__ = [
    "BambuMQTTClient",
    "PrinterConfig",
    "BambuMQTTError",
    "ConnectionError",
    "TimeoutError",
]