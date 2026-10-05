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

Printers on recent firmware that are not in LAN + Developer mode reject
unsigned commands. connect() detects that in one round trip without needing
any credentials, and send_command() signs automatically when a signer was
supplied and the printer needs it:

        with BambuMQTTClient(config, signer=my_signer) as client:
            response = client.send_command({"print": {...}})
            if msg := client.signing_status_message():
                print(msg)      # None unless the user needs to act

Supplying no signer is fully supported and is the common case.
"""

from .client import (
    BambuMQTTClient,
    BambuMQTTError,
    ConnectionError,
    TimeoutError,
    SignerProtocol,
    SigningState,
    SIGNATURE_REQUIRED_ERR,
    find_err_code,
)
from .config import PrinterConfig

__version__ = "0.1.0"

__all__ = [
    "BambuMQTTClient",
    "PrinterConfig",
    "BambuMQTTError",
    "ConnectionError",
    "TimeoutError",
    "SignerProtocol",
    "SigningState",
    "SIGNATURE_REQUIRED_ERR",
    "find_err_code",
]