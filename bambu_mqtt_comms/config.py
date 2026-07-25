"""
Configuration classes for Bambu MQTT client.
"""

from dataclasses import dataclass


@dataclass
class PrinterConfig:
    """Configuration for connecting to a Bambu printer via MQTT."""
    
    ip: str
    """Printer IP address or hostname."""
    
    serial: str
    """Printer serial number."""
    
    access_code: str
    """Printer access code (from printer display)."""
    
    port: int = 8883
    """MQTT port (default: 8883 for TLS)."""
    
    username: str = "bblp"
    """MQTT username (default: 'bblp')."""
    
    keepalive: int = 60
    """MQTT keepalive interval in seconds."""
    
    response_timeout: float = 10.0
    """Default timeout for waiting for command responses."""
    
    @property
    def request_topic(self) -> str:
        """MQTT topic for sending commands to printer."""
        return f"device/{self.serial}/request"
    
    @property
    def report_topic(self) -> str:
        """MQTT topic for receiving responses from printer."""
        return f"device/{self.serial}/report"
