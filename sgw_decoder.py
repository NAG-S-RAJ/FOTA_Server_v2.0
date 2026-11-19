"""Decodes raw Ethernet frames captured from the SGW into one-line, human-readable summaries."""

import ipaddress
import re
import struct

ETHERTYPES = {
    0x0800: "IPv4", 0x86DD: "IPv6", 0x0806: "ARP", 0x22F0: "AVTP", 0x88CC: "LLDP",
    0x88F7: "PTP", 0x8100: "VLAN", 0x88A8: "QinQ",
}
IP_PROTOCOLS = {1: "ICMP", 2: "IGMP", 6: "TCP", 17: "UDP", 58: "ICMPv6"}
WELL_KNOWN_PORTS = {
    22: "SSH", 53: "DNS", 67: "DHCP", 68: "DHCP", 80: "HTTP", 123: "NTP", 443: "HTTPS",
    5353: "mDNS", 8001: "HTTP (firmware transfer)", 13400: "DoIP", 30490: "SOME/IP-SD",
}
DOIP_PORT = 13400
SOMEIP_SD_PORT = 30490

DOIP_PAYLOAD_TYPES = {
    0x0000: "Generic NACK", 0x0001: "Vehicle identification request",
    0x0002: "Vehicle identification request (EID)", 0x0003: "Vehicle identification request (VIN)",
    0x0004: "Vehicle announcement / identification response", 0x0005: "Routing activation request",
    0x0006: "Routing activation response", 0x0007: "Alive check request", 0x0008: "Alive check response",
    0x4001: "Entity status request", 0x4002: "Entity status response",
    0x4003: "Power mode request", 0x4004: "Power mode response",
    0x8001: "Diagnostic message", 0x8002: "Diagnostic message ACK", 0x8003: "Diagnostic message NACK",
}
DOIP_ROUTING_CODES = {
    0x00: "denied: unknown source address", 0x01: "denied: all sockets registered",
    0x02: "denied: SA differs", 0x03: "denied: SA already active", 0x04: "denied: missing authentication",
    0x05: "denied: rejected confirmation", 0x06: "denied: unsupported activation type",
    0x10: "successfully activated", 0x11: "activated, confirmation required",
}
DOIP_DIAG_NACK_CODES = {
    0x02: "invalid source address", 0x03: "unknown target address", 0x04: "message too large",
    0x05: "out of memory", 0x06: "target unreachable", 0x07: "unknown network", 0x08: "transport protocol error",
}

UDS_SERVICES = {
    0x10: "DiagnosticSessionControl", 0x11: "ECUReset", 0x14: "ClearDiagnosticInformation",
    0x19: "ReadDTCInformation", 0x22: "ReadDataByIdentifier", 0x23: "ReadMemoryByAddress",
    0x24: "ReadScalingDataByIdentifier", 0x27: "SecurityAccess", 0x28: "CommunicationControl",
    0x29: "Authentication", 0x2A: "ReadDataByPeriodicIdentifier", 0x2C: "DynamicallyDefineDataIdentifier",
    0x2E: "WriteDataByIdentifier", 0x2F: "InputOutputControlByIdentifier", 0x31: "RoutineControl",
    0x34: "RequestDownload", 0x35: "RequestUpload", 0x36: "TransferData", 0x37: "RequestTransferExit",
    0x38: "RequestFileTransfer", 0x3D: "WriteMemoryByAddress", 0x3E: "TesterPresent",
    0x83: "AccessTimingParameter", 0x84: "SecuredDataTransmission", 0x85: "ControlDTCSetting",
    0x86: "ResponseOnEvent", 0x87: "LinkControl",
}
UDS_NRC = {
    0x10: "generalReject", 0x11: "serviceNotSupported", 0x12: "subFunctionNotSupported",
    0x13: "incorrectMessageLengthOrInvalidFormat", 0x14: "responseTooLong", 0x21: "busyRepeatRequest",
    0x22: "conditionsNotCorrect", 0x24: "requestSequenceError", 0x25: "noResponseFromSubnetComponent",
    0x26: "failurePreventsExecutionOfRequestedAction", 0x31: "requestOutOfRange", 0x33: "securityAccessDenied",
    0x35: "invalidKey", 0x36: "exceedNumberOfAttempts", 0x37: "requiredTimeDelayNotExpired",
    0x70: "uploadDownloadNotAccepted", 0x71: "transferDataSuspended", 0x72: "generalProgrammingFailure",
    0x73: "wrongBlockSequenceCounter", 0x78: "requestCorrectlyReceived-ResponsePending",
    0x7E: "subFunctionNotSupportedInActiveSession", 0x7F: "serviceNotSupportedInActiveSession",
}
UDS_SESSIONS = {0x01: "defaultSession", 0x02: "programmingSession", 0x03: "extendedDiagnosticSession",
                0x04: "safetySystemDiagnosticSession"}
UDS_RESETS = {0x01: "hardReset", 0x02: "keyOffOnReset", 0x03: "softReset",
              0x04: "enableRapidPowerShutDown", 0x05: "disableRapidPowerShutDown"}
UDS_ROUTINE_CONTROL = {0x01: "start", 0x02: "stop", 0x03: "requestResults"}

SOMEIP_MESSAGE_TYPES = {0x00: "REQUEST", 0x01: "REQUEST_NO_RETURN", 0x02: "NOTIFICATION",
                        0x80: "RESPONSE", 0x81: "ERROR"}
SOMEIP_SD_ENTRIES = {0x00: "FindService", 0x01: "OfferService", 0x06: "SubscribeEventgroup",
                     0x07: "SubscribeEventgroupAck"}
ICMP_TYPES = {0: "echo reply", 3: "destination unreachable", 8: "echo request", 11: "time exceeded"}
ICMPV6_TYPES = {128: "echo request", 129: "echo reply", 133: "router solicitation",
                134: "router advertisement", 135: "neighbor solicitation", 136: "neighbor advertisement"}


def mac_text(raw):
    return ":".join(f"{byte:02x}" for byte in raw)


def decode_uds(data):
    if not data:
        return "UDS (empty)"
    sid = data[0]
    if sid == 0x7F and len(data) >= 3:
        service = UDS_SERVICES.get(data[1], f"0x{data[1]:02X}")
        return f"UDS NEGATIVE RESPONSE {service} NRC 0x{data[2]:02X} {UDS_NRC.get(data[2], 'unknown')}"
    positive = sid >= 0x40 and (sid - 0x40) in UDS_SERVICES
    service_id = sid - 0x40 if positive else sid
    if service_id not in UDS_SERVICES:
        return f"UDS SID 0x{sid:02X} ({len(data)} bytes)"
    text = f"UDS {UDS_SERVICES[service_id]} {'+ve response' if positive else 'request'}"
    sub = data[1] if len(data) > 1 else None
    if sub is not None and service_id in (0x10, 0x11, 0x19, 0x28, 0x3E, 0x85, 0x27):
        sub_value = sub & 0x7F
        if service_id == 0x10:
            text += f" ({UDS_SESSIONS.get(sub_value, f'session 0x{sub_value:02X}')})"
        elif service_id == 0x11:
            text += f" ({UDS_RESETS.get(sub_value, f'reset 0x{sub_value:02X}')})"
        elif service_id == 0x27:
            text += f" (level {sub_value}: {'requestSeed' if sub_value % 2 else 'sendKey'})"
        else:
            text += f" (sub-function 0x{sub_value:02X})"
    if service_id in (0x22, 0x2E) and len(data) >= 3:
        text += f" DID 0x{data[1]:02X}{data[2]:02X}"
    if service_id == 0x31 and len(data) >= 4:
        text += f" {UDS_ROUTINE_CONTROL.get(data[1] & 0x7F, 'routine')} RID 0x{data[2]:02X}{data[3]:02X}"
    if service_id == 0x36 and len(data) >= 2:
        text += f" block {data[1]} ({max(0, len(data) - 2)} data bytes)"
    if service_id == 0x34 and positive and len(data) >= 2:
        size_length = data[1] >> 4
        if len(data) >= 2 + size_length and size_length:
            text += f" maxBlockLength {int.from_bytes(data[2:2 + size_length], 'big')}"
    return text


def decode_doip(payload):
    messages = []
    offset = 0
    while len(payload) - offset >= 8:
        version, inverse, payload_type, length = struct.unpack_from("!BBHI", payload, offset)
        if version ^ inverse != 0xFF:
            messages.append("DoIP (invalid header)")
            break
        body = payload[offset + 8: offset + 8 + length]
        name = DOIP_PAYLOAD_TYPES.get(payload_type, f"type 0x{payload_type:04X}")
        text = f"DoIP {name}"
        if payload_type == 0x8001 and len(body) >= 4:
            text += f" 0x{body[0]:02X}{body[1]:02X} -> 0x{body[2]:02X}{body[3]:02X} | {decode_uds(body[4:])}"
        elif payload_type in (0x8002, 0x8003) and len(body) >= 5:
            text += f" 0x{body[0]:02X}{body[1]:02X} -> 0x{body[2]:02X}{body[3]:02X}"
            if payload_type == 0x8003:
                text += f" ({DOIP_DIAG_NACK_CODES.get(body[4], f'code 0x{body[4]:02X}')})"
        elif payload_type == 0x0005 and len(body) >= 3:
            text += f" tester 0x{body[0]:02X}{body[1]:02X} activation type 0x{body[2]:02X}"
        elif payload_type == 0x0006 and len(body) >= 5:
            text += (f" tester 0x{body[0]:02X}{body[1]:02X} entity 0x{body[2]:02X}{body[3]:02X}"
                     f" ({DOIP_ROUTING_CODES.get(body[4], f'code 0x{body[4]:02X}')})")
        elif payload_type == 0x0004 and len(body) >= 19:
            vin = body[:17].decode("ascii", "replace")
            text += f" VIN {vin} logical address 0x{body[17]:02X}{body[18]:02X}"
        if len(body) < length:
            text += " (partial)"
        messages.append(text)
        offset += 8 + length
    return " || ".join(messages) if messages else "DoIP (partial header)"


def decode_someip(payload):
    if len(payload) < 16:
        return None
    service, method, length, client, session, protocol, interface, msg_type, return_code = \
        struct.unpack_from("!HHIHHBBBB", payload)
    if protocol != 0x01 or length < 8:
        return None
    if service == 0xFFFF and method == 0x8100:
        entries = []
        if len(payload) >= 24:
            entries_length = struct.unpack_from("!I", payload, 20)[0]
            entries_data = payload[24:24 + entries_length]
            for index in range(0, len(entries_data) - 15, 16):
                entry_type = entries_data[index]
                entry_service, instance = struct.unpack_from("!HH", entries_data, index + 4)
                ttl = int.from_bytes(entries_data[index + 9:index + 12], "big")
                name = SOMEIP_SD_ENTRIES.get(entry_type, f"entry 0x{entry_type:02X}")
                if entry_type == 0x01 and ttl == 0:
                    name = "StopOfferService"
                entries.append(f"{name} service 0x{entry_service:04X} instance 0x{instance:04X}")
        return "SOME/IP-SD " + ("; ".join(entries) if entries else "(no entries)")
    tp = " TP" if msg_type & 0x20 else ""
    kind = SOMEIP_MESSAGE_TYPES.get(msg_type & ~0x20 & 0xFF, f"type 0x{msg_type:02X}")
    text = (f"SOME/IP {kind}{tp} service 0x{service:04X} method 0x{method:04X} "
            f"client 0x{client:04X} session 0x{session:04X}")
    if return_code:
        text += f" return 0x{return_code:02X}"
    return text


def decode_application(payload, source_port, destination_port, transport):
    ports = (source_port, destination_port)
    if not payload:
        return ""
    if DOIP_PORT in ports:
        return decode_doip(payload)
    if transport == "UDP":
        someip = decode_someip(payload)
        if someip:
            return someip
    for port in ports:
        if port in WELL_KNOWN_PORTS:
            return WELL_KNOWN_PORTS[port]
    return ""


def decode_transport(protocol, data):
    if protocol == 6 and len(data) >= 20:
        source, destination, _, _, offset_flags = struct.unpack_from("!HHIIH", data)
        header_length = (offset_flags >> 12) * 4
        flags = [name for bit, name in ((0x02, "SYN"), (0x10, "ACK"), (0x01, "FIN"), (0x04, "RST"), (0x08, "PSH"))
                 if offset_flags & bit]
        payload = data[header_length:]
        application = decode_application(payload, source, destination, "TCP")
        return source, destination, f"TCP [{','.join(flags)}] payload {len(payload)}", application
    if protocol == 17 and len(data) >= 8:
        source, destination = struct.unpack_from("!HH", data)
        payload = data[8:]
        application = decode_application(payload, source, destination, "UDP")
        return source, destination, f"UDP payload {len(payload)}", application
    if protocol == 1 and data:
        return None, None, f"ICMP {ICMP_TYPES.get(data[0], f'type {data[0]}')}", ""
    if protocol == 58 and data:
        return None, None, f"ICMPv6 {ICMPV6_TYPES.get(data[0], f'type {data[0]}')}", ""
    return None, None, IP_PROTOCOLS.get(protocol, f"IP protocol {protocol}"), ""


def decode_frame(frame, sgw_mac):
    """Returns a one-line summary of an Ethernet frame (bytes starting at the destination MAC)."""
    if len(frame) < 14:
        return f"Truncated frame ({len(frame)} bytes)"
    destination, source = mac_text(frame[0:6]), mac_text(frame[6:12])
    sgw_mac = sgw_mac.lower()
    if source == sgw_mac:
        direction = f"SGW -> {destination}"
    elif destination == sgw_mac:
        direction = f"{source} -> SGW"
    else:
        direction = f"{source} -> {destination}"
    ethertype = struct.unpack_from("!H", frame, 12)[0]
    offset = 14
    vlan = ""
    while ethertype in (0x8100, 0x88A8) and len(frame) >= offset + 4:
        tci, ethertype = struct.unpack_from("!HH", frame, offset)
        vlan += f" VLAN {tci & 0x0FFF} prio {tci >> 13}"
        offset += 4
    parts = [direction + vlan]
    data = frame[offset:]
    if ethertype == 0x0800 and len(data) >= 20:
        header_length = (data[0] & 0x0F) * 4
        protocol = data[9]
        source_ip, destination_ip = ipaddress.IPv4Address(data[12:16]), ipaddress.IPv4Address(data[16:20])
        total_length = struct.unpack_from("!H", data, 2)[0]
        transport_data = data[header_length:max(header_length, total_length)]
        source_port, destination_port, transport, application = decode_transport(protocol, transport_data)
        address = (f"{source_ip}:{source_port} -> {destination_ip}:{destination_port}"
                   if source_port is not None else f"{source_ip} -> {destination_ip}")
        parts += [f"IPv4 {address}", transport]
        if application:
            parts.append(application)
    elif ethertype == 0x86DD and len(data) >= 40:
        next_header = data[6]
        source_ip, destination_ip = ipaddress.IPv6Address(data[8:24]), ipaddress.IPv6Address(data[24:40])
        payload_length = struct.unpack_from("!H", data, 4)[0]
        source_port, destination_port, transport, application = decode_transport(next_header, data[40:40 + payload_length])
        address = (f"[{source_ip}]:{source_port} -> [{destination_ip}]:{destination_port}"
                   if source_port is not None else f"{source_ip} -> {destination_ip}")
        parts += [f"IPv6 {address}", transport]
        if application:
            parts.append(application)
    elif ethertype == 0x0806 and len(data) >= 28:
        operation = struct.unpack_from("!H", data, 6)[0]
        sender_ip, target_ip = ipaddress.IPv4Address(data[14:18]), ipaddress.IPv4Address(data[24:28])
        parts.append(f"ARP who-has {target_ip} tell {sender_ip}" if operation == 1
                     else f"ARP {sender_ip} is-at {mac_text(data[8:14])}")
    else:
        parts.append(ETHERTYPES.get(ethertype, f"EtherType 0x{ethertype:04X}"))
    parts.append(f"{len(frame)} bytes")
    return " | ".join(parts)


def parse_hex_lines(lines):
    """Converts tcpdump -xx hex dump lines ('\\t0x0000:  ecfa 0300 ...') into bytes."""
    data = bytearray()
    for line in lines:
        _, _, hex_part = line.partition(":")
        data.extend(bytes.fromhex("".join(hex_part.split())))
    return bytes(data)


TSHARK_HEX_BYTES = re.compile(r"^[0-9a-fA-F]{4,}  ((?:[0-9a-fA-F]{2} {1,2}){0,15}[0-9a-fA-F]{2})")


def parse_tshark_hex_lines(lines):
    """Converts tshark -x hex dump lines ('0000  ec fa 03 00 ...   ascii') into bytes."""
    data = bytearray()
    for line in lines:
        match = TSHARK_HEX_BYTES.match(line)
        if match:
            data.extend(bytes.fromhex(match.group(1).replace(" ", "")))
    return bytes(data)
