"""The small RSA/XOR codec used by 115's ``downurl`` endpoint."""

from __future__ import annotations

import base64
import json

from .exceptions import Cloud115CipherError

_RSA_N = 0x8686980C0F5A24C4B9D43020CD2C22703FF3F450756529058B1CF88F09B8602136477198A6E2683149659BD122C33592FDB5AD47944AD1EA4D36C6B172AAD6338C3BB6AC6227502D010993AC967D1AEF00F0C8E038DE2E4D3BC2EC368AF2E9F10A6F1EDA4F7262F136420C07C331B871BF139F74F3010E3C4FE57DF3AFB71683
_RSA_E = 0x10001
_RSA_BLOCK_SIZE = 128
_RSA_MESSAGE_SIZE = 117
_RSA_KEY = b"\x8d\xa5\xa5\x8d"
_G_KEY_L = b"\x78\x06\xad\x4c\x33\x86\x5d\x18\x4c\x01\x3f\x46"
_G_KTS = bytes((
    0xf0, 0xe5, 0x69, 0xae, 0xbf, 0xdc, 0xbf, 0x8a, 0x1a, 0x45, 0xe8, 0xbe, 0x7d, 0xa6, 0x73, 0xb8,
    0xde, 0x8f, 0xe7, 0xc4, 0x45, 0xda, 0x86, 0xc4, 0x9b, 0x64, 0x8b, 0x14, 0x6a, 0xb4, 0xf1, 0xaa,
    0x38, 0x01, 0x35, 0x9e, 0x26, 0x69, 0x2c, 0x86, 0x00, 0x6b, 0x4f, 0xa5, 0x36, 0x34, 0x62, 0xa6,
    0x2a, 0x96, 0x68, 0x18, 0xf2, 0x4a, 0xfd, 0xbd, 0x6b, 0x97, 0x8f, 0x4d, 0x8f, 0x89, 0x13, 0xb7,
    0x6c, 0x8e, 0x93, 0xed, 0x0e, 0x0d, 0x48, 0x3e, 0xd7, 0x2f, 0x88, 0xd8, 0xfe, 0xfe, 0x7e, 0x86,
    0x50, 0x95, 0x4f, 0xd1, 0xeb, 0x83, 0x26, 0x34, 0xdb, 0x66, 0x7b, 0x9c, 0x7e, 0x9d, 0x7a, 0x81,
    0x32, 0xea, 0xb6, 0x33, 0xde, 0x3a, 0xa9, 0x59, 0x34, 0x66, 0x3b, 0xaa, 0xba, 0x81, 0x60, 0x48,
    0xb9, 0xd5, 0x81, 0x9c, 0xf8, 0x6c, 0x84, 0x77, 0xff, 0x54, 0x78, 0x26, 0x5f, 0xbe, 0xe8, 0x1e,
    0x36, 0x9f, 0x34, 0x80, 0x5c, 0x45, 0x2c, 0x9b, 0x76, 0xd5, 0x1b, 0x8f, 0xcc, 0xc3, 0xb8, 0xf5,
))


def _derive_key(rand_key: bytes, length: int) -> bytes:
    result = bytearray(length)
    offset = length * (length - 1)
    for index in range(length):
        result[index] = _G_KTS[offset] ^ ((rand_key[index] + _G_KTS[index * length]) & 0xFF)
        offset -= length
    return bytes(result)


def _xor(data: bytes, key: bytes) -> bytes:
    result = bytearray()
    remainder = len(data) & 3
    for index in range(remainder):
        result.append(data[index] ^ key[index])
    offset = remainder
    while offset < len(data):
        for index in range(min(len(key), len(data) - offset)):
            result.append(data[offset + index] ^ key[index])
        offset += len(key)
    return bytes(result)


def _encode(data: bytes) -> bytes:
    payload = b"\0" * 16 + _xor(_xor(data, _RSA_KEY)[::-1], _G_KEY_L)
    output = bytearray()
    for offset in range(0, len(payload), _RSA_MESSAGE_SIZE):
        chunk = payload[offset : offset + _RSA_MESSAGE_SIZE]
        padded = b"\0" + b"\2" * (126 - len(chunk)) + b"\0" + chunk
        output += pow(int.from_bytes(padded, "big"), _RSA_E, _RSA_N).to_bytes(_RSA_BLOCK_SIZE, "big")
    return base64.b64encode(output)


def _decode(encoded: str) -> bytes:
    try:
        payload = base64.b64decode(encoded)
    except Exception as exc:
        raise Cloud115CipherError("115 downurl response is not base64") from exc
    if not payload or len(payload) % _RSA_BLOCK_SIZE:
        raise Cloud115CipherError("115 downurl response has an invalid RSA block size")
    output = bytearray()
    for offset in range(0, len(payload), _RSA_BLOCK_SIZE):
        value = pow(int.from_bytes(payload[offset : offset + _RSA_BLOCK_SIZE], "big"), _RSA_E, _RSA_N)
        block = value.to_bytes(max(1, (value.bit_length() + 7) // 8), "big")
        try:
            output += block[block.index(b"\0") + 1 :]
        except ValueError as exc:
            raise Cloud115CipherError("115 downurl response has invalid RSA padding") from exc
    if len(output) < 16:
        raise Cloud115CipherError("115 downurl response is truncated")
    body = _xor(bytes(output[16:]), _derive_key(bytes(output[:16]), 12))[::-1]
    return _xor(body, _RSA_KEY)


def encrypt_downurl_payload(payload: dict[str, object]) -> str:
    return _encode(json.dumps(payload, separators=(",", ":")).encode("utf-8")).decode("ascii")


def decrypt_downurl_payload(payload: str) -> dict[str, object]:
    try:
        result = json.loads(_decode(payload))
    except Cloud115CipherError:
        raise
    except Exception as exc:
        raise Cloud115CipherError("115 downurl response is not JSON") from exc
    if not isinstance(result, dict):
        raise Cloud115CipherError("115 downurl payload is not an object")
    return result
