"""The small RSA/XOR codec used by 115's ``downurl`` endpoint."""

from __future__ import annotations

import base64
import json
from binascii import crc32
from hashlib import md5, sha1
from time import time
from urllib.parse import urlencode

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

# Cookie 秒传初始化与 downurl 的 RSA/XOR 协议完全独立。以下常量与算法严格
# 对齐插件化前的 Cloud115 SDK；不要把 downurl 的 cipher 复用于 uplb。
_UPLOAD_AES_KEY = bytes.fromhex("fb1a19d652f5aaf7bc651d0f69bf422f")
_UPLOAD_AES_IV = bytes.fromhex("69bf422f49960550a0ad44ec3446cb4c")
_UPLOAD_AES_PUBKEY = bytes.fromhex(
    "1d030e80a178dceececda377de128d8ed9ddcf55ae61ed46ea121a1cfc81"
)
_UPLOAD_CRC_SALT = b"^j>WD3Kr?J2gLFjD4W2y@"
_UPLOAD_MD5_SALT = b"Qclm8MGWUv59TnrR0XPg"


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


# ---- uplb initupload AES-CBC / LZ4 protocol --------------------------------


def _gf_mul(left: int, right: int) -> int:
    result = 0
    for _ in range(8):
        if right & 1:
            result ^= left
        high = left & 0x80
        left = (left << 1) & 0xFF
        if high:
            left ^= 0x1B
        right >>= 1
    return result


def _gf_pow(value: int, exponent: int) -> int:
    result = 1
    while exponent:
        if exponent & 1:
            result = _gf_mul(result, value)
        value = _gf_mul(value, value)
        exponent >>= 1
    return result


def _rotl8(value: int, shift: int) -> int:
    return ((value << shift) | (value >> (8 - shift))) & 0xFF


def _make_sboxes() -> tuple[tuple[int, ...], tuple[int, ...]]:
    sbox: list[int] = []
    inverse = [0] * 256
    for value in range(256):
        inverse_value = 0 if value == 0 else _gf_pow(value, 254)
        substituted = (
            inverse_value
            ^ _rotl8(inverse_value, 1)
            ^ _rotl8(inverse_value, 2)
            ^ _rotl8(inverse_value, 3)
            ^ _rotl8(inverse_value, 4)
            ^ 0x63
        )
        sbox.append(substituted)
        inverse[substituted] = value
    return tuple(sbox), tuple(inverse)


_UPLOAD_SBOX, _UPLOAD_SBOX_INV = _make_sboxes()


def _upload_round_keys(key: bytes) -> tuple[bytes, ...]:
    if len(key) != 16:
        raise ValueError("AES-128 requires a 16-byte key")
    words = [bytearray(key[offset : offset + 4]) for offset in range(0, 16, 4)]
    rcon = 1
    while len(words) < 44:
        word = bytearray(words[-1])
        if len(words) % 4 == 0:
            word = bytearray(_UPLOAD_SBOX[b] for b in (word[1], word[2], word[3], word[0]))
            word[0] ^= rcon
            rcon = _gf_mul(rcon, 2)
        word = bytearray(a ^ b for a, b in zip(word, words[-4]))
        words.append(word)
    return tuple(
        bytes(byte for word in words[offset : offset + 4] for byte in word)
        for offset in range(0, 44, 4)
    )


def _upload_add_round_key(state: list[int], key: bytes) -> None:
    for index in range(16):
        state[index] ^= key[index]


def _upload_sub_bytes(state: list[int], box: tuple[int, ...]) -> None:
    for index, value in enumerate(state):
        state[index] = box[value]


def _upload_shift_rows(state: list[int], inverse: bool = False) -> None:
    original = state[:]
    for row in range(4):
        for column in range(4):
            source_column = (column - row) % 4 if inverse else (column + row) % 4
            state[column * 4 + row] = original[source_column * 4 + row]


def _upload_mix_columns(state: list[int], inverse: bool = False) -> None:
    for column in range(4):
        offset = column * 4
        a0, a1, a2, a3 = state[offset : offset + 4]
        if inverse:
            state[offset : offset + 4] = [
                _gf_mul(a0, 14) ^ _gf_mul(a1, 11) ^ _gf_mul(a2, 13) ^ _gf_mul(a3, 9),
                _gf_mul(a0, 9) ^ _gf_mul(a1, 14) ^ _gf_mul(a2, 11) ^ _gf_mul(a3, 13),
                _gf_mul(a0, 13) ^ _gf_mul(a1, 9) ^ _gf_mul(a2, 14) ^ _gf_mul(a3, 11),
                _gf_mul(a0, 11) ^ _gf_mul(a1, 13) ^ _gf_mul(a2, 9) ^ _gf_mul(a3, 14),
            ]
        else:
            state[offset : offset + 4] = [
                _gf_mul(a0, 2) ^ _gf_mul(a1, 3) ^ a2 ^ a3,
                a0 ^ _gf_mul(a1, 2) ^ _gf_mul(a2, 3) ^ a3,
                a0 ^ a1 ^ _gf_mul(a2, 2) ^ _gf_mul(a3, 3),
                _gf_mul(a0, 3) ^ a1 ^ a2 ^ _gf_mul(a3, 2),
            ]


def _upload_encrypt_block(block: bytes, round_keys: tuple[bytes, ...]) -> bytes:
    state = list(block)
    _upload_add_round_key(state, round_keys[0])
    for key in round_keys[1:-1]:
        _upload_sub_bytes(state, _UPLOAD_SBOX)
        _upload_shift_rows(state)
        _upload_mix_columns(state)
        _upload_add_round_key(state, key)
    _upload_sub_bytes(state, _UPLOAD_SBOX)
    _upload_shift_rows(state)
    _upload_add_round_key(state, round_keys[-1])
    return bytes(state)


def _upload_decrypt_block(block: bytes, round_keys: tuple[bytes, ...]) -> bytes:
    state = list(block)
    _upload_add_round_key(state, round_keys[-1])
    for key in reversed(round_keys[1:-1]):
        _upload_shift_rows(state, inverse=True)
        _upload_sub_bytes(state, _UPLOAD_SBOX_INV)
        _upload_add_round_key(state, key)
        _upload_mix_columns(state, inverse=True)
    _upload_shift_rows(state, inverse=True)
    _upload_sub_bytes(state, _UPLOAD_SBOX_INV)
    _upload_add_round_key(state, round_keys[0])
    return bytes(state)


def _upload_pad(data: bytes) -> bytes:
    size = 16 - (len(data) % 16)
    return data + bytes((size,)) * size


def _upload_unpad(data: bytes) -> bytes:
    if not data:
        return data
    size = data[-1]
    # uplb has been observed returning zero-padded frames. Only strip valid
    # PKCS#7 so that the LZ4 decoder can consume either representation.
    if 0 < size <= 16 and data[-size:] == bytes((size,)) * size:
        return data[:-size]
    return data


def _upload_aes_cbc_encrypt(data: bytes) -> bytes:
    round_keys = _upload_round_keys(_UPLOAD_AES_KEY)
    previous = _UPLOAD_AES_IV
    output = bytearray()
    padded = _upload_pad(data)
    for offset in range(0, len(padded), 16):
        block = padded[offset : offset + 16]
        encrypted = _upload_encrypt_block(
            bytes(left ^ right for left, right in zip(block, previous)), round_keys
        )
        output.extend(encrypted)
        previous = encrypted
    return bytes(output)


def _upload_aes_cbc_decrypt(data: bytes) -> bytes:
    data = data[: len(data) & -16]
    if not data:
        raise Cloud115CipherError("115 upload response has no complete AES block")
    round_keys = _upload_round_keys(_UPLOAD_AES_KEY)
    previous = _UPLOAD_AES_IV
    output = bytearray()
    for offset in range(0, len(data), 16):
        current = data[offset : offset + 16]
        decrypted = _upload_decrypt_block(current, round_keys)
        output.extend(left ^ right for left, right in zip(decrypted, previous))
        previous = current
    return _upload_unpad(bytes(output))


def _upload_lz4_block_decompress(data: bytes) -> bytes:
    output = bytearray()
    cursor = 0
    while cursor < len(data):
        token = data[cursor]
        cursor += 1
        literal_length = token >> 4
        if literal_length == 15:
            while cursor < len(data) and data[cursor] == 255:
                literal_length += 255
                cursor += 1
            if cursor >= len(data):
                raise Cloud115CipherError("invalid 115 upload LZ4 literal length")
            literal_length += data[cursor]
            cursor += 1
        end = cursor + literal_length
        if end > len(data):
            raise Cloud115CipherError("truncated 115 upload LZ4 literals")
        output.extend(data[cursor:end])
        cursor = end
        if cursor >= len(data):
            break
        if cursor + 2 > len(data):
            raise Cloud115CipherError("invalid 115 upload LZ4 match offset")
        match_offset = data[cursor] | (data[cursor + 1] << 8)
        cursor += 2
        if match_offset <= 0 or match_offset > len(output):
            raise Cloud115CipherError("invalid 115 upload LZ4 match offset")
        match_length = token & 0x0F
        if match_length == 15:
            while cursor < len(data) and data[cursor] == 255:
                match_length += 255
                cursor += 1
            if cursor >= len(data):
                raise Cloud115CipherError("invalid 115 upload LZ4 match length")
            match_length += data[cursor]
            cursor += 1
        match_length += 4
        for _ in range(match_length):
            output.append(output[-match_offset])
    return bytes(output)


def _upload_lz4_decompress(data: bytes) -> bytes:
    output = bytearray()
    cursor = 0
    while cursor + 2 < len(data):
        compressed_length = int.from_bytes(data[cursor : cursor + 2], "little")
        cursor += 2
        if not compressed_length:
            break
        end = cursor + compressed_length
        if end > len(data):
            raise Cloud115CipherError("truncated 115 upload LZ4 frame")
        output.extend(_upload_lz4_block_decompress(data[cursor:end]))
        cursor = end
    return bytes(output)


def ecdh_encode_upload_token(timestamp: int) -> str:
    token = bytearray()
    token.extend(_UPLOAD_AES_PUBKEY[:15])
    token.extend(b"\x00s\x00\x00\x00")
    token.extend(int(timestamp).to_bytes(4, "little"))
    token.extend(_UPLOAD_AES_PUBKEY[15:])
    token.extend(b"\x00\x01\x00\x00\x00")
    token.extend(int(crc32(_UPLOAD_CRC_SALT + token) & 0xFFFFFFFF).to_bytes(4, "little"))
    return base64.b64encode(bytes(token)).decode("ascii")


def make_upload_payload(payload: dict[str, object]) -> tuple[dict[str, str], bytes]:
    payload = dict(payload)
    timestamp = int(time())
    payload["t"] = timestamp
    userkey = str(payload["userkey"])
    signature = sha1(userkey.encode("ascii"))
    signature.update(
        sha1(
            f"{payload['userid']}{payload['fileid']}{payload['target']}0".encode("ascii")
        ).hexdigest().encode("ascii")
    )
    signature.update(b"000000")
    payload["sig"] = signature.hexdigest().upper()
    token = md5(_UPLOAD_MD5_SALT)
    token.update(
        f"{payload['fileid']}{payload['filesize']}{payload['sign_key']}"
        f"{payload['sign_val']}{payload['userid']}{timestamp}".encode("ascii")
    )
    token.update(md5(str(int(payload["userid"])).encode("ascii")).hexdigest().encode("ascii"))
    token.update(str(payload["appversion"]).encode("ascii"))
    payload["token"] = token.hexdigest()
    body = urlencode(
        sorted((key, value) for key, value in payload.items() if value)
    ).encode("latin-1")
    return {"k_ec": ecdh_encode_upload_token(timestamp)}, _upload_aes_cbc_encrypt(body)


def decrypt_upload_response(content: bytes) -> dict[str, object]:
    try:
        result = json.loads(_upload_lz4_decompress(_upload_aes_cbc_decrypt(content)))
    except Cloud115CipherError:
        raise
    except (ValueError, json.JSONDecodeError) as exc:
        raise Cloud115CipherError("115 upload response is not valid encrypted JSON") from exc
    if not isinstance(result, dict):
        raise Cloud115CipherError("115 upload response is not an object")
    return result
