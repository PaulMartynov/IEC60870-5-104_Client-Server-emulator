import struct


class DeviceParametersException(Exception):
    def __init__(self, text):
        self.txt = text


def int_list_to_required_size(lst: list, size):
    while len(lst) < size:
        lst.append(0)
    return lst


def char_list_to_int_list(char_list: str, size):
    result = []
    for i in char_list:
        result.append(ord(i))
    while len(result) < size:
        result.append(0)
    return result


def int_list_to_char(lst: list):
    l2 = []
    for i in lst:
        if i == 0:
            continue
        l2.append(chr(i))
    return ''.join(l2)


def hex_to_int_list(x: str):
    rez = [int(x[i:i + 2], 16) for i in range(0, len(x), 2)]
    rez.reverse()
    return rez


def int_list_to_int(x: list):
    x.reverse()
    rez = ''
    for i in x:
        rez += hex(i)[2:]
    return int(rez, 16)


def int_to_byte_int_list(x: int):
    return [i for i in x.to_bytes((x.bit_length() + 7) // 8, 'little')]


def float_to_byte_int_list(x: float):
    result = list((struct.pack("!f", x)))
    result.reverse()
    return result


def name_1251_to_int_list(name: str):
    return [i for i in bytes(name, '1251')]


def name_1251_to_string(name: list):
    return bytearray(name).decode('1251')


def calc_crc16(data: bytes):
    data = bytearray(data)
    crc = 0xFFFF
    for b in data:
        crc ^= (0xFF & b)
        for _ in range(0, 8):
            if (crc & 0x0001) > 0:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc = (crc >> 1)

    return crc


def add_crc16(a_crc, a_ch):
    a_crc ^= (0xFF & a_ch)
    for _ in range(0, 8):
        if (a_crc & 0x0001) > 0:
            a_crc = (a_crc >> 1) ^ 0xA001
        else:
            a_crc = (a_crc >> 1)

    return a_crc


def check_crc(pack, pr):
    crc = calc_crc16(pack)
    crc = add_crc16(crc, pr)
    return int_list_to_required_size(int_to_byte_int_list(crc), 2)
