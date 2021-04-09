import struct


def int_list_to_required_size(lst: list, size):
    while len(lst) < size:
        lst.append(0)
    return lst


def invert_number(n, bits=8):
    mask = (1 << bits) - 1
    if n < 0:
        n = ((abs(n) ^ mask) + 1)
    return n & mask


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


def hex_list_to_int_list(x):
    rez = [int(i, 16) for i in x]
    return rez


def int_list_to_int(x: list):
    # x.reverse()
    rez = b''
    for i in x:
        # rez += hex(i)[2:]
        rez += i.to_bytes((i.bit_length() + 7) // 8, 'little')
    return int.from_bytes(rez, byteorder='little')  # int(rez, 16)


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
