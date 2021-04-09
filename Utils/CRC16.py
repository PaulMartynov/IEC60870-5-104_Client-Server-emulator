from Utils.IKZUtils import int_list_to_required_size, int_to_byte_int_list


def update_crc16(crc):
    for _ in range(0, 8):
        if (crc & 0x0001) > 0:
            crc = (crc >> 1) ^ 0xA001
        else:
            crc = (crc >> 1)
    return crc


def calc_crc16(data: bytes):
    data = bytearray(data)
    crc = 0xFFFF
    for b in data:
        crc ^= (0xFF & b)
        crc = update_crc16(crc)
    return crc


def add_crc16(a_crc, a_ch):
    a_crc ^= (0xFF & a_ch)
    return update_crc16(a_crc)


def check_crc(pack, pr):
    crc = calc_crc16(pack)
    crc = add_crc16(crc, pr)
    return int_list_to_required_size(int_to_byte_int_list(crc), 2)