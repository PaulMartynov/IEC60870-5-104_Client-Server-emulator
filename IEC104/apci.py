import struct

TESTFR_CON = [131, 0, 0, 0]     # \x83\x00\x00\x00
TESTFR_ACT = [67, 0, 0, 0]      # \x43\x00\x00\x00

STOPDT_CON = [35, 0, 0, 0]      # \x23\x00\x00\x00
STOPDT_ACT = [19, 0, 0, 0]      # \x13\x00\x00\x00

STARTDT_CON = [11, 0, 0, 0]     # \x0b\x00\x00\x00
STARTDT_ACT = [7, 0, 0, 0]      # \x07\x00\x00\x00


def u_frame(data):
    if data == STARTDT_ACT:
        return STARTDT_CON
    elif data == STOPDT_ACT:
        return STOPDT_CON
    elif data == TESTFR_ACT:
        return TESTFR_CON
    else:
        return []


def i_frame(send_seq_n, receive_seq_n):
    return [x for x in struct.pack('<2H', send_seq_n << 1, receive_seq_n << 1)]


def s_frame(receive_seq_n):
    return [x for x in struct.pack('<2BH', 0x01, 0x00, receive_seq_n << 1)]


def parse_i_frame(data):
    send_seq_n, receive_seq_n = struct.unpack('<2H', data)
    return send_seq_n >> 1, receive_seq_n >> 1


def parse_s_frame(data):
    receive_seq_n = struct.unpack_from('<2H', data)[1]
    return receive_seq_n >> 1
