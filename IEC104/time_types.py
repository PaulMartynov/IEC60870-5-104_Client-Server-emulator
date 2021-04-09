from datetime import datetime

import Utils.IKZUtils as Utils


def cp56time2a_to_time(buf):
    milliseconds = (buf[1] & 0xFF) << 8 | (buf[0] & 0xFF)
    microseconds = milliseconds % 1000 * 1000
    seconds = int(milliseconds // 1000)
    minutes = buf[2] & 0x3F
    hours = buf[3] & 0x1F
    day = buf[4] & 0x1F
    month = (buf[5] & 0x0F)     # Проверить на корректность
    year = (buf[6] & 0x7F) + 2000

    return datetime(year, month, day, hours, minutes, seconds, microseconds)


def time_to_cp56time2a(date):
    def to_req_size(data, size):
        while len(data) < size:
            data = '0' + data
        return data

    milliseconds = int(float('{}.{}'.format(date.second, date.microsecond)) * 1000)  # uint:16

    # IVResMinute, Bit 7 = IV (invalid time), Bit 6 = Res (spare bit),  Bit 0..5 = Minutes (0..59min)
    minutes = int('00' + to_req_size("{0:b}".format(date.minute), 6), 2)  # uint:8

    # SURes2Hour, Bit 7 = SU (1=summer time, 0=normal time), Bits 5..6 = Res2, Bits 0..4 = Hours (0..23)
    hours = int('000' + to_req_size("{0:b}".format(date.hour), 5), 2)  # uint:8

    # DOWDay, Bits 5..7 = Day of week (1..7, not used 0 !!!), Bits 0..4 = Day of month (1..31)
    day = int(to_req_size("{0:b}".format(date.isoweekday()), 3) + to_req_size("{0:b}".format(date.day), 5), 2)  # uint:8

    # Res3Month, Bits 4..7 = Res3 (spare bits), Bits 0..3 = Month (1..12)
    month = int('0000' + to_req_size("{0:b}".format(date.month), 4), 2)  # uint:8

    # Res4Year, Bit 7 = Res4, Bits 0..6 = Year (0..99)
    year = int('0' + to_req_size("{0:b}".format(int(date.strftime('%y'))), 7), 2)  # uint:8

    result = Utils.int_list_to_required_size(Utils.int_to_byte_int_list(milliseconds), 2)
    result.append(minutes)
    result.append(hours)
    result.append(day)
    result.append(month)
    result.append(year)

    return result
