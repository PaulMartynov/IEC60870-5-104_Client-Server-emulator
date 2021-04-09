import IEC104.apci as apci
import IEC104.time_types as time_types
from Utils.IKZUtils import *
from datetime import datetime


def get_packet(ssn, rsn, type_id, sq, test, positive_negative, cot, org, asdu, objects: list):
    packet = apci.i_frame(ssn, rsn)     # Control field
    packet.append(type_id)      # ASDU type id
    # Single or Sequence, structure qualifier + num of elements(0-127)
    packet.append(int("{0:b}".format(sq) + "{0:b}".format(len(objects)), 2))
    # 0 (no test), 1 (test) + 0 (positive confirm), 1 (negative confirm) + Cause of transmission
    packet.append(int("{0:b}".format(test) + "{0:b}".format(positive_negative) + "{0:b}".format(cot), 2))
    # Originator address(ORG)
    packet.append(org)
    # ASDU address
    packet += int_list_to_required_size(int_to_byte_int_list(asdu), 2)
    if sq == 1:
        packet += objects[0].get_address()
        for i in range(1, len(objects)):
            packet += objects[i].get_packet(sq=True)
    else:
        for obj in objects:
            packet += obj.get_packet()

    return [104, len(packet)] + packet


class InfoObj(object):

    def __init__(self, data):
        self.ioa = data['IOA']  # Information Object Address uint:24

    def get_address(self):
        return int_list_to_required_size(int_to_byte_int_list(self.ioa), 3)

    def get_packet(self):
        return int_list_to_required_size(int_to_byte_int_list(self.ioa), 3)


class SIQ(InfoObj):
    def __init__(self, data):
        super(SIQ, self).__init__(data)
        self.iv = 0  # 0: valid, 1: invalid, bool
        self.nt = 0  # 0: relevant, 1: not relevant, bool
        self.sb = 0  # 0: no replacement, 1: replacement, bool
        self.bl = 0  # 0: not blocked, 1: blocked, bool
        self.reserve = '000'  # reserve uint:3
        self.spi = data['Value']  # 0: off, 1: on, bool
        # |IV|NT|SB|BL|0|0|0|SPI|

    def get_packet(self, sq=False):
        if sq:
            packet = []
        else:
            packet = int_list_to_required_size(int_to_byte_int_list(self.ioa), 3)
        packet.append(
            int("{0:b}".format(self.iv) + "{0:b}".format(self.nt) + "{0:b}".format(self.sb)
                + "{0:b}".format(self.bl) + self.reserve + "{0:b}".format(self.spi), 2)
        )
        return packet


class DIQ(InfoObj):
    def __init__(self, data):
        super(DIQ, self).__init__(data)
        self.iv = 0  # 0: valid, 1: invalid, bool
        self.nt = 0  # 0: relevant, 1: not relevant, bool
        self.sb = 0  # 0: no replacement, 1: replacement, bool
        self.bl = 0  # 0: not blocked, 1: blocked, bool
        self.reserve = '00'  # reserve uint:2
        self.dpi = data['Value']  # 0: intermediate state, 1: off, 2: on, 3: undefined state uint:2

    def get_packet(self, sq=False):
        dpi = "{0:b}".format(self.dpi)
        if len(dpi) < 2:
            dpi = '0' + dpi
        if sq:
            packet = []
        else:
            packet = int_list_to_required_size(int_to_byte_int_list(self.ioa), 3)
        packet.append(
            int("{0:b}".format(self.iv) + "{0:b}".format(self.nt) + "{0:b}".format(self.sb)
                + "{0:b}".format(self.bl) + self.reserve + dpi, 2)
        )
        return packet


class QDS(InfoObj):
    def __init__(self, data):
        super(QDS, self).__init__(data)
        self.overflow = 0
        self.reserve = '000'
        self.blocked = 0
        self.substituted = 0
        self.not_topical = 0
        self.invalid = 0
        # |IV|NT|SB|BL|0|0|0|OV|


class MSpNa1(SIQ):
    type_id = 1
    name = 'M_SP_NA_1'
    description = 'Single-point information without time tag'

    def __init__(self, data):
        super(MSpNa1, self).__init__(data)


class MSpTa1(InfoObj):
    type_id = 2
    name = 'M_SP_TA_1'
    description = 'Single-point information with time tag'

    def __init__(self, data):
        super(MSpTa1, self).__init__(data)


class MDpNa1(DIQ):
    type_id = 3
    name = 'M_DP_NA_1'
    description = 'Double-point information without time tag'

    def __init__(self, data):
        super(MDpNa1, self).__init__(data)


class MDpTa1(InfoObj):
    type_id = 4
    name = 'M_DP_TA_1'
    description = 'Double-point information with time tag'


class MStNa1(InfoObj):
    type_id = 5
    name = 'M_ST_NA_1'
    description = 'Step position information'


class MStTa1(InfoObj):
    type_id = 6
    name = 'M_ST_TA_1'
    description = 'Step position information with time tag'


class MBoNa1(InfoObj):
    type_id = 7
    name = 'M_BO_NA_1'
    description = 'Bitstring of 32 bit'


class MBoTa1(InfoObj):
    type_id = 8
    name = 'M_BO_TA_1'
    description = 'Bitstring of 32 bit with time tag'


class MMeNa1(InfoObj):
    type_id = 9
    name = 'M_ME_NA_1'
    description = 'Measured value, normalized value'

    def __init__(self, data):
        super(MMeNa1, self).__init__(data)


class MMeTa1(InfoObj):
    type_id = 10
    name = 'M_ME_TA_1'
    description = 'Measured value, normalized value with time tag'


class MMeNb1(InfoObj):
    type_id = 11
    name = 'M_ME_NB_1'
    description = 'Measured value, scaled value'


class MMeTb1(InfoObj):
    type_id = 12
    name = 'M_ME_TB_1'
    description = 'Measured value, scaled value with time tag'


class MMeNc1(QDS):
    type_id = 13
    name = 'M_ME_NC_1'
    description = 'Measured value, short floating point number'
    length = 5

    def __init__(self, data):
        super(MMeNc1, self).__init__(data)
        self.value = data['Value']

    def get_packet(self, sq=False):
        if sq:
            packet = []
        else:
            packet = int_list_to_required_size(int_to_byte_int_list(self.ioa), 3)
        packet += int_list_to_required_size(float_to_byte_int_list(self.value), 4)
        packet.append(
            int("{0:b}".format(self.invalid) + "{0:b}".format(self.not_topical) + "{0:b}".format(self.substituted)
                + "{0:b}".format(self.blocked) + self.reserve + "{0:b}".format(self.overflow), 2)
        )
        return packet


class MMeTc1(InfoObj):
    type_id = 14
    name = 'M_ME_TC_1'
    description = 'Measured value, short floating point number with time tag'


class MItNa1(InfoObj):
    type_id = 15
    name = 'M_IT_NA_1'
    description = 'Integrated totals'


class MItTa1(InfoObj):
    type_id = 16
    name = 'M_IT_TA_1'
    description = 'Integrated totals with time tag'


class MEpTa1(InfoObj):
    type_id = 17
    name = 'M_EP_TA_1'
    description = 'Event of protection equipment with time tag'


class MEpTb1(InfoObj):
    type_id = 18
    name = 'M_EP_TB_1'
    description = 'Packed start events of protection equipment with time tag'


class MEpTc1(InfoObj):
    type_id = 19
    name = 'M_EP_TC_1'
    description = 'Packed output circuit information of protection equipment with time tag'


class MPsNa1(InfoObj):
    type_id = 20
    name = 'M_PS_NA_1'
    description = 'Packed single-point information with status change detection'


class MMeNd1(InfoObj):
    type_id = 21
    name = 'M_ME_ND_1'
    description = 'Measured value, normalized value without quality descriptor'


class MSpTb1(SIQ):
    type_id = 30
    name = 'M_SP_TB_1'
    description = 'Single-point information with time tag CP56Time2a'

    def __init__(self, data):
        super(MSpTb1, self).__init__(data)

    def get_packet(self, sq=False):
        packet = super(MSpTb1, self).get_packet(sq)
        packet += time_types.time_to_cp56time2a(datetime.utcnow())
        return packet


class MDpTb1(InfoObj):
    type_id = 31
    name = 'M_DP_TB_1'
    description = 'Double-point information with time tag CP56Time2a'


class MStTb1(InfoObj):
    type_id = 32
    name = 'M_ST_TB_1'
    description = 'Step position information with time tag CP56Time2a'


class MBoTb1(InfoObj):
    type_id = 33
    name = 'M_BO_TB_1'
    description = 'Bitstring of 32 bits with time tag CP56Time2a'


class MMeTd1(InfoObj):
    type_id = 34
    name = 'M_ME_TD_1'
    description = 'Measured value, normalized value with time tag CP56Time2a'


class MMeTe1(InfoObj):
    type_id = 35
    name = 'M_ME_TE_1'
    description = 'Measured value, scaled value with time tag CP56Time2a'


class MMeTf1(MMeNc1):
    type_id = 36
    name = 'M_ME_TF_1'
    description = 'Measured value, short floating point number with time tag CP56Time2a'

    def __init__(self, data):
        super(MMeTf1, self).__init__(data)

    def get_packet(self, sq=False):
        packet = super(MMeTf1, self).get_packet()
        packet += time_types.time_to_cp56time2a(datetime.utcnow())
        return packet


class MItTb1(InfoObj):
    type_id = 37
    name = 'M_IT_TB_1'
    description = 'Integrated totals with time tag CP56Time2a'


class MEpTd1(InfoObj):
    type_id = 38
    name = 'M_EP_TD_1'
    description = 'Event of protection equipment with time tag CP56Time2a'


class MEpTe1(InfoObj):
    type_id = 39
    name = 'M_EP_TE_1'
    description = 'Packed start events of protection equipment with time tag CP56Time2a'


class MEpTf1(InfoObj):
    type_id = 40
    name = 'M_EP_TF_1'
    description = 'Packed output circuit information of protection equipment with time tag CP56Time2a'


class CScNa1(InfoObj):
    type_id = 45
    name = 'C_SC_NA_1'
    description = 'Single command'

    def __init__(self, data):
        super(CScNa1, self).__init__(data)
        self.data = data['Data']

    def get_packet(self, sq=False):
        if sq:
            packet = []
        else:
            packet = super(CScNa1, self).get_packet()
        packet.append(self.data)
        return packet


class CDcNa1(InfoObj):
    type_id = 46
    name = 'C_DC_NA_1'
    description = 'Double command'


class CRcNa1(InfoObj):
    type_id = 47
    name = 'C_RC_NA_1'
    description = 'Regulating step command'


class CSeNa1(InfoObj):
    type_id = 48
    name = 'C_SE_NA_1'
    description = 'Set-point command, normalized value'


class CSeNb1(InfoObj):
    type_id = 49
    name = 'C_SE_NB_1'
    description = 'Set-point command, scaled value'


class CSeNc1(InfoObj):
    type_id = 50
    name = 'C_SE_NC_1'
    description = 'Set-point command, short floating point number'

    def __init__(self, data):
        super(CSeNc1, self).__init__(data)
        self.val = data['Data']
        self.qds = data['QDS']

    def get_packet(self, sq=False):
        if sq:
            packet = []
        else:
            packet = super(CSeNc1, self).get_packet()
        packet += int_list_to_required_size(float_to_byte_int_list(self.val), 4)
        packet.append(
            int("{0:b}".format(self.qds.invalid) + "{0:b}".format(self.qds.not_topical) +
                "{0:b}".format(self.qds.substituted) + "{0:b}".format(self.qds.blocked) + self.qds.reserve +
                "{0:b}".format(self.qds.overflow), 2)
        )

        return packet


class CBoNa1(InfoObj):
    type_id = 51
    name = 'C_BO_NA_1'
    description = 'Bitstring of 32 bit'


class CScTa1(InfoObj):
    type_id = 58
    name = 'C_SC_TA_1'
    description = 'Single command with time tag CP56Time2a'

    def __init__(self, data):
        super(CScTa1, self).__init__(data)
        self.data = data['Data']
        self.time = data['Time']

    def get_packet(self, sq=False):
        if sq:
            packet = []
        else:
            packet = super(CScTa1, self).get_packet()
        packet.append(self.data)
        packet += time_types.time_to_cp56time2a(self.time)
        return packet


class MEiNa1(InfoObj):
    type_id = 70
    name = 'M_EI_NA_1'
    description = 'End of initialization'


class CIcNa1(InfoObj):
    type_id = 100
    name = 'C_IC_NA_1'
    description = 'Interrogation command'

    def __init__(self, data):
        super(CIcNa1, self).__init__(data)
        self.c_irg = data['C_irq']

    def get_packet(self, sq=False):
        if sq:
            packet = []
        else:
            packet = super(CIcNa1, self).get_packet()
        packet.append(self.c_irg)
        return packet


class CCiNa1(InfoObj):
    type_id = 101
    name = 'C_CI_NA_1'
    description = 'Counter interrogation command'


class CRdNa1(InfoObj):
    type_id = 102
    name = 'C_RD_NA_1'
    description = 'Read command'


class CCsNa1(InfoObj):
    type_id = 103
    name = 'C_CS_NA_1'
    description = 'Clock synchronization command'

    def __init__(self, data):
        super(CCsNa1, self).__init__(data)
        self.time = data['Time']

    def get_packet(self, sq=False):
        if sq:
            packet = []
        else:
            packet = super(CCsNa1, self).get_packet()
        packet += time_types.time_to_cp56time2a(self.time)
        return packet


class CTsNa1(InfoObj):
    type_id = 104
    name = 'C_TS_NA_1'
    description = 'Test command'


class CRpNa1(InfoObj):
    type_id = 105
    name = 'C_RP_NA_1'
    description = 'Reset process command'


class CCdNa1(InfoObj):
    type_id = 106
    name = 'C_CD_NA_1'
    descripiton = 'Delay acquisition command'


class PMeNa1(InfoObj):
    type_id = 110
    name = 'P_ME_NA_1'
    description = 'Parameter of measured values, normalized value'


class PMeNb1(InfoObj):
    type_id = 111
    name = 'P_ME_NB_1'
    description = 'Parameter of measured values, scaled value'


class PMeNc1(InfoObj):
    type_id = 112
    name = 'P_ME_NC_1'
    description = 'Parameter of measured values, short floating point number'


class PAcNa1(InfoObj):
    type_id = 113
    name = 'P_AC_NA_1'
    description = 'Parameter activation'


class FFrNa1(InfoObj):
    type_id = 120
    name = 'F_FR_NA_1'
    description = 'File ready'


class FSrNa1(InfoObj):
    type_id = 121
    name = 'F_SR_NA_1'
    description = 'Section ready'


class FScNa1(InfoObj):
    type_id = 122
    name = 'F_SC_NA_1'
    description = 'Call directory, select file, call file, call section'


class FLsNa1(InfoObj):
    type_id = 123
    name = 'F_LS_NA_1'
    description = 'Last section, last segment'


class FAdNa1(InfoObj):
    type_id = 124
    name = 'F_AF_NA_1'
    description = 'ACK file, ACK section'


class FSgNa1(InfoObj):
    type_id = 125
    name = 'F_SG_NA_1'
    description = 'Segment'


class FDrTa1(InfoObj):
    type_id = 126
    name = 'F_DR_TA_1'
    description = 'Directory'


info_types = {
    1: (MSpNa1, 4),
    13: (MMeNc1, 8),
    30: (MSpTb1, 11),
    36: (MMeTf1, 15),
}

data_types = {
    'bit': [1, 30],
    'float': [13, 36]
}

commands = {
    45: CScNa1,
    50: CSeNc1,
    58: CScTa1,
    100: CIcNa1,
    103: CCsNa1
}
