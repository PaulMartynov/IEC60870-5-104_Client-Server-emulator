from IEC104.types import cp56time2a_to_time


class ASDU(object):
    def __init__(self, data):

        self.type_id = data.read('uint:8')
        self.sq = data.read('bool')                 # Single or Sequence, structure qualifier
        self.sq_count = data.read('uint:7')         # Num of elements(0-127)
        self.test = data.read('bool')               # 0 (no test), 1 (test)
        self.positive_negative = data.read('bool')  # 0 (positive confirm), 1 (negative confirm)
        self.cot = data.read('uint:6')              # Cause of transmission
        self.org = data.read('uint:8')              # Originator address(ORG)
        self.asdu = data.read('uintle:16')          # ASDU address

        self.objs = []
        if not self.sq:
            for i in range(self.sq_count):
                if self.type_id in types:
                    obj = types[self.type_id](data)
                    self.objs.append(obj)


class QDS(object):
    def __init__(self, data):
        self.invalid = data.read('bool')
        self.not_topical = data.read('bool')
        self.substituted = data.read('bool')
        self.blocked = data.read('bool')
        data.read('int:3')  # reserve
        self.reserve = '000'
        self.overflow = data.read('bool')


class InfoObj(object):

    def __init__(self, data, addr=0, sq=False):
        if sq:
            self.ioa = addr
        else:
            self.ioa = data.read("uintle:24")


class SIQ(InfoObj):
    def __init__(self, data, addr=0, sq=False):
        if sq:
            super(SIQ, self).__init__(data, addr, sq)
        else:
            super(SIQ, self).__init__(data)
        self.iv = data.read('bool')
        self.nt = data.read('bool')
        self.sb = data.read('bool')
        self.bl = data.read('bool')
        data.read('int:3')  # reserve
        self.spi = data.read('bool')

    def get_data(self):
        return {'IOA': self.ioa, 'Data': int("{0:b}".format(self.iv) + "{0:b}".format(self.nt) + "{0:b}".format(self.sb)
                                             + "{0:b}".format(self.bl) + '000' + "{0:b}".format(self.spi), 2)}


class DIQ(InfoObj):
    def __init__(self, data, addr=0, sq=False):
        if sq:
            super(DIQ, self).__init__(data, addr, sq)
        else:
            super(DIQ, self).__init__(data)
        self.iv = data.read('bool')
        self.nt = data.read('bool')
        self.sb = data.read('bool')
        self.bl = data.read('bool')
        data.read('int:2')  # reserve
        self.dpi = data.read('uint:2')

    def get_data(self):
        return {'IOA': self.ioa, 'Data': int("{0:b}".format(self.iv) + "{0:b}".format(self.nt) + "{0:b}".format(self.sb)
                                             + "{0:b}".format(self.bl) + '00' + "{0:b}".format(self.dpi), 2)}


class MSpNa1(SIQ):
    type_id = 1
    name = 'M_SP_NA_1'
    description = 'Single-point information without time tag'

    def __init__(self, data):
        super(MSpNa1, self).__init__(data)

    def get_info(self):
        return 'Type: {}. Single-point information with adr: {}, data: {}'.format(MSpNa1.type_id, self.ioa, self.spi)


class MSpTa1(InfoObj):
    type_id = 2
    name = 'M_SP_TA_1'
    description = 'Single-point information with time tag CP24Time2a'

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
    description = 'Double-point information with time tag CP24Time2a'


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


class MMeNc1(InfoObj):
    type_id = 13
    name = 'M_ME_NC_1'
    description = 'Measured value, short floating point number'
    length = 5

    def __init__(self, data):
        super(MMeNc1, self).__init__(data)

        self.val = data.read("floatle:32")

        self.qds = QDS(data)

    def get_data(self):
        return {'IOA': self.ioa, 'Data': self.val, 'QDS': self.qds}

    def get_info(self):
        return 'Type: {}. Measured value with adr: {}, data: {}'.format(MMeNc1.type_id, self.ioa, self.val)


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

        time_data = []
        for i in range(7):
            time_data.append(data.read('uint:8'))
        self.time = cp56time2a_to_time(time_data)

    def get_data(self):
        data = super(MSpTb1, self).get_data()
        data['Time'] = self.time
        return data

    def get_info(self):
        return 'Type: {}. Single-point information with adr: {}, data: {} and time tag: {}'.format(MSpTb1.type_id,
                                                                                                   self.ioa, self.spi,
                                                                                                   self.time)


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

        time_data = []
        for i in range(7):
            time_data.append(data.read('uint:8'))
        self.time = cp56time2a_to_time(time_data)

    def get_data(self):
        data = super(MMeTf1, self).get_data()
        data['Time'] = self.time
        return data

    def get_info(self):
        return 'Type: {}. Measured value with adr: {}, data: {} and time tag: {}'.format(MMeTf1.type_id, self.ioa,
                                                                                         self.val, self.time)


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


class CScNa1(SIQ):
    type_id = 45
    name = 'C_SC_NA_1'
    description = 'Single command'

    def __init__(self, data):
        super(CScNa1, self).__init__(data)

    def get_info(self):
        return 'Type: {}. Single command with adr: {}, data: {}'.format(CScNa1.type_id, self.ioa, self.spi)


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

        self.val = data.read("floatle:32")

        self.qds = QDS(data)

    def get_data(self):
        return {'IOA': self.ioa, 'Data': self.val, 'QDS': self.qds}

    def get_info(self):
        return 'Type: {}. Set-point command with adr: {}, data: {}'.format(CSeNc1.type_id, self.ioa, self.val)


class CBoNa1(InfoObj):
    type_id = 51
    name = 'C_BO_NA_1'
    description = 'Bitstring of 32 bit'


class CScTa1(SIQ):
    type_id = 58
    name = 'C_SC_TA_1'
    description = 'Single command with time tag CP56Time2a'

    def __init__(self, data):
        super(CScTa1, self).__init__(data)

        time_data = []
        for i in range(7):
            time_data.append(data.read('uint:8'))
        self.time = cp56time2a_to_time(time_data)

    def get_data(self):
        data = super(CScTa1, self).get_data()
        data['Time'] = self.time
        return data

    def get_info(self):
        return 'Type: {}. Single command with adr: {}, spi: {} and time tag: {}'.format(CScTa1.type_id, self.ioa,
                                                                                        self.spi,
                                                                                        self.time)


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
        self.c_irq = data.read('uint:8')  # Counter interrogation request qualifier

    def get_data(self):
        return {'IOA': self.ioa, 'C_irq': self.c_irq}

    def get_info(self):
        return 'Type: {}. Interrogation command with counter interrogation request qualifier: {}'.format(CIcNa1.type_id,
                                                                                                         self.c_irq)


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
        time_data = []
        for i in range(7):
            time_data.append(data.read('uint:8'))
        self.time = cp56time2a_to_time(time_data)

    def get_data(self):
        return {'IOA': self.ioa, 'Time': self.time}

    def get_info(self):
        return 'Type: {}. Clock synchronization command with time tag: {}'.format(CCsNa1.type_id, self.time)


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
    description = 'Delay acquisition command'


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


# То что можно сейчас прочитать
types = {
    1: MSpNa1,
    13: MMeNc1,
    30: MSpTb1,
    36: MMeTf1,
    45: CScNa1,
    50: CSeNc1,
    58: CScTa1,
    100: CIcNa1,
    103: CCsNa1
}
