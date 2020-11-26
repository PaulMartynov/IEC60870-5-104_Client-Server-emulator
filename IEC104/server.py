import asyncio
import json
from datetime import datetime

from bitstring import ConstBitStream
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import IEC104.apci as apci
import IEC104.r_asdu as r_asdu
import IEC104.s_asdu as s_asdu
import config
from DataBase.device_db import Device, Iec104Devices, Base, check_or_create_db
from Devices.IKZUtils import DeviceParametersException
from Devices.dev_types import iec104_types


class Iec60850Server:

    def __init__(self, sn: int, dev_type=9999, host='', port=2404):

        if dev_type not in iec104_types:
            raise DeviceParametersException('Not supported dev_type: {}'.format(dev_type))

        check_or_create_db()

        engine = create_engine(config.DATABASE_URI, connect_args={'check_same_thread': False})
        Base.metadata.bind = engine
        db_session = sessionmaker(bind=engine)
        session = db_session()
        db_device = session.query(Device).filter_by(SN=sn).first()

        self.__loop = None      # Ссылка на event_loop, необходима для добавления новых задач
        self.__server = None    # Ссылка на сервер, необходима в основном для остановки
        self.__stopped = False  # Флаг оставновки сервера
        self.__tasks = []       # Список задач
        self._sn = sn

        if db_device is None:
            self.__host = host
            self.__port = port
            self._asdu_address = 1  # uint:16
            self._types = {}
            self._sporadic_types = {}
            self._telemetry = {}
            self._signals_desc = {}
            settings = {'types': self._types,
                        'sporadic_types': self._sporadic_types,
                        'host': self.__host,
                        'port': self.__port,
                        'asdu': self._asdu_address,
                        }
            db_device = Device(SN=self._sn, dev_type=dev_type,
                               settings=str(json.dumps(settings)),
                               telemetry=str(json.dumps(self._telemetry)))
            session.add(db_device)

            iec104_db_device = Iec104Devices(SN=self._sn, signals_description=str(json.dumps(self._signals_desc)),
                                             has_event=0)
            session.add(iec104_db_device)

            session.commit()

        else:
            if db_device.dev_type != dev_type:
                raise DeviceParametersException(
                    'Device {} has another type in data_base: {}'.format(db_device.SN, db_device.dev_type))
            settings = json.loads(db_device.settings)
            self._telemetry = json.loads(db_device.telemetry)
            if host != settings['host']:
                self.__host = host
            else:
                self.__host = settings['host']
            if port != settings['port']:
                self.__port = port
            else:
                self.__port = settings['port']
            self._asdu_address = settings['asdu']
            self._types = settings['types']
            self._sporadic_types = settings['sporadic_types']
            iec104_db_device = session.query(Iec104Devices).filter_by(SN=sn).first()
            self._signals_desc = json.loads(iec104_db_device.signals_description)

        session.close()
        engine.dispose()

        self._clients = {}
        self._commands = {      # Список команд для не стандартного ответа см __command_executor
            100: self._interrogation_command_answer,
        }

    """************************
        Внутренние функции
    ************************"""

    def _save_to_db(self):
        engine = create_engine(config.DATABASE_URI, connect_args={'check_same_thread': False})
        Base.metadata.bind = engine
        db_session = sessionmaker(bind=engine)
        session = db_session()

        db_device = session.query(Device).filter_by(SN=self._sn).first()
        iec104_db_device = session.query(Iec104Devices).filter_by(SN=self._sn).first()

        settings = {'types': self._types,
                    'sporadic_types': self._sporadic_types,
                    'host': self.__host,
                    'port': self.__port,
                    'asdu': self._asdu_address,
                    }

        db_device.settings = str(json.dumps(settings))
        db_device.telemetry = str(json.dumps(self._telemetry))
        iec104_db_device.signals_description = str(json.dumps(self._signals_desc))
        session.add(db_device)
        session.add(iec104_db_device)
        session.commit()
        session.close()
        engine.dispose()

    def _load_from_db(self):
        engine = create_engine(config.DATABASE_URI, connect_args={'check_same_thread': False})
        Base.metadata.bind = engine
        db_session = sessionmaker(bind=engine)
        session = db_session()

        db_device = session.query(Device).filter_by(SN=self._sn).first()
        self._telemetry = json.loads(db_device.telemetry)

        session.close()
        engine.dispose()

    def _no_events(self):
        engine = create_engine(config.DATABASE_URI, connect_args={'check_same_thread': False})
        Base.metadata.bind = engine
        db_session = sessionmaker(bind=engine)
        session = db_session()

        iec104_db_device = session.query(Iec104Devices).filter_by(SN=self._sn).first()

        session.close()
        engine.dispose()

        return iec104_db_device.has_event == 0

    def _reset_event_flag(self):
        engine = create_engine(config.DATABASE_URI, connect_args={'check_same_thread': False})
        Base.metadata.bind = engine
        db_session = sessionmaker(bind=engine)
        session = db_session()

        iec104_db_device = session.query(Iec104Devices).filter_by(SN=self._sn).first()
        iec104_db_device.has_event = 0
        session.add(iec104_db_device)
        session.commit()
        session.close()
        engine.dispose()

    """************************
        Настройка сервера
    ************************"""

    def is_stopped(self):
        return self.__stopped

    def set_value(self, addr: int, val):
        self._telemetry[str(addr)] = val
        self._save_to_db()

    def load(self, file_path):  # Загрузка списка регистров из csv-файла
        with open(file_path, 'r', encoding='utf-8') as file:
            for line in file.readlines():
                a = line.split(';')
                a_type = a[1]
                adr = a[0]

                data = 0
                if a_type in s_asdu.data_types['float']:
                    data = 0.0
                elif a_type in s_asdu.data_types['bit']:
                    data = 0
                self._telemetry[adr] = data

                if a_type not in self._types and int(a_type) not in s_asdu.sporadic_types:
                    self._types[a_type] = []
                if int(a_type) not in s_asdu.sporadic_types:
                    if adr not in self._types[a_type]:
                        self._types[a_type].append(adr)

                if int(a_type) in s_asdu.sporadic_types:
                    if a_type not in self._sporadic_types:
                        self._sporadic_types[a_type] = []
                    if adr not in self._sporadic_types[a_type]:
                        self._sporadic_types[a_type].append(adr)

                self._signals_desc[adr] = [a[2], a[3]]

    """************************
        Внешние команды
    ************************"""
    # 100
    async def _interrogation_command_answer(self, client_id, asdu):
        print('{} Receive from {}: {}'.format(datetime.now(), client_id, asdu.objs[0].get_info()))
        # Подтверждение активации 100-й команды
        packet = s_asdu.get_packet(self._clients[client_id]['ssn'], self._clients[client_id]['rsn'], asdu.type_id,
                                   asdu.sq, asdu.test, asdu.positive_negative, 7, asdu.org, asdu.asdu,
                                   [s_asdu.commands[100]({'IOA': 0, 'C_irq': asdu.objs[0].c_irq})])

        self._clients[client_id]['ssn'] += 1

        for obj in self._types:
            tmp_1 = self._types[obj].copy()

            while len(tmp_1) != 0:
                tmp_2 = []
                for i in range(len(tmp_1)):
                    if len(tmp_2) * s_asdu.info_types[int(obj)][1] + s_asdu.info_types[int(obj)][1] > 240 \
                            or len(tmp_1) == 0:
                        break
                    adr = tmp_1.pop(0)
                    sig = s_asdu.info_types[int(obj)][0]({'IOA': int(adr), 'Value': self._telemetry[adr]})
                    tmp_2.append(sig)
                # Пакет с сигналами
                packet += s_asdu.get_packet(self._clients[client_id]['ssn'], self._clients[client_id]['rsn'], int(obj),
                                            0, 0, 0, asdu.objs[0].c_irq, 0, self._asdu_address, tmp_2)
                self._clients[client_id]['ssn'] += 1

                if len(packet) + 255 >= 1024:
                    await self._send_answer(client_id, bytearray(packet))
                    packet.clear()
        # Деактивация
        packet += s_asdu.get_packet(self._clients[client_id]['ssn'], self._clients[client_id]['rsn'], asdu.type_id,
                                    asdu.sq, asdu.test, asdu.positive_negative, 10, asdu.org, asdu.asdu,
                                    [s_asdu.commands[100]({'IOA': 0, 'C_irq': asdu.objs[0].c_irq})])

        await self._send_answer(client_id, bytearray(packet))
        self._clients[client_id]['ssn'] += 1

    async def _send_s(self, client_id):
        packet = apci.s_frame(self._clients[client_id]['rsn'])
        await self._send_answer(client_id, bytearray([104, len(packet)] + packet))

    async def _answer(self, client_id, asdu=None):
        if asdu is None or asdu.type_id not in s_asdu.commands:
            # Если это не команда или хз что, то отправляем S-пакет
            await self._send_s(client_id)
        else:
            # Если команда, то подтверждение активации или деактивации
            tmp = []
            for obj in asdu.objs:
                print('{} Receive from {}: {}'.format(datetime.now(), client_id, obj.get_info()))
                data = obj.get_data()
                tmp.append(s_asdu.commands[asdu.type_id](data))
            packet = s_asdu.get_packet(self._clients[client_id]['ssn'], self._clients[client_id]['rsn'], asdu.type_id,
                                       asdu.sq, asdu.test, asdu.positive_negative, asdu.cot + 1, asdu.org, asdu.asdu,
                                       tmp)
            await self._send_answer(client_id, bytearray(packet))
            self._clients[client_id]['ssn'] += 1

    """********************************
        Обработка входящих пакетов
    ********************************"""

    async def _send_answer(self, client_id, answer):
        self._clients[client_id]['writer'].write(answer)
        await self._clients[client_id]['writer'].drain()
        print('{} Device: {} --> {}: [{}]'.format(datetime.now(), self._sn, client_id, answer.hex(' ').upper()))

    async def __read_message(self, message, client_id):
        commands = []
        pos = 0
        while True:
            msg_ctrl_fld = message[2:6]  # Control Field
            length = message[1]

            if msg_ctrl_fld[0] & 3 == 3:  # U-FRAME
                packet = apci.u_frame(msg_ctrl_fld)
                if packet:
                    await self._send_answer(client_id, bytearray([104, len(packet)] + packet))

            elif msg_ctrl_fld[0] & 3 == 1:  # S-FRAME
                self._clients[client_id]['ack'] = apci.parse_s_frame(bytearray(msg_ctrl_fld))

            elif msg_ctrl_fld[0] & 3 == 0 or msg_ctrl_fld[0] & 3 == 2:  # I-FRAME
                asdu = r_asdu.ASDU(ConstBitStream(bytes=message, offset=6 * 8))
                self._clients[client_id]['rsn'] += 1
                commands.append((asdu.type_id, asdu))

            pos += length + 2

            if pos >= len(message):
                break

            message = message[pos:]
        self.__command_executor(commands, client_id)

    """************************
        Задания для сервера
    ************************"""

    def __command_executor(self, commands, client_id):
        for command in commands:
            if command[0] in self._commands:
                task = self.__loop.create_task(self._commands[command[0]](client_id, command[1]))
                self.__tasks.append(task)
            else:
                task = self.__loop.create_task(self._answer(client_id, command[1]))
                self.__tasks.append(task)

    async def __handle_client(self, reader, writer):
        client_id = writer.get_extra_info('peername')
        if self.__stopped:  # Крайний случай, надо проверить
            print('{} ******* Server closed error, disconnect client {}.  *******'.format(datetime.now(), client_id))
            writer.close()
            self.__server.close()
            return

        print('{} ******* Client {} connected.  *******'.format(datetime.now(), client_id))
        self._clients[client_id] = {'writer': writer, 'ssn': 0, 'rsn': 0, 'ack': 0}

        while True:

            if self._clients[client_id]['ssn'] >= 65535 or self._clients[client_id]['rsn'] >= 65535:
                writer.write(bytearray([104, len(apci.STOPDT_ACT)] + apci.STOPDT_ACT))
                await writer.drain()
                print('{} ******* Close connection with {}: ssn or rsn overflow. *******'.format(datetime.now(),
                                                                                                 client_id))
                break

            try:
                msg = await reader.read(1024)

                if not msg:
                    print('{} ******* Client {} disconnected. *******'.format(datetime.now(), client_id))
                    break

                print('{} Device: {} <-- {}: [{}]'.format(datetime.now(), self._sn, client_id, msg.hex(' ').upper()))
                message = [x for x in msg]
                await self.__read_message(message, client_id)

            except ConnectionResetError:
                print('{} ******* Client {}: reset connection. *******'.format(datetime.now(), client_id))
                break

            except OSError:
                print('{} ******* Client {}: connection lost. *******'.format(datetime.now(), client_id))
                break

        del self._clients[client_id]
        writer.close()

    async def __sporadic_message(self):
        while True:
            await asyncio.sleep(0.3)
            if self._no_events():
                continue
            telemetry_temp = self._telemetry.copy()
            self._load_from_db()

            for client_id in self._clients.keys():
                packet = []
                for obj in self._sporadic_types:
                    tmp_1 = self._sporadic_types[obj].copy()

                    while len(tmp_1) != 0:
                        tmp_2 = []
                        for i in range(len(tmp_1)):
                            if len(tmp_2) * s_asdu.info_types[int(obj)][1] + s_asdu.info_types[int(obj)][1] > 240 \
                                    or len(tmp_1) == 0:
                                break
                            adr = tmp_1.pop(0)
                            if telemetry_temp[adr] == self._telemetry[adr]:
                                continue
                            sig = s_asdu.info_types[int(obj)][0]({'IOA': int(adr), 'Value': self._telemetry[adr]})
                            tmp_2.append(sig)
                        if len(tmp_2) == 0:
                            continue
                        packet += s_asdu.get_packet(self._clients[client_id]['ssn'], self._clients[client_id]['rsn'],
                                                    int(obj), 0, 0, 0, 3, 0, self._asdu_address, tmp_2)
                        self._clients[client_id]['ssn'] += 1
                if len(packet) == 0:
                    continue

                await self._send_answer(client_id, bytearray(packet))

            self._reset_event_flag()

    async def __check_connection(self):
        while True:
            await asyncio.sleep(90)
            for client_id in self._clients.keys():
                packet = [104, len(apci.TESTFR_ACT)] + apci.TESTFR_ACT
                await self._send_answer(client_id, bytearray(packet))

    """************************
        Запуск и остановка
    ************************"""

    def stop(self):
        self.__stopped = True
        self.__loop.stop()  # На всякий случай, так как finally срабатывает не всегда
        for task in self.__tasks:
            task.cancel()
        for client_id in self._clients.keys():
            try:
                self._clients[client_id]['writer'].close()
            except KeyError:
                pass
        try:
            self.__server.close()
        except TypeError:
            # Иногда выскакивает ошибка в библиотеке asyncio, вместо списка waiters приходит None
            print('{} ******* TypeError: None type in waiters list: Python3/lib/asyncio/base_events.py *******'
                  ''.format(datetime.now()))
        finally:
            self.__loop.stop()

    def run(self):
        self.__stopped = False
        self.__loop = asyncio.get_event_loop()
        server_cor = asyncio.start_server(self.__handle_client, self.__host, self.__port)
        self.__server = self.__loop.run_until_complete(server_cor)
        check_connection_task = self.__loop.create_task(self.__check_connection())
        send_sporadic_task = self.__loop.create_task(self.__sporadic_message())
        self.__tasks.append(check_connection_task)
        self.__tasks.append(send_sporadic_task)
        try:
            self.__loop.run_forever()
        finally:
            self.__loop.run_until_complete(self.__server.wait_closed())
            self.__loop.close()
            print('{} ******* Device: {} stopped. *******'.format(datetime.now(), self._sn))
