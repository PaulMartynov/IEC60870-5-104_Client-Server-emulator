import asyncio
from datetime import datetime

from bitstring import ConstBitStream

import IEC104.apci as apci
import IEC104.r_asdu as r_asdu
import IEC104.s_asdu as s_asdu
from DataBase.DataExecutor import DataExecutor
from Devices.dev_types import iec104_types
from Utils.Exceptions import SequenceErrorException, DeviceParametersException
from Utils.logger import DeviceLogger


class Iec60870Server:

    def __init__(self, sn: int, dev_type=9999, host='', port=2404, logger=None):

        if logger is not None:
            self.__logger = logger
        else:
            self.__logger = DeviceLogger.get_logger(f'iec104_server_{sn}')

        if dev_type not in iec104_types:
            message = f'Not supported dev_type: {dev_type}'
            self.__logger.error(message)
            raise DeviceParametersException(message)

        self.__loop = None  # Ссылка на event_loop, необходима для добавления новых задач
        self.__server = None  # Ссылка на сервер, необходима в основном для остановки
        self.__stopped = False  # Флаг оставновки сервера
        self.__tasks = []  # Список задач на случай их отмены перед остановкой сервера
        self._sn = sn

        self._db_executor = DataExecutor(sn, logger=self.__logger)

        db_data = self._db_executor.load_device(dev_type, host, port)

        # Задержка для имитации работы сети.
        self._holdup = db_data['settings']['holdup']

        # Таймауты из протокола:
        self.__t0 = db_data['settings']['t0']  # Таймаут при установлении соединения
        self.__t1 = db_data['settings']['t1']  # Таймаут при посылке или тестировании APDU
        self.__t2 = db_data['settings']['t2']  # Таймаут для подтверждения в случае отсутствия сообщения с данными t2<t1
        self.__t3 = db_data['settings']['t3']  # Таймаут для посылки блоков тестирования в случае долгого простоя

        self.__k = db_data['settings']['k']  # Максимальное число неподтверждённых APDU форматта I
        self.__w = db_data['settings']['w']  # Последнее подтверждение после приёма w APDU формата I

        self.__host = db_data['settings']['host']
        self.__port = db_data['settings']['port']
        self._asdu_address = db_data['settings']['asdu']  # uint:16
        self._types = db_data['settings']['types']
        self._sporadic_types = db_data['settings']['sporadic_types']
        self._telemetry = db_data['telemetry']
        self._signals_desc = db_data['signals_desc']

        self._events = []
        self._clients = {}
        self._commands = {  # Список команд для не стандартного ответа см __command_executor
            100: self._interrogation_command_answer,
        }

    """************************
        Внутренние функции
    ************************"""

    def _save_to_db(self):
        self.__logger.debug("Saving iec104_server data to db")
        self._db_executor.save_device(
            {'types': self._types,
             'sporadic_types': self._sporadic_types,
             'host': self.__host,
             'port': self.__port,
             'asdu': self._asdu_address,
             't0': self.__t0,
             't1': self.__t1,
             't2': self.__t2,
             't3': self.__t3,
             'k': self.__k,
             'w': self.__w,
             'holdup': self._holdup}, self._telemetry, self._signals_desc)

    def _load_telemetry_from_db(self):
        self.__logger.debug("Load iec104_server telemetry from db")
        self._telemetry = self._db_executor.get_telemetry()
        self._events = self._db_executor.get_events()

    def _no_events(self):
        events_flag = self._db_executor.get_has_events() == 0
        self.__logger.debug(f"Return events flag: {events_flag}")
        return events_flag

    def _reset_event_flag(self):
        self.__logger.debug("Reset events flag")
        self._db_executor.set_has_events(0)

    """************************
        Настройка сервера
    ************************"""

    def _increase_counter(self, client_id, name):
        # переполнение
        if self._clients[client_id][name] >= 65535:
            self._clients[client_id][name] = 0
        else:
            self._clients[client_id][name] += 1

    def is_stopped(self):
        return self.__stopped

    def set_value(self, addr: int, val):
        self._telemetry[str(addr)] = val
        self._save_to_db()

    def set_holdup(self, val: float):
        self._holdup = val

    """************************
        Внешние команды
    ************************"""

    # 100
    async def _interrogation_command_answer(self, client_id, asdu):
        print('{} Receive from {}: {}'.format(datetime.now(), client_id, asdu.objs[0].get_info()))
        count_apdu = 0
        # Подтверждение активации
        packet = s_asdu.get_packet(self._clients[client_id]['ssn'], self._clients[client_id]['rsn'], asdu.type_id,
                                   asdu.sq, asdu.test, asdu.positive_negative, 7, asdu.org, asdu.asdu,
                                   [s_asdu.commands[100]({'IOA': 0, 'C_irq': asdu.objs[0].c_irq})])

        self._increase_counter(client_id, 'ssn')
        count_apdu += 1

        # Объекты
        for obj in self._types:
            group_num = str(asdu.objs[0].c_irq)
            # Если группа есть в списке то берём от туда объекты
            if group_num not in self._types[obj]:
                continue

            tmp_1 = self._types[obj][group_num].copy()

            while len(tmp_1) != 0:
                tmp_2 = []
                for i in range(len(tmp_1)):
                    if len(tmp_2) * s_asdu.info_types[int(obj)][1] + s_asdu.info_types[int(obj)][1] > 240 \
                            or len(tmp_1) == 0:
                        break
                    adr = tmp_1.pop(0)
                    sig = s_asdu.info_types[int(obj)][0]({'IOA': int(adr), 'Value': self._telemetry[adr]})
                    tmp_2.append(sig)

                packet += s_asdu.get_packet(self._clients[client_id]['ssn'], self._clients[client_id]['rsn'], int(obj),
                                            0, 0, 0, asdu.objs[0].c_irq, 0, self._asdu_address, tmp_2)
                self._increase_counter(client_id, 'ssn')
                count_apdu += 1
                if len(packet) + 255 >= 1024:
                    await self._send_i(client_id, bytearray(packet), count_apdu)
                    await asyncio.sleep(self._holdup)
                    packet.clear()

        # Деактивация
        packet += s_asdu.get_packet(self._clients[client_id]['ssn'], self._clients[client_id]['rsn'], asdu.type_id,
                                    asdu.sq, asdu.test, asdu.positive_negative, 10, asdu.org, asdu.asdu,
                                    [s_asdu.commands[100]({'IOA': 0, 'C_irq': asdu.objs[0].c_irq})])
        count_apdu += 1
        await self._send_i(client_id, bytearray(packet), count_apdu)
        self._increase_counter(client_id, 'ssn')

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

            await self._send_i(client_id, bytearray(packet), 1)
            self._increase_counter(client_id, 'ssn')

    """********************************
        Обработка входящих пакетов
    ********************************"""

    async def __send_answer(self, client_id, answer):
        self._clients[client_id]['writer'].write(answer)
        await self._clients[client_id]['writer'].drain()
        print('{} Device: {} --> {}: [{}]'.format(datetime.now(), self._sn, client_id, answer.hex(' ').upper()))

    async def _send_s(self, client_id):
        packet = apci.s_frame(self._clients[client_id]['rsn'])
        await self.__send_answer(client_id, bytearray([104, len(packet)] + packet))

    # Тестовая посылка в случае долгого простоя
    async def __send_test_fr_ack(self, client_id):
        packet = [104, len(apci.TESTFR_ACT)] + apci.TESTFR_ACT
        await self.__send_answer(client_id, bytearray(packet))

    async def _send_i(self, client_id, answer, count_apdu):
        # Проверка k и w
        if self._clients[client_id]['ack'] >= self.__k:
            await asyncio.sleep(self.__t2)
            if self._clients[client_id]['ack'] >= self.__k:
                # Если нет подтверждения в течении уставноленного времени закрываем соединение
                message = f"The transmitted {self.__k} or more frames was not confirmed!"
                self.__logger.error(message)
                raise SequenceErrorException(message)
        await self.__send_answer(client_id, answer)
        self._clients[client_id]['ack'] += count_apdu

    async def __read_message(self, msg, client_id):
        message = [x for x in msg]
        msg_ctrl_fld = message[:4]  # Котрольное поле сообщения

        if msg_ctrl_fld[0] & 3 == 3:
            # Если это U-FRAME отвечаем по стандарту протокола:
            packet = apci.u_frame_answer(msg_ctrl_fld)
            if packet:
                await self.__send_answer(client_id, bytearray([104, len(packet)] + packet))

        elif msg_ctrl_fld[0] & 3 == 1:
            # Если это S-FRAME считаем и проверяем счётчик отправленных
            received_sequence = apci.parse_s_frame(bytearray(msg_ctrl_fld))
            if self._clients[client_id]['ssn'] == received_sequence:
                # Если совпадает - сбрасываем счётчик не подтверждённых пакетов.
                self._clients[client_id]['ack'] = 0
            else:
                # Если не совпадает - бросаем исключение и закрываем соединение с клиентом.
                raise SequenceErrorException(f"Transmitted sequence error! "
                                             f"Sending frames: {self._clients[client_id]['ssn']}, "
                                             f"S-Frame received with: {received_sequence}")

        elif msg_ctrl_fld[0] & 3 == 0 or msg_ctrl_fld[0] & 3 == 2:
            # Если I-FRAME проверям оба счётчика
            received_sequence = apci.parse_i_frame(bytearray(msg_ctrl_fld))
            if self._clients[client_id]['rsn'] != received_sequence[0] \
                    or self._clients[client_id]['ssn'] != received_sequence[1]:
                # Если не совпадает хотя бы 1 - бросаем исключение и закрываем соединение с клиентом.
                raise SequenceErrorException(f"Transmitted sequence error! "
                                             f"Sending frames: {self._clients[client_id]['ssn']}, "
                                             f"Received frames: {self._clients[client_id]['rsn']}, "
                                             f"I-Frame received with: {received_sequence[1]}, "
                                             f"{received_sequence[0]}")
            # Если совпадает - сбрасываем счётчик не подтверждённых пакетов.
            self._clients[client_id]['ack'] = 0
            # Считываем ASDU
            asdu = r_asdu.ASDU(ConstBitStream(bytes=message, offset=4 * 8))
            self._increase_counter(client_id, 'rsn')
            # Передаём для подготовки ответа и выполения команд
            self.__command_executor((asdu.type_id, asdu), client_id)

    """************************
        Задания для сервера
    ************************"""

    def __command_executor(self, command, client_id):
        if command[0] in self._commands:
            # Если есть готовая команда то созадём задачу по её вызову
            self.__tasks.append(self.__loop.create_task(self._commands[command[0]](client_id, command[1])))
        else:
            # Если нет то просто отправляем подтверждение получения
            self.__tasks.append(self.__loop.create_task(self._answer(client_id, command[1])))

    async def __read_stream(self, reader, timeout):
        # Читаем сообщение или ждём его в течении установленного времени
        start_68h = await asyncio.wait_for(reader.read(1), timeout=timeout)
        apdu_size = await asyncio.wait_for(reader.read(1), timeout=timeout)
        msg = await asyncio.wait_for(reader.read(int.from_bytes(apdu_size, "little")), timeout=timeout)
        return start_68h, apdu_size, msg

    async def __read_from_socket(self, reader, client_id):
        try:
            return await self.__read_stream(reader, float(self.__t3))
        except asyncio.exceptions.TimeoutError:
            # Нет активности более t3 - отправляем TESTFR_ACK ждём ответа в течении t1
            await self.__send_test_fr_ack(client_id)
            return await self.__read_stream(reader, float(self.__t1))

    async def __handle_client(self, reader, writer):

        client_id = writer.get_extra_info('peername')
        # Крайний случай, надо проверить. Поднят флаг остановки сервера. Соединения не принимаем.
        if self.__stopped:
            print('{} ******* Server closed, disconnect client {}.  *******'.format(datetime.now(), client_id))
            writer.close()
            self.__server.close()
            return

        print('{} ******* Client {} connected.  *******'.format(datetime.now(), client_id))
        self._clients[client_id] = {'reader': reader, 'writer': writer, 'ssn': 0, 'rsn': 0, 'ack': 0}

        while True:

            try:
                # Получаем APDU
                start_68h, apdu_size, msg = await self.__read_from_socket(reader, client_id)
                # Если пустой - то закрытваем соединение
                if not msg:
                    print('{} ******* Client {} disconnected. *******'.format(datetime.now(), client_id))
                    break

                print('{} Device: {} <-- {}: [{} {} {}]'.format(datetime.now(), self._sn, client_id,
                                                                start_68h.hex(' ').upper(), apdu_size.hex(' ').upper(),
                                                                msg.hex(' ').upper()))
                # Читаем и отвечаем
                await self.__read_message(msg, client_id)

            except SequenceErrorException as ex:
                print('{} ******* Sequence Error from {}, connection closed: {}. *******'.format(
                    datetime.now(), client_id, ex.get_text()))
                await writer.drain()
                break

            except asyncio.exceptions.TimeoutError:
                print('{} ******* Connection from {} closed by timeout. *******'.format(datetime.now(), client_id))
                await writer.drain()
                break

            except ConnectionResetError:
                print('{} ******* Client {}: reset connection. *******'.format(datetime.now(), client_id))
                break

            except OSError:
                print('{} ******* Client {}: connection lost. *******'.format(datetime.now(), client_id))
                break

        del self._clients[client_id]
        writer.close()

    async def __send_sporadic(self, event_data: dict, client_id):
        packet = []
        keys = list(event_data.keys())  # Временный списко ключей объектов
        # Для каждого сопрадиечкого типа:
        for sporadic_type in self._sporadic_types:
            # Пока список не будет пуст
            while len(keys) != 0:
                tmp_list = []  # Создаём список для отправки сигналов
                for i in range(len(keys)):
                    # Проверяем что размер списка сигналов не превышает нужную длинну APDU
                    if len(tmp_list) * s_asdu.info_types[int(sporadic_type)][1] \
                            + s_asdu.info_types[int(sporadic_type)][1] > 240 or len(keys) == 0:
                        break
                    # Извлекаем ключ, он же адрес объекта
                    adr = keys.pop(0)
                    """
                    Если адрес есть в списке спорадических сигналов этого типа, то создаём объект сигнала 
                    и помещаем в список для отправки.
                    """
                    if adr in self._sporadic_types[sporadic_type]:
                        sig = s_asdu.info_types[int(sporadic_type)][0]({'IOA': int(adr), 'Value': event_data[adr]})
                        tmp_list.append(sig)

                if len(tmp_list) == 0:
                    continue
                # Создаём APDU и добавляем его в пакет для отправки
                packet += s_asdu.get_packet(self._clients[client_id]['ssn'], self._clients[client_id]['rsn'],
                                            int(sporadic_type), 0, 0, 0, 3, 0, self._asdu_address, tmp_list)

                self._increase_counter(client_id, 'ssn')

                if len(packet) == 0:
                    continue

        if len(packet) != 0:
            await self.__send_answer(client_id, bytearray(packet))

    async def __events_listener(self):
        while True:
            await asyncio.sleep(0.3)
            # Проверка есть ли новые события
            if self._no_events():
                continue
            # Выгрузка новых данных с дб
            self._load_telemetry_from_db()
            for event in self._events:
                for client_id in self._clients.keys():
                    # Создание задания отправки спорадики клиентам.
                    self.__tasks.append(self.__loop.create_task(self.__send_sporadic(event, client_id)))
                # Ождание для получения подтверждения перед отправкой следующего события
                await asyncio.sleep(self._holdup)

            self._events.clear()
            self._reset_event_flag()

    # Удаление выполненных и отменённых задач из списка
    async def __clear_tasks(self):
        while True:
            await asyncio.sleep(30)
            tmp = self.__tasks.copy()
            for task in tmp:
                if task.done() or task.cancelled():
                    while task in self.__tasks:
                        self.__tasks.remove(task)

    """************************
        Запуск и остановка
    ************************"""

    def stop(self):
        # Флаг не принимать новые подключения
        self.__stopped = True
        self.__loop.stop()  # На всякий случай, так как finally срабатывает не всегда
        # Закрываем все незвершённые задачи
        for task in self.__tasks:
            task.cancel()
        # Закрывем соединения
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
        self.__tasks.append(self.__loop.create_task(self.__events_listener()))
        self.__tasks.append(self.__loop.create_task(self.__clear_tasks()))
        try:
            self.__loop.run_forever()
        finally:
            self.__loop.run_until_complete(self.__server.wait_closed())
            self.__loop.close()
            print('{} ******* Device: {} stopped. *******'.format(datetime.now(), self._sn))
