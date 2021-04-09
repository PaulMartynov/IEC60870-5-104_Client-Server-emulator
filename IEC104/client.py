import asyncio
from datetime import datetime

from bitstring import ConstBitStream

from IEC104 import apci, r_asdu, s_asdu
from Utils.Exceptions import SequenceErrorException


class Iec60870Client:

    def __init__(self, host='localhost', port=2404):
        self.__host = host
        self.__port = port

        self.__reader = None
        self.__writer = None

        self.__ssn = 0
        self.__rsn = 0
        self.__ack = 0

        self.__asdu = 1

        self.__t0 = 30
        self.__t1 = 15
        self.__t2 = 10
        self.__t3 = 20
        self.__k = 12
        self.__w = 8
        self.__holdup = 0.3

        self._tasks = {}
        self.__tasks = []  # Список задач на случай их отмены перед остановкой клиента

        self.__loop = None

        self.__commands = {
            45: self.send_single_point_command
        }

    async def __send_answer(self, answer):
        self.__writer.write(answer)
        await self.__writer.drain()
        print('{} Client --> {}: [{}]'.format(datetime.now(), f'{self.__host}:{self.__port}', answer.hex(' ').upper()))

    async def _send_s(self):
        packet = apci.s_frame(self.__rsn)
        await self.__send_answer(bytearray([104, len(packet)] + packet))

    # Тестовая посылка в случае долгого простоя
    async def __send_test_fr_ack(self):
        packet = [104, len(apci.TESTFR_ACT)] + apci.TESTFR_ACT
        await self.__send_answer(bytearray(packet))

    async def __send_start_dt_ack(self):
        packet = [104, len(apci.STARTDT_ACT)] + apci.STARTDT_ACT
        await self.__send_answer(bytearray(packet))

    async def _send_i(self, answer, count_apdu):
        # Проверка k и w
        if self.__ack >= self.__k:
            await asyncio.sleep(self.__t2)
            if self.__ack >= self.__k:
                # Если нет подтверждения в течении уставноленного времени закрываем соединение
                message = f"The transmitted {self.__k} or more frames was not confirmed!"
                # self.__logger.error(message)
                raise SequenceErrorException(message)

        await self.__send_answer(answer)
        self.__ssn = self._increase_counter(self.__ssn)
        self.__ack += count_apdu

    async def __read_stream(self, reader, timeout):
        # Читаем сообщение или ждём его в течении установленного времени
        start_68h = await asyncio.wait_for(reader.read(1), timeout=timeout)
        apdu_size = await asyncio.wait_for(reader.read(1), timeout=timeout)
        msg = await asyncio.wait_for(reader.read(int.from_bytes(apdu_size, "little")), timeout=timeout)
        return start_68h, apdu_size, msg

    async def __read_from_socket(self):
        try:
            return await self.__read_stream(self.__reader, float(self.__t3))
        except asyncio.exceptions.TimeoutError:
            # Нет активности более t3 - отправляем TESTFR_ACK ждём ответа в течении t1
            await self.__send_test_fr_ack()
            return await self.__read_stream(self.__reader, float(self.__t1))

    def _increase_counter(self, counter):
        # переполнение
        if counter >= 65535:
            return 0
        else:
            return counter + 1

    async def _answer(self, asdu=None):
        await self._send_s()

    def __command_executor(self, command):
        self.__tasks.append(self.__loop.create_task(self._answer(command[1])))

    def add_task(self, type_id, ioa, data):
        if type_id not in self._tasks.keys():
            self._tasks[type_id] = {}
        self._tasks[type_id][ioa] = data

    async def __read_message(self, msg):
        message = [x for x in msg]
        msg_ctrl_fld = message[:4]  # Котрольное поле сообщения

        if msg_ctrl_fld[0] & 3 == 3:
            # Если это U-FRAME отвечаем по стандарту протокола:
            packet = apci.u_frame_answer(msg_ctrl_fld)
            if packet:
                await self.__send_answer(bytearray([104, len(packet)] + packet))

        elif msg_ctrl_fld[0] & 3 == 1:
            # Если это S-FRAME считаем и проверяем счётчик отправленных
            received_sequence = apci.parse_s_frame(bytearray(msg_ctrl_fld))
            if self.__ssn == received_sequence:
                # Если совпадает - сбрасываем счётчик не подтверждённых пакетов.
                self.__ack = 0
            else:
                # Если не совпадает - бросаем исключение и закрываем соединение с клиентом.
                raise SequenceErrorException(f"Transmitted sequence error! "
                                             f"Sending frames: {self.__ssn}, "
                                             f"S-Frame received with: {received_sequence}")

        elif msg_ctrl_fld[0] & 3 == 0 or msg_ctrl_fld[0] & 3 == 2:
            # Если I-FRAME проверям оба счётчика
            received_sequence = apci.parse_i_frame(bytearray(msg_ctrl_fld))
            if self.__rsn != received_sequence[0] \
                    or self.__ssn != received_sequence[1]:
                # Если не совпадает хотя бы 1 - бросаем исключение и закрываем соединение с клиентом.
                raise SequenceErrorException(f"Transmitted sequence error! "
                                             f"Sending frames: {self.__ssn}, "
                                             f"Received frames: {self.__rsn}, "
                                             f"I-Frame received with: {received_sequence[1]}, "
                                             f"{received_sequence[0]}")
            # Если совпадает - сбрасываем счётчик не подтверждённых пакетов.
            self.__ack = 0
            # Считываем ASDU
            asdu = r_asdu.ASDU(ConstBitStream(bytes=message, offset=4 * 8))
            self.__rsn = self._increase_counter(self.__rsn)
            # Передаём для подготовки ответа и выполения команд
            self.__command_executor((asdu.type_id, asdu))

    async def send_single_point_command(self, ioa, data):

        packet = s_asdu.get_packet(self.__ssn, self.__rsn, 45, 0, 0, 0, 6, 0, self.__asdu,
                                   [s_asdu.commands[45]({'IOA': ioa, 'Data': data})])

        await self._send_i(bytearray(packet), 1)

    async def __client(self):
        self.__reader, self.__writer = await asyncio.open_connection(self.__host, self.__port, loop=self.__loop)
        await self.__send_start_dt_ack()
        while True:
            try:
                start_68h, apdu_size, msg = await self.__read_from_socket()
                if not msg:
                    print('{} ******* Connection closed. *******'.format(datetime.now()))
                    break
                print('{} Client <-- {}: [{} {} {}]'.format(datetime.now(), f'{self.__host}:{self.__port}',
                                                            start_68h.hex(' ').upper(), apdu_size.hex(' ').upper(),
                                                            msg.hex(' ').upper()))
                await self.__read_message(msg)

            except SequenceErrorException as ex:
                print('{} ******* Sequence Error from {}, connection closed: {}. *******'.format(
                    datetime.now(), f'{self.__host}:{self.__port}', ex.get_text()))
                await self.__writer.drain()
                break

            except asyncio.exceptions.TimeoutError:
                print('{} ******* Connection closed by timeout. *******'.format(datetime.now()))
                await self.__writer.drain()
                break

            except ConnectionResetError:
                print('{} ******* Reset connection. *******'.format(datetime.now()))
                break

            except OSError:
                print('{} ******* Connection lost. *******'.format(datetime.now()))
                break

        self.__writer.close()

    async def __run_tasks(self):
        while True:
            await asyncio.sleep(self.__holdup)
            for type_id in self._tasks.keys():
                if type_id not in self.__commands.keys():
                    continue
                for ioa in self._tasks[type_id].keys():
                    self.__tasks.append(
                        self.__loop.create_task(self.__commands[type_id](ioa, self._tasks[type_id][ioa])))
            self._tasks = {}

    # Удаление выполненных и отменённых задач из списка
    async def __clear_tasks(self):
        while True:
            await asyncio.sleep(30)
            tmp = self.__tasks.copy()
            for task in tmp:
                if task.done() or task.cancelled():
                    while task in self.__tasks:
                        self.__tasks.remove(task)

    def run(self):
        self.__loop = asyncio.get_event_loop()
        self.__tasks.append(self.__loop.create_task(self.__clear_tasks()))
        self.__tasks.append(self.__loop.create_task(self.__run_tasks()))
        try:
            self.__loop.run_until_complete(self.__client())
        except (RuntimeError, TypeError):
            pass
        finally:
            self.__loop.close()
            print("Connection closed!")

    def stop(self):
        if self.__writer is not None:
            self.__writer.close()
        # Закрываем все незвершённые задачи
        for task in self.__tasks:
            task.cancel()
        # Закрывем соединение
        self.__loop.stop()
