import asyncio
import threading
import tkinter

from IEC104.client import Iec60870Client


# Зачатки интерфейса на Ткинтер
class ClientGUI:

    def __init__(self):
        self.master = tkinter.Tk()
        self.master.wm_geometry("+%d+%d" % ((self.master.winfo_screenwidth() - self.master.winfo_reqwidth()) / 3,
                                            (self.master.winfo_screenheight() - self.master.winfo_reqheight()) / 4))
        self.master.title("")

        self.master.geometry('480x640')
        self.frame = tkinter.Frame(self.master)
        self.__host = tkinter.Entry(self.frame, width=45)
        self.__host.grid(column=0, row=0, sticky="e", padx=2)
        self.connect_btn_text = tkinter.StringVar()
        self.connect_btn_text.set("Подключиться")
        self.connect_button = tkinter.Button(self.frame, textvariable=self.connect_btn_text, command=self.connect,
                                             width=15)
        self.connect_button.grid(column=1, row=0)
        self.frame.grid(column=0, row=0, padx=20, pady=20)
        self.commands_frame = tkinter.Frame(self.master)
        self.__ioa = tkinter.Entry(self.commands_frame, width=19)
        self.__ioa.grid(column=0, row=0, sticky="e", padx=2)
        self.__data = tkinter.Entry(self.commands_frame, width=25)
        self.__data.grid(column=1, row=0, sticky="e", padx=2)
        self.cmd_button = tkinter.Button(self.commands_frame, text="Передать", command=self.send, width=15)
        self.cmd_button.grid(column=2, row=0)
        self.commands_frame.grid(column=0, row=1, padx=20)
        self.client = None

    def __worker(self, loop):
        asyncio.set_event_loop(loop)
        self.client = Iec60870Client(host=self.__host.get())
        self.client.run()
        loop.stop()
        loop.close()

    def __stop(self):
        self.client.stop()
        self.client = None
        self.connect_btn_text.set("Подключиться")

    def send(self):
        self.client.add_task(45, int(self.__ioa.get()), int(self.__data.get()))

    def connect(self):
        if self.client is None:
            threading.Thread(target=self.__worker, args=(asyncio.new_event_loop(),)).start()
            self.connect_btn_text.set("Отключиться")
        else:
            self.__stop()

    def run(self):
        self.master.mainloop()

