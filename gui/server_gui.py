import json
import tkinter

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import config
from DataBase.device_db import Base, Iec104Devices


# Зачатки интерфейса на Ткинтер
class ServerGUI:

    def __init__(self, sn):
        self.master = tkinter.Tk()
        self.master.wm_geometry("+%d+%d" % ((self.master.winfo_screenwidth() - self.master.winfo_reqwidth()) / 3,
                                            (self.master.winfo_screenheight() - self.master.winfo_reqheight()) / 4))
        self.master.title("")

        self.master.geometry('480x640')

        self.frame_left = tkinter.Frame(self.master)
        self.frame_right = tkinter.Frame(self.master)

        self.connect_button = tkinter.Button(self.frame_right, text="Записать", command=self.write_data, width=15)
        self.connect_button.grid(column=0, row=0, sticky="ne")

        self.__dev_sn = sn

        self.frame_right.pack(side='right', padx=10, pady=15, fill='both')

    def read_data(self):
        engine = create_engine(config.DATABASE_URI, connect_args={'check_same_thread': False})
        Base.metadata.bind = engine
        db_session = sessionmaker(bind=engine)
        session = db_session()
        db_device = session.query(Iec104Devices).filter_by(SN=self.__dev_sn).first()
        description = json.loads(db_device.signals_description)

        session.add(db_device)
        session.commit()
        session.close()
        engine.dispose()

    def write_data(self):
        engine = create_engine(config.DATABASE_URI, connect_args={'check_same_thread': False})
        Base.metadata.bind = engine
        db_session = sessionmaker(bind=engine)
        session = db_session()
        db_device = session.query(Iec104Devices).filter_by(SN=self.__dev_sn).first()
        db_device.has_event = 1
        session.add(db_device)
        session.commit()
        session.close()
        engine.dispose()

    def run(self):
        self.master.mainloop()
