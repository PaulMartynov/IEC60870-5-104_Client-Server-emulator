# для настройки баз данных
from sqlalchemy import Column, ForeignKey, Integer, String

# для определения таблицы и модели
from sqlalchemy.ext.declarative import declarative_base

# создание экземпляра declarative_base
Base = declarative_base()


# Классы
class Device(Base):
    __tablename__ = 'devices'

    SN = Column(Integer, primary_key=True)
    dev_type = Column(Integer, nullable=False)
    settings = Column(String, nullable=False)
    telemetry = Column(String, nullable=False)
    peer_num = Column(Integer)
    parent = Column(ForeignKey("sets.uuid"))


class DeviceSet(Base):
    __tablename__ = 'sets'

    id = Column(Integer, primary_key=True)
    settings = Column(String, nullable=False)
    telemetry = Column(String, nullable=False)
    set_num = Column(Integer)
    uuid = Column(String, nullable=False)
    parent = Column(ForeignKey("devices.SN"))


class DeviceEvent(Base):
    __tablename__ = 'events'

    id = Column(Integer, primary_key=True)
    set_uuid = Column(ForeignKey("sets.uuid"))
    event_id = Column(Integer, nullable=False)
    event_type = Column(Integer, nullable=False)
    event_data = Column(String, nullable=False)


class Iec104Devices(Base):
    __tablename__ = 'iec104_devices'

    SN = Column(Integer, primary_key=True)
    signals_description = Column(String)
    has_event = Column(Integer, nullable=False)
