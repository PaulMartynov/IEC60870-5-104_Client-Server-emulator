import json

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import config
from DataBase.device_db import Base, Device, Iec104Devices, DeviceSet, DeviceEvent


# поверяет существует ли база по указанному пути, если нет то создаёт
def check_or_create_db():
    engine = create_engine(config.DATABASE_URI)
    Base.metadata.create_all(engine, Base.metadata.tables.values(), checkfirst=True)


def get_session():
    check_or_create_db()
    engine = create_engine(config.DATABASE_URI, connect_args={'check_same_thread': False})
    Base.metadata.bind = engine
    db_session = sessionmaker(bind=engine)

    return db_session(), engine


def delete_iec104_device(sn):
    session, engine = get_session()
    session.query(Device).filter_by(SN=sn).delete()
    session.query(Iec104Devices).filter_by(SN=sn).delete()
    session.query(DeviceEvent).filter_by(set_uuid=sn).delete()
    session.commit()
    session.close()
    engine.dispose()


def delete_gsm_device(sn):
    session, engine = get_session()
    db_sets = session.query(DeviceSet).filter_by(parent=sn).order_by(DeviceSet.set_num).all()
    for db_set in db_sets:
        session.query(DeviceEvent).filter_by(set_uuid=db_set.uuid).delete()
        session.query(Device).filter_by(parent=db_set.uuid).delete()
        session.commit()

    session.query(DeviceSet).filter_by(parent=sn).delete()
    session.commit()

    session.query(Device).filter_by(SN=sn).delete()
    session.commit()

    session.close()
    engine.dispose()


def set_triggers(sn, triggers: dict):
    session, engine = get_session()

    iec104_db_device = session.query(Iec104Devices).filter_by(SN=sn).first()
    db_device = session.query(Device).filter_by(SN=sn).first()
    telemetry = json.loads(db_device.telemetry)
    event_id = len(session.query(DeviceEvent).filter_by(set_uuid=sn).order_by(DeviceEvent.id).all())

    for adr in triggers:
        if adr in telemetry:
            telemetry[adr] = triggers[adr]

    db_device.telemetry = str(json.dumps(telemetry))
    db_event = DeviceEvent(set_uuid=sn, event_id=event_id, event_type=0,
                           event_data=str(json.dumps(triggers)))

    session.add(db_event)
    iec104_db_device.has_event = 1

    session.add(db_device)
    session.add(iec104_db_device)
    session.commit()
    session.close()
    engine.dispose()
