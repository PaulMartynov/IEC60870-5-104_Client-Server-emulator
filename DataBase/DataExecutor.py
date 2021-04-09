import json

from DataBase.device_db import Device, Iec104Devices, DeviceEvent
from DataBase.utils import get_session
from Utils.Exceptions import DeviceParametersException
from Utils.logger import DeviceLogger


class DataExecutor:

    def __init__(self, device_sn, logger=None):
        self.__sn = device_sn
        if logger is not None:
            self.__logger = logger
        else:
            self.__logger = DeviceLogger.get_logger(f'db_executor_for_{device_sn}')

    def load_device(self, dev_type=9999, host='', port=2404):
        session, engine = get_session()
        db_device = session.query(Device).filter_by(SN=self.__sn).first()
        settings = {'types': {}, 'sporadic_types': {}, 'host': host, 'port': port, 'asdu': 1, 't0': 30, 't1': 15,
                    't2': 10, 't3': 20, 'k': 12, 'w': 8, 'holdup': 0.3}
        telemetry = {}
        signals_desc = {}
        if db_device is None:
            db_device = Device(SN=self.__sn, dev_type=dev_type,
                               settings=str(json.dumps(settings)),
                               telemetry=str(json.dumps(telemetry)))
            session.add(db_device)

            iec104_db_device = Iec104Devices(SN=self.__sn, signals_description=str(json.dumps(signals_desc)),
                                             has_event=0)
            session.add(iec104_db_device)

            session.commit()

        else:
            if db_device.dev_type != dev_type:
                raise DeviceParametersException(
                    'Device {} has another type in data_base: {}'.format(db_device.SN, db_device.dev_type))
            db_settings = json.loads(db_device.settings)
            telemetry = json.loads(db_device.telemetry)
            if host != db_settings['host']:
                settings['host'] = host
            else:
                settings['host'] = db_settings['host']
            if port != db_settings['port']:
                settings['port'] = port
            else:
                settings['port'] = db_settings['port']
            settings['asdu'] = db_settings['asdu']
            settings['types'] = db_settings['types']
            settings['sporadic_types'] = db_settings['sporadic_types']
            iec104_db_device = session.query(Iec104Devices).filter_by(SN=self.__sn).first()
            signals_desc = json.loads(iec104_db_device.signals_description)

        session.close()
        engine.dispose()

        return {'settings': settings, 'telemetry': telemetry, 'signals_desc': signals_desc}

    def save_device(self, settings: dict, telemetry: dict, signals_desc: dict):
        session, engine = get_session()

        db_device = session.query(Device).filter_by(SN=self.__sn).first()
        iec104_db_device = session.query(Iec104Devices).filter_by(SN=self.__sn).first()

        db_device.settings = str(json.dumps(settings))
        db_device.telemetry = str(json.dumps(telemetry))
        iec104_db_device.signals_description = str(json.dumps(signals_desc))

        session.add(db_device)
        session.add(iec104_db_device)
        session.commit()
        session.close()
        engine.dispose()

    def get_telemetry(self):
        session, engine = get_session()

        db_device = session.query(Device).filter_by(SN=self.__sn).first()
        telemetry = json.loads(db_device.telemetry)

        session.close()
        engine.dispose()

        return telemetry

    def get_has_events(self):
        session, engine = get_session()

        iec104_db_device = session.query(Iec104Devices).filter_by(SN=self.__sn).first()
        has_event = iec104_db_device.has_event

        session.close()
        engine.dispose()

        return has_event

    def get_events(self):
        session, engine = get_session()
        db_events = session.query(DeviceEvent).filter_by(set_uuid=self.__sn).order_by(DeviceEvent.id).all()
        events = []
        for db_event in db_events:
            events.append(json.loads(db_event.event_data))

        session.query(DeviceEvent).filter_by(set_uuid=self.__sn).delete()
        session.commit()
        session.close()
        engine.dispose()
        return events

    def set_has_events(self, n):
        session, engine = get_session()

        iec104_db_device = session.query(Iec104Devices).filter_by(SN=self.__sn).first()
        iec104_db_device.has_event = n

        session.add(iec104_db_device)
        session.commit()
        session.close()
        engine.dispose()

    def set_triggers(self, triggers: dict):
        session, engine = get_session()

        iec104_db_device = session.query(Iec104Devices).filter_by(SN=self.__sn).first()
        db_device = session.query(Device).filter_by(SN=self.__sn).first()
        telemetry = json.loads(db_device.telemetry)
        event_id = len(session.query(DeviceEvent).filter_by(set_uuid=self.__sn).order_by(DeviceEvent.id).all())

        for adr in triggers:
            if adr in telemetry:
                telemetry[adr] = triggers[adr]
        db_device.telemetry = str(json.dumps(telemetry))
        db_event = DeviceEvent(set_uuid=self.__sn, event_id=event_id, event_type=0,
                               event_data=str(json.dumps(triggers)))

        session.add(db_event)
        iec104_db_device.has_event = 1

        session.add(db_device)
        session.add(iec104_db_device)
        session.commit()
        session.close()
        engine.dispose()
