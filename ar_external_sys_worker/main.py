import tzlocal
from pytz import timezone
import json
import random
import requests
from ar_external_sys_worker import mixins


class DataWorker(mixins.Logger, mixins.ExSys, mixins.AuthMe,
                 mixins.DbAuthInfoGetter):
    working_link = None

    def get_send_data(self):
        return True

    def format_send_data(self, data, *args, **kwargs):
        return data

    def get_local_id(self, data):
        return data['ex_id']

    def get_and_send_data(self):
        data = self.get_send_data()
        return self.send_data(data)

    def post_data(self, headers, link, data):
        return requests.post(url=link,
                             data=data,
                             headers=headers)

    def get_data(self, headers, link, data):
        return requests.get(url=link, data=data, headers=headers)

    def send_data(self, data):
        local_data_id = self.get_local_id(data)
        print(f"\nSENDING: id #{local_data_id}")
        data_frmt = self.format_send_data(data)
        log_id = self.log_ex_sys_sent(local_data_id)
        act_send_response = self.post_data(headers=self.headers,
                                           link=self.working_link,
                                           data=data_frmt)
        ex_sys_data_id = self.get_ex_sys_id_from_response(act_send_response)
        print(f"\nSEND RESULT: ex_id #{ex_sys_data_id}")
        self.log_ex_sys_get(ex_sys_data_id, log_id)
        return act_send_response

    def get_ex_sys_id_from_response(self, response):
        return response


class ActsWorker(DataWorker, mixins.ActWorkerMixin, mixins.ActsSQLCommands):
    table_name = 'records'
    table_id = 1
    act_id_from_response = None

    def __init__(self, sql_shell, trash_cats=None, time_start=None,
                 acts_limit=4, *args, **kwargs):
        self.sql_shell = sql_shell
        self.limit = acts_limit
        if trash_cats is None:
            trash_cats = ['ТКО', 'ПО']
        self.trash_cats_to_send = trash_cats
        self.time_start = time_start
        self.login, self.password = self.get_auth_info_from_db()
        self.set_headers(self.get_headers())

    def send_unsend_acts(self):
        data = self.get_unsend_acts()
        for act in data:
            self.send_data(act)

    def get_ex_sys_id_from_response(self, response):
        response = response.json()
        if 'error' in response:
            return response['error']
        return response[self.act_id_from_response]

    def get_photo_path(self, record_id, photo_type):
        command = "SELECT p.path FROM photos p " \
                  "INNER JOIN record_photos rp ON (p.id = rp.photo_id) " \
                  "WHERE p.photo_type={} and rp.record_id={} LIMIT 1".format(
            photo_type,
            record_id)
        response = self.sql_shell.try_execute_get(command)
        if response:
            return response[0][0]

    def get_send_data(self):
        return self.get_one_unsend_act()


class ASUActsWorker(mixins.AsuMixin, ActsWorker,
                    mixins.SignallPhotoEncoderMixin):
    def __init__(self, sql_shell, trash_cats_list, time_start, acts_limit=3):
        super().__init__(sql_shell=sql_shell, trash_cats=trash_cats_list,
                         time_start=time_start, acts_limit=acts_limit)
        self.working_link = self.get_full_endpoint(self.link_create_act)
        self.fetch_asu_auto_id_url = self.get_full_endpoint(
            "/extapi/v2/transport/?number={}'")
        self.asu_polygon_name = None

    def send_unsend_acts(self):
        data = self.get_unsend_acts()
        if not data:
            return {'error': 'no acts to send'}
        for act in data:
            response = self.send_data(act)
            asu_act_id = response.json()['id']
            photo_in = self.get_photo_path(act['ex_id'], 1)
            photo_out = self.get_photo_path(act['ex_id'], 2)
            photo_in = self.get_photo_data(photo_in)
            self.upload_photo(photo_in,
                              self.get_full_endpoint(
                                  "/extapi/v2/landfill-fact/{}/photos-arrival/".format(
                                      asu_act_id)))
            photo_out = self.get_photo_data(photo_out)
            self.upload_photo(photo_out,
                              self.get_full_endpoint(
                                  "/extapi/v2/landfill-fact/{}/photos-departure/".format(
                                      asu_act_id)
                              ))

    def upload_photo(self, photoobj, endpoint):
        data = {"file": photoobj}
        data = json.dumps(data)
        response = requests.post(endpoint, data=data, headers=self.headers)
        return response

    def send_auth_data(self, endpoint, auth_data, *args, **kwargs):
        auth_data_json = json.dumps(auth_data)
        return requests.post(endpoint, data=auth_data_json,
                             headers={"Content-Type": "application/json"},
                             *args, **kwargs)

    def set_headers(self, headers):
        headers.update({"Content-Type": "application/json"})
        self.headers = headers

    def get_ex_sys_id_from_response(self, response):
        return response.json()['id']

    def extract_token(self, auth_response):
        token = super().extract_token(auth_response)
        return f"Token {token}"

    def fetch_asu_auto_id(self, car_number):
        response = self.get_data(headers=self.headers,
                                 link=self.fetch_asu_auto_id_url.format(
                                     car_number
                                 ),
                                 data=None)
        try:
            car_id = response.json()['results'][0]['id']
        except IndexError:
            car_id = random.randrange(176, 189)
        return car_id

    def format_send_data(self, act, photo_in=None, photo_out=None):
        asu_auto_id = self.fetch_asu_auto_id(act['car_number'])
        if not act['rfid']: act['rfid'] = 'None'
        data = {
            "ext_id_landfill_weight": self.get_db_value('asu_poligon_name'),
            "ext_id_transport_weight": act["car_number"],
            "ext_id": str(act["ex_id"]),
            "time_arrival": self.format_date_for_asu(act["time_in"]),
            "time_departure": self.format_date_for_asu(act["time_out"]),
            "transport_arrival_weight": act["gross"],
            "transport_departure_weight": act["tare"],
            "allow_weight_fault": 50,
            "rfid": act['rfid'],
            "transport": asu_auto_id,
            "transport_name": act["car_number"],
            "transport_number": act["car_number"],
            'comment': ''
            # "comment": f"{act['gross_comm']}|{act['tare_comm']}|{act['add_comm']}".replace("None",""),
        }
        act_json = json.dumps(data)
        return act_json

    def format_date_for_asu(self, date):
        date = self.set_current_localzone(date)
        date = self.convert_time_to_msk(date, new_timezone='GMT')
        date = date.strftime("%Y-%m-%d %H:%M:%S")
        return date

    def set_current_localzone(self, date):
        date = date.replace(tzinfo=None)
        current_localzone = tzlocal.get_localzone()
        date = current_localzone.localize(date)
        return date

    def convert_time_to_msk(self, date, new_timezone, *args, **kwargs):
        date = date.astimezone(timezone(new_timezone))
        return date


class SignallActWorker(mixins.SignallMixin, ActsWorker,
                       mixins.SignallPhotoEncoderMixin):
    def __init__(self, sql_shell, trash_cats_list, time_start, acts_limit=5):
        super().__init__(sql_shell=sql_shell, trash_cats=trash_cats_list,
                         time_start=time_start, acts_limit=acts_limit)
        self.working_link = self.get_full_endpoint(self.link_create_act)
        self.act_id_from_response = 'act_id'

    def format_send_data(self, act, photo_in=None, photo_out=None):
        act.pop('polygon_id')
        act.pop('rfid')
        alerts = self.get_alerts(act['ex_id'])
        act['operator_comments'] = {'gross_comm': act.pop('gross_comm'),
                                    'tare_comm': act.pop('tare_comm'),
                                    'add_comm': act.pop('add_comm'),
                                    'changing_comm': act.pop(
                                        'changing_comm'),
                                    'closing_comm': act.pop(
                                        'closing_comm')}
        act['photo_in'] = self.get_photo_path(act['ex_id'], 1)
        act['photo_out'] = self.get_photo_path(act['ex_id'], 2)
        if act['photo_in']:
            photo_in = self.get_photo_data(act['photo_in'])
        if act['photo_out']:
            photo_out = self.get_photo_data(act['photo_out'])
        act_json = self.get_json(car_number=act['car_number'],
                                 ex_id=act['ex_id'], gross=act['gross'],
                                 tare=act['tare'], cargo=act['cargo'],
                                 time_in=act['time_in'].strftime(
                                     '%Y-%m-%d %H:%M:%S'),
                                 time_out=act['time_out'].strftime(
                                     '%Y-%m-%d %H:%M:%S'),
                                 alerts=alerts, carrier=act['carrier'],
                                 trash_cat=act['trash_cat'],
                                 trash_type=act['trash_type'],
                                 operator_comments=act['operator_comments'],
                                 photo_in=photo_in,
                                 photo_out=photo_out)
        return act_json


class SignallActReuploder(mixins.SignallMixin, DataWorker,
                          mixins.SignallActDeletter,
                          mixins.SignallActDBDeletter):
    def __init__(self, sql_shell, **kwargs):
        self.sql_shell = sql_shell
        self.login, self.password = self.get_auth_info_from_db()
        self.headers = self.get_headers()
        self.working_link = self.get_full_endpoint(self.del_act_url)

    def work(self, act_number):
        print(f"DEL ACT #{act_number}")
        response = self.delete_act(act_number)
        print(f"SIGNALL RESULT:{response.json()}")
        response = self.delete_act_from_send_reports(act_number)
        print(f"DB RESULT:{response}")
