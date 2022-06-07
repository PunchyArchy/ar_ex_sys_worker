import datetime
import tzlocal
from pytz import timezone
import json
import random
import requests
from ar_external_sys_worker import mixins


class DataWorker(mixins.Logger, mixins.ExSys):
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

    def get_headers(self):
        pass

    def send_data(self, data):
        local_data_id = self.get_local_id(data)
        data_frmt = self.format_send_data(data)
        log_id = self.log_ex_sys_sent(local_data_id)
        act_send_response = self.post_data(headers=self.headers,
                                           link=self.working_link,
                                           data=data_frmt)
        print(act_send_response.json())
        ex_sys_data_id = self.get_ex_sys_id_from_response(act_send_response)
        self.log_ex_sys_get(ex_sys_data_id, log_id)
        return act_send_response

    def get_ex_sys_id_from_response(self, response):
        return response


class ActsWorker(DataWorker, mixins.ActWorkerMixin, mixins.ActsSQLCommands):
    table_name = 'records'
    table_id = 1
    act_id_from_response = None

    def __init__(self, sql_shell, trash_cats=None, time_start=None,
                 *args, **kwargs):
        self.sql_shell = sql_shell
        if trash_cats is None:
            trash_cats = ['ТКО', 'ПО']
        self.trash_cats_to_send = trash_cats
        self.time_start = time_start

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


class ASUActsWorker(mixins.AsuMixin,
                    mixins.ASUAuthMe, ActsWorker,
                    mixins.SignallPhotoEncoderMixin,
                    mixins.ASUDBAPIAuthInfoGetter):
    def __init__(self, sql_shell, trash_cats_list, time_start):
        super().__init__(sql_shell=sql_shell, trash_cats=trash_cats_list,
                         time_start=time_start)
        self.login, self.password = self.get_auth_info_from_db()
        self.working_link = self.get_full_endpoint(self.link_create_act)
        self.fetch_asu_auto_id_url = self.get_full_endpoint(
            "/extapi/v2/transport/?number={}'")
        self.login, self.password = self.get_auth_info_from_db()
        self.asu_polygon_name = None
        self.set_headers(self.get_headers())

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
                                  "/extapi/v2/landfill-fact/{}/photos-arrival/".format(asu_act_id)))
            photo_out = self.get_photo_data(photo_out)
            self.upload_photo(photo_out,
                              self.get_full_endpoint(
                                  "/extapi/v2/landfill-fact/{}/photos-departure/".format(asu_act_id)
                              ))

    def upload_photo(self, photoobj, endpoint):
        data = {"file": photoobj}
        data = json.dumps(data)
        response = requests.post(endpoint, data=data, headers=self.headers)
        return response

    def set_headers(self, headers):
        headers.update({"Content-Type": "application/json"})
        self.headers = headers

    def get_ex_sys_id_from_response(self, response):
        return response.json()['id']

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
            print('No ID for car {}'.format(car_number))
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
        print(data)
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
                       mixins.SignAllAuthMe, mixins.SignallPhotoEncoderMixin,
                       mixins.SignallDbAuthInfoGetter):
    def __init__(self, sql_shell, trash_cats_list, time_start):
        super().__init__(sql_shell=sql_shell, trash_cats=trash_cats_list,
                         time_start=time_start)
        self.login, self.password = self.get_auth_info_from_db()
        self.working_link = self.get_full_endpoint(self.link_create_act)
        self.login, self.password = self.get_auth_info_from_db()
        self.act_id_from_response = 'act_id'

    def format_send_data(self, act, photo_in=None, photo_out=None):
        act.pop('polygon_id')
        act.pop('rfid')
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
                                 alerts=None, carrier=act['carrier'],
                                 trash_cat=act['trash_cat'],
                                 trash_type=act['trash_type'],
                                 operator_comments=act['operator_comments'],
                                 photo_in=photo_in,
                                 photo_out=photo_out)
        return act_json


class SignallActChecker(mixins.SignallMixin, ActsWorker,
                        mixins.SignAllAuthMe, mixins.ActsSQLCommands,
                        mixins.SignallActsGetter,
                        mixins.SignallDbAuthInfoGetter):
    def __init__(self, sql_shell, login, password, **kwargs):
        self.sql_shell = sql_shell
        self.login = login
        self.password = password
        self.working_link = self.get_full_endpoint(self.get_acts_url)
        self.tc_column_name = 'name'

    def work(self, start_date: str, end_date: str,
             polygon_name: str, page: int = 1):
        headers = self.get_headers()
        signal_acts = []
        data_frmt = self.get_acts_command(start_date, end_date, polygon_name,
                                          page)
        data_frmt = json.dumps(data_frmt)
        print(data_frmt)
        print("GETTING ACTS FROM", self.working_link)
        act_send_response = self.post_data(headers=headers,
                                           link=self.working_link,
                                           data=data_frmt)
        print('HEADERS:', headers,
              'URL:', self.working_link,
              'DATA:', data_frmt)
        act_send_response = act_send_response.json()
        signal_acts += act_send_response['acts']
        pages = act_send_response['pages']
        signal_acts_amount_weight = act_send_response['weight']
        wdb_acts_info = self.get_wdb_tonnage(
            " time_in > '{}' and time_in::date <= '{}' and tc.{}='ТКО' ".format(
                start_date, end_date, self.tc_column_name))
        print(wdb_acts_info)
        print('\nОбщее количестов актов с Signall:', signal_acts_amount_weight)
        print("Общее количество актов с WDB:",
              wdb_acts_info['info'][0]['count'])
        print('Общий тоннаж акттов с SignAll:', act_send_response['tonnage'])
        print("Тоннаж с WDB:", wdb_acts_info['info'][0]['sum'], '\n')
        count = 2
        while count <= pages:
            print(f'Собираем список {count} из {pages}')
            data_frmt = self.get_acts_command(start_date, end_date,
                                              polygon_name,
                                              count)
            data_frmt = json.dumps(data_frmt)
            act_send_response = self.post_data(headers=headers,
                                               link=self.working_link,
                                               data=data_frmt)
            act_send_response = act_send_response.json()
            signal_acts += act_send_response['acts']
            count += 1
        print('Списки собраны')
        print(len(signal_acts))
        if 'error' in act_send_response:
            print("FAILED:", act_send_response)
            return
        my_acts = self.get_acts_period(start_date, end_date,
                                       " and tc.{}='ТКО' and time_out is not null".format(
                                           self.tc_column_name))
        if my_acts['status'] == 'success':
            my_acts_ids = [int(act['ex_id']) for act in my_acts['info']]
            my_acts_ids.sort()
            my_acts_weight = {int(act['ex_id']): int(act['cargo']) for act in
                              my_acts['info']}
            signal_act_id = [int(act['number']) for act in signal_acts]
            signal_act_id.sort()
            signal_acts_weight = {int(act['number']): int(act['netto']) for act
                                  in signal_acts}
            print("MY ACTS", len(my_acts_ids))
            print('Signall_act_id', len(signal_act_id))
            print("Есть на WBD но нет на Сигнале",
                  list(set(my_acts_ids) - set(signal_act_id)))
            print("Есть на Сигнале но нет на wdb",
                  list(set(signal_act_id) - set(my_acts_ids)))
            print('Дубли на Сигнале:', set([x for x in signal_act_id if
                                            signal_act_id.count(x) > 1]))
            print('Дубли на WDB:',
                  set([x for x in my_acts_ids if my_acts_ids.count(x) > 1]))
            anomalys = {}
            for number, weight in my_acts_weight.items():
                try:
                    if not signal_acts_weight[number] == weight:
                        anomalys[number] = {}
                        anomalys[number]['signall_weight'] = \
                            signal_acts_weight[number]
                        anomalys[number]['wdb_weight'] = weight
                except KeyError:
                    anomalys[number] = 'На SignAll нет акта'
            print('Аномалии по весам', anomalys)
            return {'wdb_acts_amount': len(my_acts_ids),
                    'signall_acts_amount': len(signal_act_id),
                    'no_signall_but_wdb': list(
                        set(my_acts_ids) - set(signal_act_id)),
                    'no_wdb_but_signall': list(
                        set(signal_act_id) - set(my_acts_ids)),
                    'signall_doubles': set([x for x in signal_act_id if
                                            signal_act_id.count(x) > 1]),
                    'wdb_doubles': set(
                        [x for x in my_acts_ids if my_acts_ids.count(x) > 1]),
                    'anomalys': anomalys,
                    }


class SignallActCheckerOldWDB(SignallActChecker):
    def __init__(self, sql_shell, login, password):
        super().__init__(sql_shell, login, password)
        self.tc_column_name = 'cat_name'

    def get_acts_all_command(self):
        command = "SELECT r.id as ex_id, a.car_number, r.brutto as gross, " \
                  "r.tara as tare, r.cargo, r.time_in, r.time_out, " \
                  "clients.inn as carrier, tc.cat_name as trash_cat, " \
                  "tt.name as trash_type, oc.gross as gross_comm," \
                  "oc.tare as tare_comm, oc.additional as add_comm, " \
                  "oc.changing as changing_comm, oc.closing as closing_comm," \
                  "dro.owner as polygon_id " \
                  "FROM records r " \
                  "INNER JOIN clients ON (r.carrier=clients.id) " \
                  "INNER JOIN trash_cats tc ON (r.trash_cat=tc.id) " \
                  "INNER JOIN trash_types tt ON (r.trash_type=tt.id) " \
                  "LEFT JOIN operator_comments oc ON (r.id=oc.record_id) " \
                  "LEFT JOIN auto a ON (a.id=r.auto) " \
                  "LEFT JOIN duo_records_owning dro on r.id = dro.record " \
                  "WHERE not time_out is null "
        return command


class SignallActReuploder(mixins.SignallMixin, DataWorker,
                          mixins.SignAllAuthMe,
                          mixins.SignallActDeletter,
                          mixins.SignallActDBDeletter,
                          mixins.SignallDbAuthInfoGetter):
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
