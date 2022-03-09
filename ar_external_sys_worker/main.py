import datetime
import requests
from ar_external_sys_worker import mixins


class DataWorker(mixins.Logger, mixins.ExSys):
    working_link = None

    def get_send_data(self):
        return True

    def format_send_data(self, data, *args, **kwargs):
        return data

    def get_local_id(self, data):
        return True

    def get_headers(self):
        return None

    def get_and_send_data(self):
        data = self.get_send_data()
        return self.send_data(data)

    def post_data(self, headers, link, data):
        return requests.post(url=link,
                      data=data,
                      headers=headers)

    def send_data(self, data):
        data_frmt = self.format_send_data(data)
        local_data_id = self.get_local_id(data)
        log_id = self.log_ex_sys_sent(local_data_id)
        headers = self.get_headers()
        act_send_response = self.post_data(headers=headers,
                                           link=self.working_link,
                                           data=data_frmt)
        ex_sys_data_id = self.get_ex_sys_id_from_response(act_send_response)
        self.log_ex_sys_get(ex_sys_data_id, log_id)
        return True

    def get_ex_sys_id_from_response(self, response):
        pass


class SignallActWorker(mixins.SignallMixin, DataWorker, mixins.ActWorkerMixin,
                       mixins.SignAllAuthMe, mixins.SignallPhotoEncoderMixin,
                       mixins.ActsSQLCommands):
    def __init__(self, sql_shell, login, password):
        self.login = login
        self.password = password
        self.sql_shell = sql_shell
        self.working_link = self.get_full_endpoint(self.link_create_act)

    def get_local_id(self, data):
        return data['ex_id']

    def format_send_data(self, act, photo_in=None, photo_out=None):
        act.pop('polygon_id')
        if act['alerts']:
            act['alerts'] = act.pop('alerts').split('|')
        act['operator_comments'] = {'gross_comm': act.pop('gross_comm'),
                                    'tare_comm': act.pop('tare_comm'),
                                    'add_comm': act.pop('add_comm'),
                                    'changing_comm': act.pop(
                                        'changing_comm'),
                                    'closing_comm': act.pop(
                                        'closing_comm')}
        act['photo_in'] = self.get_photo_path(act['ex_id'], 1)
        act['photo_out'] = self.get_photo_path(act['ex_id'], 2)
        if photo_in:
            photo_in = self.get_photo_data(photo_in)
        if photo_out:
            photo_out = self.get_photo_data(photo_out)
        act_json = self.get_json(car_number=act['car_number'],
                                 ex_id=act['ex_id'], gross=act['gross'],
                                 tare=act['tare'], cargo=act['cargo'],
                                 time_in=act['time_in'].strftime(
                                     '%Y-%m-%d %H:%M:%S'),
                                 time_out=act['time_out'].strftime(
                                     '%Y-%m-%d %H:%M:%S'),
                                 alerts=act['alerts'], carrier=act['carrier'],
                                 trash_cat=act['trash_cat'],
                                 trash_type=act['trash_type'],
                                 operator_comments=act['operator_comments'],
                                 photo_in=photo_in,
                                 photo_out=photo_out)
        return act_json

    def get_photo_path(self, record_id, photo_type):
        command = "SELECT p.path FROM photos p " \
                  "INNER JOIN record_photos rp ON (p.id = rp.photo_id) " \
                  "WHERE p.photo_type={} and rp.record_id={} LIMIT 1".format(
            photo_type,
            record_id)
        response = self.sql_shell.try_execute_get(command)
        if response:
            return response[0][0]

    def get_headers(self):
        token = self.get_token()
        headers = {'Authorization': token}
        return headers

    def get_send_data(self):
        return self.get_one_unsend_act()

    def get_ex_sys_id_from_response(self, response):
        response = response.json()
        return response['act_id']

    def send_unsend_acts(self):
        data = self.get_unsend_acts()
        for act in data:
            self.send_data(act)
