import unittest
import os
from wsqluse.wsqluse import Wsqluse
from ar_external_sys_worker import mixins


class TestCase(unittest.TestCase):
    sql_shell = Wsqluse(dbname=os.environ.get('DB_NAME'),
                        user=os.environ.get('DB_USER'),
                        password=os.environ.get('DB_PASS'),
                        host=os.environ.get('DB_HOST'))

    def test_sql_commands(self):
        inst = mixins.ActsSQLCommands()
        inst.sql_shell = self.sql_shell
        res = inst.get_alerts(1)
        print(res)

    def test_get_ex_sys_id(self):
        class TestGetExSys(mixins.ExSysActIdExtractor):
            sql_shell = self.sql_shell
            ex_sys_id = 1
        inst = TestGetExSys()
        response = inst.extract_act_ex_id(1)
        print('RES', response)

    def test_get_signall_id(self):
        inst = mixins.SignallActDBDeletter()
        inst.sql_shell = self.sql_shell
        res = inst.select_signall_id_from_send_reports(14)
        print(res)
        inst.delete_act_from_send_reports(14)

if __name__ == "__main__":
    unittest.main()
