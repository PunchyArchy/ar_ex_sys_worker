import unittest
import os
from wsqluse.wsqluse import Wsqluse
from ar_external_sys_worker import mixins


class TestCase(unittest.TestCase):
    sql_shell = Wsqluse(dbname=os.environ.get('DB_NAME'),
                        user=os.environ.get('DB_USER'),
                        password=os.environ.get('DB_PASS'),
                        host=os.environ.get('DB_HOST'))

    def testSignallAuthMe(self):
        login = '1@signal.com'
        password = 'd4GExhec'
        inst = mixins.SignAllAuthMe(login=login,
                                    password=password)
        response = inst.auth_me()
        self.assertTrue(response.status_code == 200)
        token = inst.get_token()

    def test_ActsGetter(self):
        inst = mixins.ActsSQLCommands()
        inst.table_id = 1
        response = self.sql_shell.try_execute_get(inst.get_unsend_command())

    def test_SignAllAuthWorker(self):
        inst = mixins.SignallAuth(self.sql_shell, 1)
        res = inst.auth_me()
        print(res.json())

    def test_DuoAcstSQLCommands(self):
        inst = mixins.DuoAcstSQLCommands()
        inst.polygon_id = 1
        inst.table_id = 1
        command = inst.get_unsend_command()
        print(command)
        acts = self.sql_shell.try_execute_get(command)
        print("ACTS", acts)

if __name__ == "__main__":
    unittest.main()
