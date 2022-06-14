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


if __name__ == "__main__":
    unittest.main()
