import unittest
import psycopg2
from psycopg2.pool import SimpleConnectionPool


class TestConnectionToDB(unittest.TestCase):

    def setUp(self):
        self.DB_PORT = "5432"
        self.DB_HOST = "0.0.0.0"
        self.DB_ROOT_USERNAME = "sbt"
        self.DB_ROOT_PASSWORD = "test_pswd"
        self.DB_NAME = "test_db"
        self.DB_TEST_USERNAME = "test_user"
        self.DB_TEST_PASSWORD = "test_pswd2024"

        conn = psycopg2.connect(dbname=self.DB_NAME, user=self.DB_ROOT_USERNAME, host=self.DB_HOST, port=self.DB_PORT,
                                password=self.DB_ROOT_PASSWORD)
        cur = conn.cursor()
        cur.execute("CREATE USER {} WITH PASSWORD \'{}\';".format(self.DB_TEST_USERNAME, self.DB_TEST_PASSWORD))
        conn.commit()
        conn.close()

    def test_success_connection(self):
        conn = psycopg2.connect(dbname=self.DB_NAME, user=self.DB_TEST_USERNAME, host=self.DB_HOST, port=self.DB_PORT,
                                password=self.DB_TEST_PASSWORD)
        self.assertEqual(False, conn.closed)

    def test_wrong_password(self):
        with self.assertRaises(psycopg2.OperationalError) as cm:
            psycopg2.connect(dbname=self.DB_NAME, user=self.DB_TEST_USERNAME, host=self.DB_HOST, port=self.DB_PORT,
                             password='test_pswd1')
        self.assertRegex(str(cm.exception),
                         r".*FATAL:  password authentication failed for user \"{}\".*".format(self.DB_TEST_USERNAME))

    def test_missing_password(self):
        with self.assertRaises(psycopg2.OperationalError) as cm:
            psycopg2.connect(dbname=self.DB_NAME, user=self.DB_TEST_USERNAME, host=self.DB_HOST,
                             port=self.DB_PORT, password='')
        self.assertRegex(str(cm.exception), r".*no password supplied")

    def test_not_existed_user(self):
        not_existed_user = "test_user1"
        with self.assertRaises(psycopg2.OperationalError) as cm:
            psycopg2.connect(dbname=self.DB_NAME, user="test_user1", host=self.DB_HOST, port=self.DB_PORT,
                             password=self.DB_TEST_PASSWORD)
        self.assertRegex(str(cm.exception),
                         r".*FATAL:  password authentication failed for user \"{}\".*".format(not_existed_user))

    def test_valid_user_with_space(self):
        spase_user_name = "test_user "
        with self.assertRaises(psycopg2.OperationalError) as cm:
            psycopg2.connect(dbname=self.DB_NAME, user="test_user ", host=self.DB_HOST,
                             port=self.DB_PORT, password=self.DB_TEST_PASSWORD)
        self.assertRegex(str(cm.exception),
                         r".*FATAL:  password authentication failed for user \"{}\".*".format(spase_user_name))

    def test_invalid_host(self):
        invalid_host = "somehost"
        with self.assertRaises(psycopg2.OperationalError) as cm:
            psycopg2.connect(dbname=self.DB_NAME, user=self.DB_TEST_USERNAME, host=invalid_host, port=self.DB_PORT,
                             password=self.DB_TEST_PASSWORD)
        self.assertRegex(str(cm.exception),
                         r"could not translate host name \"{}\" to address: nodename nor servname provided, or not known"
                         .format(invalid_host))

    def test_missing_host(self):
        missing_host = ""
        with self.assertRaises(psycopg2.OperationalError) as cm:
            psycopg2.connect(dbname=self.DB_NAME, user=self.DB_TEST_USERNAME, host=missing_host, port=self.DB_PORT,
                             password=self.DB_TEST_PASSWORD)
        self.assertRegex(str(cm.exception),
                         r".*Is the server running locally and accepting connections on that socket?")

    def test_invalid_port(self):
        invalid_port = "0000"
        with self.assertRaises(psycopg2.OperationalError) as cm:
            psycopg2.connect(dbname=self.DB_NAME, user=self.DB_TEST_USERNAME, host=self.DB_HOST, port=invalid_port,
                             password=self.DB_TEST_PASSWORD)
        self.assertRegex(str(cm.exception),
                         r".*invalid port number: \"{}\".*".format(invalid_port))

    def test_missing_port(self):
        conn = psycopg2.connect(dbname=self.DB_NAME, user=self.DB_TEST_USERNAME, host=self.DB_HOST,
                                password=self.DB_TEST_PASSWORD)
        self.assertEqual(False, conn.closed)

    def test_invalid_db_name(self):
        invalid_db_name = "test_db1"
        with self.assertRaises(psycopg2.OperationalError) as cm:
            psycopg2.connect(dbname=invalid_db_name, user=self.DB_TEST_USERNAME, host=self.DB_HOST, port=self.DB_PORT,
                             password=self.DB_TEST_PASSWORD)
        self.assertRegex(str(cm.exception),
                         r".*FATAL:  database \"{}\" does not exist".format(invalid_db_name))

    def test_missing_db_name(self):
        with self.assertRaises(psycopg2.OperationalError) as cm:
            psycopg2.connect(user=self.DB_TEST_USERNAME, host=self.DB_HOST, port=self.DB_PORT,
                             password=self.DB_TEST_PASSWORD)
        self.assertRegex(str(cm.exception),
                         r".*FATAL:  database \"{}\" does not exist".format(self.DB_TEST_USERNAME))

    def test_max_connections(self):
        with self.assertRaises(psycopg2.OperationalError) as cm:
            SimpleConnectionPool(minconn=100, maxconn=110, dbname=self.DB_NAME, user=self.DB_TEST_USERNAME,
                                 host=self.DB_HOST, port=self.DB_PORT, password=self.DB_TEST_PASSWORD)
        self.assertRegex(str(cm.exception),
                         r".*FATAL:  remaining connection slots are reserved for roles with the SUPERUSER attribute")

    def test_several_connections(self):
        SimpleConnectionPool(minconn=5, maxconn=10, dbname=self.DB_NAME, user=self.DB_TEST_USERNAME,
                             host=self.DB_HOST, port=self.DB_PORT, password=self.DB_TEST_PASSWORD)

    def test_block_user(self):
        conn = psycopg2.connect(dbname=self.DB_NAME, user=self.DB_ROOT_USERNAME, host=self.DB_HOST, port=self.DB_PORT,
                                password=self.DB_ROOT_PASSWORD)
        cur = conn.cursor()
        cur.execute("ALTER ROLE {} WITH NOLOGIN;".format(self.DB_TEST_USERNAME))
        conn.commit()
        conn.close()
        with self.assertRaises(psycopg2.OperationalError) as cm:
            psycopg2.connect(dbname=self.DB_NAME, user=self.DB_TEST_USERNAME, host=self.DB_HOST, port=self.DB_PORT,
                         password=self.DB_TEST_PASSWORD)
        self.assertRegex(str(cm.exception),
                         r".*FATAL:  role \"{}\" is not permitted to log in".format(self.DB_TEST_USERNAME))

    def tearDown(self):
        conn = psycopg2.connect(dbname=self.DB_NAME, user=self.DB_ROOT_USERNAME, host=self.DB_HOST, port=self.DB_PORT,
                                password=self.DB_ROOT_PASSWORD)
        cur = conn.cursor()
        cur.execute("DROP USER IF EXISTS {};".format(self.DB_TEST_USERNAME))
        conn.commit()
        conn.close()

if __name__ == '__main__':
    unittest.main()
