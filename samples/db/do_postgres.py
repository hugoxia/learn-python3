import psycopg2
import psycopg2.pool
import psycopg2.extras
import logging


class Database():
    def __init__(self, app=None, db=None):
        """create a connecting pool.
            它包含一个默认的conn和cursor
            但是它的数个执行方法都会从连接池重新申请一个连接
            执行完之后会放回连接池"""
        self.log = logging.getLogger('cloud.db')
        self.log.info("db pool inited")
        if not hasattr(self, 'pool'):
            self.pool = psycopg2.pool.ThreadedConnectionPool(
                1, 10,
                host="127.0.0.1",
                port=5432,
                database="test",
                user="hugoxia",
                password="hugoxia")
        if not hasattr(self, 'conn'):
            self.conn = self.pool.getconn()
            self.conn.autocommit = True
        if not hasattr(self, 'cursor'):
            self.cursor = self.conn.cursor()

    def select(self, sql, *args, **kwargs):
        """execute a select sql"""
        self.log.debug("This SQL will be execute:\n%s" % sql)
        conn = self.pool.getconn()
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(sql, *args, **kwargs)
        if cursor.rowcount >= 1:
            row_count = str(cursor.rowcount)
        else:
            row_count = 'No'
        self.log.debug("%s rows fetched." % row_count)
        return_list = cursor.fetchall()
        cursor.close()
        self.pool.putconn(conn)
        return return_list

    def select_row(self, sql, *args, **kwargs):
        """execute a select sql and return the first row"""
        result = self.select(sql, *args, **kwargs)
        if result:
            return result[0]
        else:
            return None

    def select_dict(self, sql, *args, **kwargs):
        """execute a select sql and return a dict , column name is the keys"""
        self.log.debug("This SQL will be execute:\n%s" % sql)
        conn = self.pool.getconn()
        conn.autocommit = True
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute(sql, *args, **kwargs)
        if cursor.rowcount >= 1:
            row_count = str(cursor.rowcount)
        else:
            row_count = 'No'
        self.log.debug("%s rows fetched." % row_count)
        return_list = cursor.fetchall()
        cursor.close()
        self.pool.putconn(conn)
        return return_list

    def select_row_dict(self, sql, *args, **kwargs):
        """execute a select sql and return the first row"""
        result = self.select_dict(sql, *args, **kwargs)
        if result:
            return result[0]
        else:
            return None

    def do(self, sql, *args, **kwargs):
        """execute a non-select sql"""
        self.log.debug("This SQL will be execute:\n%s" % sql)
        conn = self.pool.getconn()
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(sql, *args, **kwargs)
        if cursor.rowcount >= 1:
            row_count = str(cursor.rowcount)
        else:
            row_count = 'No'
        self.log.debug("%s rows changed." % row_count)
        cursor.close()
        self.pool.putconn(conn)
        return None

    def get_cursor(self):
        """get a normal corsor"""
        pass

    # TODO 事务相关还没写
    def execute_in_work(self, sql, **kwargs):
        self.conn.autocommit = 1

    def execute_parallel(self, sql, **kwargs):
        """run many sql in parallel, TODO"""
        self.conn.autocommit = 0
        pass

    def commit(self):
        self.conn.commit()

    # TODO 判断结果有效性
    def get_value(self, sql, *args, **kwargs):
        """if select one row one cloumn, return a value"""
        result = self.select_row(sql, *args, **kwargs)
        if result:
            return result[0]
        else:
            return None

    def get_list(self, sql, *args, **kwargs):
        """if select one column,return as a line tuple"""
        result = self.select(sql, *args, **kwargs)
        if result:
            return [line[0] for line in result]
        else:
            return None

    def get_dict(self, sql, *args, **kwargs):
        """if select two column,return a dict,the 1st col is key"""
        result = self.select(sql, *args, **kwargs)
        if result:
            return {line[0]: line[1] for line in result}
        else:
            return None


db = Database()


class TransactionCursor():
    """get a connection and do something in work"""
    global db

    def __enter__(self):
        self.conn = db.pool.getconn()
        self.conn.autocommit = False
        self.cursor = self.conn.cursor()
        return self.cursor

    def __exit__(self, type, value, traceback):
        if type:
            self.conn.rollback()
            self.cursor.close()
            db.pool.putconn(self.conn)
            db.log.debug("事务内执行出错,回滚")
            return False
        else:
            self.conn.commit()
            self.cursor.close()
            db.pool.putconn(self.conn)
            db.log.debug("事务执行结束")
