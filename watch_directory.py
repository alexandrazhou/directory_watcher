import os
from optparse import OptionParser
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from sqlalchemy import create_engine, MetaData
from sqlalchemy import Table, Column, Text


class EventHandler(FileSystemEventHandler):

    def set_db_engine(self, engine, dbtable):
        self.engine = engine
        self.dbtable = dbtable

    def on_created(self, event):
        connection = engine.connect()
        if not event.is_directory:
            full_directory, file = os.path.split(event.src_path)
            directory = os.path.basename(full_directory)
            try:
                trans = connection.begin()
                connection.execute(
                    self.dbtable.delete().where(
                        self.dbtable.c.directory==directory).where(
                        self.dbtable.c.file==None))
                connection.execute(
                    self.dbtable.insert(),
                    full_directory=full_directory,
                    directory=directory,
                    file=file)
                trans.commit()
            except:
                trans.rollback()
                raise
        else:
            directory = os.path.basename(event.src_path)
            try:
                trans = connection.begin()
                connection.execute(
                    self.dbtable.insert(),
                    full_directory=event.src_path,
                    directory=directory,
                    file=None)
                trans.commit()
            except:
                trans.rollback()
                raise
        connection.close()

    def on_deleted(self, event):
        connection = engine.connect()
        if not event.is_directory:
            full_directory, file = os.path.split(event.src_path)
            directory = os.path.basename(full_directory)
            try:
                trans = connection.begin()
                connection.execute(
                    self.dbtable.delete().where(
                    self.dbtable.c.directory==directory).where(
                    self.dbtable.c.file==file))
                connection.execute(
                    self.dbtable.insert(),
                    full_directory=full_directory,
                    directory=directory,
                    file=None)
                trans.commit()
            except:
                trans.rollback()
                raise
        else:
            file = None
            directory = os.path.basename(event.src_path)
            try:
                trans = connection.begin()
                connection.execute(
                    self.dbtable.delete(),
                    directory=directory)
                trans.commit()
            except:
                trans.rollback()
                raise
        connection.close()

    def on_moved(self, event):
        connection = engine.connect()
        if not event.is_directory:
            full_src_directory, src_file = os.path.split(event.src_path)
            src_directory = os.path.basename(full_src_directory)
            full_dest_directory, dest_file = os.path.split(event.dest_path)
            dest_directory = os.path.basename(full_dest_directory)
            try:
                trans = connection.begin()
                connection.execute(
                    self.dbtable.delete().where(
                        self.dbtable.c.directory==src_directory).where(
                        self.dbtable.c.file==src_file))
                connection.execute(
                    self.dbtable.delete().where(
                        self.dbtable.c.directory==dest_directory).where(
                        self.dbtable.c.file==None))
                connection.execute(
                    self.dbtable.insert(),
                    full_directory=full_dest_directory,
                    directory=dest_directory,
                    file=dest_file)
                r = connection.execute(
                    self.dbtable.select().where(self.dbtable.c.directory==src_directory))
                r = [x for x in r]
                if not r and os.path.exists(os.path.dirname(event.src_path)):
                    connection.execute(
                        self.dbtable.insert(),
                        directory=src_directory,
                        full_directory=full_src_directory,
                        file=None)

                trans.commit()
            except:
                trans.rollback()
                raise
        else:
            src_directory = os.path.basename(event.src_path)
            dest_directory = os.path.basename(event.dest_path)
            try:
                trans = connection.begin()
                connection.execute(
                    self.dbtable.delete().where(
                    self.dbtable.c.directory==src_directory))
                connection.execute(
                    self.dbtable.insert(),
                    full_director=event.dest_path,
                    directory=dest_directory)
                trans.commit()
            except:
                trans.rollback()
                raise
        connection.close()


# Command line arguments
parser = OptionParser()
parser.add_option(
    "-i", "--initial", dest="initialize", action="store_true",
    default=False, help="Initialize the database")
parser.add_option(
    "-w", "--watch", dest="watch", action="store_true",
    default=False, help="Watch for changes on the filesystem.")
# parser.add_option(
#     "-x", "--silent", dest="silen", action="store_true",
#     default=False, help="Make the process run deamonically")
parser.add_option(
    "-r", "--directory", dest="directory", help="directory to query",
    metavar="DIRECTORY")
parser.add_option(
    "-d", "--database", dest="database", help="Database to be used",
    metavar="DATABASE")
parser.add_option(
    "-u", "--database-user", dest="database_user", help="Database user",
    metavar="DATABASE_USER")
parser.add_option(
    "-p", "--database-password", dest="database_password",
    help="Database password", metavar="DATABASE_PASSWORD")
parser.add_option(
    "-l", "--database-url", dest="database_url", help="Database url",
    metavar="URL")
parser.add_option(
    "-o", "--database-port", dest="database_port", help="Database port",
    metavar="PORT")
parser.add_option(
    "-t", "--database-table", dest="database_table", help="Database table",
    metavar="TABLE_NAME"
)
parser.add_option(
    "-s", "--database-schema", dest="database_schema", help="Database schema",
    metavar="SCHEMA_NAME"
)

(options, args) = parser.parse_args()

# Database connection

engine = create_engine(
    'postgresql+psycopg2://'
    '%(db_user)s:%(db_passwd)s@'
    '%(db_url)s:%(db_port)s/%('
    'db_name)s' % {
        'db_user': options.database_user,
        'db_passwd': options.database_password,
        'db_url': options.database_url,
        'db_port': options.database_port,
        'db_name': options.database})
if options.database_schema:
    meta = MetaData(bind=engine, schema=options.database_schema)
else:
    meta = MetaData(bind=engine)

dbtable = Table(
    options.database_table,
    meta,
    Column('directory', Text),
    Column('full_directory', Text),
    Column('file', Text))

# Initialize the database
if options.initialize:
    connection = engine.connect()
    connection.execute(dbtable.delete())
    path = options.directory
    for d in os.walk(path):
        for f in d[2]:
            try:
                trans = connection.begin()
                # r1 = connection.execute(dbtable.select())
                connection.execute(
                    dbtable.insert(),
                    directory=os.path.split(d[0])[-1],
                    full_directory=d[0],
                    file=f)
                trans.commit()
            except:
                trans.rollback()
                raise
        if not d[1] and not d[2]:
            try:
                trans = connection.begin()
                # r1 = connection.execute(dbtable.select())
                connection.execute(
                    dbtable.insert(),
                    directory=os.path.split(d[0])[-1],
                    full_directory=d[0],
                    file=None)
                trans.commit()
            except:
                trans.rollback()
                raise
    connection.close()

# The watching directory for changes, which are logged in DB
if options.watch:
    import logging

    path = options.directory
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    event_handler = EventHandler()
    event_handler.set_db_engine(engine, dbtable)
    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

