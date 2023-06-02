import logging
import warnings
import pandas as pd

from sqlalchemy import create_engine, MetaData, Table, Column, Date, Integer, Time, Float

warnings.filterwarnings("ignore", category=DeprecationWarning)
logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)


class ETL:

    """
    This class is to build and execute end-to-end ETL pipeline.
    Data sources : CSV Files
    Database: PostgreSQL
    """

    def __init__(self, data_sources: dict):
        
        logging.info('Init Data Pipeline.')
        self.ds = data_sources
        self.df = None

    def get_data(self):
        
        logging.info('[EXTRACTION] Read CSV files.')
        self.df = {table_name: pd.read_csv(elem['filepath'], na_values=['nan', 'NaN'], parse_dates=elem['date_cols']) for table_name, elem in self.ds.items()}
        logging.info('[EXTRACTION] DONE.')
        
    def transform(self):
        
        logging.info('[TRANSFORMATION] Computing total working hours for each employee.')
        self.df['timesheets'] = self.df['timesheets_raw'].dropna(subset=['checkin', 'checkout'])
        self.df['timesheets'].loc[:, 'date_checkin'] = pd.to_datetime(
            self.df['timesheets'].apply(lambda r: str(r['date']) + ' ' + r['checkin'], axis=1)
        )
        self.df['timesheets'].loc[:, 'date_checkout'] = pd.to_datetime(
            self.df['timesheets'].apply(lambda r: str(r['date']) + ' ' + r['checkout'], axis=1)
        )
        self.df['timesheets'].loc[:, 'total_hours'] = self.df['timesheets'].apply(lambda r: r['date_checkout'] - r['date_checkin'], axis=1)
        self.df['timesheets'].loc[:, 'total_hours'] = self.df['timesheets']['total_hours'].dt.total_seconds()/60/60
        self.df['timesheets'].loc[:, 'year'] = self.df['timesheets']['date'].dt.year
        self.df['timesheets'].loc[:, 'month'] = self.df['timesheets']['date'].dt.month
        self.df['timesheets'].drop(['timesheet_id', 'date', 'checkin', 'checkout', 'date_checkin', 'date_checkout'], axis=1, inplace=True)        
        
        self.df['timesheets'] = self.df['timesheets'].groupby(['year', 'month', 'employee_id'])['total_hours'].sum().reset_index()
        
        logging.info('[TRANSFORMATION] Computing total salary and working hours for each branch.')
        self.df['branch_salary'] = self.df['employees_raw'].set_index('employe_id').join(self.df['timesheets'].set_index('employee_id'), how='left')
        
        self.df['branch_salary'].drop(['join_date', 'resign_date'], axis=1, inplace=True)
        self.df['branch_salary'] = self.df['branch_salary'].groupby(['year', 'month', 'branch_id'])[['salary','total_hours']].sum().reset_index()
        
        logging.info('[TRANSFORMATION] Computing hourly salary of each branch.')
        self.df['branch_salary'].loc[:, 'salary_per_hour'] = self.df['branch_salary'].apply(lambda r: r['salary']/r['total_hours'], axis=1)
        self.df['branch_salary'].drop(['total_hours', 'salary'], axis=1, inplace=True)
        
        logging.info('[TRANSFORMATION] DONE.')
        
    def build_schema(self, engine):
        
        logging.info('Establish database and tables.')
        
        # Create the Metadata Object
        metadata_obj = MetaData()

        # Define the profile table

        # create table
        employees = Table(
            'employees',
            metadata_obj,
            Column('employee_id', Integer, primary_key=True),
            Column('branch_id', Integer),
            Column('salary', Integer),
            Column('join_date', Date),
            Column('resign_date', Date),
        )

        timesheets = Table(
            'timesheets',
            metadata_obj,
            Column('timesheet_id', Integer, primary_key=True),
            Column('employee_id', Integer),
            Column('date', Date),
            Column('checkin', Time),
            Column('checkout', Time),
        )

        branch_salary = Table(
            'branch_salary',
            metadata_obj,
            Column('branch_id', Integer),
            Column('year', Integer),
            Column('month', Integer),
            Column('salary_per_hour', Float),
        )

        # Create the profile table
        metadata_obj.create_all(engine)
        
    def loadToDB(self, engine):
        
        logging.info('[LOAD] Load data into PostgreSQL DB.')
        self.df['employees_raw'].to_sql('employees', con=engine, if_exists='replace')
        self.df['timesheets_raw'].to_sql('timesheets', con=engine, if_exists='replace')
        self.df['branch_salary'].to_sql('branch_salary', con=engine, if_exists='replace')
        logging.info('[LOAD] DONE.')


if __name__ == '__main__':

    # Establish database engine
    engine = create_engine('postgresql+psycopg2://postgres:postgres@localhost:5433/mekariDB')

    # Create data pipeline
    # Please changes the filepath 
    dataPipe = ETL(data_sources={
        'timesheets_raw': 
        {
            'filepath': 'C:\\Users\\mmirz\\Dokumen\\Projects\\Mekari\\data_source\\timesheets.csv',
            'date_cols': ['date']
        },
        'employees_raw':
        {
            'filepath': 'C:\\Users\\mmirz\\Dokumen\\Projects\\Mekari\\data_source\\employees.csv',
            'date_cols': ['join_date', 'resign_date']
        }
    })

    # Execute data pipeline
    dataPipe.get_data()
    dataPipe.build_schema(engine=engine)
    dataPipe.transform()
    dataPipe.loadToDB(engine=engine)