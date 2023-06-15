import os

from pydantic import BaseModel


class SSBTable:
    """
    Base Class of the Star Schema Benchmark Pythonic/Pydantic Wrapper. Derived classes must define a table file name
    with scale factor format, a record schema, and a translation method. Classes will generate records as-needed once.
    """
    _table_file_name = "sf{scale_factor}.tbl"

    class TableRecord(BaseModel):
        pass

    def __init__(self, scale_factor=100, fill_to=0, no_storage=False):
        self._fp = open(
            os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                'dbs',
                self._table_file_name.format(scale_factor=scale_factor)
            ), 'r'
        )
        self._no_storage = no_storage

        if not no_storage:
            self._db = []
            self._done = False
            if fill_to > 0:
                for _ in self.records:
                    if len(self._db) >= fill_to:
                        break
                else:
                    raise IndexError("not enough records were parsable to satisfy fill_to amount")
                self._done = True
                self._fp.close()
        else:
            self._done = False

    def _translate(self, db_text: str) -> TableRecord:
        raise NotImplemented

    @property
    def records(self) -> TableRecord:
        if self._no_storage:
            self._fp.seek(0)
            for row in self._fp.readlines():
                yield self._translate(row)
        else:
            for r in self._db:
                yield r

            if not self._done:
                for row in self._fp.readlines():
                    self._db.append(self._translate(row))
                    yield self._db[-1]
                self._done = True
                self._fp.close()

    def get_generated_record(self, index) -> TableRecord:
        if self._no_storage:
            raise RuntimeError("cannot index records when the DB is set to no_storage mode")

        if index < 0:
            raise IndexError
        elif index >= len(self._db):
            raise IndexError
        else:
            return self._db[index]


class SSBEncoding:
    _encoding = {}

    @classmethod
    def convert(cls, raw: str) -> int:
        return cls._encoding[raw]


class SSBRegionEncoding(SSBEncoding):
    AFRICA = 0
    AMERICA = 1
    ASIA = 2
    EUROPE = 3
    MIDDLE_EAST = 4

    _encoding = {
        "AFRICA": AFRICA,
        "AMERICA": AMERICA,
        "ASIA": ASIA,
        "EUROPE": EUROPE,
        "MIDDLE EAST": MIDDLE_EAST,
    }


class SSBNationEncoding(SSBEncoding):
    ALGERIA = 0
    ARGENTINA = 1
    BRAZIL = 2
    CANADA = 3
    EGYPT = 4
    ETHIOPIA = 5
    FRANCE = 6
    GERMANY = 7
    INDIA = 8
    INDONESIA = 9
    IRAN = 10
    IRAQ = 11
    JAPAN = 12
    JORDAN = 13
    KENYA = 14
    MOROCCO = 15
    MOZAMBIQUE = 16
    PERU = 17
    CHINA = 18
    ROMANIA = 19
    SAUDI_ARABIA = 20
    VIETNAM = 21
    RUSSIA = 22
    UNITED_KINGDOM = 23
    UNITED_STATES = 24

    _encoding = {
        "ALGERIA": ALGERIA,
        "ARGENTINA": ARGENTINA,
        "BRAZIL": BRAZIL,
        "CANADA": CANADA,
        "EGYPT": EGYPT,
        "ETHIOPIA": ETHIOPIA,
        "FRANCE": FRANCE,
        "GERMANY": GERMANY,
        "INDIA": INDIA,
        "INDONESIA": INDONESIA,
        "IRAN": IRAN,
        "IRAQ": IRAQ,
        "JAPAN": JAPAN,
        "JORDAN": JORDAN,
        "KENYA": KENYA,
        "MOROCCO": MOROCCO,
        "MOZAMBIQUE": MOZAMBIQUE,
        "PERU": PERU,
        "CHINA": CHINA,
        "ROMANIA": ROMANIA,
        "SAUDI ARABIA": SAUDI_ARABIA,
        "VIETNAM": VIETNAM,
        "RUSSIA": RUSSIA,
        "UNITED KINGDOM": UNITED_KINGDOM,
        "UNITED STATES": UNITED_STATES,
    }


class SSBCityEncoding(SSBEncoding):
    _prefixes = ["ALGERIA  ", "ARGENTINA", "BRAZIL   ", "CANADA   ", "EGYPT    ", "ETHIOPIA ", "FRANCE   ",
                 "GERMANY  ", "INDIA    ", "INDONESIA", "IRAN     ", "IRAQ     ", "JAPAN    ", "JORDAN   ",
                 "KENYA    ", "MOROCCO  ", "MOZAMBIQU", "PERU     ", "CHINA    ", "ROMANIA  ", "SAUDI ARA",
                 "VIETNAM  ", "RUSSIA   ", "UNITED KI", "UNITED ST"]
    _encoding = {}
    for __p in _prefixes:
        for __i in range(10):
            _encoding[f"{__p}{__i}"] = len(_encoding)


class SSBMFGREncoding(SSBEncoding):
    MFGR_1 = 0
    MFGR_2 = 1
    MFGR_3 = 2
    MFGR_4 = 3
    MFGR_5 = 4

    _encoding = {
        "MFGR#1": MFGR_1,
        "MFGR#2": MFGR_2,
        "MFGR#3": MFGR_3,
        "MFGR#4": MFGR_4,
        "MFGR#5": MFGR_5,
    }


class SSBBrandEncoding(SSBEncoding):
    _encoding = {}
    for __i in range(1000):
        _encoding[f"MFGR#{__i//200+1}{(__i%200)//40+1}{__i%40+1}"] = len(_encoding)


class SSBCategoryEncoding(SSBEncoding):
    _encoding = {}
    for __i in range(25):
        _encoding[f"MFGR#{__i//5+1}{__i%5+1}"] = len(_encoding)


class SSBDateTable(SSBTable):
    _table_file_name = "sf{scale_factor}-date.tbl"

    class TableRecord(BaseModel):
        date_key: int
        date: str
        day_of_week: str
        month: str
        year: int
        year_month_num: int
        year_month: str
        day_num_in_week: int
        day_num_in_month: int
        day_num_in_year: int
        month_num_in_year: int
        week_num_in_year: int
        selling_season: str
        last_day_in_week_flag: bool
        last_day_in_month_flag: bool
        holiday_flag: bool
        weekday_flag: bool

    def _translate(self, db_text: str) -> TableRecord:
        db_columns = db_text.split('|')
        return self.TableRecord(
            date_key=db_columns[0],
            date=db_columns[1],
            day_of_week=db_columns[2],
            month=db_columns[3],
            year=db_columns[4],
            year_month_num=db_columns[5],
            year_month=db_columns[6],
            day_num_in_week=db_columns[7],
            day_num_in_month=db_columns[8],
            day_num_in_year=db_columns[9],
            month_num_in_year=db_columns[10],
            week_num_in_year=db_columns[11],
            selling_season=db_columns[12],
            last_day_in_week_flag=db_columns[13],
            last_day_in_month_flag=db_columns[14],
            holiday_flag=db_columns[15],
            weekday_flag=db_columns[16],
        )


class SSBSupplierTable(SSBTable):
    _table_file_name = "sf{scale_factor}-supplier.tbl"
    _city_encoder = SSBCityEncoding()
    _nation_encoder = SSBNationEncoding()
    _region_encoder = SSBRegionEncoding()

    class TableRecord(BaseModel):
        supplier_key: int
        name: str
        address: str
        city: int
        nation: int
        region: int
        phone: str

    def _translate(self, db_text: str) -> TableRecord:
        db_columns = db_text.split('|')
        return self.TableRecord(
            supplier_key=db_columns[0],
            name=db_columns[1],
            address=db_columns[2],
            city=self._city_encoder.convert(db_columns[3]),
            nation=self._nation_encoder.convert(db_columns[4]),
            region=self._region_encoder.convert(db_columns[5]),
            phone=db_columns[6],
        )


class SSBPartTable(SSBTable):
    _table_file_name = "sf{scale_factor}-part.tbl"
    _mfgr_encoder = SSBMFGREncoding()
    _brand_encoder = SSBBrandEncoding()
    _category_encoder = SSBCategoryEncoding()

    class TableRecord(BaseModel):
        part_key: int
        name: str
        mfgr: int
        category: int
        brand: int
        color: str
        type: str
        size: int
        container: str

    def _translate(self, db_text: str) -> TableRecord:
        db_columns = db_text.split('|')
        return self.TableRecord(
            part_key=db_columns[0],
            name=db_columns[1],
            mfgr=self._mfgr_encoder.convert(db_columns[2]),
            category=self._category_encoder.convert(db_columns[3]),
            brand=self._brand_encoder.convert(db_columns[4]),
            color=db_columns[5],
            type=db_columns[6],
            size=db_columns[7],
            container=db_columns[8],
        )


class SSBCustomerTable(SSBTable):
    _table_file_name = "sf{scale_factor}-customer.tbl"
    _city_encoder = SSBCityEncoding()
    _nation_encoder = SSBNationEncoding()
    _region_encoder = SSBRegionEncoding()

    class TableRecord(BaseModel):
        customer_key: int
        name: str
        address: str
        city: int
        nation: int
        region: int
        phone: str
        mktsegmenmt: str

    def _translate(self, db_text: str) -> TableRecord:
        db_columns = db_text.split('|')
        return self.TableRecord(
            customer_key=db_columns[0],
            name=db_columns[1],
            address=db_columns[2],
            city=self._city_encoder.convert(db_columns[3]),
            nation=self._nation_encoder.convert(db_columns[4]),
            region=self._region_encoder.convert(db_columns[5]),
            phone=db_columns[6],
            mktsegmenmt=db_columns[7],
        )


class SSBLineOrderTable(SSBTable):
    _table_file_name = "sf{scale_factor}-lineorder-truncated.tbl"

    class TableRecord(BaseModel):
        order_key: int
        line_number: int
        customer_key: int
        part_key: int
        supplier_key: int
        order_date_key: int
        order_priority: str
        ship_priority: str
        quantity: int
        extended_price: int
        order_total_price: int
        discount: int
        revenue: int
        supply_cost: int
        tax: int
        commit_date_key: int
        ship_mode: str

    def _translate(self, db_text: str) -> TableRecord:
        db_columns = db_text.split('|')
        return self.TableRecord(
            order_key=db_columns[0],
            line_number=db_columns[1],
            customer_key=db_columns[2],
            part_key=db_columns[3],
            supplier_key=db_columns[4],
            order_date_key=db_columns[5],
            order_priority=db_columns[6],
            ship_priority=db_columns[7],
            quantity=db_columns[8],
            extended_price=db_columns[9],
            order_total_price=db_columns[10],
            discount=db_columns[11],
            revenue=db_columns[12],
            supply_cost=db_columns[13],
            tax=db_columns[14],
            commit_date_key=db_columns[15],
            ship_mode=db_columns[16],
        )
