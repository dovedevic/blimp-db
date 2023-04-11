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

    class TableRecord(BaseModel):
        supplier_key: int
        name: str
        address: str
        city: str
        nation: str
        region: str
        phone: str

    def _translate(self, db_text: str) -> TableRecord:
        db_columns = db_text.split('|')
        return self.TableRecord(
            supplier_key=db_columns[0],
            name=db_columns[1],
            address=db_columns[2],
            city=db_columns[3],
            nation=db_columns[4],
            region=db_columns[5],
            phone=db_columns[6],
        )


class SSBPartTable(SSBTable):
    _table_file_name = "sf{scale_factor}-part.tbl"

    class TableRecord(BaseModel):
        part_key: int
        name: str
        mfgr: str
        category: str
        brand: str
        color: str
        type: str
        size: int
        container: str

    def _translate(self, db_text: str) -> TableRecord:
        db_columns = db_text.split('|')
        return self.TableRecord(
            part_key=db_columns[0],
            name=db_columns[1],
            mfgr=db_columns[2],
            category=db_columns[3],
            brand=db_columns[4],
            color=db_columns[5],
            type=db_columns[6],
            size=db_columns[7],
            container=db_columns[8],
        )


class SSBCustomerTable(SSBTable):
    _table_file_name = "sf{scale_factor}-customer.tbl"

    class TableRecord(BaseModel):
        customer_key: int
        name: str
        address: str
        city: str
        nation: str
        region: str
        phone: str
        mktsegmenmt: str

    def _translate(self, db_text: str) -> TableRecord:
        db_columns = db_text.split('|')
        return self.TableRecord(
            customer_key=db_columns[0],
            name=db_columns[1],
            address=db_columns[2],
            city=db_columns[3],
            nation=db_columns[4],
            region=db_columns[5],
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
