from typing import List, Any

from src.simulators.result import RuntimeResult

from studies.star_schema_benchmark.queries.q1.q1_1 import \
    SSBQuery1p1BlimpVQuantityDiscountDate, SSBQuery1p1BlimpQuantityDiscountDate
from studies.star_schema_benchmark.queries.q1.q1_2 import \
    SSBQuery1p2BlimpVQuantityDiscountDate, SSBQuery1p2BlimpQuantityDiscountDate
from studies.star_schema_benchmark.queries.q1.q1_3 import \
    SSBQuery1p3BlimpVQuantityDiscountDate, SSBQuery1p3BlimpQuantityDiscountDate

from studies.star_schema_benchmark.queries.q2.q2_1 import \
    SSBQuery2p1BlimpVSupplierPartDate, SSBQuery2p1BlimpSupplierPartDate, \
    SSBQuery2p1BlimpVPartSupplierDate, SSBQuery2p1BlimpPartSupplierDate
from studies.star_schema_benchmark.queries.q2.q2_2 import \
    SSBQuery2p2BlimpVSupplierPartDate, SSBQuery2p2BlimpSupplierPartDate, \
    SSBQuery2p2BlimpVPartSupplierDate, SSBQuery2p2BlimpPartSupplierDate
from studies.star_schema_benchmark.queries.q2.q2_3 import \
    SSBQuery2p3BlimpVSupplierPartDate, SSBQuery2p3BlimpSupplierPartDate, \
    SSBQuery2p3BlimpVPartSupplierDate, SSBQuery2p3BlimpPartSupplierDate

from studies.star_schema_benchmark.queries.q3.q3_1 import \
    SSBQuery3p1BlimpVSupplierCustomerDate, SSBQuery3p1BlimpSupplierCustomerDate, \
    SSBQuery3p1BlimpVCustomerSupplierDate, SSBQuery3p1BlimpCustomerSupplierDate
from studies.star_schema_benchmark.queries.q3.q3_2 import \
    SSBQuery3p2BlimpVSupplierCustomerDate, SSBQuery3p2BlimpSupplierCustomerDate, \
    SSBQuery3p2BlimpVCustomerSupplierDate, SSBQuery3p2BlimpCustomerSupplierDate
from studies.star_schema_benchmark.queries.q3.q3_3 import \
    SSBQuery3p3BlimpVSupplierCustomerDate, SSBQuery3p3BlimpSupplierCustomerDate, \
    SSBQuery3p3BlimpVCustomerSupplierDate, SSBQuery3p3BlimpCustomerSupplierDate
from studies.star_schema_benchmark.queries.q3.q3_4 import \
    SSBQuery3p4BlimpVSupplierCustomerDate, SSBQuery3p4BlimpSupplierCustomerDate, \
    SSBQuery3p4BlimpVCustomerSupplierDate, SSBQuery3p4BlimpCustomerSupplierDate

from studies.star_schema_benchmark.queries.q4.q4_1 import \
    SSBQuery4p1BlimpVCustomerPartSupplierDate, SSBQuery4p1BlimpVCustomerSupplierPartDate, \
    SSBQuery4p1BlimpVSupplierCustomerPartDate, SSBQuery4p1BlimpVPartCustomerSupplierDate, \
    SSBQuery4p1BlimpVSupplierPartCustomerDate, SSBQuery4p1BlimpVPartSupplierCustomerDate, \
    SSBQuery4p1BlimpCustomerPartSupplierDate, SSBQuery4p1BlimpCustomerSupplierPartDate, \
    SSBQuery4p1BlimpSupplierCustomerPartDate, SSBQuery4p1BlimpPartCustomerSupplierDate, \
    SSBQuery4p1BlimpSupplierPartCustomerDate, SSBQuery4p1BlimpPartSupplierCustomerDate
from studies.star_schema_benchmark.queries.q4.q4_2 import \
    SSBQuery4p2BlimpVCustomerPartSupplierDate, SSBQuery4p2BlimpVCustomerSupplierPartDate, \
    SSBQuery4p2BlimpVSupplierCustomerPartDate, SSBQuery4p2BlimpVPartCustomerSupplierDate, \
    SSBQuery4p2BlimpVSupplierPartCustomerDate, SSBQuery4p2BlimpVPartSupplierCustomerDate, \
    SSBQuery4p2BlimpCustomerPartSupplierDate, SSBQuery4p2BlimpCustomerSupplierPartDate, \
    SSBQuery4p2BlimpSupplierCustomerPartDate, SSBQuery4p2BlimpPartCustomerSupplierDate, \
    SSBQuery4p2BlimpSupplierPartCustomerDate, SSBQuery4p2BlimpPartSupplierCustomerDate
from studies.star_schema_benchmark.queries.q4.q4_3 import \
    SSBQuery4p3BlimpVCustomerPartSupplierDate, SSBQuery4p3BlimpVCustomerSupplierPartDate, \
    SSBQuery4p3BlimpVSupplierCustomerPartDate, SSBQuery4p3BlimpVPartCustomerSupplierDate, \
    SSBQuery4p3BlimpVSupplierPartCustomerDate, SSBQuery4p3BlimpVPartSupplierCustomerDate, \
    SSBQuery4p3BlimpCustomerPartSupplierDate, SSBQuery4p3BlimpCustomerSupplierPartDate, \
    SSBQuery4p3BlimpSupplierCustomerPartDate, SSBQuery4p3BlimpPartCustomerSupplierDate, \
    SSBQuery4p3BlimpSupplierPartCustomerDate, SSBQuery4p3BlimpPartSupplierCustomerDate


class QueryFlights:
    """Defines all the SSB Query flights with varying join, filter, and emit orders."""
    qf1_1 = [
        SSBQuery1p1BlimpQuantityDiscountDate,
        SSBQuery1p1BlimpVQuantityDiscountDate,
    ]

    qf1_2 = [
        SSBQuery1p2BlimpQuantityDiscountDate,
        SSBQuery1p2BlimpVQuantityDiscountDate,
    ]

    qf1_3 = [
        SSBQuery1p3BlimpQuantityDiscountDate,
        SSBQuery1p3BlimpVQuantityDiscountDate,
    ]

    qf2_1 = [
        SSBQuery2p1BlimpSupplierPartDate,
        SSBQuery2p1BlimpPartSupplierDate,
        SSBQuery2p1BlimpVSupplierPartDate,
        SSBQuery2p1BlimpVPartSupplierDate,
    ]

    qf2_2 = [
        SSBQuery2p2BlimpSupplierPartDate,
        SSBQuery2p2BlimpPartSupplierDate,
        SSBQuery2p2BlimpVSupplierPartDate,
        SSBQuery2p2BlimpVPartSupplierDate,
    ]

    qf2_3 = [
        SSBQuery2p3BlimpSupplierPartDate,
        SSBQuery2p3BlimpPartSupplierDate,
        SSBQuery2p3BlimpVSupplierPartDate,
        SSBQuery2p3BlimpVPartSupplierDate,
    ]

    qf3_1 = [
        SSBQuery3p1BlimpSupplierCustomerDate,
        SSBQuery3p1BlimpCustomerSupplierDate,
        SSBQuery3p1BlimpVSupplierCustomerDate,
        SSBQuery3p1BlimpVCustomerSupplierDate,
    ]

    qf3_2 = [
        SSBQuery3p2BlimpSupplierCustomerDate,
        SSBQuery3p2BlimpCustomerSupplierDate,
        SSBQuery3p2BlimpVSupplierCustomerDate,
        SSBQuery3p2BlimpVCustomerSupplierDate,
    ]

    qf3_3 = [
        SSBQuery3p3BlimpSupplierCustomerDate,
        SSBQuery3p3BlimpCustomerSupplierDate,
        SSBQuery3p3BlimpVSupplierCustomerDate,
        SSBQuery3p3BlimpVCustomerSupplierDate,
    ]

    qf3_4 = [
        SSBQuery3p4BlimpSupplierCustomerDate,
        SSBQuery3p4BlimpCustomerSupplierDate,
        SSBQuery3p4BlimpVSupplierCustomerDate,
        SSBQuery3p4BlimpVCustomerSupplierDate,
    ]

    qf4_1 = [
        SSBQuery4p1BlimpCustomerPartSupplierDate,
        SSBQuery4p1BlimpCustomerSupplierPartDate,
        SSBQuery4p1BlimpSupplierCustomerPartDate,
        SSBQuery4p1BlimpPartCustomerSupplierDate,
        SSBQuery4p1BlimpSupplierPartCustomerDate,
        SSBQuery4p1BlimpPartSupplierCustomerDate,
        SSBQuery4p1BlimpVCustomerPartSupplierDate,
        SSBQuery4p1BlimpVCustomerSupplierPartDate,
        SSBQuery4p1BlimpVSupplierCustomerPartDate,
        SSBQuery4p1BlimpVPartCustomerSupplierDate,
        SSBQuery4p1BlimpVSupplierPartCustomerDate,
        SSBQuery4p1BlimpVPartSupplierCustomerDate,
    ]

    qf4_2 = [
        SSBQuery4p2BlimpCustomerPartSupplierDate,
        SSBQuery4p2BlimpCustomerSupplierPartDate,
        SSBQuery4p2BlimpSupplierCustomerPartDate,
        SSBQuery4p2BlimpPartCustomerSupplierDate,
        SSBQuery4p2BlimpSupplierPartCustomerDate,
        SSBQuery4p2BlimpPartSupplierCustomerDate,
        SSBQuery4p2BlimpVCustomerPartSupplierDate,
        SSBQuery4p2BlimpVCustomerSupplierPartDate,
        SSBQuery4p2BlimpVSupplierCustomerPartDate,
        SSBQuery4p2BlimpVPartCustomerSupplierDate,
        SSBQuery4p2BlimpVSupplierPartCustomerDate,
        SSBQuery4p2BlimpVPartSupplierCustomerDate,
    ]

    qf4_3 = [
        SSBQuery4p3BlimpCustomerPartSupplierDate,
        SSBQuery4p3BlimpCustomerSupplierPartDate,
        SSBQuery4p3BlimpSupplierCustomerPartDate,
        SSBQuery4p3BlimpPartCustomerSupplierDate,
        SSBQuery4p3BlimpSupplierPartCustomerDate,
        SSBQuery4p3BlimpPartSupplierCustomerDate,
        SSBQuery4p3BlimpVCustomerPartSupplierDate,
        SSBQuery4p3BlimpVCustomerSupplierPartDate,
        SSBQuery4p3BlimpVSupplierCustomerPartDate,
        SSBQuery4p3BlimpVPartCustomerSupplierDate,
        SSBQuery4p3BlimpVSupplierPartCustomerDate,
        SSBQuery4p3BlimpVPartSupplierCustomerDate,
    ]

    @staticmethod
    def run_query_flight(flight: List, display_runtime_output=False, **query_kwargs):
        """

        Run a flight of provided queries. Pass any additional parameters to the query initializer.
        """
        while flight:
            flight[0] = flight[0](**query_kwargs)
            # this code block is for quick debugging of the hashtable statistics should they be needed before runtime
            # flight[0]._setup()
            # flight[0].supplier_join_hash_table.get_statistics(display=True)
            # flight[0].part_join_hash_table.get_statistics(display=True)
            # flight[0].customer_join_hash_table.get_statistics(display=True)
            # flight[0].date_join_hash_table.get_statistics(display=True)
            # exit()
            _, runtimes = flight[0].run_query(
                display_runtime_output=display_runtime_output
            )  # type: Any, List[RuntimeResult]
            data_frame = *[r.runtime / 1000000 for r in runtimes], str(flight[0].__class__.__name__)  # convert to ms
            for column in data_frame:
                print(column, end='\t')
            print()
            del flight[0]

    @classmethod
    def run_qf1p1(cls, display_runtime_output=False, **query_kwargs):
        """Run all queries defined by Q1.1"""
        print("Q1.1")
        cls.run_query_flight(cls.qf1_1, display_runtime_output=display_runtime_output, **query_kwargs)

    @classmethod
    def run_qf1p2(cls, display_runtime_output=False, **query_kwargs):
        """Run all queries defined by Q1.2"""
        print("Q1.2")
        cls.run_query_flight(cls.qf1_2, display_runtime_output=display_runtime_output, **query_kwargs)

    @classmethod
    def run_qf1p3(cls, display_runtime_output=False, **query_kwargs):
        """Run all queries defined by Q1.3"""
        print("Q1.3")
        cls.run_query_flight(cls.qf1_3, display_runtime_output=display_runtime_output, **query_kwargs)

    @classmethod
    def run_qf1(cls, display_runtime_output=False, **query_kwargs):
        """Run all queries defined by Q1"""
        cls.run_qf1p1(display_runtime_output=display_runtime_output, **query_kwargs)
        cls.run_qf1p2(display_runtime_output=display_runtime_output, **query_kwargs)
        cls.run_qf1p3(display_runtime_output=display_runtime_output, **query_kwargs)

    @classmethod
    def run_qf2p1(cls, display_runtime_output=False, **query_kwargs):
        """Run all queries defined by Q2.1"""
        print("Q2.1")
        cls.run_query_flight(cls.qf2_1, display_runtime_output=display_runtime_output, **query_kwargs)

    @classmethod
    def run_qf2p2(cls, display_runtime_output=False, **query_kwargs):
        """Run all queries defined by Q2.2"""
        print("Q2.2")
        cls.run_query_flight(cls.qf2_2, display_runtime_output=display_runtime_output, **query_kwargs)

    @classmethod
    def run_qf2p3(cls, display_runtime_output=False, **query_kwargs):
        """Run all queries defined by Q2.3"""
        print("Q2.3")
        cls.run_query_flight(cls.qf2_3, display_runtime_output=display_runtime_output, **query_kwargs)

    @classmethod
    def run_qf2(cls, display_runtime_output=False, **query_kwargs):
        """Run all queries defined by Q2"""
        cls.run_qf2p1(display_runtime_output=display_runtime_output, **query_kwargs)
        cls.run_qf2p2(display_runtime_output=display_runtime_output, **query_kwargs)
        cls.run_qf2p3(display_runtime_output=display_runtime_output, **query_kwargs)

    @classmethod
    def run_qf3p1(cls, display_runtime_output=False, **query_kwargs):
        """Run all queries defined by Q3.1"""
        print("Q3.1")
        cls.run_query_flight(cls.qf3_1, display_runtime_output=display_runtime_output, **query_kwargs)

    @classmethod
    def run_qf3p2(cls, display_runtime_output=False, **query_kwargs):
        """Run all queries defined by Q3.2"""
        print("Q3.2")
        cls.run_query_flight(cls.qf3_2, display_runtime_output=display_runtime_output, **query_kwargs)

    @classmethod
    def run_qf3p3(cls, display_runtime_output=False, **query_kwargs):
        """Run all queries defined by Q3.2"""
        print("Q3.3")
        cls.run_query_flight(cls.qf3_3, display_runtime_output=display_runtime_output, **query_kwargs)

    @classmethod
    def run_qf3p4(cls, display_runtime_output=False, **query_kwargs):
        """Run all queries defined by Q3.4"""
        print("Q3.4")
        cls.run_query_flight(cls.qf3_4, display_runtime_output=display_runtime_output, **query_kwargs)

    @classmethod
    def run_qf3(cls, display_runtime_output=False, **query_kwargs):
        """Run all queries defined by Q3"""
        cls.run_qf3p1(display_runtime_output=display_runtime_output, **query_kwargs)
        cls.run_qf3p2(display_runtime_output=display_runtime_output, **query_kwargs)
        cls.run_qf3p3(display_runtime_output=display_runtime_output, **query_kwargs)
        cls.run_qf3p4(display_runtime_output=display_runtime_output, **query_kwargs)

    @classmethod
    def run_qf4p1(cls, display_runtime_output=False, **query_kwargs):
        """Run all queries defined by Q4.1"""
        print("Q4.1")
        cls.run_query_flight(cls.qf4_1, display_runtime_output=display_runtime_output, **query_kwargs)

    @classmethod
    def run_qf4p2(cls, display_runtime_output=False, **query_kwargs):
        """Run all queries defined by Q4.2"""
        print("Q4.2")
        cls.run_query_flight(cls.qf4_2, display_runtime_output=display_runtime_output, **query_kwargs)

    @classmethod
    def run_qf4p3(cls, display_runtime_output=False, **query_kwargs):
        """Run all queries defined by Q4.3"""
        print("Q4.3")
        cls.run_query_flight(cls.qf4_3, display_runtime_output=display_runtime_output, **query_kwargs)

    @classmethod
    def run_qf4(cls, display_runtime_output=False, **query_kwargs):
        """Run all queries defined by Q4"""
        cls.run_qf4p1(display_runtime_output=display_runtime_output, **query_kwargs)
        cls.run_qf4p2(display_runtime_output=display_runtime_output, **query_kwargs)
        cls.run_qf4p3(display_runtime_output=display_runtime_output, **query_kwargs)

    @classmethod
    def run_all(cls, display_runtime_output=False, **query_kwargs):
        """Run all queries defined in this query flight"""
        cls.run_qf1(display_runtime_output=display_runtime_output, **query_kwargs)
        cls.run_qf2(display_runtime_output=display_runtime_output, **query_kwargs)
        cls.run_qf3(display_runtime_output=display_runtime_output, **query_kwargs)
        cls.run_qf4(display_runtime_output=display_runtime_output, **query_kwargs)


class BestQueryFlights(QueryFlights):
    """
    These results come from running the full sweep defined by QueryFlights and selecting the best performing queries for
    each BLIMP/-V category.
    """
    qf1_1 = [
        SSBQuery1p1BlimpQuantityDiscountDate,
        SSBQuery1p1BlimpVQuantityDiscountDate,
    ]

    qf1_2 = [
        SSBQuery1p2BlimpQuantityDiscountDate,
        SSBQuery1p2BlimpVQuantityDiscountDate,
    ]

    qf1_3 = [
        SSBQuery1p3BlimpQuantityDiscountDate,
        SSBQuery1p3BlimpVQuantityDiscountDate,
    ]

    qf2_1 = [
        SSBQuery2p1BlimpPartSupplierDate,
        SSBQuery2p1BlimpVPartSupplierDate,
    ]

    qf2_2 = [
        SSBQuery2p2BlimpPartSupplierDate,
        SSBQuery2p2BlimpVPartSupplierDate,
    ]

    qf2_3 = [
        SSBQuery2p3BlimpPartSupplierDate,
        SSBQuery2p3BlimpVPartSupplierDate,
    ]

    qf3_1 = [
        SSBQuery3p1BlimpCustomerSupplierDate,
        SSBQuery3p1BlimpVCustomerSupplierDate,
    ]

    qf3_2 = [
        SSBQuery3p2BlimpCustomerSupplierDate,
        SSBQuery3p2BlimpVCustomerSupplierDate,
    ]

    qf3_3 = [
        SSBQuery3p3BlimpCustomerSupplierDate,
        SSBQuery3p3BlimpVCustomerSupplierDate,
    ]

    qf3_4 = [
        SSBQuery3p4BlimpCustomerSupplierDate,
        SSBQuery3p4BlimpVCustomerSupplierDate,
    ]

    qf4_1 = [
        SSBQuery4p1BlimpCustomerSupplierPartDate,
        SSBQuery4p1BlimpVCustomerSupplierPartDate,
    ]

    qf4_2 = [
        SSBQuery4p2BlimpCustomerSupplierPartDate,
        SSBQuery4p2BlimpVCustomerSupplierPartDate,
    ]

    qf4_3 = [
        SSBQuery4p3BlimpPartSupplierCustomerDate,
        SSBQuery4p3BlimpVCustomerSupplierPartDate,
    ]
