from typing import List, Any

from src.simulators.result import RuntimeResult

from studies.simple_query_benchmark.queries.selection.select import \
    SQBSelectLT1BlimpV, SQBSelectLT5BlimpV, SQBSelectLT25BlimpV, \
    SQBSelectLT1Blimp, SQBSelectLT5Blimp, SQBSelectLT25Blimp

from studies.simple_query_benchmark.queries.selection.select_index import \
    SQBSelectIndexLT1BlimpV, SQBSelectIndexLT5BlimpV, SQBSelectIndexLT25BlimpV, \
    SQBSelectIndexLT1Blimp, SQBSelectIndexLT5Blimp, SQBSelectIndexLT25Blimp

from studies.simple_query_benchmark.queries.semijoin.semijoin import \
    SQBSemiJoin1BlimpV, SQBSemiJoin5BlimpV, SQBSemiJoin25BlimpV, \
    SQBSemiJoin1Blimp, SQBSemiJoin5Blimp, SQBSemiJoin25Blimp

from studies.simple_query_benchmark.queries.semijoin.semijoin_index import \
    SQBSemiJoinIndex1BlimpV, SQBSemiJoinIndex5BlimpV, SQBSemiJoinIndex25BlimpV, \
    SQBSemiJoinIndex1Blimp, SQBSemiJoinIndex5Blimp, SQBSemiJoinIndex25Blimp

from studies.simple_query_benchmark.queries.join.join import \
    SQBJoin1BlimpV, SQBJoin5BlimpV, SQBJoin25BlimpV, \
    SQBJoin1Blimp, SQBJoin5Blimp, SQBJoin25Blimp

from studies.simple_query_benchmark.queries.aggregate.aggregate import \
    SQBAggregateBlimpV, SQBAggregateBlimp

from studies.simple_query_benchmark.queries.group_by_aggregate.group_by_aggregate import \
    SQBGroupByAggregateBlimp


class QueryFlights:
    """Defines all the SQB Query flights"""
    select = [
        SQBSelectLT1BlimpV,
        SQBSelectLT5BlimpV,
        SQBSelectLT25BlimpV,
        SQBSelectLT1Blimp,
        SQBSelectLT5Blimp,
        SQBSelectLT25Blimp,
    ]

    select_index = [
        SQBSelectIndexLT1BlimpV,
        SQBSelectIndexLT5BlimpV,
        SQBSelectIndexLT25BlimpV,
        SQBSelectIndexLT1Blimp,
        SQBSelectIndexLT5Blimp,
        SQBSelectIndexLT25Blimp,
    ]

    semijoin = [
        SQBSemiJoin1BlimpV,
        SQBSemiJoin5BlimpV,
        SQBSemiJoin25BlimpV,
        SQBSemiJoin1Blimp,
        SQBSemiJoin5Blimp,
        SQBSemiJoin25Blimp
    ]

    semijoin_index = [
        SQBSemiJoinIndex1BlimpV,
        SQBSemiJoinIndex5BlimpV,
        SQBSemiJoinIndex25BlimpV,
        SQBSemiJoinIndex1Blimp,
        SQBSemiJoinIndex5Blimp,
        SQBSemiJoinIndex25Blimp
    ]

    join = [
        SQBJoin1BlimpV,
        SQBJoin5BlimpV,
        SQBJoin25BlimpV,
        SQBJoin1Blimp,
        SQBJoin5Blimp,
        SQBJoin25Blimp
    ]

    aggregate = [
        SQBAggregateBlimpV,
        SQBAggregateBlimp,
    ]

    group_by_aggregate = [
        SQBGroupByAggregateBlimp
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
            # flight[0].join_hash_table.get_statistics(display=True)
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
    def run_select(cls, display_runtime_output=False, **query_kwargs):
        """Run all queries defined by select"""
        print("Selection")
        cls.run_query_flight(cls.select, display_runtime_output=display_runtime_output, **query_kwargs)

    @classmethod
    def run_select_index(cls, display_runtime_output=False, **query_kwargs):
        """Run all queries defined by select index"""
        print("Selection Index")
        cls.run_query_flight(cls.select_index, display_runtime_output=display_runtime_output, **query_kwargs)

    @classmethod
    def run_semijoin(cls, display_runtime_output=False, **query_kwargs):
        """Run all queries defined by semijoin"""
        print("SemiJoin")
        cls.run_query_flight(cls.semijoin, display_runtime_output=display_runtime_output, **query_kwargs)

    @classmethod
    def run_semijoin_index(cls, display_runtime_output=False, **query_kwargs):
        """Run all queries defined by semijoin index"""
        print("SemiJoin Index")
        cls.run_query_flight(cls.semijoin_index, display_runtime_output=display_runtime_output, **query_kwargs)

    @classmethod
    def run_join(cls, display_runtime_output=False, **query_kwargs):
        """Run all queries defined by join"""
        print("Join")
        cls.run_query_flight(cls.join, display_runtime_output=display_runtime_output, **query_kwargs)

    @classmethod
    def run_aggregate(cls, display_runtime_output=False, **query_kwargs):
        """Run all queries defined by aggregate"""
        print("Aggregate")
        cls.run_query_flight(cls.aggregate, display_runtime_output=display_runtime_output, **query_kwargs)

    @classmethod
    def run_group_by_aggregate(cls, display_runtime_output=False, **query_kwargs):
        """Run all queries defined by group by aggregate"""
        print("Group By Aggregate")
        cls.run_query_flight(cls.group_by_aggregate, display_runtime_output=display_runtime_output, **query_kwargs)

    @classmethod
    def run_all(cls, display_runtime_output=False, **query_kwargs):
        """Run all queries defined in this query flight"""
        cls.run_select(display_runtime_output=display_runtime_output, **query_kwargs)
        cls.run_select_index(display_runtime_output=display_runtime_output, **query_kwargs)
        cls.run_semijoin(display_runtime_output=display_runtime_output, **query_kwargs)
        cls.run_semijoin_index(display_runtime_output=display_runtime_output, **query_kwargs)
        cls.run_join(display_runtime_output=display_runtime_output, **query_kwargs)
        cls.run_aggregate(display_runtime_output=display_runtime_output, **query_kwargs)
        cls.run_group_by_aggregate(display_runtime_output=display_runtime_output, **query_kwargs)
